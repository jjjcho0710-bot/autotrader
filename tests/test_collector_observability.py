"""
tests/test_collector_observability.py  ([AT] fix/collector-observability)

배경: 10/30 11:47 KST 이후 1분봉(stock_ohlcv) 수집이 멈췄는데, 실패가 전부 삼켜지고
("N/N 완료"는 스킵도 성공으로 셈), 헬스 엔드포인트도 이 상태를 못 보여 5일간 알림이 없었다.

고정하는 동작
1. get_price: 오류 응답의 rt_cd/msg_cd/msg1을 토큰·키 마스킹 후 로깅, ClientTimeout 지정,
   EGW00201은 백오프 재시도, EGW00121/00123은 Redis 키 삭제 후 재발급·1회 재시도.
2. _collect_one은 insert 성공 여부를 반환, collect_all은 실제 insert 수를 반환(0건이면 ERROR).
3. 수집 사이클을 asyncio.wait_for로 감싸 hang 시 루프가 묶이지 않고, 장중 insert 0건이
   10분 이상이면 텔레그램 경보를 일 1회만 보낸다.
4. /api/health/full에 stock_ohlcv 지연 항목, /api/market/checklist의 kis_realtime·bots 예외 수정.
"""
import asyncio
import importlib.util
import json
import logging
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
# data_collector/ 를 sys.path에 넣지 않는다: stock_trader 테스트들이 `import main`으로 stock_trader/main.py를
# 기대하므로, 같은 이름의 data_collector/main.py가 앞서 잡히면 전체 실행에서 충돌한다(아래는 파일 경로 직접 로드).
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _stub_missing_runtime_deps():
    try:
        import asyncpg  # noqa: F401
    except ImportError:
        stub = types.ModuleType("asyncpg")
        stub.Pool = object
        sys.modules["asyncpg"] = stub
    try:
        import redis.asyncio  # noqa: F401
    except ImportError:
        redis_mod = types.ModuleType("redis")
        redis_asyncio_mod = types.ModuleType("redis.asyncio")
        redis_asyncio_mod.Redis = object
        redis_mod.asyncio = redis_asyncio_mod
        sys.modules["redis"] = redis_mod
        sys.modules["redis.asyncio"] = redis_asyncio_mod
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        stub = types.ModuleType("aiohttp")
        stub.ClientSession = object
        stub.TCPConnector = lambda *a, **kw: None
        stub.ClientTimeout = lambda *a, **kw: ("timeout", kw)
        sys.modules["aiohttp"] = stub
    try:
        import fastapi  # noqa: F401
    except ImportError:
        stub = types.ModuleType("fastapi")

        class _FastAPI:
            def __init__(self, *a, **kw):
                pass

            def post(self, *a, **kw):
                return lambda fn: fn

            def get(self, *a, **kw):
                return lambda fn: fn

        stub.FastAPI = _FastAPI
        stub.Request = object
        sys.modules["fastapi"] = stub
    try:
        import uvicorn  # noqa: F401
    except ImportError:
        stub = types.ModuleType("uvicorn")
        stub.Server = lambda *a, **kw: None
        stub.Config = lambda *a, **kw: None
        sys.modules["uvicorn"] = stub


_stub_missing_runtime_deps()

from common.config import config  # noqa: E402
import data_collector.collectors.kis_collector as kc  # noqa: E402
from data_collector.collectors.kis_collector import KISCollector, mask_secrets  # noqa: E402

KST = timezone(timedelta(hours=9))
SECRET_TOKEN = "SECRETTOKEN1234567890abcdef"


# ── 공용 가짜 HTTP 세션 ───────────────────────────────────
class _Resp:
    def __init__(self, data, status=200):
        self._data = data
        self.status = status

    async def json(self):
        return self._data


class _Ctx:
    def __init__(self, resp=None, exc=None):
        self._resp, self._exc = resp, exc

    async def __aenter__(self):
        if self._exc:
            raise self._exc
        return self._resp

    async def __aexit__(self, *a):
        return False


class FakeSession:
    """get() 호출마다 미리 정해 둔 응답(dict) 또는 예외를 순서대로 돌려준다."""

    def __init__(self, script):
        self.script = list(script)
        self.calls = []

    def get(self, url, **kw):
        self.calls.append(kw)
        item = self.script.pop(0)
        if isinstance(item, Exception):
            return _Ctx(exc=item)
        return _Ctx(resp=_Resp(item))


OK = {"rt_cd": "0", "output": {"stck_prpr": "70000", "stck_oprc": "69000", "stck_hgpr": "71000",
                               "stck_lwpr": "68000", "acml_vol": "123", "prdy_ctrt": "1.5"}}


def _err(msg_cd, msg1="오류", rt_cd="1"):
    return {"rt_cd": rt_cd, "msg_cd": msg_cd, "msg1": msg1}


def _collector(script):
    c = KISCollector()
    c.session = FakeSession(script)
    KISCollector._shared_token = SECRET_TOKEN
    KISCollector._last_reissue_at = -1e9
    KISCollector._last_reissue_ok = False
    return c


class TestMaskSecrets(unittest.TestCase):
    def test_masks_token_bearer_jwt_and_keys(self):
        KISCollector._shared_token = SECRET_TOKEN
        with patch.object(type(config), "kis_app_key", property(lambda s: "APPKEY-ABCDEFG")), \
             patch.object(type(config), "kis_app_secret", property(lambda s: "APPSECRET-XYZ123")), \
             patch.object(type(config), "kis_account_no", property(lambda s: "50123456-01")):
            text = (f"token={SECRET_TOKEN} key=APPKEY-ABCDEFG sec=APPSECRET-XYZ123 acct=50123456-01 "
                    f"Bearer abc.def-ghi jwt=eyJhbGciOi.eyJzdWIiOi.SflKxwRJSM")
            out = mask_secrets(text)
        for leaked in (SECRET_TOKEN, "APPKEY-ABCDEFG", "APPSECRET-XYZ123", "50123456", "abc.def-ghi", "eyJhbGciOi"):
            self.assertNotIn(leaked, out)
        self.assertIn("********", out)


class TestGetPrice(unittest.IsolatedAsyncioTestCase):
    async def test_success_and_timeout_is_set(self):
        c = _collector([OK])
        res = await c.get_price("005930")
        self.assertEqual(res["price"], 70000)
        self.assertIn("timeout", c.session.calls[0])  # ClientTimeout 지정(기본 약 300초 의존 제거)
        self.assertEqual(kc.REQUEST_TIMEOUT_SEC, 10)

    async def test_error_response_logged_with_codes_and_masked(self):
        c = _collector([_err("EGW99999", f"bad token {SECRET_TOKEN}")])
        with self.assertLogs(kc.logger, level="WARNING") as cm:
            res = await c.get_price("005930")
        self.assertEqual(res, {})
        joined = "\n".join(cm.output)
        self.assertIn("rt_cd=1", joined)
        self.assertIn("msg_cd=EGW99999", joined)
        self.assertIn("msg1=", joined)
        self.assertNotIn(SECRET_TOKEN, joined)

    async def test_missing_output_is_failure_not_zero_price(self):
        """output 없는 200 응답은 예전엔 price 0이 됐다 — 이제 {} + 경고."""
        c = _collector([{"rt_cd": "1", "msg_cd": "X", "msg1": "m"}])
        with self.assertLogs(kc.logger, level="WARNING"):
            self.assertEqual(await c.get_price("005930"), {})

    async def test_rate_limit_backoff_then_success(self):
        c = _collector([_err("EGW00201", "초당 거래건수를 초과"), OK])
        sleeps = []

        async def fake_sleep(s):
            sleeps.append(s)

        with patch.object(kc.asyncio, "sleep", fake_sleep), self.assertLogs(kc.logger, level="WARNING"):
            res = await c.get_price("005930")
        self.assertEqual(res["price"], 70000)
        self.assertEqual(sleeps, [kc.RATE_LIMIT_BACKOFF_SEC])
        self.assertEqual(len(c.session.calls), 2)

    async def test_rate_limit_gives_up_after_retries(self):
        c = _collector([_err("EGW00201")] * (kc.RATE_LIMIT_RETRIES + 1))
        with patch.object(kc.asyncio, "sleep", AsyncMock()), self.assertLogs(kc.logger, level="WARNING"):
            self.assertEqual(await c.get_price("005930"), {})
        self.assertEqual(len(c.session.calls), kc.RATE_LIMIT_RETRIES + 1)

    async def test_token_expired_reissues_once_and_retries(self):
        for code in ("EGW00121", "EGW00123"):
            c = _collector([_err(code, "만료된 token"), OK])
            c._get_token = AsyncMock(return_value=True)
            with self.assertLogs(kc.logger, level="WARNING"):
                res = await c.get_price("005930")
            self.assertEqual(res["price"], 70000, code)
            c._get_token.assert_awaited_once_with(force_new=True)

    async def test_token_expired_retries_only_once(self):
        c = _collector([_err("EGW00121"), _err("EGW00121")])
        c._get_token = AsyncMock(return_value=True)
        with self.assertLogs(kc.logger, level="WARNING"):
            self.assertEqual(await c.get_price("005930"), {})
        self.assertEqual(len(c.session.calls), 2)  # 원 요청 + 재시도 1회

    async def test_token_reissue_failure_returns_empty(self):
        c = _collector([_err("EGW00121")])
        c._get_token = AsyncMock(return_value=False)
        with self.assertLogs(kc.logger, level="WARNING"):
            self.assertEqual(await c.get_price("005930"), {})
        self.assertEqual(len(c.session.calls), 1)

    async def test_exception_logged_masked_returns_empty(self):
        c = _collector([RuntimeError(f"boom {SECRET_TOKEN}")])
        with self.assertLogs(kc.logger, level="ERROR") as cm:
            self.assertEqual(await c.get_price("005930"), {})
        self.assertNotIn(SECRET_TOKEN, "\n".join(cm.output))


class TestTokenRefresh(unittest.IsolatedAsyncioTestCase):
    async def test_force_new_deletes_redis_key_and_reissues(self):
        c = _collector([])
        redis = MagicMock()
        redis.delete = AsyncMock()
        redis.get = AsyncMock(return_value=b"OLD")
        redis.setex = AsyncMock()

        class _Post:
            async def __aenter__(s):
                return _Resp({"access_token": "NEWTOKEN"})

            async def __aexit__(s, *a):
                return False

        c.session.post = lambda *a, **kw: _Post()
        with patch.object(kc.cache, "client", redis, create=True):
            ok = await c._get_token(force_new=True)
        self.assertTrue(ok)
        redis.delete.assert_awaited_once()
        redis.get.assert_not_awaited()  # force_new면 낡은 Redis 값을 복원하지 않는다
        self.assertEqual(KISCollector._shared_token, "NEWTOKEN")
        self.assertEqual(redis.setex.await_args.args[2], "NEWTOKEN")

    async def test_refresh_token_cooldown_prevents_reissue_storm(self):
        c = _collector([])
        c._get_token = AsyncMock(return_value=True)
        KISCollector._shared_token = "T"
        self.assertTrue(await c._refresh_token())
        self.assertTrue(await c._refresh_token())  # 쿨다운 안: 재발급 호출 없이 직전 결과
        c._get_token.assert_awaited_once()


class TestCollectCounting(unittest.IsolatedAsyncioTestCase):
    async def test_collect_one_returns_bool(self):
        c = _collector([])
        with patch.object(kc.asyncio, "sleep", AsyncMock()), \
             patch.object(kc.cache, "set_price", AsyncMock(), create=True), \
             patch.object(kc.db, "insert_stock_ohlcv", AsyncMock(), create=True) as ins:
            c.get_price = AsyncMock(return_value={"price": 100, "open": 1, "high": 2, "low": 1, "volume": 3})
            self.assertTrue(await c._collect_one("005930"))
            ins.assert_awaited_once()
            c.get_price = AsyncMock(return_value={})
            self.assertFalse(await c._collect_one("005930"))
            c.get_price = AsyncMock(return_value={"price": 0})
            with self.assertLogs(kc.logger, level="WARNING"):
                self.assertFalse(await c._collect_one("005930"))
            self.assertEqual(ins.await_count, 1)

    async def test_collect_all_counts_actual_inserts(self):
        c = _collector([])
        c._collect_one = AsyncMock(side_effect=[True, False, RuntimeError("x"), True])
        with patch.object(kc.asyncio, "sleep", AsyncMock()), \
             patch.object(kc.db, "get_watchlist_symbols", AsyncMock(return_value=["a", "b", "c", "d"]), create=True), \
             self.assertLogs(kc.logger, level="INFO") as cm:
            n = await c.collect_all()
        self.assertEqual(n, 2)
        self.assertIn("insert 2/4", "\n".join(cm.output))

    async def test_collect_all_zero_inserts_logs_error_not_success(self):
        c = _collector([])
        c._collect_one = AsyncMock(return_value=False)
        with patch.object(kc.asyncio, "sleep", AsyncMock()), \
             patch.object(kc.db, "get_watchlist_symbols", AsyncMock(return_value=["a", "b"]), create=True), \
             self.assertLogs(kc.logger, level="INFO") as cm:
            n = await c.collect_all()
        self.assertEqual(n, 0)
        self.assertTrue(any(r.levelno == logging.ERROR and "insert 0건" in r.getMessage()
                            for r in cm.records))
        self.assertNotIn("수집 완료", "\n".join(cm.output))

    async def test_redis_cache_failure_does_not_block_db_insert(self):
        c = _collector([])
        c.get_price = AsyncMock(return_value={"price": 100, "open": 1, "high": 2, "low": 1, "volume": 3})
        with patch.object(kc.asyncio, "sleep", AsyncMock()), \
             patch.object(kc.cache, "set_price", AsyncMock(side_effect=RuntimeError("redis down")), create=True), \
             patch.object(kc.db, "insert_stock_ohlcv", AsyncMock(), create=True) as ins, \
             self.assertLogs(kc.logger, level="WARNING"):
            self.assertTrue(await c._collect_one("005930"))
        ins.assert_awaited_once()


# ── data_collector/main.py ───────────────────────────────
_spec = importlib.util.spec_from_file_location("data_collector_main_obs", REPO_ROOT / "data_collector" / "main.py")
dcm = importlib.util.module_from_spec(_spec)
sys.modules["data_collector_main_obs"] = dcm
# main.py가 `from collectors...`를 쓰므로 로드하는 동안에만 data_collector/를 sys.path에 둔다(로드 후 제거).
_dc_dir = str(REPO_ROOT / "data_collector")
sys.path.insert(0, _dc_dir)
try:
    _spec.loader.exec_module(dcm)
finally:
    try:
        sys.path.remove(_dc_dir)
    except ValueError:
        pass


def _kst(h, m, d=6):  # 2026-10-06 화요일
    return datetime(2026, 10, d, h, m, 0, tzinfo=KST)


class TestInsertStallAlert(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.dc = dcm.DataCollector()
        self.sent = []

        async def fake_send(text):
            self.sent.append(text)

        p = patch("common.telegram.send_stock", fake_send)
        p.start()
        self.addCleanup(p.stop)

    async def test_no_alert_before_threshold(self):
        await self.dc._check_insert_stall(_kst(9, 0), 0)
        await self.dc._check_insert_stall(_kst(9, 9), 0)
        self.assertEqual(self.sent, [])

    async def test_alert_after_10_minutes_once_per_day(self):
        await self.dc._check_insert_stall(_kst(9, 0), 0)
        await self.dc._check_insert_stall(_kst(9, 10), 0)
        await self.dc._check_insert_stall(_kst(9, 11), 0)
        await self.dc._check_insert_stall(_kst(12, 0), 0)
        self.assertEqual(len(self.sent), 1)
        self.assertIn("분봉 수집 중단", self.sent[0])

    async def test_insert_resets_idle_clock(self):
        await self.dc._check_insert_stall(_kst(9, 0), 0)
        await self.dc._check_insert_stall(_kst(9, 8), 5)   # insert 있음 → 기준 갱신
        await self.dc._check_insert_stall(_kst(9, 17), 0)  # 마지막 insert 후 9분
        self.assertEqual(self.sent, [])
        await self.dc._check_insert_stall(_kst(9, 18), 0)  # 10분
        self.assertEqual(len(self.sent), 1)

    async def test_alert_again_next_day(self):
        await self.dc._check_insert_stall(_kst(9, 0), 0)
        await self.dc._check_insert_stall(_kst(9, 11), 0)
        await self.dc._check_insert_stall(_kst(9, 0, d=7), 0)
        await self.dc._check_insert_stall(_kst(9, 11, d=7), 0)
        self.assertEqual(len(self.sent), 2)

    async def test_telegram_failure_does_not_raise(self):
        async def boom(text):
            raise RuntimeError("tg down")

        with patch("common.telegram.send_stock", boom):
            await self.dc._check_insert_stall(_kst(9, 0), 0)
            await self.dc._check_insert_stall(_kst(9, 11), 0)  # 예외 전파 없음


class TestLoopCycleTimeout(unittest.IsolatedAsyncioTestCase):
    async def _run_one_cycle(self, collect_all):
        dc = dcm.DataCollector()
        dc.kis = MagicMock()
        dc.kis.collect_all = collect_all
        dc.daily = AsyncMock()
        dc._check_insert_stall = AsyncMock()
        config.COLLECT_ALWAYS = False
        utc_now = datetime(2026, 10, 6, 1, 0, 0)  # 화요일 KST 10:00

        def fake_now(tz=None):
            if tz is None:
                return utc_now
            return utc_now.replace(tzinfo=timezone.utc).astimezone(tz)

        async def fake_sleep(_s):
            dc.running = False

        with patch.object(dcm, "datetime") as mock_dt, \
             patch.object(dcm.asyncio, "sleep", fake_sleep), \
             patch.object(dcm.cache, "set_bot_status", new=AsyncMock()):
            mock_dt.now.side_effect = fake_now
            dc.running = True
            await dc._loop()
        return dc

    async def test_hung_cycle_is_cancelled_and_loop_continues(self):
        async def hang():
            await asyncio.Event().wait()

        with patch.object(dcm, "CYCLE_TIMEOUT_SEC", 0.05), self.assertLogs(dcm.logger, level="ERROR") as cm:
            dc = await self._run_one_cycle(hang)
        self.assertIn("타임아웃", "\n".join(cm.output))
        dc._check_insert_stall.assert_awaited_once()
        self.assertEqual(dc._check_insert_stall.await_args.args[1], 0)

    async def test_insert_count_is_passed_to_stall_check(self):
        dc = await self._run_one_cycle(AsyncMock(return_value=7))
        self.assertEqual(dc._check_insert_stall.await_args.args[1], 7)

    async def test_collect_all_exception_counts_as_zero(self):
        with self.assertLogs(dcm.logger, level="ERROR"):
            dc = await self._run_one_cycle(AsyncMock(side_effect=RuntimeError("x")))
        self.assertEqual(dc._check_insert_stall.await_args.args[1], 0)


# ── dashboard ────────────────────────────────────────────
for _m in ["fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
           "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai"]:
    if _m not in sys.modules:
        sys.modules[_m] = MagicMock()


class _Acq:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *a):
        return False


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _Acq(self.conn)


class _Conn:
    def __init__(self, created_at=None, ts=None, fail=None):
        self.created_at, self.ts, self.fail = created_at, ts, fail

    async def fetchval(self, q, *a):
        if self.fail:
            raise self.fail
        if "MAX(created_at)" in q:
            return self.created_at
        return 1

    async def fetchrow(self, q, *a):
        if self.fail:
            raise self.fail
        if self.created_at is None and self.ts is None:
            return None
        return {"ts": self.ts, "created_at": self.created_at, "close": 70000}


class _Redis:
    def __init__(self, bots):
        self.bots = bots

    async def hgetall(self, key):
        return self.bots


class TestDashboardHealth(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import dashboard.main as dm
        self.dm = dm

    def test_age_seconds_handles_aware_and_naive(self):
        now = datetime(2026, 10, 6, 10, 0, 0, tzinfo=KST)
        aware = datetime(2026, 10, 6, 0, 58, 0, tzinfo=timezone.utc)   # KST 09:58
        naive = datetime(2026, 10, 6, 9, 58, 0)                        # KST 벽시계로 해석
        self.assertEqual(self.dm._age_seconds(now, aware), 120)
        self.assertEqual(self.dm._age_seconds(now, naive), 120)

    async def test_ohlcv_check_ok_during_market_when_fresh(self):
        now = datetime(2026, 10, 6, 10, 0, 0, tzinfo=KST)
        conn = _Conn(created_at=now - timedelta(seconds=90))
        with patch.object(self.dm, "db_pool", _Pool(conn)):
            r = await self.dm._stock_ohlcv_freshness_check(now)
        self.assertEqual((r["status"], r["lag_seconds"], r["market_hours"]), ("ok", 90, True))

    async def test_ohlcv_check_warning_and_error_during_market(self):
        now = datetime(2026, 10, 6, 10, 0, 0, tzinfo=KST)
        for lag, want in ((400, "warning"), (700, "error")):
            conn = _Conn(created_at=now - timedelta(seconds=lag))
            with patch.object(self.dm, "db_pool", _Pool(conn)):
                r = await self.dm._stock_ohlcv_freshness_check(now)
            self.assertEqual(r["status"], want)

    async def test_ohlcv_check_stale_is_ok_outside_market_hours(self):
        now = datetime(2026, 10, 6, 20, 0, 0, tzinfo=KST)
        conn = _Conn(created_at=now - timedelta(hours=5))
        with patch.object(self.dm, "db_pool", _Pool(conn)):
            r = await self.dm._stock_ohlcv_freshness_check(now)
        self.assertEqual((r["status"], r["market_hours"]), ("ok", False))
        # 휴장일(개천절 대체공휴일 10/5) 장중 시각도 장외 취급
        hol = datetime(2026, 10, 5, 10, 0, 0, tzinfo=KST)
        with patch.object(self.dm, "db_pool", _Pool(_Conn(created_at=hol - timedelta(days=3)))):
            r = await self.dm._stock_ohlcv_freshness_check(hol)
        self.assertEqual(r["status"], "ok")

    async def test_ohlcv_check_empty_table_and_db_error(self):
        now = datetime(2026, 10, 6, 10, 0, 0, tzinfo=KST)
        with patch.object(self.dm, "db_pool", _Pool(_Conn())):
            self.assertEqual((await self.dm._stock_ohlcv_freshness_check(now))["status"], "error")
        with patch.object(self.dm, "db_pool", _Pool(_Conn(fail=RuntimeError("db")))):
            self.assertEqual((await self.dm._stock_ohlcv_freshness_check(now))["status"], "error")


class TestMarketChecklist(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import dashboard.main as dm
        self.dm = dm

    async def _run(self, conn, redis):
        with patch.object(self.dm, "db_pool", _Pool(conn)), patch.object(self.dm, "redis_client", redis):
            return await self.dm._market_checklist_raw()

    async def test_kis_realtime_no_longer_raises_naive_aware_error(self):
        now = datetime.now(KST)
        # 운영 형태: ts는 KST 벽시계가 UTC 라벨, created_at은 정상 timestamptz
        wall = now.replace(tzinfo=timezone.utc)
        conn = _Conn(created_at=now - timedelta(seconds=30), ts=wall)
        res = await self._run(conn, _Redis({}))
        k = res["checks"]["kis_realtime"]
        self.assertNotIn("offset-naive", k["msg"])
        self.assertTrue(k["ok"], k)
        self.assertIn("초 전", k["msg"])

    async def test_kis_realtime_stale_is_not_ok(self):
        now = datetime.now(KST)
        conn = _Conn(created_at=now - timedelta(days=5), ts=now.replace(tzinfo=timezone.utc))
        res = await self._run(conn, _Redis({}))
        self.assertFalse(res["checks"]["kis_realtime"]["ok"])

    async def test_bots_reads_redis_without_name_error(self):
        conn = _Conn(created_at=datetime.now(KST), ts=datetime.now(timezone.utc))
        redis = _Redis({"stock_trader": json.dumps({"status": "running"}),
                        b"data_collector": json.dumps({"status": "error"}).encode(),
                        "bad": "not-json"})
        res = await self._run(conn, redis)
        b = res["checks"]["bots"]
        self.assertTrue(b["ok"], b)
        self.assertNotIn("not defined", b["msg"])
        self.assertIn("stock_trader:running", b["msg"])
        self.assertIn("data_collector:error", b["msg"])

    async def test_bots_empty_is_not_ok(self):
        conn = _Conn(created_at=datetime.now(KST), ts=datetime.now(timezone.utc))
        res = await self._run(conn, _Redis({}))
        self.assertFalse(res["checks"]["bots"]["ok"])


if __name__ == "__main__":
    unittest.main()
