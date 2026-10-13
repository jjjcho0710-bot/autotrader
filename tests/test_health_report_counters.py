"""
tests/test_health_report_counters.py - [AT] feat/health-report 회귀 테스트 (PM 승인)

dashboard/main.py에 추가한 두 개의 "한 줄" 기록 지점을 검증한다(둘 다 매매 판단에는
영향을 주지 않는 관측용 Redis 카운터이며, health_report가 읽기만 한다):
1. _run_scheduled_job(name="morning_routine")이 종료될 때 소요시간(초)을
   health:scan_timing:YYYY-MM-DD에 저장(7일 TTL)하는지 — 다른 잡 이름에서는 저장하지
   않는지도 함께 확인한다.
2. _get_stock_positions_raw()가 토큰 만료로 force_new 재발급을 할 때
   health:token_reissue:YYYY-MM-DD를 INCR+EXPIRE(7일)하는지 — EGW00201(레이트리밋,
   재발급 아님) 경로에서는 증가하지 않는지도 확인한다.
"""
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "stock_trader"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


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
        stub.ClientTimeout = lambda *a, **kw: None

        class ServerTimeoutError(Exception):
            pass

        stub.ServerTimeoutError = ServerTimeoutError
        sys.modules["aiohttp"] = stub
    for mod_name in [
        "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
        "google", "google.generativeai",
    ]:
        if mod_name not in sys.modules:
            sys.modules[mod_name] = MagicMock()


_stub_missing_runtime_deps()

import dashboard.main as dm  # noqa: E402


class FakeResponse:
    def __init__(self, data: dict, status: int = 200):
        self._data = data
        self.status = status

    def __await__(self):
        async def _coro():
            return self
        return _coro().__await__()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._data


class FakeSequenceSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.get_calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, *args, **kwargs):
        idx = min(self.get_calls, len(self._responses) - 1)
        resp = self._responses[idx]
        self.get_calls += 1
        return FakeResponse(resp)


class FakeRedisCounter:
    """get/setex/incr/expire를 지원하는 대역 — health:token_reissue:* 카운터 검증용."""

    def __init__(self):
        self.store = {}
        self.incr_calls = []
        self.expire_calls = []

    async def get(self, key):
        v = self.store.get(key)
        return str(v) if v is not None else None

    async def setex(self, key, ttl, value):
        self.store[key] = value

    async def incr(self, key):
        self.incr_calls.append(key)
        self.store[key] = int(self.store.get(key, 0)) + 1
        return self.store[key]

    async def expire(self, key, ttl):
        self.expire_calls.append((key, ttl))


class TestTokenReissueCounter(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        dm._stock_positions_cache = None
        dm._stock_positions_cache_ts = 0.0
        dm._stock_positions_lock = None

    async def test_force_new_reissue_increments_health_counter(self):
        """만료 토큰 응답(EGW00123)으로 force_new 재발급이 일어나면
        health:token_reissue:YYYY-MM-DD가 INCR+EXPIRE(7일)돼야 한다."""
        session = FakeSequenceSession([
            {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "기간이 만료된 token 입니다"},
            {"rt_cd": "0", "output1": [], "output2": [{"tot_evlu_amt": "1000000",
                                                         "scts_evlu_amt": "0",
                                                         "prvs_rcdl_excc_amt": "1000000"}]},
        ])
        fake_redis = FakeRedisCounter()
        dm.redis_client = fake_redis

        async def fake_get_kis_token(force_new=False):
            return "TOK-NEW" if force_new else "TOK-OLD"

        with patch("dashboard.main.get_kis_token", AsyncMock(side_effect=fake_get_kis_token)), \
             patch("aiohttp.ClientSession", return_value=session):
            result = await dm._get_stock_positions_raw()

        self.assertTrue(result["success"])
        self.assertEqual(len(fake_redis.incr_calls), 1)
        key = fake_redis.incr_calls[0]
        self.assertTrue(key.startswith("health:token_reissue:"))
        self.assertEqual(fake_redis.expire_calls, [(key, 86400 * 7)])
        self.assertEqual(fake_redis.store[key], 1)

    async def test_rate_limit_egw00201_does_not_increment_counter(self):
        """EGW00201(레이트리밋)은 재발급이 아니므로 카운터가 증가하면 안 된다."""
        session = FakeSequenceSession([
            {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다"},
            {"rt_cd": "0", "output1": [], "output2": [{"tot_evlu_amt": "1000000",
                                                         "scts_evlu_amt": "0",
                                                         "prvs_rcdl_excc_amt": "1000000"}]},
        ])
        fake_redis = FakeRedisCounter()
        dm.redis_client = fake_redis

        with patch("dashboard.main.get_kis_token", AsyncMock(return_value="TOK-OLD")), \
             patch("aiohttp.ClientSession", return_value=session), \
             patch("asyncio.sleep", AsyncMock()):
            result = await dm._get_stock_positions_raw()

        self.assertTrue(result["success"])
        self.assertEqual(fake_redis.incr_calls, [])

    async def test_redis_failure_during_counter_write_does_not_break_reissue(self):
        """카운터 기록이 실패해도(Redis 장애) 토큰 재발급·포지션 조회 자체는 영향받지 않는다."""
        session = FakeSequenceSession([
            {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "기간이 만료된 token 입니다"},
            {"rt_cd": "0", "output1": [], "output2": [{"tot_evlu_amt": "1000000",
                                                         "scts_evlu_amt": "0",
                                                         "prvs_rcdl_excc_amt": "1000000"}]},
        ])

        class _BoomRedis(FakeRedisCounter):
            async def incr(self, key):
                raise ConnectionError("redis down")

        dm.redis_client = _BoomRedis()

        async def fake_get_kis_token(force_new=False):
            return "TOK-NEW" if force_new else "TOK-OLD"

        with patch("dashboard.main.get_kis_token", AsyncMock(side_effect=fake_get_kis_token)), \
             patch("aiohttp.ClientSession", return_value=session):
            result = await dm._get_stock_positions_raw()

        self.assertTrue(result["success"])


class TestScanTimingRecording(unittest.IsolatedAsyncioTestCase):
    async def test_morning_routine_records_elapsed_to_redis(self):
        """morning_routine 잡이 끝나면 health:scan_timing:YYYY-MM-DD에 소요시간(초)이
        저장돼야 한다."""
        fake_redis = FakeRedisCounter()
        dm.redis_client = fake_redis

        async def fake_job():
            return "done"

        import datetime as _dt
        result = await dm._run_scheduled_job(
            "morning_routine", _dt.time(8, 30), fake_job)

        self.assertEqual(result, "done")
        keys = [k for k in fake_redis.store if k.startswith("health:scan_timing:")]
        self.assertEqual(len(keys), 1)
        self.assertGreaterEqual(float(fake_redis.store[keys[0]]), 0.0)

    async def test_other_job_names_do_not_write_scan_timing(self):
        """morning_routine이 아닌 다른 잡은 health:scan_timing:*을 기록하지 않는다."""
        fake_redis = FakeRedisCounter()
        dm.redis_client = fake_redis

        async def fake_job():
            return "done"

        import datetime as _dt
        await dm._run_scheduled_job("closing_routine", _dt.time(15, 40), fake_job)

        keys = [k for k in fake_redis.store if k.startswith("health:scan_timing:")]
        self.assertEqual(keys, [])


class TestBuildAndMaybeSendHealthReport(unittest.IsolatedAsyncioTestCase):
    """_build_and_maybe_send_health_report()가 health_report.build_health_report()에
    pool/redis/콜러블을 올바르게 주입하고, send 플래그에 따라 텔레그램 발송 여부가
    갈리는지만 확인한다(KIS 호출은 _reconcile_dry_run_counts 내부 일이라 여기선 모킹)."""

    async def test_send_true_calls_send_telegram_with_report_text(self):
        fake_report = {"text": "✅ 이상 없음", "status": "ok", "issues": [], "sections": {}}
        with patch("dashboard.main._health_report.build_health_report",
                   AsyncMock(return_value=fake_report)) as mock_build, \
             patch("dashboard.main._send_telegram", AsyncMock()) as mock_send:
            result = await dm._build_and_maybe_send_health_report(send=True)

        self.assertEqual(result, fake_report)
        mock_send.assert_awaited_once_with("✅ 이상 없음", dest="personal")
        kwargs = mock_build.call_args.kwargs
        self.assertIs(kwargs["pool"], dm.db_pool)
        self.assertIs(kwargs["reconcile_dry_run_fn"], dm._reconcile_dry_run_counts)
        self.assertIs(kwargs["get_positions_fn"], dm.get_stock_positions)

    async def test_send_false_does_not_call_send_telegram(self):
        fake_report = {"text": "✅ 이상 없음", "status": "ok", "issues": [], "sections": {}}
        with patch("dashboard.main._health_report.build_health_report",
                   AsyncMock(return_value=fake_report)), \
             patch("dashboard.main._send_telegram", AsyncMock()) as mock_send:
            await dm._build_and_maybe_send_health_report(send=False)

        mock_send.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
