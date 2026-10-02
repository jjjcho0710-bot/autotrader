"""
tests/test_balance_cache.py - dashboard/main.py get_stock_balance() 캐시 검증
([AT] fix/balance-cache)

배경: get_stock_balance()(/api/balance/stock)에 캐시가 없어서 채팅·자동 실행 주문의
사이징(_get_balance_for_sizing)과 한도 계산이 호출될 때마다 KIS 매수가능조회(inquire-psbl-order)
를 불렀고, 10/2 실측으로 속도제한(EGW00201)에 자주 걸렸다. get_stock_positions(이미 캐시+
invalidate_stock_positions_cache 보유)와 동일한 패턴(락+TTL 캐시)을 적용한다.

검증 항목:
1. 캐시 히트 시 KIS 호출 없음
2. TTL(15초) 만료 후 재조회
3. 실패 응답(HTTP 비정상/rt_cd!=0)은 캐시하지 않음
4. 동시 호출 시 KIS 실제 호출 1회(직렬화)
5. invalidate_stock_positions_cache() 호출 시 잔고 캐시도 함께 무효화(같은 지점에서 비워짐)
6. 주문 체결 후 명시적 invalidate_stock_balance_cache() 무효화
7. 실제 운영 경로(route_late 사이징, _jarvis_chat_impl → order_handler.handle_trade_command →
   _compute_buy_sizing_cap → _get_balance_for_sizing → get_stock_balance)에서 캐시가 적용되는
   end-to-end 검증
"""
import asyncio
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "stock_trader"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _stub_missing_runtime_deps():
    """requirements.txt의 asyncpg/redis/aiohttp/fastapi가 테스트 환경에 없어도
    임포트할 수 있도록 더미로 대체한다(tests/test_kis_rate_limit_and_cache.py와 동일)."""
    try:
        import asyncpg  # noqa: F401
    except ImportError:
        stub = __import__("types").ModuleType("asyncpg")
        stub.Pool = object
        sys.modules["asyncpg"] = stub

    try:
        import redis.asyncio  # noqa: F401
    except ImportError:
        import types
        redis_mod = types.ModuleType("redis")
        redis_asyncio_mod = types.ModuleType("redis.asyncio")
        redis_asyncio_mod.Redis = object
        redis_mod.asyncio = redis_asyncio_mod
        sys.modules["redis"] = redis_mod
        sys.modules["redis.asyncio"] = redis_asyncio_mod

    try:
        import aiohttp  # noqa: F401
    except ImportError:
        import types
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
from tests.test_order_handler import FakePool, FakeRedis  # noqa: E402


class FakeResponse:
    """tests/test_kis_rate_limit_and_cache.py의 FakeResponse와 동일 계약 —
    `await sess.get(...)` / `await sess.post(...)` 양쪽 다 직접 await 가능해야 한다."""

    def __init__(self, data: dict, status: int = 200, delay: float = 0.0):
        self._data = data
        self.status = status
        self._delay = delay

    def __await__(self):
        async def _coro():
            if self._delay > 0:
                await asyncio.sleep(self._delay)
            return self
        return _coro().__await__()

    async def json(self):
        return self._data


class FakeSession:
    def __init__(self, handler):
        self.handler = handler

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, *args, **kwargs):
        return self.handler(url, *args, **kwargs)

    def post(self, url, *args, **kwargs):
        return self.handler(url, *args, **kwargs)


class FakeBalanceConfig:
    kis_base_url = "https://example.invalid"
    kis_app_key = "key"
    kis_app_secret = "secret"
    kis_account_no = "12345678-01"
    KIS_IS_PAPER = True


def _balance_handler(call_log, output=None, status=200, rt_cd="0", delay=0.0):
    """오직 inquire-psbl-order 호출만 call_log에 센다(토큰 발급 POST는 별도 집계하지 않음) —
    PM 요구사항("동시 요청 시 KIS 1회")의 "KIS 호출"은 실제 잔고 조회 호출을 뜻한다."""
    output = output if output is not None else {"ord_psbl_cash": "1000000", "tot_evlu_amt": "5000000"}

    def handler(url, *args, **kwargs):
        if url.endswith("/oauth2/tokenP"):
            return FakeResponse({"access_token": "FAKE_TOKEN"})
        call_log.append(url)
        return FakeResponse({"rt_cd": rt_cd, "output": output}, status=status, delay=delay)
    return handler


class TestStockBalanceCache(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # 모듈 전역 캐시 상태 초기화 — 테스트 간 오염 방지(dashboard.main은 모듈 단위로 1회만
        # import되므로 이전 테스트가 채워둔 캐시가 남아있을 수 있다)
        dm._stock_balance_cache = None
        dm._stock_balance_cache_ts = 0.0
        dm._stock_balance_lock = None
        dm._stock_positions_cache = None
        dm._stock_positions_cache_ts = 0.0
        self._config_patch = patch.object(dm, "config", FakeBalanceConfig())
        self._config_patch.start()
        self.addCleanup(self._config_patch.stop)

    async def test_cache_hit_within_ttl_skips_kis_call(self):
        """캐시 히트 시 KIS 호출 없이 동일 결과를 즉시 반환해야 한다"""
        call_log = []
        with patch("aiohttp.ClientSession", return_value=FakeSession(_balance_handler(call_log))):
            r1 = await dm._get_stock_balance_raw()
            r2 = await dm._get_stock_balance_raw()

        self.assertEqual(len(call_log), 1, "두 번째 호출은 캐시에서 응답해 KIS를 재호출하면 안 된다")
        self.assertEqual(r1, r2)
        self.assertEqual(r1["data"]["cash"], 1000000)

    async def test_ttl_expiry_triggers_refetch(self):
        """TTL(15초) 경계 테스트 — 만료 직전까지는 캐시, 만료 순간부터는 재조회한다"""
        call_log = []
        with patch("aiohttp.ClientSession", return_value=FakeSession(_balance_handler(call_log))), \
             patch("time.time") as mock_time:
            mock_time.return_value = 1_000_000.0
            await dm._get_stock_balance_raw()
            self.assertEqual(len(call_log), 1)

            # TTL 경계 바로 아래(14.99초 경과) — 여전히 캐시 적용
            mock_time.return_value = 1_000_000.0 + dm._STOCK_BALANCE_CACHE_TTL - 0.01
            await dm._get_stock_balance_raw()
            self.assertEqual(len(call_log), 1, "TTL 만료 전인데 재조회하면 안 된다")

            # TTL 경계 초과(15.01초 경과) — 캐시 만료, 재조회해야 함
            mock_time.return_value = 1_000_000.0 + dm._STOCK_BALANCE_CACHE_TTL + 0.01
            await dm._get_stock_balance_raw()
            self.assertEqual(len(call_log), 2, "TTL 만료 후에는 KIS를 재조회해야 한다")

    async def test_failure_response_not_cached(self):
        """실패 응답(rt_cd != 0, 속도제한 등)은 캐시하지 않아 다음 호출이 즉시 재시도해야 한다"""
        call_log = []
        handler = _balance_handler(call_log, output={}, status=500, rt_cd="1")
        with patch("aiohttp.ClientSession", return_value=FakeSession(handler)):
            r1 = await dm._get_stock_balance_raw()
            r2 = await dm._get_stock_balance_raw()

        self.assertFalse(r1.get("success"))
        self.assertFalse(r2.get("success"))
        self.assertEqual(len(call_log), 2, "실패 응답은 캐시되면 안 되므로 두 번째 호출도 KIS를 다시 불러야 한다")
        self.assertIsNone(dm._stock_balance_cache)

    async def test_exception_response_not_cached(self):
        """세션/네트워크 예외(except 분기)도 캐시하지 않아야 한다"""
        def boom_handler(url, *args, **kwargs):
            raise ConnectionError("연결 거부")

        with patch("aiohttp.ClientSession", return_value=FakeSession(boom_handler)):
            r1 = await dm._get_stock_balance_raw()
            r2 = await dm._get_stock_balance_raw()

        self.assertFalse(r1.get("success"))
        self.assertFalse(r2.get("success"))
        self.assertIsNone(dm._stock_balance_cache)

    async def test_concurrent_calls_trigger_single_kis_request(self):
        """동시 10건 호출 시 KIS 실제 호출(inquire-psbl-order)은 1회만 발생해야 한다(asyncio.Lock 직렬화)"""
        call_log = []
        handler = _balance_handler(call_log, delay=0.05)
        with patch("aiohttp.ClientSession", return_value=FakeSession(handler)):
            results = await asyncio.gather(*[dm._get_stock_balance_raw() for _ in range(10)])

        self.assertEqual(len(results), 10)
        self.assertEqual(len(call_log), 1, "동시 요청이 몰려도 KIS 호출은 1회여야 한다")
        for r in results:
            self.assertTrue(r["success"])
            self.assertEqual(r["data"]["cash"], 1000000)

    async def test_explicit_invalidate_clears_cache(self):
        """invalidate_stock_balance_cache() 호출 후에는 캐시가 비워져 재조회해야 한다"""
        call_log = []
        with patch("aiohttp.ClientSession", return_value=FakeSession(_balance_handler(call_log))):
            await dm._get_stock_balance_raw()
            self.assertEqual(len(call_log), 1)

            await dm._get_stock_balance_raw()
            self.assertEqual(len(call_log), 1, "무효화 전에는 캐시가 적용돼야 한다")

            dm.invalidate_stock_balance_cache()

            await dm._get_stock_balance_raw()
            self.assertEqual(len(call_log), 2, "무효화 후에는 재조회해야 한다")

    async def test_invalidate_stock_positions_cache_also_clears_balance_cache(self):
        """보유 목록 캐시 무효화 호출부(invalidate_stock_positions_cache)가 잔고 캐시도
        같은 지점에서 함께 비워야 한다 — 채팅 주문/신호 경로 execute 성공 시 호출되는
        지점과 동일하다([AT] order-result-reconcile, [AT] fix/balance-cache)."""
        call_log = []
        with patch("aiohttp.ClientSession", return_value=FakeSession(_balance_handler(call_log))):
            await dm._get_stock_balance_raw()
            self.assertEqual(len(call_log), 1)

            await dm._get_stock_balance_raw()
            self.assertEqual(len(call_log), 1, "무효화 전에는 캐시가 적용돼야 한다")

            dm.invalidate_stock_positions_cache()
            self.assertIsNone(dm._stock_balance_cache, "invalidate_stock_positions_cache는 잔고 캐시도 비워야 한다")

            await dm._get_stock_balance_raw()
            self.assertEqual(len(call_log), 2, "invalidate_stock_positions_cache 이후에는 잔고도 재조회해야 한다")


class FakeSizingConfig:
    kis_base_url = "https://example.invalid"
    kis_app_key = "key"
    kis_app_secret = "secret"
    kis_account_no = "12345678-01"
    KIS_IS_PAPER = True
    RISK_PER_TRADE_PCT = 0.75
    INITIAL_SEED_KRW = 10_000_000


class _QuoteOrBalanceSession:
    """실제 운영 경로(end-to-end) 테스트용 — _fetch_kis_inquire_price(시세조회, GET만)와
    get_stock_balance(토큰 POST + 매수가능조회 GET)가 같은 aiohttp.ClientSession 패치를
    공유하므로, URL로 분기해 둘 다 응답해야 한다."""

    def __init__(self, quote_output: dict, balance_output: dict, balance_call_log: list):
        self._quote_output = quote_output
        self._balance_output = balance_output
        self._balance_call_log = balance_call_log

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def post(self, url, *args, **kwargs):
        return FakeResponse({"access_token": "FAKE_TOKEN"})

    def get(self, url, *args, **kwargs):
        if "inquire-psbl-order" in url:
            self._balance_call_log.append(url)
            return FakeResponse({"rt_cd": "0", "output": self._balance_output})
        if "inquire-price" in url:
            return FakeResponse({"output": self._quote_output})
        return FakeResponse({"output": {}})


class TestBalanceCacheEndToEndSizing(unittest.IsolatedAsyncioTestCase):
    """실제 운영 경로(route_late 사이징)에서 잔고 캐시가 적용되는지 end-to-end로 검증.
    _get_balance_for_sizing/get_stock_balance를 패치로 바이패스하지 않고, 실제
    _jarvis_chat_impl → router.intent_router.route_late → order_handler.handle_trade_command
    → _compute_buy_sizing_cap 경로를 그대로 실행한다. 캐시가 없다면(되돌리면) 매수 지시
    2건이 KIS 매수가능조회를 2회 부르므로 이 테스트는 실패한다.

    KIS 주문 자체는 실패로 응답하게 한다 — 주문 체결 성공은 invalidate_stock_positions_cache를
    통해 잔고 캐시를 즉시 비우므로(요구사항 2, 별도로 TestStockBalanceCache에서 검증됨), 체결
    성공을 끼워 넣으면 두 번째 지시의 사이징 조회가 "체결 후 무효화"와 "TTL 캐시 재사용" 중
    어느 쪽 때문에 재조회했는지 구분할 수 없다. 이 테스트는 순수하게 사이징 경로의 캐시
    재사용만 검증한다."""

    async def test_two_buy_commands_within_ttl_share_single_balance_fetch(self):
        dm._stock_balance_cache = None
        dm._stock_balance_cache_ts = 0.0
        dm._stock_balance_lock = None
        dm._stock_positions_cache = None
        dm._stock_positions_cache_ts = 0.0

        universe = MagicMock()
        universe.resolve_symbol = AsyncMock(side_effect=[
            ("207940", "삼성바이오로직스"),
            ("005930", "삼성전자"),
        ])

        kis_order_calls = []

        async def fake_kis_order(symbol, price, qty, is_buy):
            kis_order_calls.append((symbol, price, qty, is_buy))
            return {"success": False, "error": "모의 주문 실패(사이징 캐시 검증용 — 체결 무효화와 분리)"}

        balance_call_log = []
        session = _QuoteOrBalanceSession(
            quote_output={"stck_prpr": "400000", "hts_kor_isnm": "종목"},
            balance_output={"ord_psbl_cash": "10000000", "tot_evlu_amt": "10000000"},
            balance_call_log=balance_call_log,
        )

        patches = [
            patch.object(dm, "universe", universe),
            patch.object(dm, "db_pool", FakePool()),
            patch.object(dm, "redis_client", FakeRedis()),
            patch.object(dm, "config", FakeSizingConfig()),
            patch.object(dm, "get_kis_token", new=AsyncMock(return_value="FAKE_TOKEN")),
            patch.object(dm, "_kis_stock_order", new=fake_kis_order),
            patch.object(dm, "get_stock_positions", new=AsyncMock(return_value={"success": True, "data": []})),
            patch.object(dm, "_send_telegram", new=AsyncMock()),
            patch.object(dm, "_log_journal", new=AsyncMock()),
            patch.object(dm, "_get_recent_daily_ohlcv_for_sizing", new=AsyncMock(return_value=[])),
            patch.object(dm, "_get_market_warning_for_gate",
                         new=AsyncMock(return_value={"mrkt_warn_cls_code": "00", "vi_cls_code": "N"})),
            patch.object(dm.intent_router, "route_early", new=AsyncMock(return_value=None)),
            patch("aiohttp.ClientSession", return_value=session),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        res1 = await dm._jarvis_chat_impl({"message": "삼성바이오로직스 1주 매수", "channel": "web"})
        res2 = await dm._jarvis_chat_impl({"message": "삼성전자 1주 매수", "channel": "web"})

        self.assertTrue(res1["success"])
        self.assertTrue(res2["success"])
        self.assertEqual(len(kis_order_calls), 2, "매수 지시 2건 모두 사이징을 통과해 주문 시도까지 도달해야 한다")
        self.assertEqual(
            len(balance_call_log), 1,
            "사이징이 호출된 매수 지시 2건이 15초 TTL 안에 들어오면 실제 KIS 매수가능조회는 "
            "1회만 발생해야 한다 — 캐시가 없다면(되돌리면) 2회가 되어 이 테스트는 실패한다",
        )


if __name__ == "__main__":
    unittest.main()
