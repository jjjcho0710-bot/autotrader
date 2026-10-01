"""
tests/test_stock_trader_buy_cycle_balance_reuse.py - [AT] fix/kis-rate-limit-and-buy-retry

배경: 10/1 실측 — 매수 사이클이 종목마다 KISTrader.get_balance()를 호출해 KIS 호출이
급증했고 "초당 거래건수 초과"(KIS 속도제한)에 걸렸다. 수정 B-1: 잔고는 사이클 시작
시 1회만 조회해 재사용하고, 매수 체결마다 체결 금액만큼 로컬 cash를 차감해 같은
사이클의 다음 종목 판단에 반영한다.
"""
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
STOCK_TRADER_DIR = REPO_ROOT / "stock_trader"
for p in (REPO_ROOT, STOCK_TRADER_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _stub_missing_runtime_deps():
    """requirements.txt의 asyncpg/redis/aiohttp가 테스트 환경에 없어도
    _run_cycle()을 검증할 수 있도록 import만 되는 더미로 대체한다.
    실제 패키지가 설치되어 있으면 아무 것도 하지 않는다."""
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
        stub.ClientTimeout = lambda *a, **kw: None
        sys.modules["aiohttp"] = stub


_stub_missing_runtime_deps()

import main  # noqa: E402  (stock_trader/main.py)


class _FakeConn:
    async def fetch(self, query, *args, **kwargs):
        return []

    async def fetchval(self, query, *args, **kwargs):
        return 0

    async def fetchrow(self, query, *args, **kwargs):
        return None


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakePool:
    def __init__(self):
        self._conn = _FakeConn()

    def acquire(self):
        return _Acquire(self._conn)


class _FakeRedisClient:
    async def get(self, key):
        return None

    async def setex(self, key, ttl, val):
        pass

    async def delete(self, *keys):
        pass

    async def hset(self, *args, **kwargs):
        pass


class _FakeBuyStrategy:
    """전략 신호 판단을 우회하고 항상 BUY를 반환하는 대역."""

    def generate_signal(self, symbol, prices):
        return "BUY"


class _FakeHttpResponse:
    def __init__(self, data):
        self._data = data

    async def json(self):
        return self._data


class _FakeHttpSession:
    """aiohttp.ClientSession 대역: Jarvis 신호 요청 payload를 기록하고 종목별로
    미리 정해둔 응답을 돌려준다."""

    def __init__(self, responses, calls):
        self._responses = responses
        self._calls = calls

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None, timeout=None):
        self._calls.append(json)
        symbol = json.get("symbol")
        data = self._responses.get(symbol, {"executed": False, "jarvis_reply": "SKIP"})
        return _FakeHttpResponse(data)


def _make_bot(balance_result, jarvis_responses, calls):
    bot = main.StockTrader()
    bot.strategies = {"MA크로스": {"is_active": True, "params": {"max_positions": 5}}}
    bot.positions = {}
    bot.build_strategy = MagicMock(return_value=_FakeBuyStrategy())

    trader = MagicMock()
    trader.get_positions = AsyncMock(return_value=[])
    trader.get_current_price = AsyncMock(return_value=60000)
    trader.get_market_warning = AsyncMock(return_value={"mrkt_warn_cls_code": "00"})
    trader.get_balance = AsyncMock(return_value=balance_result)
    bot.trader = trader

    def _session_factory(*a, **kw):
        return _FakeHttpSession(jarvis_responses, calls)

    return bot, _session_factory


class TestBuyCycleBalanceReuse(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fake_redis = _FakeRedisClient()
        self._patches = [
            patch.object(main.db, "pool", _FakePool(), create=True),
            patch.object(main.db, "get_watchlist_symbols",
                         AsyncMock(return_value=["005930", "000660"])),
            patch.object(main.db, "get_recent_ohlcv", AsyncMock(
                return_value=[{"close": 60000 + i} for i in range(25)])),
            patch.object(main.cache, "client", self.fake_redis),
        ]
        for p in self._patches:
            p.start()
        self.addAsyncCleanup(self._stop_patches)

    async def _stop_patches(self):
        for p in self._patches:
            p.stop()

    async def test_balance_fetched_once_per_cycle_regardless_of_symbol_count(self):
        """종목이 여러 개여도 KISTrader.get_balance()는 사이클당 1회만 호출돼야 한다
        (종목마다 조회하면 KIS 호출이 급증해 초당 거래건수 제한에 걸리는 문제의 원인)."""
        calls = []
        bot, session_factory = _make_bot(
            {"cash": 5_000_000, "total": 10_000_000},
            jarvis_responses={
                "005930": {"executed": False, "jarvis_reply": "SKIP"},
                "000660": {"executed": False, "jarvis_reply": "SKIP"},
            },
            calls=calls,
        )

        with patch("aiohttp.ClientSession", side_effect=session_factory):
            await bot._run_cycle()

        bot.trader.get_balance.assert_awaited_once()
        # 두 종목 모두 잔고 재조회 없이 Jarvis 판단 요청까지 정상 도달해야 한다
        self.assertEqual(len(calls), 2)

    async def test_cash_deducted_after_buy_reflects_in_next_symbol_request(self):
        """첫 종목 매수 체결 후에는 체결 금액만큼 로컬 cash가 줄어 같은 사이클의
        다음 종목 Jarvis 요청에 반영돼야 한다."""
        calls = []
        bot, session_factory = _make_bot(
            {"cash": 5_000_000, "total": 10_000_000},
            jarvis_responses={
                "005930": {"executed": True, "jarvis_reply": "EXECUTE"},
                "000660": {"executed": False, "jarvis_reply": "SKIP"},
            },
            calls=calls,
        )

        with patch("aiohttp.ClientSession", side_effect=session_factory):
            await bot._run_cycle()

        bot.trader.get_balance.assert_awaited_once()
        self.assertEqual(len(calls), 2)
        first_call, second_call = calls
        self.assertEqual(first_call["symbol"], "005930")
        self.assertEqual(second_call["symbol"], "000660")

        buy_amount = first_call["price"] * first_call["qty"]
        self.assertEqual(second_call["cash"], first_call["cash"] - buy_amount)
        # 첫 종목 체결이 두 번째 종목 포지션에 영향을 주지 않았는지도 함께 확인
        self.assertIn("005930", bot.positions)
        self.assertNotIn("000660", bot.positions)


if __name__ == "__main__":
    unittest.main()
