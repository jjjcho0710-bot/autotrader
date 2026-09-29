"""
stock_trader/main.py 매수 스캔 단계에서 KISTrader.get_balance()가 stale=True를
반환(KIS 조회 실패 → 캐시된 옛 예수금 사용)할 때, Jarvis 판단 요청 사유 문구와
로그에 지연 표시가 붙는지 검증한다.

배경: [AT] fix/stale-cash-indicator (PM 지시) — get_balance() 실패 시 캐시된
예수금이 마치 최신 값처럼 조용히 표시되는 문제를 막기 위해 stale 플래그를 추가하고,
이 값을 쓰는 모든 표시 지점(6시간 리포트, 매수판단 사유/로그)에 반영한다.
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


def _make_bot(balance_result):
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
    return bot


class TestBuyScanStaleCashIndicator(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fake_redis = _FakeRedisClient()
        self._patches = [
            patch.object(main.db, "pool", _FakePool(), create=True),
            patch.object(main.db, "get_watchlist_symbols", AsyncMock(return_value=["005930"])),
            patch.object(main.db, "get_recent_ohlcv", AsyncMock(
                return_value=[{"close": 60000 + i} for i in range(25)])),
            patch.object(main.cache, "client", self.fake_redis),
            # Jarvis 신호 전달(HTTP POST)은 이 테스트의 관심사가 아니므로 즉시 실패시켜
            # 실네트워크 호출 없이 로그/사유 문구만 검증한다 (호출부는 예외를 잡아 로그만 남김).
            patch("aiohttp.ClientSession", side_effect=RuntimeError("network blocked in test")),
        ]
        for p in self._patches:
            p.start()
        self.addAsyncCleanup(self._stop_patches)

    async def _stop_patches(self):
        for p in self._patches:
            p.stop()

    async def test_stale_balance_marks_jarvis_reason_and_log(self):
        bot = _make_bot({"cash": 1_000_000, "total": 0, "stale": True})

        with self.assertLogs("stock-trader", level="INFO") as cm:
            await bot._run_cycle()

        log_output = "\n".join(cm.output)
        self.assertIn("Jarvis 판단 요청", log_output)
        self.assertIn("⚠️(마지막 확인: 지연됨)", log_output)

    async def test_fresh_balance_has_no_stale_marker(self):
        bot = _make_bot({"cash": 1_000_000, "total": 5_000_000})

        with self.assertLogs("stock-trader", level="INFO") as cm:
            await bot._run_cycle()

        log_output = "\n".join(cm.output)
        self.assertIn("Jarvis 판단 요청", log_output)
        self.assertNotIn("지연됨", log_output)


if __name__ == "__main__":
    unittest.main()
