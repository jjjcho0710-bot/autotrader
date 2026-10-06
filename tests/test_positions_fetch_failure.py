"""
stock_trader/main.py 보유 포지션 조회 실패 처리 회귀 테스트.

배경: [AT] fix/positions-fetch-failure (PM 지시, 2026-10-06)
10/5(공휴일) get_positions() 실패 시 self.positions가 {}로 덮어써져
1) 최대 보유 종목수 검사(len(self.positions)>=max_positions)가 0으로 통과해
   불필요한 신규 매수 스캔(AI 판단)이 돌았고
2) 그 사이클의 보유 종목 손절·익절 평가가 조용히 사라졌다.
수정: 조회 실패(예외)와 실제 빈 보유([])를 구분해, 실패 시에는 self.positions를
덮어쓰지 않고 직전 상태를 유지하며 그 사이클의 신규 진입 스캔만 건너뛴다.
연속 3회 실패부터 개인방에 "보유 조회 연속 실패" 알림을 1회만 보낸다.
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


class _FakeRedisClient:
    """cache.client를 흉내내는 최소 인메모리 Redis 대역"""

    def __init__(self):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, val):
        self.store[key] = val

    async def delete(self, *keys):
        for k in keys:
            self.store.pop(k, None)


def _make_position(qty=10):
    return {
        "symbol": "005930", "name": "삼성전자",
        "avg_price": 70000, "cur_price": 60000,  # -14.3% 손실 → 손절선(-7%) 도달
        "qty": qty, "sellable_qty": qty,
    }


def _make_bot(initial_positions, max_positions=5):
    bot = main.StockTrader()
    bot.strategies = {
        "MA크로스": {"is_active": True, "params": {"stop_loss": -7, "max_positions": max_positions}},
    }
    bot.positions = dict(initial_positions)
    bot.trader = MagicMock()
    return bot


class _BaseTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fake_redis = _FakeRedisClient()
        self._patches = [
            patch.object(main.cache, "client", self.fake_redis),
            patch.object(main.cache, "set_bot_status", AsyncMock()),
            patch.object(main.db, "insert_trade", AsyncMock()),
            patch("common.telegram.send_stock", AsyncMock()),
            patch("common.alert_throttle.should_send_symbol_alert", AsyncMock(return_value=True)),
        ]
        for p in self._patches:
            p.start()
        self.addAsyncCleanup(self._stop_patches)

    async def _stop_patches(self):
        for p in self._patches:
            p.stop()


class TestLoadPositionsDistinguishesFailureFromEmpty(_BaseTest):
    """(a) 조회 예외 시 직전 positions 유지 / (b) 실제 빈 보유는 그대로 0으로 처리"""

    async def test_fetch_exception_keeps_previous_positions_and_returns_false(self):
        pos = _make_position()
        bot = _make_bot({"005930": pos})
        bot.trader.get_positions = AsyncMock(side_effect=ConnectionError("타임아웃"))

        ok = await bot._load_positions()

        self.assertFalse(ok)
        self.assertEqual(bot.positions, {"005930": pos})
        self.assertEqual(bot._positions_fail_count, 1)

    async def test_fetch_success_with_real_empty_holdings_overwrites_to_empty(self):
        pos = _make_position()
        bot = _make_bot({"005930": pos})
        bot.trader.get_positions = AsyncMock(return_value=[])

        ok = await bot._load_positions()

        self.assertTrue(ok)
        self.assertEqual(bot.positions, {})
        self.assertEqual(bot._positions_fail_count, 0)


class TestConsecutiveFailureAlert(_BaseTest):
    """(d) 연속 실패 3회째부터 개인방 알림 1회만"""

    async def test_third_consecutive_failure_sends_alert_exactly_once(self):
        bot = _make_bot({})
        bot.trader.get_positions = AsyncMock(side_effect=ConnectionError("실패"))

        with patch.object(main.StockTrader, "_notify_error", new_callable=AsyncMock) as mock_notify:
            await bot._load_positions()
            self.assertEqual(mock_notify.await_count, 0)
            await bot._load_positions()
            self.assertEqual(mock_notify.await_count, 0)
            await bot._load_positions()
            self.assertEqual(mock_notify.await_count, 1)
            await bot._load_positions()  # 4회째는 추가 알림 없음
            self.assertEqual(mock_notify.await_count, 1)

    async def test_recovery_resets_failure_counter(self):
        bot = _make_bot({})
        bot.trader.get_positions = AsyncMock(side_effect=ConnectionError("실패"))
        await bot._load_positions()
        await bot._load_positions()
        self.assertEqual(bot._positions_fail_count, 2)

        bot.trader.get_positions = AsyncMock(return_value=[])
        await bot._load_positions()
        self.assertEqual(bot._positions_fail_count, 0)


class TestRunCycleEndToEnd(_BaseTest):
    """(c)(e) 실제 _run_cycle() 경로: 조회 실패 사이클에서도 직전 보유 종목의 손절
    평가가 수행되고, 신규 진입 스캔(watchlist 조회)은 건너뛴다."""

    async def test_fetch_failure_still_runs_stop_loss_but_skips_new_entry_scan(self):
        pos = _make_position(qty=10)
        # 한도 여유 있음(5) — 수정 전 버그였다면 self.positions가 {}로 덮어써져
        # 0 < 5 로 신규 진입 스캔까지 진행됐을 상황
        bot = _make_bot({"005930": pos}, max_positions=5)
        bot.trader.get_positions = AsyncMock(side_effect=ConnectionError("조회 실패"))
        bot.trader.sell = AsyncMock(return_value={"success": True, "filled_qty": 10})

        with patch.object(main.db, "get_watchlist_symbols", new_callable=AsyncMock) as mock_watchlist, \
             patch.object(main.StockTrader, "_daily_stop_count", AsyncMock(return_value=0)):
            await bot._run_cycle()
            mock_watchlist.assert_not_awaited()

        bot.trader.sell.assert_awaited()
        self.assertEqual(bot._positions_fail_count, 1)

    async def test_fetch_success_with_real_empty_holdings_runs_new_entry_scan(self):
        pos = _make_position(qty=10)
        bot = _make_bot({"005930": pos}, max_positions=5)
        bot.trader.get_positions = AsyncMock(return_value=[])  # 실제로 전량 매도된 정상 상태
        bot.trader.get_balance = AsyncMock(return_value={"cash": 1_000_000, "total": 5_000_000})

        with patch.object(main.db, "get_watchlist_symbols", new_callable=AsyncMock) as mock_watchlist, \
             patch.object(main.StockTrader, "_daily_stop_count", AsyncMock(return_value=0)), \
             patch.object(main.config, "STOCK_SYMBOLS", []):
            mock_watchlist.return_value = []
            await bot._run_cycle()
            mock_watchlist.assert_awaited()

        self.assertEqual(bot.positions, {})
        self.assertEqual(bot._positions_fail_count, 0)


if __name__ == "__main__":
    unittest.main()
