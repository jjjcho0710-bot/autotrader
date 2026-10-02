"""
stock_trader/main.py +10% 이상 절반확정·트레일링 스탑 회귀 테스트.

배경: 차등 익절 정책 2단계(PM 승인, 착수 승인 2026-09-29). +10% 이상 구간(EXIT_BAND_HALF_LOCK_TRAIL)에서
1) 최초 진입 시 보유수량의 절반을 즉시 시장가로 확정 매도한다. half_lock_done:{symbol} Redis 키로
   중복 실행을 막는다.
2) 절반확정 이후 잔여 수량은 트레일링 스탑으로 관리한다: 매 사이클 고점을 trailing_high:{symbol}에
   갱신하고, 현재가가 고점 대비 -TRAILING_STOP_PCT%(기본 3%) 하락하면 잔여 수량을 전량 매도한다.
3) 전량 매도 완료 시 half_lock_done, trailing_high 키를 정리해 다음 재매수 시 깨끗한 상태로 시작한다.
4) 부분체결 시에는(절반확정·트레일링 매도 모두) 잔량을 유지하고 다음 사이클에 재시도한다.
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
    """cache.client를 흉내내는 최소 인메모리 Redis 대역(get/setex/delete만 지원)"""

    def __init__(self):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, val):
        self.store[key] = val

    async def delete(self, *keys):
        for k in keys:
            self.store.pop(k, None)


def _make_position(symbol="005930", qty=10, avg_price=10000, cur_price=11000):
    return {
        "symbol": symbol, "name": "삼성전자",
        "avg_price": avg_price, "cur_price": cur_price,
        "qty": qty, "sellable_qty": qty,
    }


def _make_bot(pos):
    bot = main.StockTrader()
    # max_positions=0 → 포지션 루프 처리 후 신규 진입 스캔 없이 바로 return
    bot.strategies = {"MA크로스": {"is_active": True, "params": {"stop_loss": -7, "max_positions": 0}}}
    bot.positions = {pos["symbol"]: pos}
    bot.trader = MagicMock()
    bot.trader.get_positions = AsyncMock(return_value=[pos])
    return bot


class _BaseCycleTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fake_redis = _FakeRedisClient()
        self._patches = [
            patch.object(main.cache, "client", self.fake_redis),
            patch.object(main.db, "insert_trade", AsyncMock()),
            patch("common.telegram.send_stock", AsyncMock()),
            patch("common.alert_throttle.should_send_symbol_alert", AsyncMock(return_value=True)),
            patch("common.alert_throttle.reset_symbol_alert", AsyncMock()),
        ]
        for p in self._patches:
            p.start()
        self.addAsyncCleanup(self._stop_patches)

    async def _stop_patches(self):
        for p in self._patches:
            p.stop()


class TestHalfLockRunsOnceAtPlus10Percent(_BaseCycleTest):
    async def test_first_cycle_sells_half_and_sets_done_flag(self):
        pos = _make_position(qty=10, avg_price=10000, cur_price=11000)  # +10%
        bot = _make_bot(pos)
        bot.trader.sell = AsyncMock(return_value={"success": True, "filled_qty": 5})

        await bot._run_cycle()

        bot.trader.sell.assert_awaited_once_with(
            "005930", 11000, 5, strategy="MA크로스_절반확정", avg_price=10000)
        self.assertEqual(self.fake_redis.store.get("half_lock_done:005930"), "1")
        self.assertEqual(self.fake_redis.store.get("trailing_high:005930"), "11000")
        self.assertEqual(bot.positions["005930"]["qty"], 5)
        self.assertEqual(bot.positions["005930"]["sellable_qty"], 5)

    async def test_second_cycle_does_not_repeat_half_lock_sell(self):
        pos = _make_position(qty=10, avg_price=10000, cur_price=11000)
        bot = _make_bot(pos)
        bot.trader.sell = AsyncMock(return_value={"success": True, "filled_qty": 5})
        await bot._run_cycle()  # 1회차: 절반확정 실행

        # 2회차: 잔여 포지션(5주)에 대해 동일 가격(+10%)으로 재조회
        remaining_pos = _make_position(symbol="005930", qty=5, avg_price=10000, cur_price=11000)
        bot.positions = {"005930": remaining_pos}
        bot.trader.get_positions = AsyncMock(return_value=[remaining_pos])
        bot.trader.sell.reset_mock()

        await bot._run_cycle()

        bot.trader.sell.assert_not_awaited()  # 이미 절반확정 완료 → 재실행 금지


class TestTrailingHighUpdates(_BaseCycleTest):
    async def test_high_price_updates_trailing_high_when_price_rises(self):
        # 절반확정이 이미 끝난 상태를 직접 세팅
        self_redis = _FakeRedisClient()
        pos = _make_position(symbol="005930", qty=5, avg_price=10000, cur_price=12000)  # +20%
        bot = _make_bot(pos)
        with patch.object(main.cache, "client", self_redis), \
             patch.object(main.db, "insert_trade", AsyncMock()), \
             patch("common.telegram.send_stock", AsyncMock()):
            self_redis.store["half_lock_done:005930"] = "1"
            self_redis.store["trailing_high:005930"] = "11000"  # 이전 고점
            bot.trader.sell = AsyncMock()

            await bot._run_cycle()

            self.assertEqual(self_redis.store["trailing_high:005930"], "12000")
            bot.trader.sell.assert_not_awaited()  # 고점 갱신만, 하락폭 0% → 매도 없음


class TestTrailingStopTriggersFullSell(_BaseCycleTest):
    async def test_drop_3_percent_from_high_sells_remaining_qty(self):
        high = 12000
        cur_price = int(high * (1 - 0.03))  # 고점 대비 정확히 -3%
        pos = _make_position(symbol="005930", qty=5, avg_price=10000, cur_price=cur_price)
        bot = _make_bot(pos)
        self.fake_redis.store["half_lock_done:005930"] = "1"
        self.fake_redis.store["trailing_high:005930"] = str(high)
        bot.trader.sell = AsyncMock(return_value={"success": True, "filled_qty": 5})

        await bot._run_cycle()

        bot.trader.sell.assert_awaited_once_with(
            "005930", cur_price, 5, strategy="MA크로스_트레일링스탑", avg_price=10000)
        self.assertNotIn("005930", bot.positions)

    async def test_drop_below_3_percent_from_high_does_not_sell(self):
        high = 12000
        cur_price = int(high * (1 - 0.02))  # 고점 대비 -2% (문턱 미달)
        pos = _make_position(symbol="005930", qty=5, avg_price=10000, cur_price=cur_price)
        bot = _make_bot(pos)
        self.fake_redis.store["half_lock_done:005930"] = "1"
        self.fake_redis.store["trailing_high:005930"] = str(high)
        bot.trader.sell = AsyncMock()

        await bot._run_cycle()

        bot.trader.sell.assert_not_awaited()
        self.assertIn("005930", bot.positions)


class TestRedisKeysClearedAfterFullExit(_BaseCycleTest):
    async def test_full_trailing_stop_sell_clears_half_lock_and_trailing_keys(self):
        high = 12000
        cur_price = int(high * (1 - 0.03))
        pos = _make_position(symbol="005930", qty=5, avg_price=10000, cur_price=cur_price)
        bot = _make_bot(pos)
        self.fake_redis.store["half_lock_done:005930"] = "1"
        self.fake_redis.store["trailing_high:005930"] = str(high)
        bot.trader.sell = AsyncMock(return_value={"success": True, "filled_qty": 5})

        await bot._run_cycle()

        self.assertNotIn("half_lock_done:005930", self.fake_redis.store)
        self.assertNotIn("trailing_high:005930", self.fake_redis.store)


class TestPartialFillKeepsRemainder(_BaseCycleTest):
    async def test_half_lock_partial_fill_keeps_remaining_qty_and_sets_done_flag(self):
        pos = _make_position(symbol="005930", qty=10, avg_price=10000, cur_price=11000)
        bot = _make_bot(pos)
        # 절반(5주) 목표 중 3주만 체결
        bot.trader.sell = AsyncMock(return_value={"success": True, "filled_qty": 3, "partial": True})

        await bot._run_cycle()

        self.assertIn("005930", bot.positions)
        self.assertEqual(bot.positions["005930"]["qty"], 7)
        self.assertEqual(bot.positions["005930"]["sellable_qty"], 7)
        # 부분체결이라도 절반확정 시도는 완료된 것으로 간주해 중복 실행을 막는다
        self.assertEqual(self.fake_redis.store.get("half_lock_done:005930"), "1")

    async def test_trailing_stop_partial_fill_keeps_remaining_qty_and_keeps_keys(self):
        high = 12000
        cur_price = int(high * (1 - 0.03))
        pos = _make_position(symbol="005930", qty=5, avg_price=10000, cur_price=cur_price)
        bot = _make_bot(pos)
        self.fake_redis.store["half_lock_done:005930"] = "1"
        self.fake_redis.store["trailing_high:005930"] = str(high)
        bot.trader.sell = AsyncMock(return_value={"success": True, "filled_qty": 2, "partial": True})

        await bot._run_cycle()

        self.assertIn("005930", bot.positions)
        self.assertEqual(bot.positions["005930"]["qty"], 3)
        self.assertEqual(bot.positions["005930"]["sellable_qty"], 3)
        # 아직 전량 매도되지 않았으므로 다음 사이클 재시도를 위해 키를 유지해야 한다
        self.assertEqual(self.fake_redis.store.get("half_lock_done:005930"), "1")
        self.assertEqual(self.fake_redis.store.get("trailing_high:005930"), str(high))


if __name__ == "__main__":
    unittest.main()
