"""
[AT] fix/sell-partial-fill-tracking 회귀 테스트.

배경: 10/16 002030(아세아) 손절 SELL이 KIS=3주 체결됐는데 DB=2.0으로 기록된 사고.
KISTrader.sell()은 접수 후 1.5+3+7초(총 11.5초) 안에 관찰된 최댓값을 최종값으로 쓰는데
([AT] fix/filled-qty-final), 그 윈도 안에 KIS가 전량 체결을 아직 반영하지 못하면
filled_qty < qty로 "부분체결"이 확정돼 반환된다. stock_trader/main.py._run_cycle()은
이 값을 그대로 trade_history에 기록하고 잔여수량은 "다음 사이클에 재시도"로만 처리할 뿐,
공용 common/late_fill_tracker.register_partial_fill()을 호출하지 않아 뒤늦게 반영되는
나머지 체결분을 잡아낼 방법이 전혀 없었다(router/handlers/order_handler.py,
stark/execution_guard.py는 이미 이 등록을 하고 있다 — stock_trader만 빠져 있었다).

이 파일은 다음을 검증한다:
1) 손절 매도가 부분체결(filled_qty < 주문수량)로 확정되면 register_partial_fill이
   올바른 인자(주문번호·요청수량·기록된수량·전략·가격·평단가)로 호출된다.
2) 손절 매도가 전량체결이면 register_partial_fill을 호출하지 않는다(불필요한 추적 등록 없음).
3) 주문번호(order_no)가 없으면(체결 조회 API 실패로 인한 fill_unconfirmed 등) 호출하지
   않는다(안전한 가드).
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


class FakeRedisClient:
    def __init__(self):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, val):
        self.store[key] = val

    async def delete(self, *keys):
        for k in keys:
            self.store.pop(k, None)

    async def scan_iter(self, match="*"):
        import fnmatch
        for k in list(self.store.keys()):
            if fnmatch.fnmatch(k, match):
                yield k


def _make_position(qty=3):
    return {
        "symbol": "002030", "name": "아세아",
        "avg_price": 150000, "cur_price": 137000,  # -8.7% → 손절선(-7%) 도달
        "qty": qty, "sellable_qty": qty,
    }


def _make_bot(pos):
    bot = main.StockTrader()
    bot.strategies = {"MA크로스": {"is_active": True, "params": {"stop_loss": -7, "max_positions": 0}}}
    bot.positions = {"002030": pos}
    bot.trader = MagicMock()
    bot.trader.get_positions = AsyncMock(return_value=[pos])
    return bot


class _BaseTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fake_redis = FakeRedisClient()
        self._patches = [
            patch.object(main.cache, "client", self.fake_redis),
            patch.object(main.db, "insert_trade", AsyncMock()),
            patch("common.telegram.send_stock", AsyncMock()),
            patch("common.alert_throttle.should_send_symbol_alert", AsyncMock(return_value=True)),
            patch.object(main, "register_partial_fill", AsyncMock()),
        ]
        for p in self._patches:
            p.start()
        self.addAsyncCleanup(self._stop_patches)

    async def _stop_patches(self):
        for p in self._patches:
            p.stop()


class TestStopLossPartialFillRegistersLateFillTracking(_BaseTest):
    async def test_partial_fill_registers_late_fill_tracker(self):
        """3주 손절 주문이 2주만 체결 확정되면(KIS=3, DB=2 사고 재현) 나머지 1주를
        뒤늦게 잡아내도록 register_partial_fill이 호출돼야 한다."""
        pos = _make_position(qty=3)
        bot = _make_bot(pos)
        bot.trader.sell = AsyncMock(return_value={
            "success": True, "order_no": "0009009999", "filled_qty": 2,
            "avg_fill_price": 137000.0, "partial": True,
        })

        await bot._run_cycle()

        main.register_partial_fill.assert_awaited_once()
        _, kwargs = main.register_partial_fill.await_args
        self.assertEqual(kwargs["order_no"], "0009009999")
        self.assertEqual(kwargs["symbol"], "002030")
        self.assertEqual(kwargs["side"], "SELL")
        self.assertEqual(kwargs["bot"], "stock_trader")
        self.assertEqual(kwargs["requested_qty"], 3)
        self.assertEqual(kwargs["recorded_qty"], 2)
        self.assertEqual(kwargs["strategy"], "MA크로스_손절")
        self.assertEqual(kwargs["price"], 137000.0)
        self.assertEqual(kwargs["cost_basis_avg_price"], 150000)

        # 부분체결이므로 trade_history엔 체결된 2주만 기록되고, 잔여 1주는 포지션에 남아
        # 다음 사이클에 즉시 재시도된다(기존 동작 유지, 회귀 없음).
        main.db.insert_trade.assert_awaited_once()
        _, insert_kwargs = main.db.insert_trade.await_args
        self.assertEqual(insert_kwargs["quantity"], 2)
        self.assertEqual(bot.positions["002030"]["qty"], 1)

    async def test_full_fill_does_not_register_late_fill_tracker(self):
        """전량 체결이면(부분체결 아님) 불필요한 추적 등록을 하지 않는다."""
        pos = _make_position(qty=3)
        bot = _make_bot(pos)
        bot.trader.sell = AsyncMock(return_value={
            "success": True, "order_no": "0009008888", "filled_qty": 3,
            "avg_fill_price": 137000.0,
        })

        await bot._run_cycle()

        main.register_partial_fill.assert_not_awaited()
        self.assertNotIn("002030", bot.positions)

    async def test_partial_fill_without_order_no_does_not_register(self):
        """체결 조회 API 실패 등으로 order_no가 없으면 등록을 시도하지 않는다(안전 가드)."""
        pos = _make_position(qty=3)
        bot = _make_bot(pos)
        bot.trader.sell = AsyncMock(return_value={
            "success": True, "filled_qty": 2, "avg_fill_price": 137000.0, "partial": True,
        })

        await bot._run_cycle()

        main.register_partial_fill.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
