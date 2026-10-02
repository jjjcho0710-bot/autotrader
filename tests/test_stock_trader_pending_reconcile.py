"""
stock_trader/main.py 지연 체결(pending) 처리 회귀 테스트.

배경: [AT] fix/stock-trader-fill-reconcile — KISTrader.buy()/sell()이 재확인 2회(+3초, +7초)
에도 체결을 확인 못 해 success=False, pending=True를 반환하면:
1) 손절 등 매도 실행 경로(_run_cycle)가 이를 실제 실패로 오인해 30분 억제·실패 알림을
   보내면 안 된다(end-to-end — 배선을 되돌리면 이 테스트가 실패해야 한다).
2) 백그라운드 _reconcile_pending_orders()가 pending 주문을 30초마다 재조회해 지연 체결을
   trade_history에 기록하고 "✅ 지연 체결 확인" 알림을 보낸 뒤 Redis 컨텍스트를 정리한다.
3) 10분이 지나도 체결 확인이 안 되면 "⚠️ 주문 결과 불명"을 1회만 알리고 자동 재주문 없이 정리한다.
"""
import json
import sys
import time as _time
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
    """cache.client를 흉내내는 최소 인메모리 Redis 대역(get/setex/delete/scan_iter만 지원)"""

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


def _make_position(qty=10):
    return {
        "symbol": "005930", "name": "삼성전자",
        "avg_price": 70000, "cur_price": 60000,  # -14.3% 손실 → 손절선(-7%) 도달
        "qty": qty, "sellable_qty": qty,
    }


def _make_bot(pos):
    bot = main.StockTrader()
    bot.strategies = {"MA크로스": {"is_active": True, "params": {"stop_loss": -7, "max_positions": 0}}}
    bot.positions = {"005930": pos}
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
        ]
        for p in self._patches:
            p.start()
        self.addAsyncCleanup(self._stop_patches)

    async def _stop_patches(self):
        for p in self._patches:
            p.stop()


class TestStopLossPendingIsNotTreatedAsFailure(_BaseTest):
    async def test_pending_sell_result_skips_suppression_and_failure_alert(self):
        """손절 매도가 pending(체결 확인 대기)이면 30분 억제키·실패 알림·포지션 삭제가
        없어야 한다. 이 분기(elif result.get("pending"))를 제거하면(배선을 되돌리면)
        아래 assertNotIn이 실패해 회귀를 바로 잡아낸다."""
        pos = _make_position(qty=10)
        bot = _make_bot(pos)
        bot.trader.sell = AsyncMock(return_value={
            "success": False, "pending": True, "order_no": "0009000001",
            "error": "주문 접수, 체결 확인 대기",
        })

        with patch("common.telegram.send_stock", new_callable=AsyncMock) as mock_send:
            await bot._run_cycle()

        self.assertNotIn("sell_fail_suppress:005930", self.fake_redis.store)
        self.assertNotIn("sell_fail_streak:005930", self.fake_redis.store)
        mock_send.assert_not_awaited()
        # pending 중에는 포지션을 지우지 않는다(백그라운드 재확인이 확정지을 때까지 보유 유지)
        self.assertIn("005930", bot.positions)


class TestReconcilePendingOrders(_BaseTest):
    async def test_confirms_delayed_sell_fill_records_trade_and_alerts(self):
        """pending 매도가 백그라운드 재확인에서 체결 확인되면 trade_history에 기록하고
        "지연 체결 확인" 알림을 보낸 뒤 Redis 컨텍스트를 정리해야 한다."""
        bot = main.StockTrader()
        bot.trader = MagicMock()
        bot.trader._get_filled_qty = AsyncMock(return_value=(10, 59000.0))

        ctx = {
            "symbol": "005930", "side": "sell", "qty": 10, "price": 60000,
            "strategy": "MA크로스_손절", "avg_price": 70000.0,
            "accepted_at": _time.time() - 60,
        }
        self.fake_redis.store["order_pending:0009000001"] = json.dumps(ctx)
        self.fake_redis.store["order_pending_lock:005930:sell"] = "0009000001"

        with patch("common.telegram.send_stock", new_callable=AsyncMock) as mock_send:
            await bot._reconcile_one_pending_order("order_pending:0009000001")

        main.db.insert_trade.assert_awaited_once()
        _, kwargs = main.db.insert_trade.await_args
        self.assertEqual(kwargs["side"], "SELL")
        self.assertEqual(kwargs["quantity"], 10)
        self.assertEqual(kwargs["price"], 59000)
        self.assertEqual(kwargs["pnl"], int((59000.0 - 70000.0) * 10))
        self.assertEqual(kwargs["strategy"], "MA크로스_손절_지연체결확인")

        mock_send.assert_awaited_once()
        self.assertIn("지연 체결 확인", mock_send.await_args.args[0])

        self.assertNotIn("order_pending:0009000001", self.fake_redis.store)
        self.assertNotIn("order_pending_lock:005930:sell", self.fake_redis.store)

    async def test_still_unfilled_before_timeout_does_nothing(self):
        """10분 타임아웃 전이면 아직 불명 알림을 보내지 않고 pending을 그대로 유지한다."""
        bot = main.StockTrader()
        bot.trader = MagicMock()
        bot.trader._get_filled_qty = AsyncMock(return_value=(0, 0.0))

        ctx = {
            "symbol": "900070", "side": "sell", "qty": 5, "price": 10000,
            "strategy": "MA크로스_AI익절HALF", "avg_price": 9000.0,
            "accepted_at": _time.time() - 60,  # 10분(600초) 미만 경과
        }
        self.fake_redis.store["order_pending:0009000002"] = json.dumps(ctx)

        with patch("common.telegram.send_stock", new_callable=AsyncMock) as mock_send:
            await bot._reconcile_one_pending_order("order_pending:0009000002")

        main.db.insert_trade.assert_not_awaited()
        mock_send.assert_not_awaited()
        self.assertIn("order_pending:0009000002", self.fake_redis.store)

    async def test_unknown_after_timeout_alerts_once_and_does_not_reorder(self):
        """10분이 지나도 미체결이면 '결과 불명'을 1회만 알리고 정리하며, 재주문은 하지 않는다."""
        bot = main.StockTrader()
        bot.trader = MagicMock()
        bot.trader._get_filled_qty = AsyncMock(return_value=(0, 0.0))

        ctx = {
            "symbol": "900070", "side": "sell", "qty": 5, "price": 10000,
            "strategy": "MA크로스_AI익절HALF", "avg_price": 9000.0,
            "accepted_at": _time.time() - 700,  # 10분(600초) 초과 경과
        }
        self.fake_redis.store["order_pending:0009000003"] = json.dumps(ctx)
        self.fake_redis.store["order_pending_lock:900070:sell"] = "0009000003"

        with patch("common.telegram.send_stock", new_callable=AsyncMock) as mock_send:
            await bot._reconcile_one_pending_order("order_pending:0009000003")

        mock_send.assert_awaited_once()
        self.assertIn("결과 불명", mock_send.await_args.args[0])
        self.assertNotIn("order_pending:0009000003", self.fake_redis.store)
        self.assertNotIn("order_pending_lock:900070:sell", self.fake_redis.store)
        # 자동 재주문 없음(매수/매도 호출 자체가 이 경로에 없음)을 trader mock 호출로 확인
        bot.trader.buy.assert_not_called()
        bot.trader.sell.assert_not_called()

        # 같은 주문을 다시 넣어도(지연 삭제 등 경합 상황 가정) 같은 알림은 1회만 간다
        self.fake_redis.store["order_pending:0009000003"] = json.dumps(ctx)
        with patch("common.telegram.send_stock", new_callable=AsyncMock) as mock_send2:
            await bot._reconcile_one_pending_order("order_pending:0009000003")
        mock_send2.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
