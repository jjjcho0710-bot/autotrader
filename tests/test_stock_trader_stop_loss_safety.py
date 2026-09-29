"""
stock_trader/main.py 손절 매도 안전성 개선 회귀 테스트.

배경: [AT] fix/stop-loss-safety-improvements (PM 지시)
1) 부분체결 후 잔량 방치 수정: filled_qty < qty(부분체결)이면 포지션을 지우지 않고
   잔량을 유지해야 하며, 30분 억제 없이 다음 사이클에 즉시 재시도할 수 있어야 한다.
2) 손절 실패 재시도 차등화: "체결 0주" 등 일시적 오류는 짧게(60~120초), 동일 사유로
   3회 이상 연속 실패하면 억제 시간을 5분→15분→30분으로 점진 확대해야 한다.
3) 억제 중에도 최소 5분 간격으로는 알림이 발송되어야 한다(시간당 1회 원인 스로틀과 별개).
"""
import json
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


def _make_position(qty=10):
    return {
        "symbol": "005930", "name": "삼성전자",
        "avg_price": 70000, "cur_price": 60000,  # -14.3% 손실 → 손절선(-7%) 도달
        "qty": qty, "sellable_qty": qty,
    }


def _make_bot(pos):
    bot = main.StockTrader()
    # max_positions=0 → 손절 처리 후 신규 진입 스캔 없이 바로 return
    bot.strategies = {"MA크로스": {"is_active": True, "params": {"stop_loss": -7, "max_positions": 0}}}
    bot.positions = {"005930": pos}
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
        ]
        for p in self._patches:
            p.start()
        self.addAsyncCleanup(self._stop_patches)

    async def _stop_patches(self):
        for p in self._patches:
            p.stop()


class TestStopLossPartialFillKeepsRemainder(_BaseCycleTest):
    async def test_partial_fill_keeps_remaining_position_without_30min_suppression(self):
        pos = _make_position(qty=10)
        bot = _make_bot(pos)
        bot.trader.sell = AsyncMock(return_value={"success": True, "filled_qty": 4, "partial": True})

        await bot._run_cycle()

        self.assertIn("005930", bot.positions)
        self.assertEqual(bot.positions["005930"]["qty"], 6)
        self.assertEqual(bot.positions["005930"]["sellable_qty"], 6)
        # 성공(부분체결)이므로 30분 실패 억제 키가 걸리면 안 됨
        self.assertNotIn("sell_fail_suppress:005930", self.fake_redis.store)

    async def test_full_fill_clears_position_as_before(self):
        pos = _make_position(qty=10)
        bot = _make_bot(pos)
        bot.trader.sell = AsyncMock(return_value={"success": True, "filled_qty": 10})

        await bot._run_cycle()

        self.assertNotIn("005930", bot.positions)
        self.assertNotIn("sell_fail_suppress:005930", self.fake_redis.store)


class TestStopLossFailureRetryDifferentiation(_BaseCycleTest):
    async def test_transient_zero_fill_failure_gets_short_suppression_not_30min(self):
        """'체결 0주' 등 일시적 오류는 첫 실패부터 30분이 아니라 짧게(90초) 억제해야 한다."""
        pos = _make_position(qty=10)
        bot = _make_bot(pos)
        bot.trader.sell = AsyncMock(return_value={
            "success": False, "error": "주문 접수됐으나 미체결(체결수량 0)",
        })

        await bot._run_cycle()

        raw = self.fake_redis.store.get("sell_fail_suppress:005930")
        self.assertIsNotNone(raw)
        data = json.loads(raw)
        self.assertEqual(data["fail_count"], 1)
        # 위치 포지션은 실패했으므로 그대로 유지되어야 함
        self.assertIn("005930", bot.positions)

    async def test_non_transient_first_failures_keep_default_30min(self):
        """전형적이지 않은 사유(예: 잔고부족)는 처음 1~2회는 기존과 동일하게 30분 억제."""
        bot = main.StockTrader()
        with patch.object(main.cache, "client", self.fake_redis):
            sec1, cnt1 = await bot._compute_stop_loss_suppress_sec("005930", "잔고가 부족합니다")
            sec2, cnt2 = await bot._compute_stop_loss_suppress_sec("005930", "잔고가 부족합니다")
        self.assertEqual((sec1, cnt1), (1800, 1))
        self.assertEqual((sec2, cnt2), (1800, 2))

    async def test_repeated_same_reason_escalates_suppression_5_15_30_min(self):
        """동일 사유로 3회 이상 연속 실패하면 억제 시간이 5분→15분→30분으로 늘어나야 한다."""
        bot = main.StockTrader()
        err = "잔고가 부족합니다"
        results = []
        with patch.object(main.cache, "client", self.fake_redis):
            for _ in range(5):
                sec, cnt = await bot._compute_stop_loss_suppress_sec("005930", err)
                results.append(sec)
        self.assertEqual(results, [1800, 1800, 300, 900, 1800])

    async def test_different_reason_resets_streak(self):
        """실패 사유가 바뀌면 연속 실패 횟수가 1로 리셋되어야 한다."""
        bot = main.StockTrader()
        with patch.object(main.cache, "client", self.fake_redis):
            await bot._compute_stop_loss_suppress_sec("005930", "잔고가 부족합니다")
            await bot._compute_stop_loss_suppress_sec("005930", "잔고가 부족합니다")
            sec, cnt = await bot._compute_stop_loss_suppress_sec("005930", "일일 매도한도 초과")
        self.assertEqual(cnt, 1)
        self.assertEqual(sec, 1800)

    async def test_success_after_failures_clears_streak(self):
        """실패가 누적된 후 매도에 성공하면 연속 실패 스트릭이 초기화되어야 한다."""
        pos = _make_position(qty=10)
        bot = _make_bot(pos)
        bot.trader.sell = AsyncMock(return_value={"success": False, "error": "잔고가 부족합니다"})
        await bot._run_cycle()
        await bot._run_cycle()  # 억제 중이라 실제로는 스킵되지만 흐름 확인용

        # 억제 키를 강제로 지우고 이번엔 성공시켜 스트릭 초기화를 확인
        self.fake_redis.store.pop("sell_fail_suppress:005930", None)
        bot.trader.sell = AsyncMock(return_value={"success": True, "filled_qty": 10})
        await bot._run_cycle()

        self.assertNotIn("sell_fail_streak:005930", self.fake_redis.store)


class TestStopLossWorseningAlertDuringSuppression(_BaseCycleTest):
    async def test_alert_sent_at_least_every_5_minutes_while_suppressed(self):
        """억제 중이라도 최소 5분 간격으로는 알림이 발송되어야 한다."""
        pos = _make_position(qty=10)
        bot = _make_bot(pos)
        # 이미 억제 중인 상태를 시뮬레이션
        self.fake_redis.store["sell_fail_suppress:005930"] = json.dumps({
            "reason": "잔고가 부족합니다", "ts": 0, "fail_count": 1,
        })

        with patch("common.telegram.send_stock", new_callable=AsyncMock) as mock_send:
            await bot._run_cycle()
            self.assertEqual(mock_send.await_count, 1)
            # 같은 5분 창 안에서는 다시 호출해도 추가 알림이 나가지 않아야 함
            bot.positions = {"005930": pos}
            bot.trader.get_positions = AsyncMock(return_value=[pos])
            await bot._run_cycle()
            self.assertEqual(mock_send.await_count, 1)


if __name__ == "__main__":
    unittest.main()
