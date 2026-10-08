"""
common/late_fill_tracker.py 단위 테스트.

배경: 10/8 부국철강 100주 매도 주문이 접수 후 34주, 44주로 늘다가 결국 100주 전량
체결됐다. dashboard의 _kis_stock_order는 주문 후 최대 약 11초 안에 확인된 체결
수량만 기록하므로, 이후에 체결되는 수량은 trade_history에 남지 않았다. 이 테스트는
부분체결로 등록된 주문을 주기적으로 재조회해 증가분만 추가 기록하는 로직을 검증한다:
1) 부분체결 후 나중에 전량 체결
2) 부분체결 후 더 늘지 않음(증가분 없으면 기록/알림 없음)
3) 재조회 실패(다음 주기 재시도, 현재 상태 보존)
4) 이미 기록한 수량과 같을 때 중복 기록 없음(= 2와 동일한 가드 재확인)
5) 장 마감 이후 한 번 더 확인한 뒤 추적 종료
"""
import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from common import late_fill_tracker as lft  # noqa: E402

KST = timezone(timedelta(hours=9))


class FakeConn:
    def __init__(self):
        self.inserted = []

    async def execute(self, query, *args):
        assert "trade_history" in query
        self.inserted.append(args)
        return "INSERT 1"


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakePool:
    def __init__(self):
        self._conn = FakeConn()

    def acquire(self):
        return _AcquireCtx(self._conn)


class FakeRedis:
    """common.late_fill_tracker이 쓰는 get/setex/delete/scan_iter만 지원하는 인메모리 대역."""

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


class TestRegisterPartialFill(unittest.IsolatedAsyncioTestCase):
    async def test_registers_when_partial(self):
        redis = FakeRedis()
        await lft.register_partial_fill(
            redis, order_no="O1", symbol="026940", side="SELL", bot="stock_trader",
            requested_qty=100, recorded_qty=34, strategy="손절", price=10000,
            cost_basis_avg_price=12000,
        )
        raw = redis.store["late_fill_pending:O1"]
        ctx = json.loads(raw)
        self.assertEqual(ctx["recorded_qty"], 34)
        self.assertEqual(ctx["requested_qty"], 100)
        self.assertFalse(ctx["post_close_check_done"])

    async def test_does_not_register_when_already_full(self):
        """체결 수량이 이미 주문 수량과 같으면(전량체결) 등록하지 않는다."""
        redis = FakeRedis()
        await lft.register_partial_fill(
            redis, order_no="O2", symbol="005930", side="BUY", bot="stock_trader",
            requested_qty=10, recorded_qty=10, strategy="수동지시", price=70000,
        )
        self.assertEqual(redis.store, {})


class _BaseLoopTest(unittest.IsolatedAsyncioTestCase):
    def _make_ctx(self, **overrides):
        ctx = {
            "order_no": "O1", "symbol": "026940", "side": "SELL", "bot": "stock_trader",
            "requested_qty": 100, "recorded_qty": 34, "strategy": "손절", "price": 10000,
            "cost_basis_avg_price": 12000, "registered_at": 0.0, "post_close_check_done": False,
        }
        ctx.update(overrides)
        return ctx


class TestPollLateFillsIncreasesAndFullFill(_BaseLoopTest):
    async def test_increment_is_recorded_and_notified(self):
        """34주 → 44주로 늘면 증가분 10주만 추가 INSERT하고 누적 수량을 알려야 한다."""
        redis = FakeRedis()
        pool = FakePool()
        redis.store["late_fill_pending:O1"] = json.dumps(self._make_ctx())
        sent = []

        async def get_filled_qty(order_no, symbol, is_buy):
            return 44, 9800.0

        async def send_telegram(text):
            sent.append(text)

        with _frozen_market_open():
            await lft.poll_late_fills_once(redis, pool, get_filled_qty, send_telegram)

        self.assertEqual(len(pool._conn.inserted), 1)
        args = pool._conn.inserted[0]
        # (bot, asset_type, symbol, side, price, quantity, amount, strategy, pnl)
        self.assertEqual(args[2], "026940")
        self.assertEqual(args[3], "SELL")
        self.assertEqual(args[5], 10.0)  # 증가분만(44-34)
        self.assertEqual(args[8], (9800.0 - 12000) * 10)  # 매도 pnl은 증가분 기준

        self.assertTrue(sent)
        self.assertIn("+10", sent[0])
        self.assertIn("44/100", sent[0])

        ctx = json.loads(redis.store["late_fill_pending:O1"])
        self.assertEqual(ctx["recorded_qty"], 44)

    async def test_full_fill_after_partial_stops_tracking(self):
        """부분체결 후 나중에 전량 체결되면 남은 증가분을 기록하고 추적을 끝낸다(키 삭제)."""
        redis = FakeRedis()
        pool = FakePool()
        redis.store["late_fill_pending:O1"] = json.dumps(self._make_ctx())

        async def get_filled_qty(order_no, symbol, is_buy):
            return 100, 9700.0

        async def send_telegram(text):
            pass

        with _frozen_market_open():
            await lft.poll_late_fills_once(redis, pool, get_filled_qty, send_telegram)

        self.assertEqual(len(pool._conn.inserted), 1)
        self.assertEqual(pool._conn.inserted[0][5], 66.0)  # 100-34
        self.assertNotIn("late_fill_pending:O1", redis.store)


class TestPollLateFillsNoIncrease(_BaseLoopTest):
    async def test_same_quantity_records_nothing_and_keeps_tracking(self):
        """부분체결 후 더 늘지 않았으면(이미 기록한 수량과 동일) 추가 기록·알림이 없어야
        하고, 추적은 계속된다(장중이면 키가 그대로 남아야 함)."""
        redis = FakeRedis()
        pool = FakePool()
        redis.store["late_fill_pending:O1"] = json.dumps(self._make_ctx())
        sent = []

        async def get_filled_qty(order_no, symbol, is_buy):
            return 34, 9800.0

        async def send_telegram(text):
            sent.append(text)

        with _frozen_market_open():
            await lft.poll_late_fills_once(redis, pool, get_filled_qty, send_telegram)

        self.assertEqual(pool._conn.inserted, [])
        self.assertEqual(sent, [])
        self.assertIn("late_fill_pending:O1", redis.store)
        ctx = json.loads(redis.store["late_fill_pending:O1"])
        self.assertEqual(ctx["recorded_qty"], 34)


class TestPollLateFillsRequeryFailure(_BaseLoopTest):
    async def test_requery_failure_keeps_state_for_next_cycle(self):
        """재조회 자체가 실패(None)하면 기록/알림 없이 다음 주기에 재시도할 수 있도록
        Redis 상태를 그대로 보존해야 한다."""
        redis = FakeRedis()
        pool = FakePool()
        redis.store["late_fill_pending:O1"] = json.dumps(self._make_ctx())
        sent = []

        async def get_filled_qty(order_no, symbol, is_buy):
            return None, None

        async def send_telegram(text):
            sent.append(text)

        with _frozen_market_open():
            await lft.poll_late_fills_once(redis, pool, get_filled_qty, send_telegram)

        self.assertEqual(pool._conn.inserted, [])
        self.assertEqual(sent, [])
        ctx = json.loads(redis.store["late_fill_pending:O1"])
        self.assertEqual(ctx["recorded_qty"], 34)  # 변경되지 않음


class TestPollLateFillsMarketClose(_BaseLoopTest):
    async def test_stops_after_one_more_check_past_market_close(self):
        """장 마감(15:30 KST) 이후 첫 확인에서는 추적을 유지(한 번 더 확인 예정 표시)하고,
        그다음 확인에서는 증가가 없어도 추적을 끝내야 한다."""
        redis = FakeRedis()
        pool = FakePool()
        redis.store["late_fill_pending:O1"] = json.dumps(self._make_ctx())

        async def get_filled_qty(order_no, symbol, is_buy):
            return 34, 9800.0  # 늘지 않음

        async def send_telegram(text):
            pass

        with _frozen_market_closed():
            await lft.poll_late_fills_once(redis, pool, get_filled_qty, send_telegram)
        self.assertIn("late_fill_pending:O1", redis.store)
        ctx = json.loads(redis.store["late_fill_pending:O1"])
        self.assertTrue(ctx["post_close_check_done"])

        with _frozen_market_closed():
            await lft.poll_late_fills_once(redis, pool, get_filled_qty, send_telegram)
        self.assertNotIn("late_fill_pending:O1", redis.store)


def _frozen_market_open():
    from unittest.mock import patch
    return patch.object(
        lft, "_is_market_closed_kst",
        lambda now=None: False,
    )


def _frozen_market_closed():
    from unittest.mock import patch
    return patch.object(
        lft, "_is_market_closed_kst",
        lambda now=None: True,
    )


if __name__ == "__main__":
    unittest.main()
