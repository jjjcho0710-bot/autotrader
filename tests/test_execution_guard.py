"""
stark/execution_guard.py 단위 테스트: 룰 기반 안전장치(precheck)와 실행 결과 후처리(execute).

핵심 검증: 당일 잔고없음 매도 억제, 당일 2회 손절 시 신규매수 차단, 안전장치 확인
자체가 실패하면 보수적으로 매수를 막는지, 그리고 execute()가 성공/실패 각각에서
매매기록·텔레그램·캐시무효화·매매일지를 빠짐없이 수행하는지.
"""
import asyncio
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from stark import execution_guard  # noqa: E402


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakeTradeHistoryConnection:
    def __init__(self, stop_loss_count=0, raise_on_fetchval=False):
        self.stop_loss_count = stop_loss_count
        self.raise_on_fetchval = raise_on_fetchval
        self.inserted = []

    async def fetchval(self, query, *args):
        if self.raise_on_fetchval:
            raise RuntimeError("DB down")
        assert "trade_history" in query
        return self.stop_loss_count

    async def execute(self, query, *args):
        assert "trade_history" in query
        self.inserted.append(args)
        return "INSERT 1"


class FakePool:
    def __init__(self, stop_loss_count=0, raise_on_fetchval=False):
        self._conn = FakeTradeHistoryConnection(stop_loss_count, raise_on_fetchval)

    def acquire(self):
        return _AcquireCtx(self._conn)


class FakeRedis:
    def __init__(self, store=None):
        self.store = store or {}
        self.deleted = []

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, value):
        self.store[key] = value

    async def delete(self, key):
        self.deleted.append(key)
        self.store.pop(key, None)


class TestPrecheck(unittest.IsolatedAsyncioTestCase):
    async def test_passes_when_nothing_blocks(self):
        pool = FakePool(stop_loss_count=0)
        result = await execution_guard.precheck("005930", "buy", "stock_trader", pool=pool, redis=FakeRedis())
        self.assertIsNone(result)

    async def test_sell_fail_suppress_blocks_sell(self):
        redis = FakeRedis({"sell_fail_suppress:005930": "1"})
        result = await execution_guard.precheck("005930", "sell", "stock_trader", pool=None, redis=redis)
        self.assertEqual(result, {"blocked": "sell_fail_suppress"})

    async def test_suppress_key_does_not_affect_buy(self):
        redis = FakeRedis({"sell_fail_suppress:005930": "1"})
        pool = FakePool(stop_loss_count=0)
        result = await execution_guard.precheck("005930", "buy", "stock_trader", pool=pool, redis=redis)
        self.assertIsNone(result)

    async def test_two_stop_losses_blocks_new_buy(self):
        pool = FakePool(stop_loss_count=2)
        result = await execution_guard.precheck("005930", "buy", "stock_trader", pool=pool, redis=FakeRedis())
        self.assertEqual(result, {"blocked": "daily_stop_loss_limit"})

    async def test_one_stop_loss_does_not_block(self):
        pool = FakePool(stop_loss_count=1)
        result = await execution_guard.precheck("005930", "buy", "stock_trader", pool=pool, redis=FakeRedis())
        self.assertIsNone(result)

    async def test_stop_loss_check_only_applies_to_stock_trader(self):
        pool = FakePool(stop_loss_count=5)
        result = await execution_guard.precheck("BTC", "buy", "crypto_trader", pool=pool, redis=FakeRedis())
        self.assertIsNone(result)

    async def test_guard_check_failure_blocks_conservatively(self):
        pool = FakePool(raise_on_fetchval=True)
        result = await execution_guard.precheck("005930", "buy", "stock_trader", pool=pool, redis=FakeRedis())
        self.assertIn("error", result)


def make_signal(**overrides):
    base = {"symbol": "005930", "name": "삼성전자", "bot": "stock_trader", "action": "buy",
            "price": 70000, "qty": 1, "strategy": "MA크로스", "reason": "골든크로스"}
    base.update(overrides)
    return base


def make_decision(**overrides):
    base = {"is_small": False, "reply": "EXECUTE: 강한 확신"}
    base.update(overrides)
    return base


class TestExecute(unittest.IsolatedAsyncioTestCase):
    async def test_success_records_trade_and_notifies(self):
        pool = FakePool()
        redis = FakeRedis({"cache:positions:stock": "1", "cache:account:stock": "1"})
        sent, journaled, memory_saved = [], [], []

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            journaled.append(args)

        async def save_trade_memory(**kwargs):
            memory_saved.append(kwargs)

        async def code_to_name(symbol):
            return "삼성전자"

        result = await execution_guard.execute(
            make_signal(), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=save_trade_memory,
            code_to_name_fn=code_to_name,
        )
        self.assertEqual(result, {"success": True, "executed": True, "jarvis_reply": "EXECUTE: 강한 확신"})
        self.assertEqual(len(pool._conn.inserted), 1)
        self.assertEqual(len(sent), 1)
        self.assertEqual(len(journaled), 1)
        self.assertEqual(len(memory_saved), 1)
        self.assertEqual(redis.deleted, ["cache:positions:stock", "cache:account:stock"])

    async def test_no_balance_failure_sets_suppress_key_once(self):
        pool = FakePool()
        redis = FakeRedis()
        sent = []

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": False, "error": "매도가능 수량 부족"}

        async def send_telegram(text):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        result = await execution_guard.execute(
            make_signal(action="sell"), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
        )
        self.assertFalse(result["success"])
        self.assertIn("sell_fail_suppress:005930", redis.store)
        self.assertEqual(len(sent), 1)

        # 두 번째 실패는 이미 억제 키가 있으므로 재알림하지 않는다
        result2 = await execution_guard.execute(
            make_signal(action="sell"), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
        )
        self.assertFalse(result2["success"])
        self.assertEqual(len(sent), 1)  # 여전히 1건 — 재알림 안 됨

    async def test_other_failure_notifies_without_suppress_key(self):
        pool = FakePool()
        redis = FakeRedis()
        sent = []

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": False, "error": "시장가 주문 거부"}

        async def send_telegram(text):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        result = await execution_guard.execute(
            make_signal(), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
        )
        self.assertFalse(result["success"])
        self.assertNotIn("sell_fail_suppress:005930", redis.store)
        self.assertEqual(len(sent), 1)


if __name__ == "__main__":
    unittest.main()
