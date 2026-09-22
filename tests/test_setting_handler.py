"""
router/handlers/setting_handler.py 단위 테스트.

validate_setting은 순수 함수라 바로 테스트하고, apply_strategy_settings/handle은
tests/test_decision_logger.py와 동일한 방식의 FakePool/FakeConnection(+FakeRedis)으로
strategy_config UPDATE와 redis publish 호출을 검증한다.
"""
import asyncio
import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from router.handlers import setting_handler  # noqa: E402


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakeStrategyConnection:
    def __init__(self, rows):
        self.rows = rows  # [{"id","name","is_active","params"}]
        self.updates = []  # [(id, params)]

    async def fetch(self, query, *args):
        assert "strategy_config" in query
        return list(self.rows)

    async def execute(self, query, *args):
        assert "UPDATE strategy_config" in query
        params_json, row_id = args
        self.updates.append((row_id, json.loads(params_json)))
        return "UPDATE 1"


class FakePool:
    def __init__(self, rows):
        self._conn = FakeStrategyConnection(rows)

    def acquire(self):
        return _AcquireCtx(self._conn)


class FakeRedis:
    def __init__(self):
        self.published = []

    async def publish(self, channel, message):
        self.published.append((channel, json.loads(message)))


class TestValidateSetting(unittest.TestCase):
    def test_stop_loss_within_range_normalizes_to_negative(self):
        ok, v = setting_handler.validate_setting("stop_loss", 5)
        self.assertTrue(ok)
        self.assertEqual(v, -5.0)

    def test_stop_loss_out_of_range_rejected(self):
        ok, msg = setting_handler.validate_setting("stop_loss", 20)
        self.assertFalse(ok)
        self.assertIn("허용범위", msg)

    def test_take_profit_within_range(self):
        ok, v = setting_handler.validate_setting("take_profit", 3)
        self.assertTrue(ok)
        self.assertEqual(v, 3.0)

    def test_buy_amount_within_range(self):
        ok, v = setting_handler.validate_setting("buy_amount", 1000000)
        self.assertTrue(ok)
        self.assertEqual(v, 1000000)

    def test_max_buy_percent_of_cash_accepts_both_forms(self):
        ok1, v1 = setting_handler.validate_setting("max_buy_percent_of_cash", 70)
        ok2, v2 = setting_handler.validate_setting("max_buy_percent_of_cash", 0.7)
        self.assertTrue(ok1 and ok2)
        self.assertEqual(v1, 0.7)
        self.assertEqual(v2, 0.7)

    def test_unknown_key_rejected(self):
        ok, msg = setting_handler.validate_setting("unknown_key", 1)
        self.assertFalse(ok)
        self.assertIn("알 수 없는 설정", msg)

    def test_non_numeric_value_rejected(self):
        ok, msg = setting_handler.validate_setting("stop_loss", "abc")
        self.assertFalse(ok)
        self.assertIn("숫자가 아님", msg)


class TestApplyStrategySettings(unittest.TestCase):
    def test_applies_to_all_stock_trader_strategies_and_publishes(self):
        rows = [
            {"id": 1, "name": "MA크로스", "is_active": True, "params": {"stop_loss": -5}},
            {"id": 2, "name": "RSI", "is_active": True, "params": "{}"},
        ]
        pool = FakePool(rows)
        redis = FakeRedis()

        applied = asyncio.run(setting_handler.apply_strategy_settings(pool, redis, {"stop_loss": -7}))

        self.assertEqual(applied, ["MA크로스", "RSI"])
        self.assertEqual(len(pool._conn.updates), 2)
        self.assertEqual(pool._conn.updates[0][1]["stop_loss"], -7)
        self.assertEqual(len(redis.published), 2)


class TestHandle(unittest.IsolatedAsyncioTestCase):
    async def test_no_change_keyword_returns_none(self):
        reply = await setting_handler.handle("손절 -7%", None, None, None)
        self.assertIsNone(reply)

    async def test_out_of_range_stop_loss_rejected_before_db_write(self):
        pool = FakePool([])
        sent = []

        async def send_telegram(text, **kw):
            sent.append(text)

        reply = await setting_handler.handle("손절 20%로 바꿔줘", pool, FakeRedis(), send_telegram)
        self.assertIn("허용 범위", reply)
        self.assertEqual(pool._conn.updates, [])
        self.assertEqual(sent, [])  # 범위 밖이면 텔레그램도 보내지 않아야 함

    async def test_valid_stop_loss_change_applies_and_notifies(self):
        rows = [{"id": 1, "name": "MA크로스", "is_active": True, "params": {}}]
        pool = FakePool(rows)
        sent = []

        async def send_telegram(text, **kw):
            sent.append(text)

        reply = await setting_handler.handle("손절 -5%로 변경해줘", pool, FakeRedis(), send_telegram)
        self.assertIn("설정 변경 완료", reply)
        self.assertEqual(pool._conn.updates[0][1]["stop_loss"], -5.0)
        self.assertEqual(len(sent), 1)


if __name__ == "__main__":
    unittest.main()
