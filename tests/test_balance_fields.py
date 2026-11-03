"""[AT] fix/balance-fields — eval_krw 소스, 총자산 미확인 시 매수 보류, 서킷브레이커 SKIP 기록, 설정 응답 문구."""
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "stock_trader"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))
for mod_name in ["fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
                 "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai"]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

from market.universe import Universe  # noqa: E402
from router.handlers import order_handler, setting_handler  # noqa: E402
from tests.test_order_handler_sizing_cap import TestBuySizingCap  # noqa: E402
from tests.test_max_positions_setting import FakePool as SPool, _rows  # noqa: E402


class TestChatSizingHoldsWhenTotalUnknown(unittest.IsolatedAsyncioTestCase):
    async def test_total_zero_holds_buy_instead_of_seed_fallback(self):
        helper = TestBuySizingCap()
        orders = []
        kwargs = helper._kwargs(price=50_000, get_balance_fn=None)

        async def get_balance():
            return {"cash": 1_000_000, "total": 0}

        async def kis_order(symbol, price, qty, is_buy):
            orders.append(qty)
            return {"success": True}
        kwargs["get_balance_fn"] = get_balance
        kwargs["kis_order_fn"] = kis_order
        reply = await order_handler.handle_trade_command("삼성바이오로직스 5주 매수", **kwargs)
        self.assertIn("매수를 보류", reply)
        self.assertEqual(orders, [])

    async def test_total_known_still_sizes(self):
        helper = TestBuySizingCap()

        async def get_balance():
            return {"total": 10_000_000}
        reply = await order_handler.handle_trade_command(
            "삼성바이오로직스 5주 매수", **helper._kwargs(price=50_000, get_balance_fn=get_balance))
        self.assertIn("5주 매수 완료", reply)


class TestBalanceEndpointTotal(unittest.IsolatedAsyncioTestCase):
    async def test_source_has_no_psbl_order_tot_evlu_read(self):
        src = (REPO_ROOT / "dashboard" / "main.py").read_text(encoding="utf-8")
        self.assertNotIn('int(output.get("tot_evlu_amt", 0))', src)


class TestSettingResponses(unittest.IsolatedAsyncioTestCase):
    async def _run(self, msg):
        sent = []

        async def tg(text, **kw):
            sent.append(text)
        pool = SPool(_rows())
        r = MagicMock()
        r.publish = AsyncMock()
        r.lpush = AsyncMock()
        r.ltrim = AsyncMock()
        r.expire = AsyncMock()
        reply = await setting_handler.handle(msg, pool, r, tg)
        return reply, sent, pool

    async def test_take_profit_response_notes_reference_only(self):
        reply, sent, _ = await self._run("익절 4% 변경해줘")
        self.assertIn("참고용", reply)
        self.assertIn("청산에는 반영되지 않습니다", reply)
        self.assertIn("참고용", sent[0])

    async def test_stop_loss_only_has_no_take_profit_note(self):
        reply, _, _ = await self._run("손절 -5% 변경해줘")
        self.assertNotIn("참고용", reply)

    async def test_multiple_stop_loss_numbers_not_saved_and_asks_again(self):
        for msg in ("손절 -7%에서 -5%로 변경", "손절 5% 6% 변경해줘"):
            reply, sent, pool = await self._run(msg)
            self.assertIn("여러 개", reply, msg)
            self.assertEqual(pool.conn.update_count, 0, msg)
            self.assertEqual(sent, [])

    async def test_stop_loss_plus_take_profit_is_not_multiple(self):
        reply, _, pool = await self._run("손절 -5% 익절 4% 변경해줘")
        self.assertNotIn("여러 개", reply)
        self.assertEqual(pool.conn.update_count, 3 - 1)  # 활성 2행 fixture 기준 행 수


class TestCircuitBreakerSkipRecord(unittest.IsolatedAsyncioTestCase):
    async def _signal(self, log_side_effect=None):
        import dashboard.main as dm
        req = MagicMock()
        req.json = AsyncMock(return_value={"bot": "stock_trader", "action": "buy", "symbol": "005930",
                                           "name": "삼성전자", "price": 70000, "qty": 1, "strategy": "MA크로스"})
        logged = AsyncMock(side_effect=log_side_effect)
        with patch.object(dm.execution_guard, "precheck", AsyncMock(return_value={"blocked": "daily_stop_loss_limit"})), \
                patch("stark.decision_logger.log_decision", logged), \
                patch.object(dm, "db_pool", MagicMock()), patch.object(dm, "_code_to_name", AsyncMock(return_value="삼성전자")):
            res = await dm._jarvis_signal_impl(req)
        return res, logged

    async def test_blocked_records_skip_with_reason(self):
        res, logged = await self._signal()
        self.assertEqual(res.get("blocked"), "daily_stop_loss_limit")
        logged.assert_awaited_once()
        args, kw = logged.await_args
        self.assertEqual(args[1:3], ("005930", "SKIP"))
        self.assertIn("daily_stop_loss_limit", kw["reason"])

    async def test_record_failure_does_not_break_block(self):
        res, _ = await self._signal(log_side_effect=RuntimeError("db down"))
        self.assertTrue(res["success"])
        self.assertEqual(res["blocked"], "daily_stop_loss_limit")


if __name__ == "__main__":
    unittest.main()
