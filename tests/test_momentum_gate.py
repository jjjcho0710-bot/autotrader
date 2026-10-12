"""
모멘텀 게이트 테스트 ([AT] feat/momentum-gate).

배경: 최근 거래 데이터에서 직전 5거래일 수익률(종가 기준)이 음수인 종목의 AI 자동매수
성과가 나빴다. AI 자동매수(stark.execution_guard.execute()) 직전에만 이 수익률을 확인해
0% 미달이면 차단하고(journal에 SKIP, 사유 momentum_gate), 8% 이상이면 차단 없이 shadow
로그만 남긴다(과열 사후평가용). 수동(채팅) 매수·사용자 승인 매수는 차단하지 않고 경고
문구만 첨부한다. 종가 데이터가 부족/조회 실패하면 게이트를 적용하지 않고 기존 동작을
유지한다(새로 차단하지 않음).

검증 항목:
  1) stark.execution_guard.check_momentum() 단위 — 음수 차단 판정, 0% 경계, 8% shadow,
     데이터 부족/조회 실패 시 미차단, 임계값·lookback 설정 반영.
  2) execute() 통합 — AI 자동매수가 실제로 차단/통과/shadow 로그를 내는지.
  3) handle_trade_command()/handle_proposal_response() 통합 — 수동·제안승인 매수는
     차단하지 않고 경고 문구만 답장에 붙는지.

revert 확인(수동): stark/execution_guard.py execute()의 "(1b) 모멘텀 게이트" 블록을
주석 처리하면 test_blocks_automatic_buy_on_negative_momentum이 실패하는 것을 확인했다
(결과 "executed": True로 통과돼버림) — 게이트가 실제로 차단을 담당하고 있음을 보증.
"""
import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from stark import execution_guard  # noqa: E402
from tests.test_execution_guard import (  # noqa: E402
    FakePool as EgFakePool,
    FakeRedis as EgFakeRedis,
    make_decision,
    make_signal,
)


# ── 1) check_momentum() 단위 테스트 ──────────────────────────────
class TestCheckMomentum(unittest.IsolatedAsyncioTestCase):
    async def test_negative_return_is_drop(self):
        pool = EgFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 97]})  # -3%
        result = await execution_guard.check_momentum("005930", pool=pool)
        self.assertAlmostEqual(result["pct"], -3.0)
        self.assertTrue(result["is_drop"])
        self.assertFalse(result["is_shadow"])
        self.assertFalse(result["insufficient_data"])

    async def test_zero_pct_is_not_drop(self):
        """수익률 >= 0%는 차단 대상이 아니다(경계값 — 요구사항: '< 0%'만 차단)."""
        pool = EgFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 100]})
        result = await execution_guard.check_momentum("005930", pool=pool)
        self.assertEqual(result["pct"], 0.0)
        self.assertFalse(result["is_drop"])

    async def test_positive_return_below_shadow_is_not_drop_or_shadow(self):
        pool = EgFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 103]})  # +3%
        result = await execution_guard.check_momentum("005930", pool=pool)
        self.assertFalse(result["is_drop"])
        self.assertFalse(result["is_shadow"])

    async def test_shadow_threshold_flags_without_counting_as_drop(self):
        pool = EgFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 109]})  # +9%
        result = await execution_guard.check_momentum("005930", pool=pool)
        self.assertAlmostEqual(result["pct"], 9.0)
        self.assertFalse(result["is_drop"])
        self.assertTrue(result["is_shadow"])

    async def test_exact_shadow_boundary_counts_as_shadow(self):
        pool = EgFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 108]})  # +8.0%
        result = await execution_guard.check_momentum("005930", pool=pool)
        self.assertAlmostEqual(result["pct"], 8.0)
        self.assertTrue(result["is_shadow"])

    async def test_insufficient_data_does_not_block(self):
        pool = EgFakePool(daily_closes={"005930": [100, 98]})  # lookback+1=6개 미달
        result = await execution_guard.check_momentum("005930", pool=pool)
        self.assertIsNone(result["pct"])
        self.assertTrue(result["insufficient_data"])
        self.assertFalse(result["is_drop"])
        self.assertFalse(result["is_shadow"])

    async def test_no_data_at_all_does_not_block(self):
        pool = EgFakePool()  # daily_closes 미설정
        result = await execution_guard.check_momentum("005930", pool=pool)
        self.assertIsNone(result["pct"])
        self.assertTrue(result["insufficient_data"])
        self.assertFalse(result["is_drop"])

    async def test_fetch_failure_does_not_block(self):
        pool = EgFakePool(raise_on_daily_closes=True)
        result = await execution_guard.check_momentum("005930", pool=pool)
        self.assertIsNone(result["pct"])
        self.assertTrue(result["insufficient_data"])
        self.assertFalse(result["is_drop"])

    async def test_explicit_thresholds_override_config_defaults(self):
        pool = EgFakePool(daily_closes={"005930": [100, 100, 99]})  # lookback=2 -> pct=-1%
        result = await execution_guard.check_momentum(
            "005930", pool=pool, lookback_days=2,
            drop_threshold_pct=-2.0, shadow_threshold_pct=5.0,
        )
        self.assertAlmostEqual(result["pct"], -1.0)
        self.assertFalse(result["is_drop"])  # -1% >= -2%(커스텀 임계값)이므로 차단 아님
        self.assertEqual(result["lookback_days"], 2)
        self.assertEqual(result["drop_threshold_pct"], -2.0)
        self.assertEqual(result["shadow_threshold_pct"], 5.0)

    async def test_config_defaults_are_honored_when_not_overridden(self):
        """임계값은 common/config.py 설정값으로 분리돼 있어 변경하면 즉시 반영돼야 한다."""
        from common.config import config
        original = config.MOMENTUM_GATE_DROP_THRESHOLD_PCT
        try:
            config.MOMENTUM_GATE_DROP_THRESHOLD_PCT = -5.0
            pool = EgFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 97]})  # -3%
            result = await execution_guard.check_momentum("005930", pool=pool)
            self.assertFalse(result["is_drop"])  # -3% >= -5%(설정값)이므로 차단 아님
        finally:
            config.MOMENTUM_GATE_DROP_THRESHOLD_PCT = original


# ── 2) execute() 통합 테스트 (AI 자동매수 경로) ──────────────────
class TestExecuteMomentumGate(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        execution_guard.reset_buy_lock()

    async def test_blocks_automatic_buy_on_negative_momentum(self):
        pool = EgFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 97]})  # -3%
        redis = EgFakeRedis()
        journaled = []

        async def kis_order(symbol, price, qty, is_buy):
            raise AssertionError("모멘텀 게이트가 차단해야 하는데 주문이 실행됐다")

        async def send_telegram(text):
            pass

        async def log_journal(*args, **kwargs):
            journaled.append(args)

        async def code_to_name(symbol):
            return "삼성전자"

        result = await execution_guard.execute(
            make_signal(), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
        )
        self.assertFalse(result["executed"])
        self.assertTrue(result["skipped"])
        self.assertEqual(result["blocked"], "momentum_gate")
        self.assertIn("momentum_gate", result["reason"])

        # journal(trade_journal)에 SKIP + 사유 momentum_gate가 기록됐는지
        self.assertEqual(len(journaled), 1)
        bot, symbol, name, action, strategy, reason, jarvis_decision, jarvis_reason = journaled[0][:8]
        self.assertEqual(jarvis_decision, "SKIP")
        self.assertIn("momentum_gate", jarvis_reason)

        # stark_decisions(decision_logger.log_decision)에도 SKIP으로 기록됐는지
        self.assertEqual(len(pool._conn.decisions), 1)

    async def test_zero_pct_does_not_block(self):
        pool = EgFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 100]})  # 0%
        redis = EgFakeRedis()

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            pass

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
        self.assertTrue(result["executed"])

    async def test_shadow_threshold_does_not_block_but_logs(self):
        pool = EgFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 109]})  # +9%
        redis = EgFakeRedis()

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            pass

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        with self.assertLogs("stark.execution_guard", level="INFO") as cm:
            result = await execution_guard.execute(
                make_signal(), make_decision(), pool=pool, redis=redis,
                kis_order_fn=kis_order, send_telegram_fn=send_telegram,
                log_journal_fn=log_journal, save_trade_memory_fn=None,
                code_to_name_fn=code_to_name,
            )
        self.assertTrue(result["executed"])
        self.assertTrue(any("과열 shadow" in m for m in cm.output))

    async def test_insufficient_data_does_not_block_ai_auto_buy(self):
        pool = EgFakePool()  # daily_closes 미설정 → 데이터 없음(기존 동작 유지)
        redis = EgFakeRedis()

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            pass

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
        self.assertTrue(result["executed"])

    async def test_sell_action_is_not_affected(self):
        """매도 경로는 모멘텀 게이트와 무관하다 — 음수 수익률이어도 매도는 그대로 진행."""
        pool = EgFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 97]})
        redis = EgFakeRedis()

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            pass

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
        self.assertTrue(result["executed"])


# ── 3) 수동(채팅)·제안승인 매수는 차단하지 않고 경고만 — handle_trade_command ──
from market.universe import Universe  # noqa: E402
from router.handlers import order_handler  # noqa: E402
from tests.test_order_handler import (  # noqa: E402
    FakeConfig,
    FakePool as OhFakePool,
    FakeRedis as OhFakeRedis,
    make_quote_fn,
)


def _quote_fn(price: int = 70000, name: str = "삼성전자"):
    return make_quote_fn({"output": {"stck_prpr": str(price), "hts_kor_isnm": name}})


async def _ok_positions():
    return {"success": True, "data": []}


async def _ok_warning(symbol):
    return {"mrkt_warn_cls_code": "00", "vi_cls_code": "N"}


class TestHandleTradeCommandMomentumWarning(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        execution_guard.reset_buy_lock()

    def _kwargs(self, *, pool, order_source_tag="chat"):
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})

        async def get_kis_token():
            return "FAKE_TOKEN"

        self.orders = []

        async def kis_order(symbol, price, qty, is_buy):
            self.orders.append((symbol, price, qty, is_buy))
            return {"success": True}

        return dict(
            pool=pool, redis=OhFakeRedis(), universe=universe,
            get_kis_token_fn=get_kis_token, config=FakeConfig(), kis_order_fn=kis_order,
            get_stock_positions_fn=_ok_positions, get_market_warning_fn=_ok_warning,
            get_quote_fn=_quote_fn(), order_source_tag=order_source_tag,
        )

    async def _run(self, msg, *, pool, order_source_tag="chat"):
        async def send_telegram(text, **kw):
            pass

        async def log_journal(*args, **kwargs):
            pass

        kwargs = self._kwargs(pool=pool, order_source_tag=order_source_tag)
        kwargs["send_telegram_fn"] = send_telegram
        kwargs["log_journal_fn"] = log_journal
        return await order_handler.handle_trade_command(msg, **kwargs)

    async def test_manual_chat_buy_not_blocked_but_warned(self):
        pool = OhFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 97]})  # -3%
        reply = await self._run("삼성전자 2주 매수", pool=pool, order_source_tag="chat")
        self.assertIn("매수 완료", reply)       # 차단되지 않음(실제 체결)
        self.assertEqual(len(self.orders), 1)
        self.assertIn("모멘텀 경고", reply)      # 경고 문구는 첨부됨

    async def test_advice_approved_buy_not_blocked_but_warned(self):
        pool = OhFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 97]})  # -3%
        reply = await self._run("삼성전자 2주 매수", pool=pool, order_source_tag="advice_approved")
        self.assertIn("매수 완료", reply)
        self.assertEqual(len(self.orders), 1)
        self.assertIn("모멘텀 경고", reply)

    async def test_positive_momentum_has_no_warning(self):
        pool = OhFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 103]})  # +3%
        reply = await self._run("삼성전자 2주 매수", pool=pool)
        self.assertIn("매수 완료", reply)
        self.assertNotIn("모멘텀 경고", reply)

    async def test_insufficient_data_has_no_warning_and_still_passes(self):
        pool = OhFakePool()  # daily_closes 없음
        reply = await self._run("삼성전자 2주 매수", pool=pool)
        self.assertIn("매수 완료", reply)
        self.assertEqual(len(self.orders), 1)
        self.assertNotIn("모멘텀 경고", reply)

    async def test_sell_is_unaffected(self):
        pool = OhFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 97]})

        async def positions():
            return {"success": True, "data": [{"symbol": "005930", "qty": 5, "avg_price": 60000}]}

        kwargs = self._kwargs(pool=pool)
        kwargs["get_stock_positions_fn"] = positions

        async def send_telegram(text, **kw):
            pass

        async def log_journal(*args, **kwargs):
            pass

        kwargs["send_telegram_fn"] = send_telegram
        kwargs["log_journal_fn"] = log_journal
        reply = await order_handler.handle_trade_command("삼성전자 2주 매도", **kwargs)
        self.assertNotIn("모멘텀 경고", reply)


# ── 4) 사용자가 승인한 제안(proposal) 매수 — handle_proposal_response ──
class TestHandleProposalResponseMomentumWarning(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        execution_guard.reset_buy_lock()

    async def test_proposal_approved_buy_not_blocked_but_warned(self):
        pool = OhFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 97]})  # -3%
        redis = OhFakeRedis({
            "proposal:latest": "005930",
            "proposal:005930": json.dumps(
                {"symbol": "005930", "name": "삼성전자", "price": 70000, "qty": 1}),
        })

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def log_journal(*args, **kwargs):
            pass

        async def send_telegram(text, **kw):
            pass

        async def get_positions():
            return {"success": True, "data": []}

        reply = await order_handler.handle_proposal_response(
            "사자", redis=redis, kis_order_fn=kis_order, log_journal_fn=log_journal,
            send_telegram_fn=send_telegram, get_positions_fn=get_positions, pool=pool,
        )
        self.assertIn("매수 체결", reply)
        self.assertIn("모멘텀 경고", reply)

    async def test_proposal_approved_buy_no_warning_when_momentum_ok(self):
        pool = OhFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 103]})  # +3%
        redis = OhFakeRedis({
            "proposal:latest": "005930",
            "proposal:005930": json.dumps(
                {"symbol": "005930", "name": "삼성전자", "price": 70000, "qty": 1}),
        })

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def log_journal(*args, **kwargs):
            pass

        async def send_telegram(text, **kw):
            pass

        async def get_positions():
            return {"success": True, "data": []}

        reply = await order_handler.handle_proposal_response(
            "사자", redis=redis, kis_order_fn=kis_order, log_journal_fn=log_journal,
            send_telegram_fn=send_telegram, get_positions_fn=get_positions, pool=pool,
        )
        self.assertIn("매수 체결", reply)
        self.assertNotIn("모멘텀 경고", reply)


if __name__ == "__main__":
    unittest.main()
