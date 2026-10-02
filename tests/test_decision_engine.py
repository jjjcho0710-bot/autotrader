"""
stark/decision_engine.py 단위 테스트.

핵심 검증 포인트(STARK_PLAN 4번·6번 원칙):
1. EXECUTE/EXECUTE_SMALL/SKIP/PROPOSE 판정 파싱이 원본과 동일하게 동작하는가.
2. PROPOSE는 소액 자동승격되는가.
3. AI 실패 시 ML 확률로 매수를 시도하던 폴백이 제거되어, reason에 ML 확률이 있어도
   무조건 보수적 SKIP으로 처리되는가(PM 승인, 2026-10-02 — ML 확률 기반 폴백 매수 제거).
4. 판단마다(SKIP 포함) stark_decisions에 반드시 기록되는가 — 이게 없으면
   "0건인 날"의 원인을 사후에 알 수 없다는 게 STARK_PLAN의 핵심 불만이었다.
5. decide()는 절대 주문을 실행하지 않는다(판단과 실행의 물리적 분리).
"""
import asyncio
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from stark import decision_engine  # noqa: E402


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakeDecisionConnection:
    def __init__(self):
        self.inserted = []

    async def fetchval(self, query, *args):
        assert "INSERT INTO stark_decisions" in query
        self.inserted.append(args)
        return len(self.inserted)


class FakePool:
    def __init__(self):
        self._conn = FakeDecisionConnection()

    def acquire(self):
        return _AcquireCtx(self._conn)


def make_signal(**overrides):
    base = {"symbol": "005930", "name": "삼성전자", "action": "buy",
            "price": 70000, "qty": 2, "strategy": "MA크로스", "reason": "골든크로스"}
    base.update(overrides)
    return base


class TestDecide(unittest.IsolatedAsyncioTestCase):
    async def test_execute_verdict_never_calls_order_execution(self):
        """decide()는 판단 결과 dict만 반환해야 하며, kis_order 등 실행 관련 인자를
        아예 받지 않는다는 사실 자체가 '판단과 실행의 물리적 분리'를 보장한다."""
        pool = FakePool()

        async def ask_llm(prompt, session_id):
            return "EXECUTE: 조건 대부분 충족"

        decision = await decision_engine.decide(make_signal(), "prompt", ask_llm_fn=ask_llm, pool=pool)
        self.assertEqual(decision["verdict"], "EXECUTE")
        self.assertTrue(decision["should_execute"])
        self.assertFalse(decision["is_small"])
        self.assertFalse(decision["ai_failed"])

    async def test_execute_small_verdict(self):
        pool = FakePool()

        async def ask_llm(prompt, session_id):
            return "EXECUTE_SMALL: 일부 조건 충족"

        decision = await decision_engine.decide(make_signal(), "prompt", ask_llm_fn=ask_llm, pool=pool)
        self.assertTrue(decision["should_execute"])
        self.assertTrue(decision["is_small"])

    async def test_skip_verdict_does_not_execute(self):
        pool = FakePool()

        async def ask_llm(prompt, session_id):
            return "SKIP: 근거 부족"

        decision = await decision_engine.decide(make_signal(), "prompt", ask_llm_fn=ask_llm, pool=pool)
        self.assertFalse(decision["should_execute"])
        self.assertEqual(pool._conn.inserted[0][2], "SKIP")  # (pool,symbol,decision,...)의 decision 위치

    async def test_propose_auto_upgrades_to_small_execute_for_buy(self):
        pool = FakePool()

        async def ask_llm(prompt, session_id):
            return "PROPOSE: 규칙 밖 강신호"

        decision = await decision_engine.decide(make_signal(action="buy"), "prompt", ask_llm_fn=ask_llm, pool=pool)
        self.assertTrue(decision["should_execute"])
        self.assertTrue(decision["is_small"])
        self.assertIn("자동 실행", decision["reply"])

    async def test_propose_does_not_upgrade_for_sell(self):
        pool = FakePool()

        async def ask_llm(prompt, session_id):
            return "PROPOSE: 규칙 밖 강신호"

        decision = await decision_engine.decide(make_signal(action="sell"), "prompt", ask_llm_fn=ask_llm, pool=pool)
        self.assertFalse(decision["should_execute"])

    async def test_ai_failure_never_executes_even_with_high_ml_probability_in_reason(self):
        """ML 확률 폴백 매수는 제거됐다(PM 승인, 2026-10-02 — buy_prob 90~100% 구간조차
        적중률 43.9%/평균수익 -0.51%로 확률이 높을수록 잘 맞는 관계가 없음이 확인됨).
        AI 응답 실패 시 reason에 ML 확률이 아무리 높게 적혀 있어도 무조건 SKIP이다."""
        pool = FakePool()

        async def ask_llm(prompt, session_id):
            return ""  # 빈 응답 = AI 실패로 간주

        decision = await decision_engine.decide(
            make_signal(action="buy", reason="ML매수확률: 95%"), "prompt", ask_llm_fn=ask_llm, pool=pool)
        self.assertTrue(decision["ai_failed"])
        self.assertFalse(decision["should_execute"])
        self.assertEqual(decision["source"], "rule")
        self.assertIn("보수적 SKIP", decision["reply"])

    async def test_ai_failure_without_ml_probability_defaults_to_skip(self):
        pool = FakePool()

        async def ask_llm(prompt, session_id):
            return "❌ 오류"

        decision = await decision_engine.decide(make_signal(reason="사유 없음"), "prompt", ask_llm_fn=ask_llm, pool=pool)
        self.assertTrue(decision["ai_failed"])
        self.assertFalse(decision["should_execute"])

    async def test_every_decision_outcome_is_logged_to_stark_decisions(self):
        """SKIP·EXECUTE·PROPOSE·AI폴백 어떤 경우든 stark_decisions에 기록되어야 한다
        (원본 jarvis_signal에는 이 호출 자체가 아예 없었음 — STARK v2에서 새로 연결)."""
        pool = FakePool()

        async def ask_llm(prompt, session_id):
            return "SKIP: 근거 부족"

        await decision_engine.decide(make_signal(), "prompt", ask_llm_fn=ask_llm, pool=pool)
        self.assertEqual(len(pool._conn.inserted), 1)
        symbol, decision_label = pool._conn.inserted[0][0], pool._conn.inserted[0][2]
        self.assertEqual(symbol, "005930")
        self.assertEqual(decision_label, "SKIP")


class TestConfidence(unittest.IsolatedAsyncioTestCase):
    """stark_decisions.confidence가 NULL로 비는 문제 보강: 판정별 확신도를 채워
    log_decision에 실제로 전달되는지, 반환 dict에도 노출되는지 검증한다."""

    async def _decide(self, reply_text, **signal_overrides):
        pool = FakePool()

        async def ask_llm(prompt, session_id):
            return reply_text

        decision = await decision_engine.decide(make_signal(**signal_overrides), "prompt", ask_llm_fn=ask_llm, pool=pool)
        # log_decision 호출 인자 순서: symbol, name, decision, confidence, reason, ...
        logged_confidence = pool._conn.inserted[0][3]
        return decision, logged_confidence

    async def test_execute_confidence_is_high(self):
        decision, logged = await self._decide("EXECUTE: 강한 확신")
        self.assertEqual(decision["confidence"], 0.85)
        self.assertEqual(logged, 0.85)

    async def test_execute_small_confidence_is_moderate(self):
        decision, logged = await self._decide("EXECUTE_SMALL: 일부 조건")
        self.assertEqual(decision["confidence"], 0.65)
        self.assertEqual(logged, 0.65)

    async def test_propose_auto_upgrade_confidence_differs_from_execute_small(self):
        """PROPOSE는 stark_decisions에는 EXECUTE_SMALL로 기록되지만(is_small=True),
        규칙 밖 강신호라는 출처를 확신도에 남기기 위해 일반 EXECUTE_SMALL(0.65)과는
        다른 PROPOSE 전용 확신도(0.70)를 써야 한다 — 라벨만 보고는 구분이 안 되므로."""
        decision, logged = await self._decide("PROPOSE: 규칙 밖 강신호", action="buy")
        self.assertEqual(decision["confidence"], 0.70)
        self.assertEqual(logged, 0.70)

    async def test_skip_confidence_is_low(self):
        decision, logged = await self._decide("SKIP: 근거 부족")
        self.assertEqual(decision["confidence"], 0.30)
        self.assertEqual(logged, 0.30)

    async def test_ai_failure_confidence_uses_low_skip_heuristic_regardless_of_reason(self):
        """ML 확률 폴백이 제거됐으므로 AI 실패 시 confidence는 reason 내용과 무관하게
        SKIP과 동일한 낮은 고정값(0.30)이다."""
        pool = FakePool()

        async def ask_llm(prompt, session_id):
            return ""

        decision = await decision_engine.decide(
            make_signal(action="buy", reason="ML매수확률: 82%"), "prompt", ask_llm_fn=ask_llm, pool=pool)
        self.assertEqual(decision["confidence"], 0.30)
        self.assertEqual(pool._conn.inserted[0][3], 0.30)

    async def test_ai_failure_without_reason_confidence_is_also_low_skip_heuristic(self):
        pool = FakePool()

        async def ask_llm(prompt, session_id):
            return "❌ 오류"

        decision = await decision_engine.decide(
            make_signal(reason="사유 없음"), "prompt", ask_llm_fn=ask_llm, pool=pool)
        self.assertEqual(decision["confidence"], 0.30)


if __name__ == "__main__":
    unittest.main()
