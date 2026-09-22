"""
stark/decision_engine.py 단위 테스트.

핵심 검증 포인트(STARK_PLAN 4번·6번 원칙):
1. EXECUTE/EXECUTE_SMALL/SKIP/PROPOSE 판정 파싱이 원본과 동일하게 동작하는가.
2. PROPOSE는 소액 자동승격되는가.
3. AI 실패 시 ML 확률 폴백이 원본의 (의도가 불분명하지만) 소문자 "buy"만 인식하는
   동작을 그대로 보존하는가.
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

    async def test_ai_failure_falls_back_to_ml_probability_lowercase_buy(self):
        pool = FakePool()

        async def ask_llm(prompt, session_id):
            return ""  # 빈 응답 = AI 실패로 간주

        decision = await decision_engine.decide(
            make_signal(action="buy", reason="ML매수확률: 80%"), "prompt", ask_llm_fn=ask_llm, pool=pool)
        self.assertTrue(decision["ai_failed"])
        self.assertTrue(decision["should_execute"])
        self.assertEqual(decision["source"], "rule")

    async def test_ai_failure_uppercase_buy_never_executes_legacy_quirk_preserved(self):
        """원본 코드는 ML 폴백에서 action=='buy'(소문자)만 확인했다 — 'BUY'(대문자)는
        확률이 아무리 높아도 실행되지 않는 게 원본 그대로의 동작이다."""
        pool = FakePool()

        async def ask_llm(prompt, session_id):
            return ""

        decision = await decision_engine.decide(
            make_signal(action="BUY", reason="ML매수확률: 95%"), "prompt", ask_llm_fn=ask_llm, pool=pool)
        self.assertTrue(decision["ai_failed"])
        self.assertFalse(decision["should_execute"])

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


if __name__ == "__main__":
    unittest.main()
