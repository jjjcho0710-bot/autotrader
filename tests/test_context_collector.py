"""
stark/context_collector.py 단위 테스트.

build_analysis_prompt는 순수 함수라 조립 결과 문자열을 바로 검증하고, collect()는
tests/test_decision_logger.py와 같은 FakePool/FakeConnection으로 trade_journal 조회를
검증한다.
"""
import asyncio
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from stark import context_collector  # noqa: E402


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakeJournalConnection:
    def __init__(self, hist_rows):
        self.hist_rows = hist_rows

    async def fetch(self, query, *args):
        assert "trade_journal" in query
        return list(self.hist_rows)


class FakePool:
    def __init__(self, hist_rows=None):
        self._conn = FakeJournalConnection(hist_rows or [])

    def acquire(self):
        return _AcquireCtx(self._conn)


class FakeRedis:
    def __init__(self, daily_plan=None):
        self.daily_plan = daily_plan

    async def get(self, key):
        assert key == "jarvis:daily_plan"
        return self.daily_plan


class TestBuildAnalysisPrompt(unittest.TestCase):
    def test_includes_core_signal_fields(self):
        signal = {"symbol": "005930", "name": "삼성전자", "action": "buy",
                   "price": 70000, "qty": 2, "strategy": "MA크로스", "reason": "골든크로스"}
        prompt = context_collector.build_analysis_prompt(signal, {})
        self.assertIn("삼성전자(005930)", prompt)
        self.assertIn("방향: 매수", prompt)
        self.assertIn("현재가: 70,000원", prompt)
        self.assertIn("매수금액: 140,000원", prompt)
        self.assertIn("골든크로스", prompt)
        self.assertIn("EXECUTE_SMALL", prompt)  # 판정 안내 문구 포함 확인

    def test_missing_context_fields_use_placeholders(self):
        signal = {"symbol": "000660", "action": "sell", "price": 100, "qty": 1}
        prompt = context_collector.build_analysis_prompt(signal, {})
        self.assertIn("(작전 없음", prompt)
        self.assertIn("[주인 지시사항", prompt)
        self.assertIn("(없음)", prompt)


class TestCollect(unittest.IsolatedAsyncioTestCase):
    async def test_collect_computes_self_history_drift_and_warns_on_repeated_skip(self):
        pool = FakePool(hist_rows=[
            {"jarvis_decision": "SKIP", "price": 100.0},
            {"jarvis_decision": "SKIP", "price": 100.0},
        ])
        redis = FakeRedis(daily_plan="오늘은 신중하게")

        async def get_portfolio_context():
            return "[포트폴리오 요약]"

        async def get_active_directives():
            return "- 지시 없음"

        async def get_jarvis_lessons(n):
            return "교훈 없음"

        async def get_jarvis_knowledge(n):
            return "지식 없음"

        async def analyze_chart(symbol, name):
            return "[차트]"

        signal = {"symbol": "005930", "name": "삼성전자", "bot": "stock_trader", "price": 105.0}
        ctx = await context_collector.collect(
            signal, pool=pool, redis=redis,
            get_portfolio_context=get_portfolio_context,
            get_active_directives=get_active_directives,
            get_jarvis_lessons=get_jarvis_lessons,
            get_jarvis_knowledge=get_jarvis_knowledge,
            analyze_chart=analyze_chart,
        )
        self.assertEqual(ctx["daily_plan"], "오늘은 신중하게")
        self.assertIn("SKIP 2회", ctx["self_history"])
        self.assertIn("추세가 확인되면", ctx["self_history"])  # drift>=1.0 & skip>=2 경고 문구

    async def test_optional_dependency_failures_degrade_to_empty_string(self):
        """lessons/knowledge/chart_ctx는 원본처럼 try/except로 감싸여 있어 실패해도
        전체 컨텍스트 수집이 죽지 않고 빈 문자열로 대체되어야 한다."""
        pool = FakePool(hist_rows=[])

        async def boom(*a, **kw):
            raise RuntimeError("dependency down")

        async def get_portfolio_context():
            return "[포트폴리오]"

        async def get_active_directives():
            return "- 지시 없음"

        signal = {"symbol": "005930", "price": 0}
        ctx = await context_collector.collect(
            signal, pool=pool, redis=FakeRedis(),
            get_portfolio_context=get_portfolio_context,
            get_active_directives=get_active_directives,
            get_jarvis_lessons=boom,
            get_jarvis_knowledge=boom,
            analyze_chart=boom,
        )
        self.assertEqual(ctx["lessons_txt"], "")
        self.assertEqual(ctx["knowledge_txt"], "")
        self.assertEqual(ctx["chart_ctx"], "")
        self.assertEqual(ctx["portfolio_ctx"], "[포트폴리오]")


if __name__ == "__main__":
    unittest.main()
