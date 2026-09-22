"""router/handlers/chart_handler.py 단위 테스트."""
import asyncio
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from market.universe import Universe  # noqa: E402
from router.handlers import chart_handler  # noqa: E402


class TestChartHandler(unittest.IsolatedAsyncioTestCase):
    async def test_no_chart_keyword_returns_none(self):
        reply = await chart_handler.handle("삼성전자 얼마야", Universe(None), None)
        self.assertIsNone(reply)

    async def test_symbol_not_resolved_returns_none(self):
        async def analyze_chart(symbol, name):
            raise AssertionError("호출되면 안 됨")

        reply = await chart_handler.handle("듣보잡회사 차트 보여줘", Universe(None), analyze_chart)
        self.assertIsNone(reply)

    async def test_resolved_symbol_with_data_returns_chart_text(self):
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})

        async def analyze_chart(symbol, name):
            return f"[차트 리서치] {name}"

        reply = await chart_handler.handle("삼성전자 차트 어때", universe, analyze_chart)
        self.assertEqual(reply, "[차트 리서치] 삼성전자")

    async def test_resolved_symbol_without_data_returns_warning(self):
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})

        async def analyze_chart(symbol, name):
            return ""

        reply = await chart_handler.handle("삼성전자 차트", universe, analyze_chart)
        self.assertIn("가져오지 못했어요", reply)


if __name__ == "__main__":
    unittest.main()
