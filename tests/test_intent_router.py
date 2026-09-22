"""
router/intent_router.py 단위 테스트: route_early/route_late가 각 handler를 원본과
동일한 우선순위로 호출하는지, 아무 handler도 안 걸리면 None(Tier 2 AI 분류로)을
반환하는지 검증한다.
"""
import asyncio
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from market.universe import Universe  # noqa: E402
from router import intent_router  # noqa: E402


def make_ctx(**overrides):
    defaults = dict(
        pool=None, redis=None, universe=Universe(None), config=None,
        stock_name_map={}, session_id="advice",
        send_telegram=None, get_kis_token=None, kis_order=None,
        analyze_chart=None, get_stock_positions=None, log_journal=None,
        web_research=None, jarvis_chat=None, channel="web",
    )
    defaults.update(overrides)
    return intent_router.RouterContext(**defaults)


class TestRouteEarly(unittest.IsolatedAsyncioTestCase):
    async def test_directive_wins_over_setting_for_directive_message(self):
        # "지시:" 접두는 directive_handler 전담 — setting_handler와 겹칠 여지 없음
        ctx = make_ctx(pool=_FakePool(), redis=_FakeRedis())
        reply = await intent_router.route_early("지시: 손절 -5% 이하로 낮추지 마", ctx)
        self.assertIn("지시", reply)
        self.assertIn("저장 완료", reply)

    async def test_no_pattern_matches_returns_none(self):
        ctx = make_ctx(pool=_FakePool(), redis=_FakeRedis())
        reply = await intent_router.route_early("오늘 날씨 어때?", ctx)
        self.assertIsNone(reply)


class TestRouteLate(unittest.IsolatedAsyncioTestCase):
    async def test_chart_keyword_short_circuits_before_advice_check(self):
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})

        async def analyze_chart(symbol, name):
            return f"[차트] {name}"

        ctx = make_ctx(universe=universe, analyze_chart=analyze_chart, redis=_FakeRedis())
        reply = await intent_router.route_late("삼성전자 차트 어때", ctx)
        self.assertEqual(reply, "[차트] 삼성전자")

    async def test_no_pattern_matches_returns_none(self):
        ctx = make_ctx(redis=_FakeRedis())
        reply = await intent_router.route_late("오늘 날씨 어때?", ctx)
        self.assertIsNone(reply)


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self):
        self.rows = []
        self._next_id = 1

    async def fetch(self, query, *args):
        return list(self.rows)

    async def fetchval(self, query, *args):
        row = {"id": self._next_id, "content": args[0], "is_active": True}
        self.rows.append(row)
        self._next_id += 1
        return row["id"]

    async def execute(self, query, *args):
        return "UPDATE 1"


class _FakePool:
    def __init__(self):
        self._conn = _FakeConn()

    def acquire(self):
        return _AcquireCtx(self._conn)


class _FakeRedis:
    def __init__(self):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, value):
        self.store[key] = value


if __name__ == "__main__":
    unittest.main()
