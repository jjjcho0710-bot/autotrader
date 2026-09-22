"""
router/handlers/watchlist_handler.py 단위 테스트: 감시종목 조회/추가/삭제 명령 처리와
add_symbol(priority 포함) upsert 헬퍼를 검증한다.
"""
import asyncio
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from market.universe import Universe  # noqa: E402
from router.handlers import watchlist_handler  # noqa: E402


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakeWatchlistConnection:
    def __init__(self, rows=None):
        self.rows = rows or []  # [{"symbol","name","is_active","priority"}]
        self.executed = []

    async def fetch(self, query, *args):
        assert "watchlist" in query
        return [r for r in self.rows if r.get("is_active", True)]

    async def execute(self, query, *args):
        self.executed.append((query, args))
        if "INSERT INTO watchlist" in query and "priority" in query:
            symbol, name = args[0], args[1]
            self._upsert(symbol, name, priority=True)
        elif "INSERT INTO watchlist" in query and "added_by" in query and "reason" in query and len(args) >= 3:
            # 두 가지 형태: (symbol, name, added_by포함) 또는 (code, added_by, msg)
            if "name" in query and "$2" in query and len(args) == 3:
                symbol, name, _msg = args
                self._upsert(symbol, name)
            else:
                symbol, _msg = args[0], args[1]
                self._upsert(symbol, None)
        elif "INSERT INTO watchlist" in query:
            symbol, name = args[0], args[1]
            self._upsert(symbol, name)
        elif "UPDATE watchlist SET is_active=FALSE" in query:
            symbol = args[0]
            for r in self.rows:
                if r["symbol"] == symbol:
                    r["is_active"] = False
        return "OK"

    def _upsert(self, symbol, name, priority=False):
        for r in self.rows:
            if r["symbol"] == symbol:
                r["is_active"] = True
                r["name"] = name or r.get("name")
                if priority:
                    r["priority"] = True
                return
        self.rows.append({"symbol": symbol, "name": name, "is_active": True, "priority": priority})


class FakePool:
    def __init__(self, rows=None):
        self._conn = FakeWatchlistConnection(rows)

    def acquire(self):
        return _AcquireCtx(self._conn)


class TestHandleCommand(unittest.IsolatedAsyncioTestCase):
    async def test_list_empty(self):
        pool = FakePool([])
        universe = Universe(None)
        reply = await watchlist_handler.handle_command("감시 종목 보여줘", pool, universe, {})
        self.assertIn("없어요", reply)

    async def test_list_shows_active_rows(self):
        pool = FakePool([{"symbol": "005930", "name": "삼성전자", "is_active": True}])
        universe = Universe(None)
        reply = await watchlist_handler.handle_command("감시 종목 목록", pool, universe, {})
        self.assertIn("005930", reply)
        self.assertIn("삼성전자", reply)

    async def test_add_via_stock_name_map(self):
        pool = FakePool([])
        universe = Universe(None)
        stock_name_map = {"삼성전자": ("005930", "삼성전자")}
        reply = await watchlist_handler.handle_command("삼성전자 감시 종목 추가해줘", pool, universe, stock_name_map)
        self.assertIn("추가했어요", reply)
        self.assertEqual(pool._conn.rows[0]["symbol"], "005930")

    async def test_add_already_existing_reports_duplicate(self):
        pool = FakePool([{"symbol": "005930", "name": "삼성전자", "is_active": True}])
        universe = Universe(None)
        stock_name_map = {"삼성전자": ("005930", "삼성전자")}
        reply = await watchlist_handler.handle_command("삼성전자 감시 종목 추가해줘", pool, universe, stock_name_map)
        self.assertIn("이미 감시 종목", reply)

    async def test_add_via_six_digit_code(self):
        pool = FakePool([])
        universe = Universe(None)
        reply = await watchlist_handler.handle_command("000660 감시 종목 추가해줘", pool, universe, {})
        self.assertIn("000660", reply)
        self.assertEqual(pool._conn.rows[0]["symbol"], "000660")

    async def test_remove_via_stock_name_map(self):
        pool = FakePool([{"symbol": "005930", "name": "삼성전자", "is_active": True}])
        universe = Universe(None)
        stock_name_map = {"삼성전자": ("005930", "삼성전자")}
        reply = await watchlist_handler.handle_command("삼성전자 감시 종목 제거해줘", pool, universe, stock_name_map)
        self.assertIn("제거했어요", reply)
        self.assertFalse(pool._conn.rows[0]["is_active"])

    async def test_remove_no_match_returns_guidance(self):
        pool = FakePool([])
        universe = Universe(None)
        reply = await watchlist_handler.handle_command("동원산업 감시 종목 제거해줘", pool, universe, {})
        self.assertIn("종목명을 찾지 못했어요", reply)

    async def test_unrelated_message_returns_none(self):
        reply = await watchlist_handler.handle_command("오늘 날씨 어때", FakePool([]), Universe(None), {})
        self.assertIsNone(reply)


class TestHandleQuickAdd(unittest.IsolatedAsyncioTestCase):
    async def test_adds_when_symbol_resolves(self):
        pool = FakePool([])
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})

        async def web_research(q, name=""):
            return "웹 조사 결과"

        reply = await watchlist_handler.handle_quick_add("삼성전자 감시 추가해줘", pool, universe, web_research)
        self.assertIn("감시종목에 추가했어요", reply)
        self.assertEqual(pool._conn.rows[0]["symbol"], "005930")

    async def test_falls_back_to_web_research_when_unresolved(self):
        pool = FakePool([])
        universe = Universe(None)

        async def web_research(q, name=""):
            return f"웹 조사: {q}"

        reply = await watchlist_handler.handle_quick_add("동원산업 감시 추가해줘", pool, universe, web_research)
        self.assertIn("웹 조사: 동원산업", reply)
        self.assertIn("정식 종목명으로 다시 시도", reply)
        self.assertEqual(pool._conn.rows, [])

    async def test_url_message_is_ignored(self):
        reply = await watchlist_handler.handle_quick_add(
            "https://x.com 감시 추가해줘", FakePool([]), Universe(None), None)
        self.assertIsNone(reply)


class TestAddSymbol(unittest.IsolatedAsyncioTestCase):
    async def test_priority_flag_sets_priority_column(self):
        pool = FakePool([])
        await watchlist_handler.add_symbol(pool, "323410", "카카오뱅크", priority=True)
        self.assertTrue(pool._conn.rows[0]["priority"])


if __name__ == "__main__":
    unittest.main()
