"""
learning/repository.py의 get_knowledge_entries/deactivate_knowledge_entry와
router/handlers/knowledge_handler.py 단위 테스트.

"지식 보여줘"가 learning_rules(신규 구조화 규칙)와 jarvis_notes(레거시 메모)를 모두
합쳐 보여주는지, "지식 삭제 N"/"지식 삭제 RN"이 각각 올바른 테이블만 지우는지를
검증한다 — 이 통합이 이번 변경의 핵심(예전엔 jarvis_notes만 보였음).
"""
import asyncio
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from learning.repository import deactivate_knowledge_entry, get_knowledge_entries  # noqa: E402
from router.handlers import knowledge_handler  # noqa: E402


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakeKnowledgeConnection:
    def __init__(self, rules=None, notes=None):
        # rules: [{"rule_id","rule_text","created_at","is_active"}]
        # notes: [{"id","content","created_at","is_active"}]
        self.rules = rules or []
        self.notes = notes or []

    async def fetch(self, query, *args):
        if "learning_rules" in query:
            rows = [r for r in self.rules if r.get("is_active", True)]
        elif "jarvis_notes" in query:
            rows = [n for n in self.notes if n.get("is_active", True) and n.get("category", "knowledge") == "knowledge"]
        else:
            raise AssertionError(f"unexpected query: {query}")
        rows.sort(key=lambda r: r["created_at"], reverse=True)
        return rows

    async def execute(self, query, *args):
        if "learning_rules" in query:
            rule_id = args[0]
            n = 0
            for r in self.rules:
                if r["rule_id"] == rule_id and r.get("is_active", True):
                    r["is_active"] = False
                    n = 1
            return f"UPDATE {n}"
        if "jarvis_notes" in query:
            note_id = args[0]
            n = 0
            for note in self.notes:
                if note["id"] == note_id and note.get("is_active", True):
                    note["is_active"] = False
                    n = 1
            return f"UPDATE {n}"
        raise AssertionError(f"unexpected query: {query}")


class FakePool:
    def __init__(self, rules=None, notes=None):
        self._conn = FakeKnowledgeConnection(rules, notes)

    def acquire(self):
        return _AcquireCtx(self._conn)


class TestGetKnowledgeEntries(unittest.TestCase):
    def test_combines_rules_and_notes_sorted_by_recency(self):
        pool = FakePool(
            rules=[{"rule_id": 1, "rule_text": "손절 후 재진입 금지", "created_at": 2}],
            notes=[{"id": 5, "content": "레거시 메모", "created_at": 1}],
        )
        entries = asyncio.run(get_knowledge_entries(limit=20, pool=pool))
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]["display_id"], "R1")  # created_at=2가 더 최신
        self.assertEqual(entries[1]["display_id"], "#5")

    def test_no_pool_returns_empty(self):
        entries = asyncio.run(get_knowledge_entries(limit=20, pool=None))
        self.assertEqual(entries, [])


class TestDeactivateKnowledgeEntry(unittest.TestCase):
    def test_r_prefix_deactivates_rule_only(self):
        pool = FakePool(
            rules=[{"rule_id": 1, "rule_text": "x", "created_at": 1, "is_active": True}],
            notes=[{"id": 1, "content": "y", "created_at": 1, "is_active": True}],
        )
        ok = asyncio.run(deactivate_knowledge_entry("R1", pool=pool))
        self.assertTrue(ok)
        self.assertFalse(pool._conn.rules[0]["is_active"])
        self.assertTrue(pool._conn.notes[0]["is_active"])  # note는 안 건드림

    def test_plain_number_deactivates_note_only(self):
        pool = FakePool(
            rules=[{"rule_id": 1, "rule_text": "x", "created_at": 1, "is_active": True}],
            notes=[{"id": 1, "content": "y", "created_at": 1, "is_active": True}],
        )
        ok = asyncio.run(deactivate_knowledge_entry("1", pool=pool))
        self.assertTrue(ok)
        self.assertFalse(pool._conn.notes[0]["is_active"])
        self.assertTrue(pool._conn.rules[0]["is_active"])  # rule은 안 건드림

    def test_not_found_returns_false(self):
        pool = FakePool(rules=[], notes=[])
        ok = asyncio.run(deactivate_knowledge_entry("R99", pool=pool))
        self.assertFalse(ok)


class TestKnowledgeHandler(unittest.IsolatedAsyncioTestCase):
    async def test_handle_list_empty(self):
        pool = FakePool(rules=[], notes=[])
        reply = await knowledge_handler.handle_list("학습한 내용 보여줘", pool)
        self.assertIn("아직 학습한 자료가 없어요", reply)

    async def test_handle_list_shows_combined_entries(self):
        pool = FakePool(
            rules=[{"rule_id": 1, "rule_text": "손절 후 재진입 금지", "created_at": 1}],
            notes=[{"id": 5, "content": "레거시 메모", "created_at": 2}],
        )
        reply = await knowledge_handler.handle_list("배운 지식 알려줘", pool)
        self.assertIn("R1", reply)
        self.assertIn("#5", reply)

    async def test_handle_list_ignores_url_messages(self):
        reply = await knowledge_handler.handle_list("https://x.com 이거 배운거 있어?", FakePool())
        self.assertIsNone(reply)

    async def test_handle_delete_by_rule_id(self):
        pool = FakePool(rules=[{"rule_id": 9, "rule_text": "x", "created_at": 1, "is_active": True}])
        reply = await knowledge_handler.handle_delete("지식 삭제 R9", pool)
        self.assertIn("R9", reply)
        self.assertFalse(pool._conn.rules[0]["is_active"])

    async def test_handle_delete_not_found_returns_none(self):
        reply = await knowledge_handler.handle_delete("지식 삭제 999", FakePool())
        self.assertIsNone(reply)

    async def test_handle_delete_no_match_returns_none(self):
        reply = await knowledge_handler.handle_delete("오늘 날씨 어때", FakePool())
        self.assertIsNone(reply)


if __name__ == "__main__":
    unittest.main()
