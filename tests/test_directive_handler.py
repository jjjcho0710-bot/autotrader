"""
router/handlers/directive_handler.py 단위 테스트: 지시 저장/목록/취소와
[[ACTION]] 프로토콜용 충돌 해소 저장(save_directive_with_conflict_resolution)을 검증한다.
"""
import asyncio
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from router.handlers import directive_handler  # noqa: E402


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakeDirectiveConnection:
    def __init__(self, rows=None):
        self.rows = rows or []  # [{"id","content","is_active"}]
        self._next_id = (max((r["id"] for r in self.rows), default=0)) + 1

    async def fetch(self, query, *args):
        assert "jarvis_notes" in query
        return [r for r in self.rows if r.get("is_active", True)]

    async def fetchval(self, query, *args):
        assert "INSERT INTO jarvis_notes" in query
        content = args[0]
        row = {"id": self._next_id, "content": content, "is_active": True}
        self.rows.append(row)
        self._next_id += 1
        return row["id"]

    async def execute(self, query, *args):
        assert "UPDATE jarvis_notes" in query
        row_id = args[-1]  # 두 UPDATE 패턴 모두 마지막 인자가 id
        for r in self.rows:
            if r["id"] == row_id:
                r["is_active"] = False
        return "UPDATE 1"


class FakePool:
    def __init__(self, rows=None):
        self._conn = FakeDirectiveConnection(rows)

    def acquire(self):
        return _AcquireCtx(self._conn)


class FakeRedis:
    def __init__(self):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, value):
        self.store[key] = value


class TestHandleList(unittest.IsolatedAsyncioTestCase):
    async def test_empty_list(self):
        pool = FakePool([])
        reply = await directive_handler.handle("지시 목록", pool, FakeRedis())
        self.assertIn("없습니다", reply)

    async def test_lists_active_directives(self):
        pool = FakePool([{"id": 1, "content": "손절 -5% 이하로 낮추지 마"}])
        reply = await directive_handler.handle("지시사항", pool, FakeRedis())
        self.assertIn("#1", reply)
        self.assertIn("손절 -5%", reply)


class TestHandleCancel(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_deactivates_and_stamps_plan(self):
        pool = FakePool([{"id": 7, "content": "테스트 지시", "is_active": True}])
        redis = FakeRedis()
        reply = await directive_handler.handle("지시 취소 7", pool, redis)
        self.assertIn("#7", reply)
        self.assertFalse(pool._conn.rows[0]["is_active"])
        self.assertIn("jarvis:daily_plan", redis.store)


class TestHandleSave(unittest.IsolatedAsyncioTestCase):
    async def test_saves_new_directive_with_colon_prefix(self):
        pool = FakePool([])
        reply = await directive_handler.handle("지시: 손절 -5% 이하로 낮추지 마", pool, FakeRedis())
        self.assertIn("저장 완료", reply)
        self.assertEqual(len(pool._conn.rows), 1)
        self.assertEqual(pool._conn.rows[0]["content"], "손절 -5% 이하로 낮추지 마")

    async def test_too_short_directive_is_ignored(self):
        pool = FakePool([])
        reply = await directive_handler.handle("지시: 짧음", pool, FakeRedis())
        # "짧음"은 4자 미만이라 저장되지 않고 None(일반 대화로)
        self.assertIsNone(reply)

    async def test_unrelated_message_returns_none(self):
        reply = await directive_handler.handle("오늘 날씨 어때?", FakePool([]), FakeRedis())
        self.assertIsNone(reply)


class TestSaveDirectiveWithConflictResolution(unittest.IsolatedAsyncioTestCase):
    async def test_relax_directive_deactivates_matching_old_directive(self):
        pool = FakePool([{"id": 3, "content": "매수금액 5만원 초과 제안 금지", "is_active": True}])
        redis = FakeRedis()

        result = await directive_handler.save_directive_with_conflict_resolution(
            pool, redis, "5만원 초과 매수 제안도 허용해줘")

        self.assertIsNotNone(result)
        self.assertEqual(result["deactivated"], ["#3"])
        self.assertFalse(pool._conn.rows[0]["is_active"])
        # 새 지시가 추가로 저장됨
        self.assertEqual(len(pool._conn.rows), 2)

    async def test_non_relax_directive_does_not_touch_existing(self):
        pool = FakePool([{"id": 3, "content": "매수금액 5만원 제한", "is_active": True}])
        result = await directive_handler.save_directive_with_conflict_resolution(
            pool, FakeRedis(), "카카오는 신중하게 봐줘")

        self.assertEqual(result["deactivated"], [])
        self.assertTrue(pool._conn.rows[0]["is_active"])

    async def test_short_content_not_saved(self):
        result = await directive_handler.save_directive_with_conflict_resolution(
            FakePool([]), FakeRedis(), "짧음")
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
