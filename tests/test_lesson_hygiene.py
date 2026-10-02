"""
[AT] fix/lesson-hygiene 테스트.

배경: dashboard/main.py _get_jarvis_lessons(category='lesson' 최신순 조회)가 is_active를
보지 않아, PM이 편향 유도 교훈 10건을 is_active=FALSE로 꺼도 최신 3개가 계속 매수 판단·
오늘의 작전·능동제안·주간예습·매도판단 프롬프트에 들어갔다(10/2). 또 야간 복기·주간 복습이
AI가 쓴 문장을 매일 jarvis_notes(lesson)에 자동 저장해 같은 편향이 재생성됐다.

검증 항목:
(a)(b) _get_jarvis_lessons/_training_summary_raw가 is_active=FALSE 행을 제외하고
       is_active=TRUE만 최신순·limit개로 반환하는지.
(c) LESSON_AUTOSAVE_ENABLED(기본 False)이면 야간 복기·주간 복습이 jarvis_notes에
    INSERT하지 않고(텔레그램 발송은 유지), True면 기존대로 저장하는지.
(d) 가장 중요 — 실제 판단 경로(stark/context_collector.collect)에 dashboard의 진짜
    _get_jarvis_lessons를 그대로 주입해, 비활성 교훈이 프롬프트 컨텍스트(lessons_txt)에
    섞이지 않는지 end-to-end로 검증한다. _get_jarvis_lessons의 is_active=TRUE 조건을
    지우고 재실행하면 이 테스트가 실패함을 직접 확인했다(아래 docstring 참고).

픽스처는 실제 jarvis_notes 테이블 모양(id, category, content, is_active, created_at,
migrations/V001__baseline_tables.sql 183행)을 흉내낸다.
"""
import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

import dashboard.main as dm  # noqa: E402
from stark import context_collector  # noqa: E402

KST = timezone(timedelta(hours=9))
NOW = datetime(2026, 10, 2, 12, 0, tzinfo=KST)

# 10/2 PM이 is_active=FALSE로 끈 공격 유도 교훈(94,91 등)과 같은 취지의 샘플 +
# 살아있는 정상 교훈 샘플. id는 실제 사고의 id 일부를 그대로 반영했다.
LESSON_ROWS = [
    {"id": 94, "category": "lesson", "is_active": False, "created_at": NOW - timedelta(hours=1),
     "content": "교훈: 반복 SKIP 후 1% 오르면 과감히 진입하라"},
    {"id": 91, "category": "lesson", "is_active": False, "created_at": NOW - timedelta(hours=2),
     "content": "교훈: 기회 놓침을 최소화하라"},
    {"id": 80, "category": "lesson", "is_active": True, "created_at": NOW - timedelta(hours=3),
     "content": "교훈: 손절 기준을 지켜라"},
    {"id": 70, "category": "lesson", "is_active": True, "created_at": NOW - timedelta(hours=4),
     "content": "교훈: 거래량 급증 종목을 주의하라"},
    {"id": 60, "category": "lesson", "is_active": True, "created_at": NOW - timedelta(hours=5),
     "content": "교훈: 분산 투자를 유지하라"},
]


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


class FakeLessonConnection:
    """jarvis_notes(category='lesson') 테스트 더블 — 쿼리 문자열에서 WHERE 절을 파싱해
    흉내낸다. is_active=TRUE 조건이 쿼리에서 빠지면(회귀) 비활성 행도 그대로 섞여 나온다
    — 그래야 "쿼리를 실제로 고쳤는지"를 증명하는 테스트가 된다(값만 미리 필터링해 돌려주는
    목이면 코드가 안 고쳐져도 통과해버린다)."""

    def __init__(self, rows):
        self._rows = rows

    async def fetch(self, query, *args):
        q = " ".join(query.split())
        if "jarvis_notes" not in q or "category='lesson'" not in q:
            return []  # 같은 커넥션으로 묶여 호출되는 다른 쿼리(예: 일별 채점 집계)
        rows = list(self._rows)
        if "is_active=TRUE" in q:
            rows = [r for r in rows if r["is_active"]]
        rows = sorted(rows, key=lambda r: r["created_at"], reverse=True)
        if args and isinstance(args[-1], int):
            rows = rows[:args[-1]]
        elif "LIMIT 6" in q:
            rows = rows[:6]
        elif "LIMIT 20" in q:
            rows = rows[:20]
        if "SELECT content FROM" in q:
            return [{"content": r["content"]} for r in rows]
        return [{"content": r["content"], "created_at": r["created_at"]} for r in rows]


class TestGetJarvisLessonsFiltersInactive(unittest.IsolatedAsyncioTestCase):
    async def test_inactive_lessons_excluded(self):
        """(a) is_active=FALSE인 교훈(94, 91)은 결과에 전혀 섞이지 않아야 한다."""
        with patch.object(dm, "db_pool", FakePool(FakeLessonConnection(LESSON_ROWS))):
            result = await dm._get_jarvis_lessons(limit=5)
        self.assertNotIn("반복 SKIP 후 1% 오르면", result)
        self.assertNotIn("기회 놓침을 최소화하라", result)

    async def test_active_lessons_returned_newest_first_within_limit(self):
        """(b) 활성 교훈만, 최신순으로, limit개까지 반환해야 한다."""
        with patch.object(dm, "db_pool", FakePool(FakeLessonConnection(LESSON_ROWS))):
            result = await dm._get_jarvis_lessons(limit=2)
        lines = [ln for ln in result.split("\n") if ln.strip()]
        self.assertEqual(len(lines), 2)
        self.assertIn("손절 기준을 지켜라", lines[0])
        self.assertIn("거래량 급증 종목을 주의하라", lines[1])


class TestTrainingSummaryFiltersInactive(unittest.IsolatedAsyncioTestCase):
    async def test_training_summary_excludes_inactive_lessons(self):
        """(a)(b) 트레이닝 페이지용 _training_summary_raw도 동일하게 is_active=TRUE만."""
        with patch.object(dm, "db_pool", FakePool(FakeLessonConnection(LESSON_ROWS))):
            res = await dm._training_summary_raw(days=14)
        contents = [item["content"] for item in res["lessons"]]
        self.assertNotIn("교훈: 반복 SKIP 후 1% 오르면 과감히 진입하라", contents)
        self.assertNotIn("교훈: 기회 놓침을 최소화하라", contents)
        self.assertEqual(len(contents), 3)  # 활성 3건만


class _EveningReviewConn:
    def __init__(self):
        self.executed = []

    async def fetch(self, query, *args):
        if "trade_journal" in query:
            return [{"symbol": "005930", "name": "삼성전자",
                      "jarvis_decision": "SKIP", "eval_pnl_rate": 1.2}]
        return []  # trade_history — 체결은 없어도 판단 기록만으로 복기 진행

    async def execute(self, query, *args):
        self.executed.append((query, args))


class _WeeklyReviewConn:
    def __init__(self):
        self.executed = []

    async def fetch(self, query, *args):
        if "trade_journal" in query:
            return [{"symbol": "005930", "name": "삼성전자", "jarvis_decision": "SKIP",
                      "eval_pnl_rate": 1.2, "strategy": "MA크로스", "d": date(2026, 9, 28)}]
        return []  # trade_history

    async def execute(self, query, *args):
        self.executed.append((query, args))


class _NoopRedis:
    async def get(self, key):
        return None


class TestLessonAutosaveGate(unittest.IsolatedAsyncioTestCase):
    """(c) LESSON_AUTOSAVE_ENABLED(기본 False)이면 야간 복기·주간 복습이 jarvis_notes에
    저장하지 않고, 텔레그램 발송은 그대로 유지돼야 한다."""

    async def test_evening_review_skips_db_insert_when_autosave_disabled(self):
        self.assertFalse(dm.LESSON_AUTOSAVE_ENABLED, "기본값은 False여야 한다")
        conn = _EveningReviewConn()
        sent = []

        async def fake_send_telegram(text, **kw):
            sent.append(text)

        with patch.object(dm, "db_pool", FakePool(conn)), \
             patch.object(dm, "redis_client", _NoopRedis()), \
             patch.object(dm, "_ask_openwebui", new=AsyncMock(return_value="교훈: 다음엔 신중하게")), \
             patch.object(dm, "_send_telegram", new=fake_send_telegram):
            await dm._jarvis_evening_review()

        self.assertEqual(conn.executed, [], "자동저장이 꺼져 있으면 INSERT가 전혀 없어야 한다")
        self.assertEqual(len(sent), 1, "DB 저장과 무관하게 복기 텔레그램은 그대로 가야 한다")
        self.assertIn("복기", sent[0])

    async def test_evening_review_saves_when_autosave_enabled(self):
        conn = _EveningReviewConn()
        sent = []

        async def fake_send_telegram(text, **kw):
            sent.append(text)

        with patch.object(dm, "LESSON_AUTOSAVE_ENABLED", True), \
             patch.object(dm, "db_pool", FakePool(conn)), \
             patch.object(dm, "redis_client", _NoopRedis()), \
             patch.object(dm, "_ask_openwebui", new=AsyncMock(return_value="교훈: 다음엔 신중하게")), \
             patch.object(dm, "_send_telegram", new=fake_send_telegram):
            await dm._jarvis_evening_review()

        self.assertEqual(len(conn.executed), 1, "켜져 있으면 기존대로 1건 저장해야 한다")
        self.assertIn("INSERT INTO jarvis_notes", conn.executed[0][0])
        self.assertEqual(len(sent), 1)

    async def test_weekly_review_skips_db_insert_when_autosave_disabled(self):
        conn = _WeeklyReviewConn()
        sent = []

        async def fake_send_telegram(text, **kw):
            sent.append(text)

        review_text = "주간 교훈: 반복 매도는 피하라\n주간 교훈: 손절을 지켜라"
        with patch.object(dm, "db_pool", FakePool(conn)), \
             patch.object(dm, "_ask_openwebui", new=AsyncMock(return_value=review_text)), \
             patch.object(dm, "_send_telegram", new=fake_send_telegram):
            await dm._jarvis_weekly_review()

        self.assertEqual(conn.executed, [], "자동저장이 꺼져 있으면 INSERT가 전혀 없어야 한다")
        self.assertEqual(len(sent), 1, "DB 저장과 무관하게 주간 복습 텔레그램은 그대로 가야 한다")
        self.assertIn("교훈 0건 저장", sent[0])  # 저장이 없었음을 정확히 보고

    async def test_weekly_review_saves_when_autosave_enabled(self):
        conn = _WeeklyReviewConn()
        sent = []

        async def fake_send_telegram(text, **kw):
            sent.append(text)

        review_text = "주간 교훈: 반복 매도는 피하라\n주간 교훈: 손절을 지켜라"
        with patch.object(dm, "LESSON_AUTOSAVE_ENABLED", True), \
             patch.object(dm, "db_pool", FakePool(conn)), \
             patch.object(dm, "_ask_openwebui", new=AsyncMock(return_value=review_text)), \
             patch.object(dm, "_send_telegram", new=fake_send_telegram):
            await dm._jarvis_weekly_review()

        self.assertEqual(len(conn.executed), 2, "켜져 있으면 기존대로 2건 저장해야 한다")
        self.assertIn("교훈 2건 저장", sent[0])


class TestContextCollectorExcludesInactiveLessons(unittest.IsolatedAsyncioTestCase):
    """(d) 가장 중요: 실제 매수/매도 판단 경로(stark/context_collector.collect)에
    dashboard의 진짜 _get_jarvis_lessons를 그대로 주입해, 비활성 교훈이 판단 프롬프트
    컨텍스트에 섞이지 않는지 end-to-end로 검증한다. dashboard/main.py 1199행의
    "AND is_active=TRUE" 조건을 지우고 이 테스트를 돌리면 실패한다 — 수정 중 직접
    제거→재실행→실패 확인 후 복원했다(보고서 참고)."""

    async def test_collect_lessons_txt_excludes_inactive(self):
        async def get_portfolio_context():
            return "[포트폴리오]"

        async def get_active_directives():
            return "- 지시 없음"

        async def get_jarvis_knowledge(n):
            return "지식 없음"

        async def get_position_management_principles(n):
            return "물타기 원칙 없음"

        async def analyze_chart(symbol, name):
            return "[차트]"

        class _NoHistConn:
            async def fetch(self, query, *args):
                return []  # 오늘 판단 이력·공시 없음

        signal = {"symbol": "005930", "name": "삼성전자", "bot": "stock_trader",
                  "action": "buy", "price": 70000, "qty": 1}

        with patch.object(dm, "db_pool", FakePool(FakeLessonConnection(LESSON_ROWS))):
            ctx = await context_collector.collect(
                signal, pool=FakePool(_NoHistConn()), redis=_NoopRedis(),
                get_portfolio_context=get_portfolio_context,
                get_active_directives=get_active_directives,
                get_jarvis_lessons=dm._get_jarvis_lessons,  # 실제 함수(모의 아님)
                get_jarvis_knowledge=get_jarvis_knowledge,
                get_position_management_principles=get_position_management_principles,
                analyze_chart=analyze_chart,
            )

        self.assertNotIn("반복 SKIP 후 1% 오르면", ctx["lessons_txt"])
        self.assertNotIn("기회 놓침을 최소화하라", ctx["lessons_txt"])
        self.assertIn("손절 기준을 지켜라", ctx["lessons_txt"])


if __name__ == "__main__":
    unittest.main()
