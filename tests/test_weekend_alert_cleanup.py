"""
tests/test_weekend_alert_cleanup.py - [AT] fix/weekend-alert-cleanup

배경(10/3 토 주말 알림 사고 보고):
1) 지식 정리 완료("📚 지식 정리 완료: ...") 메시지가 learning/curator.py에서
   send_telegram_fn(msg, broadcast=True)로 발송돼 개인방+채널(both)로 나갔다.
   개인방에만 가도록 dest="personal"로 고정한다.
2) _jarvis_weekend_study_report()의 "반복 놓친 종목"이 SKIP 1회짜리까지 포함했다.
   2회 이상만 "반복"으로 보여주도록 HAVING COUNT(*) >= 2를 추가한다.
3) 같은 함수의 "판단 {total}건 (실행 {ex} / 보류 {sk})"가 jarvis_decision이
   EXECUTE/EXECUTE_SMALL/SKIP 외의 값(또는 NULL)인 행을 조용히 누락해 합계가
   총 판단건수보다 작게 나왔다(예: 판단 100건인데 실행16+보류72=88). 남는 건수를
   "기타"로 명시해 항상 합계가 총건수와 맞도록 한다.
4) learning/curator.py의 원칙 성과 통계(K79/K80/K82/K83 등)가 매번 같은 판단에
   함께 인용되는 원칙들은 (적용, 적중)이 완전히 같아 전부 같은 %로 찍혔다.
   (적용, 적중)이 동일한 원칙은 한 줄로 합쳐 표시한다.

21:00 "통합 일일보고"가 주말에 _jarvis_weekend_study_report로 대체되던 경로는
이미 feat/market-calendar(테스트: test_market_calendar_scheduler_holiday_skip.py)에서
제거되었음을 그 테스트가 별도로 확인하고 있다 — 이 파일에서는 재검증하지 않는다.
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
from learning import curator  # noqa: E402

KST = timezone(timedelta(hours=9))
NOW = datetime(2026, 10, 3, 10, 0, tzinfo=KST)  # 토요일


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


# ---------------------------------------------------------------------------
# 2)+3) _jarvis_weekend_study_report
# ---------------------------------------------------------------------------

class FakeWeekendConnection:
    """_jarvis_weekend_study_report가 실행하는 쿼리들을 쿼리 문자열로 구분해 흉내낸다."""

    def __init__(self, wk_row, miss_raw_counts):
        self._wk_row = wk_row
        # miss_raw_counts: {name: count} — 실제 miss_rows 쿼리의 GROUP BY 결과를 미리
        # 계산해두고, 쿼리 문자열에 HAVING COUNT(*) >= 2가 있을 때만 count>=2인 것만 돌려준다
        # (코드에서 HAVING을 지우는 회귀가 생기면 1회짜리도 같이 반환되어 테스트가 깨진다).
        self._miss_raw_counts = miss_raw_counts

    async def fetchrow(self, query, *args):
        if "COUNT(*) AS total" in query:
            return self._wk_row
        raise AssertionError(f"unexpected fetchrow: {query}")

    async def fetch(self, query, *args):
        if "GROUP BY COALESCE(name, symbol)" in query:
            has_having = "HAVING COUNT(*) >= 2" in query
            rows = []
            for name, n in self._miss_raw_counts.items():
                if has_having and n < 2:
                    continue
                rows.append({"nm": name, "n": n, "avg_r": 1.5})
            return rows
        if "category='lesson'" in query:
            return []
        if "category='knowledge_core'" in query:
            return []
        raise AssertionError(f"unexpected fetch: {query}")

    async def fetchval(self, query, *args):
        return 0


def _frozen_datetime_at(year, month, day, hour, minute):
    class _Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(year, month, day, hour, minute, tzinfo=KST)
    return _Frozen


class TestWeekendStudyReportNumbers(unittest.IsolatedAsyncioTestCase):
    async def test_unclassified_judgments_shown_as_etc_not_dropped(self):
        """판단 100건, 실행 16 / 보류 72 뿐이면 나머지 12건이 '기타'로 명시되어야 한다."""
        wk_row = {"total": 100, "ex": 16, "sk": 72, "missed": 5, "good_skip": 60}
        conn = FakeWeekendConnection(wk_row, miss_raw_counts={})
        patches = [
            patch.object(dm, "datetime", _frozen_datetime_at(2026, 10, 3, 21, 0)),
            patch.object(dm, "db_pool", FakePool(conn)),
            patch.object(dm, "_ask_openwebui", new=AsyncMock(return_value="")),
            patch.object(dm, "_send_telegram", new=AsyncMock()),
            patch.object(dm, "redis_client", AsyncMock()),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        await dm._jarvis_weekend_study_report()

        msg = dm._send_telegram.call_args.args[0]
        self.assertIn("판단 100건", msg)
        self.assertIn("실행 16", msg)
        self.assertIn("보류 72", msg)
        self.assertIn("기타 12", msg)

    async def test_fully_classified_judgments_show_no_etc_bucket(self):
        """실행+보류가 총 판단건수와 이미 맞으면 '기타'를 덧붙이지 않는다(회귀 방지)."""
        wk_row = {"total": 88, "ex": 16, "sk": 72, "missed": 5, "good_skip": 60}
        conn = FakeWeekendConnection(wk_row, miss_raw_counts={})
        patches = [
            patch.object(dm, "datetime", _frozen_datetime_at(2026, 10, 3, 21, 0)),
            patch.object(dm, "db_pool", FakePool(conn)),
            patch.object(dm, "_ask_openwebui", new=AsyncMock(return_value="")),
            patch.object(dm, "_send_telegram", new=AsyncMock()),
            patch.object(dm, "redis_client", AsyncMock()),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        await dm._jarvis_weekend_study_report()

        msg = dm._send_telegram.call_args.args[0]
        self.assertIn("판단 88건", msg)
        self.assertNotIn("기타", msg)

    async def test_single_occurrence_miss_excluded_from_repeat_list(self):
        """1회짜리 SKIP-상승은 '반복 놓친 종목'에 나오지 않고, 2회 이상만 나온다."""
        wk_row = {"total": 10, "ex": 5, "sk": 5, "missed": 2, "good_skip": 3}
        conn = FakeWeekendConnection(
            wk_row, miss_raw_counts={"원샷종목": 1, "반복종목": 3})
        patches = [
            patch.object(dm, "datetime", _frozen_datetime_at(2026, 10, 3, 21, 0)),
            patch.object(dm, "db_pool", FakePool(conn)),
            patch.object(dm, "_ask_openwebui", new=AsyncMock(return_value="")),
            patch.object(dm, "_send_telegram", new=AsyncMock()),
            patch.object(dm, "redis_client", AsyncMock()),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        await dm._jarvis_weekend_study_report()

        msg = dm._send_telegram.call_args.args[0]
        self.assertIn("반복종목", msg)
        self.assertNotIn("원샷종목", msg)


# ---------------------------------------------------------------------------
# 1)+4) learning/curator.jarvis_knowledge_curate
# ---------------------------------------------------------------------------

class FakeCuratorConnection:
    def __init__(self, stats_rows):
        self._stats_rows = stats_rows

    async def fetch(self, query, *args):
        if "FROM learning_rules" in query:
            return []
        if "category='knowledge' AND is_active=TRUE" in query and "LIMIT 80" in query:
            return [{"id": i, "content": f"원칙 {i}"} for i in range(3)]
        if "FROM principle_stats ps JOIN jarvis_notes n" in query:
            return self._stats_rows
        raise AssertionError(f"unexpected fetch: {query}")

    async def execute(self, query, *args):
        return None


class TestKnowledgeCurateTelegramDest(unittest.IsolatedAsyncioTestCase):
    async def test_knowledge_curate_sends_personal_only(self):
        """'지식 정리 완료' 메시지는 채널이 아니라 개인방(dest='personal')으로만 간다."""
        conn = FakeCuratorConnection(stats_rows=[])
        pool = FakePool(conn)
        send_telegram_fn = AsyncMock()
        ask_llm_fn = AsyncMock(return_value="[진입] 테스트 원칙 1\n[청산] 테스트 원칙 2")

        await curator.jarvis_knowledge_curate(
            pool=pool, ask_llm_fn=ask_llm_fn, send_telegram_fn=send_telegram_fn)

        send_telegram_fn.assert_awaited_once()
        _, kwargs = send_telegram_fn.call_args
        self.assertEqual(kwargs.get("dest"), "personal")
        self.assertNotIn("broadcast", kwargs)

    async def test_identical_stat_principles_merged_into_one_line(self):
        """(적용,적중)이 완전히 같은 원칙(K79/K80/K82/K83)은 한 줄로 합쳐 같은 %가
        네 번 중복 표시되지 않는다. 다른 통계를 가진 원칙(K50)은 분리된 채로 남는다."""
        stats_rows = [
            {"principle_id": 79, "applied": 100, "hits": 43, "content": "원칙A"},
            {"principle_id": 80, "applied": 100, "hits": 43, "content": "원칙B"},
            {"principle_id": 82, "applied": 100, "hits": 43, "content": "원칙C"},
            {"principle_id": 83, "applied": 100, "hits": 43, "content": "원칙D"},
            {"principle_id": 50, "applied": 10, "hits": 5, "content": "원칙E"},
        ]
        conn = FakeCuratorConnection(stats_rows=stats_rows)
        pool = FakePool(conn)
        send_telegram_fn = AsyncMock()
        ask_llm_fn = AsyncMock(return_value="[진입] 테스트 원칙 1\n[청산] 테스트 원칙 2")

        await curator.jarvis_knowledge_curate(
            pool=pool, ask_llm_fn=ask_llm_fn, send_telegram_fn=send_telegram_fn)

        msg = send_telegram_fn.call_args.args[0]
        # 43%가 한 줄에만 등장 (네 줄로 중복되지 않음)
        self.assertEqual(msg.count("43%"), 1)
        self.assertIn("K79/K80/K82/K83", msg)
        # 통계가 다른 원칙은 별도 줄로 남는다
        self.assertIn("K50", msg)
        self.assertIn("50%", msg)


if __name__ == "__main__":
    unittest.main()
