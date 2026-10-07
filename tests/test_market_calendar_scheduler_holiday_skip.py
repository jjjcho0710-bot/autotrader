"""
tests/test_market_calendar_scheduler_holiday_skip.py - [AT] feat/market-calendar

dashboard/main.py _jarvis_scheduler() 실제 호출부 end-to-end 검증.

배경(PM 보고 실측 사고): 10/5(월, 개천절 대체공휴일)에 평일 스케줄러가 휴장일을
몰라 15:40 마감 결산("오늘 거래 없음", 시간외 가격 변동이 반영된 "오늘 수익
+44,000원")이 그대로 나갔다. 10/9(한글날)도 같은 위험이 있었다.

이 테스트는 스케줄러의 `now.weekday() >= 5` 조건에 `not is_trading_day(today)`를
더한 수정을 되돌리면(= 휴장일을 평일로 오판하면) 깨진다. `_jarvis_scheduler`는
`while True`라 자연히 끝나지 않으므로, `asyncio.sleep`을 두 번째 호출에서 예외를
던지게 해 한 틱만 실행시키고, `asyncio.create_task`는 실제로 태스크를 만들지 않고
코루틴만 수집해 테스트에서 직접 await한다(실행 시점을 테스트가 통제).
"""
import sys
import unittest
from datetime import datetime as _real_datetime
from unittest.mock import AsyncMock, MagicMock, patch

for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

import dashboard.main as dm  # noqa: E402


class _StopLoop(Exception):
    """asyncio.sleep을 가로채 while True 스케줄러 루프를 한 틱 뒤 멈추기 위한 신호."""


class _FakeRedisGate:
    def __init__(self):
        self.store = {}

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True


def _frozen_datetime_at(year, month, day, hour, minute):
    class _Frozen(_real_datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(year, month, day, hour, minute, tzinfo=dm.KST)
    return _Frozen


class TestSchedulerSkipsClosingBlockOnHoliday(unittest.IsolatedAsyncioTestCase):
    async def _run_one_tick(self, frozen_datetime):
        created_coros = []

        def fake_create_task(coro):
            created_coros.append(coro)
            return MagicMock()

        calls = {"n": 0}

        async def fake_sleep(_seconds):
            calls["n"] += 1
            if calls["n"] >= 2:
                raise _StopLoop()

        patches = [
            patch.object(dm, "datetime", frozen_datetime),
            patch.object(dm, "redis_client", _FakeRedisGate()),
            patch("asyncio.sleep", new=fake_sleep),
            patch("asyncio.create_task", new=fake_create_task),
            patch.object(dm, "_send_telegram", new=AsyncMock()),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        with self.assertRaises(_StopLoop):
            await dm._jarvis_scheduler()

        # create_task로 넘겨진 코루틴은 (호출 자체가 이미 Mock에 기록되므로) 실행하지
        # 않고 닫기만 한다 — "coroutine was never awaited" 경고만 방지.
        for coro in created_coros:
            coro.close()

    async def test_confirmed_holiday_monday_skips_closing_block(self):
        """2026-10-05(월, 개천절 대체공휴일) 15:40 — 마감 결산/채점·복기가 예약되지 않는다."""
        mock_closing = AsyncMock(name="_jarvis_closing_report")
        mock_score = AsyncMock(name="_score_then_review")
        mock_manual = AsyncMock(name="_manual_collect")
        with patch.object(dm, "_jarvis_closing_report", mock_closing), \
             patch.object(dm, "_score_then_review", mock_score), \
             patch.object(dm, "_manual_collect", mock_manual):
            await self._run_one_tick(_frozen_datetime_at(2026, 10, 5, 15, 40))
        mock_closing.assert_not_called()
        mock_score.assert_not_called()

    async def test_plain_weekday_still_schedules_closing_block(self):
        """휴장일 처리를 추가해도 정상 평일 15:40 마감 결산은 그대로 예약된다(회귀 아님)."""
        mock_closing = AsyncMock(name="_jarvis_closing_report")
        mock_score = AsyncMock(name="_score_then_review")
        mock_manual = AsyncMock(name="_manual_collect")
        with patch.object(dm, "_jarvis_closing_report", mock_closing), \
             patch.object(dm, "_score_then_review", mock_score), \
             patch.object(dm, "_manual_collect", mock_manual):
            await self._run_one_tick(_frozen_datetime_at(2026, 10, 6, 15, 40))
        mock_closing.assert_called_once()
        mock_score.assert_called_once()


class TestUnifiedDailyReportSkipsOnHolidayAndWeekend(unittest.IsolatedAsyncioTestCase):
    """_jarvis_unified_daily_report()는 주말·휴장일 모두 건너뛴다(학습보고 대체 제거)."""

    async def test_confirmed_holiday_skips_without_substituting_study_report(self):
        with patch.object(dm, "datetime", _frozen_datetime_at(2026, 10, 5, 21, 0)), \
             patch.object(dm, "_jarvis_weekend_study_report", new=AsyncMock()) as mock_study, \
             patch.object(dm, "_send_telegram", new=AsyncMock()) as mock_send:
            await dm._jarvis_unified_daily_report()
        mock_study.assert_not_awaited()
        mock_send.assert_not_awaited()

    async def test_saturday_skips_without_substituting_study_report(self):
        with patch.object(dm, "datetime", _frozen_datetime_at(2026, 10, 3, 21, 0)), \
             patch.object(dm, "_jarvis_weekend_study_report", new=AsyncMock()) as mock_study, \
             patch.object(dm, "_send_telegram", new=AsyncMock()) as mock_send:
            await dm._jarvis_unified_daily_report()
        mock_study.assert_not_awaited()
        mock_send.assert_not_awaited()


class TestSchedulerSkipsMorningScanOnHoliday(unittest.IsolatedAsyncioTestCase):
    """[AT] fix/morning-scan-fallback: 08:30 전종목 스캔 블록도 휴장일에는 예약되지 않아야
    한다(= 스캔을 안 하므로 실패 알림도 안 나간다). 로직은 `_jarvis_scheduler()`의
    `now.weekday() >= 5 or not is_trading_day(today)` 공통 분기라 TestSchedulerSkipsClosingBlockOnHoliday
    와 동일한 틱 실행 헬퍼를 쓴다(클래스 분리 — 서로 다른 블록의 mock 대상을 섞지 않기 위함)."""

    async def _run_one_tick(self, frozen_datetime):
        created_coros = []

        def fake_create_task(coro):
            created_coros.append(coro)
            return MagicMock()

        calls = {"n": 0}

        async def fake_sleep(_seconds):
            calls["n"] += 1
            if calls["n"] >= 2:
                raise _StopLoop()

        patches = [
            patch.object(dm, "datetime", frozen_datetime),
            patch.object(dm, "redis_client", _FakeRedisGate()),
            patch("asyncio.sleep", new=fake_sleep),
            patch("asyncio.create_task", new=fake_create_task),
            patch.object(dm, "_send_telegram", new=AsyncMock()),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        with self.assertRaises(_StopLoop):
            await dm._jarvis_scheduler()

        for coro in created_coros:
            coro.close()

    async def test_confirmed_holiday_monday_skips_morning_scan(self):
        """2026-10-05(월, 개천절 대체공휴일) 08:30 — 전종목 스캔이 호출되지 않는다."""
        mock_scanner = AsyncMock(name="_jarvis_stock_scanner")
        mock_analysis = AsyncMock(name="_jarvis_auto_analysis")
        mock_plan = AsyncMock(name="_jarvis_daily_plan")
        with patch.object(dm, "_jarvis_stock_scanner", mock_scanner), \
             patch.object(dm, "_jarvis_auto_analysis", mock_analysis), \
             patch.object(dm, "_jarvis_daily_plan", mock_plan):
            await self._run_one_tick(_frozen_datetime_at(2026, 10, 5, 8, 30))
        mock_scanner.assert_not_called()

    async def test_saturday_skips_morning_scan(self):
        """토요일 08:30 — 주말이므로 전종목 스캔이 호출되지 않는다."""
        mock_scanner = AsyncMock(name="_jarvis_stock_scanner")
        mock_analysis = AsyncMock(name="_jarvis_auto_analysis")
        mock_plan = AsyncMock(name="_jarvis_daily_plan")
        with patch.object(dm, "_jarvis_stock_scanner", mock_scanner), \
             patch.object(dm, "_jarvis_auto_analysis", mock_analysis), \
             patch.object(dm, "_jarvis_daily_plan", mock_plan):
            await self._run_one_tick(_frozen_datetime_at(2026, 10, 3, 8, 30))
        mock_scanner.assert_not_called()

    async def test_plain_weekday_still_schedules_morning_scan(self):
        """휴장일 가드를 추가해도 정상 평일 08:30 스캔은 그대로 실행된다(회귀 아님)."""
        mock_scanner = AsyncMock(name="_jarvis_stock_scanner")
        mock_analysis = AsyncMock(name="_jarvis_auto_analysis")
        mock_plan = AsyncMock(name="_jarvis_daily_plan")
        with patch.object(dm, "_jarvis_stock_scanner", mock_scanner), \
             patch.object(dm, "_jarvis_auto_analysis", mock_analysis), \
             patch.object(dm, "_jarvis_daily_plan", mock_plan):
            await self._run_one_tick(_frozen_datetime_at(2026, 10, 6, 8, 30))
        mock_scanner.assert_called_once()


if __name__ == "__main__":
    unittest.main()
