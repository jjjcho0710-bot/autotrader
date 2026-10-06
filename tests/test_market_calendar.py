"""
common/market_calendar.py 단위 테스트 ([AT] feat/market-calendar).

배경: 시스템 전체에 공휴일 판별이 없어 평일 스케줄이 공휴일에도 정상 개장일처럼
돌았다(10/5 개천절 대체공휴일 15:40 마감 결산 오발송, 10/9 한글날도 동일 위험).
이 모듈은 순수 함수라 의존성 모킹 없이 바로 임포트해 검증한다.
"""
import sys
import unittest
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from common.market_calendar import (  # noqa: E402
    CALENDAR_COVERS_THROUGH,
    holiday_reason,
    is_calendar_stale,
    is_trading_day,
)


class TestIsTradingDay(unittest.TestCase):
    def test_confirmed_2026_holidays_are_closed(self):
        """PM 확인 사실: 10/5, 10/9, 12/25, 12/31은 휴장이다."""
        for d in (date(2026, 10, 5), date(2026, 10, 9),
                  date(2026, 12, 25), date(2026, 12, 31)):
            with self.subTest(d=d):
                self.assertFalse(is_trading_day(d))

    def test_plain_weekdays_are_trading_days(self):
        for d in (date(2026, 10, 1), date(2026, 10, 6),
                  date(2026, 10, 7), date(2026, 10, 8)):
            with self.subTest(d=d):
                self.assertTrue(is_trading_day(d))

    def test_saturday_and_sunday_are_closed(self):
        self.assertFalse(is_trading_day(date(2026, 10, 3)))  # 토
        self.assertFalse(is_trading_day(date(2026, 10, 4)))  # 일

    def test_holiday_reason_returns_text_for_known_holiday(self):
        self.assertEqual(holiday_reason(date(2026, 10, 9)), "한글날")

    def test_holiday_reason_is_none_for_weekend_and_plain_weekday(self):
        self.assertIsNone(holiday_reason(date(2026, 10, 3)))  # 토 — 명시 휴장일 집합엔 없음
        self.assertIsNone(holiday_reason(date(2026, 10, 6)))


class TestCalendarStaleness(unittest.TestCase):
    def test_date_within_coverage_is_not_stale(self):
        self.assertFalse(is_calendar_stale(CALENDAR_COVERS_THROUGH))
        self.assertFalse(is_calendar_stale(date(2026, 10, 6)))

    def test_date_past_coverage_is_stale(self):
        self.assertTrue(is_calendar_stale(date(2027, 1, 1)))

    def test_dates_beyond_coverage_still_default_to_weekday_as_trading_day(self):
        """달력 범위를 넘으면 공휴일 반영 없이 평일=거래일로 안전하게 축소 동작한다
        (완전 실패하지 않음) — is_calendar_stale()로 호출측이 별도로 경고해야 한다."""
        self.assertTrue(is_calendar_stale(date(2027, 1, 4)))
        self.assertTrue(is_trading_day(date(2027, 1, 4)))  # 2027-01-04는 월요일


if __name__ == "__main__":
    unittest.main()
