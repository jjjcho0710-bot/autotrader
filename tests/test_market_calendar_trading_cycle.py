"""
tests/test_market_calendar_trading_cycle.py - [AT] feat/market-calendar

stock_trader/main.py StockTrader._loop() 실제 호출부 end-to-end 검증.
배경: 10/5(개천절 대체공휴일)·10/9(한글날)에 평일 스케줄로 매매 사이클이 돌았다.
이 테스트는 common/market_calendar.is_trading_day() 적용을 되돌리면(=주말만
걸러내던 예전 조건으로 되돌리면) 깨진다.

추가로 "달력에 없는 휴장일" 안전망(삼성전자 일봉 최신 날짜 확인)도 검증한다.
이 안전망이 휴장으로 판정하면 그 사이클만이 아니라 그날 하루 종일 매매 사이클을
건너뛰어야 한다(5분만 쉬고 다시 매매하는 회귀를 막는다).
"""
import sys
import types
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
STOCK_TRADER_DIR = REPO_ROOT / "stock_trader"
for p in (REPO_ROOT, STOCK_TRADER_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _stub_missing_runtime_deps():
    try:
        import asyncpg  # noqa: F401
    except ImportError:
        stub = types.ModuleType("asyncpg")
        stub.Pool = object
        sys.modules["asyncpg"] = stub

    try:
        import redis.asyncio  # noqa: F401
    except ImportError:
        redis_mod = types.ModuleType("redis")
        redis_asyncio_mod = types.ModuleType("redis.asyncio")
        redis_asyncio_mod.Redis = object
        redis_mod.asyncio = redis_asyncio_mod
        sys.modules["redis"] = redis_mod
        sys.modules["redis.asyncio"] = redis_asyncio_mod

    try:
        import aiohttp  # noqa: F401
    except ImportError:
        stub = types.ModuleType("aiohttp")
        stub.ClientSession = object
        stub.ClientTimeout = lambda *a, **kw: None
        sys.modules["aiohttp"] = stub


_stub_missing_runtime_deps()

import main  # noqa: E402  (stock_trader/main.py)

KST = timezone(timedelta(hours=9))


class TestLoopHolidayGating(unittest.IsolatedAsyncioTestCase):
    """StockTrader._loop() 한 사이클 — 달력상 휴장일이면 _run_cycle을 건너뛴다."""

    async def asyncSetUp(self):
        self.trader = main.StockTrader()
        self.trader.running = False
        self.trader._run_cycle = AsyncMock()
        self.trader._notify_kis_recovered_if_needed = AsyncMock()
        self.trader.trader.get_daily_ohlcv = AsyncMock(return_value=[])  # 호출 안 되는 걸 기본값으로

    async def _run_one_cycle(self, fixed_now: datetime):
        async def fake_sleep(_seconds):
            self.trader.running = False

        with patch.object(main, "datetime") as mock_dt, \
             patch("asyncio.sleep", new=fake_sleep), \
             patch.object(main.cache, "set_bot_status", new=AsyncMock()):
            mock_dt.now.side_effect = lambda tz=None: (fixed_now if tz else fixed_now.replace(tzinfo=None))
            self.trader.running = True
            await self.trader._loop()

    async def test_confirmed_holiday_monday_skips_cycle(self):
        """2026-10-05(월, 개천절 대체공휴일) 10:00 — 평일·장시간이지만 휴장일이라 건너뛴다."""
        await self._run_one_cycle(datetime(2026, 10, 5, 10, 0, tzinfo=KST))
        self.trader._run_cycle.assert_not_awaited()

    async def test_confirmed_holiday_friday_skips_cycle(self):
        """2026-10-09(금, 한글날)도 동일."""
        await self._run_one_cycle(datetime(2026, 10, 9, 10, 0, tzinfo=KST))
        self.trader._run_cycle.assert_not_awaited()

    async def test_plain_weekday_still_runs_cycle(self):
        """휴장일이 아닌 평일(10/6 화)은 그대로 사이클이 돈다 — 과도한 차단 아님을 확인."""
        await self._run_one_cycle(datetime(2026, 10, 6, 10, 0, tzinfo=KST))
        self.trader._run_cycle.assert_awaited_once()

    async def test_weekend_still_skips_cycle(self):
        """기존 주말 차단 동작은 그대로 유지된다(회귀 아님)."""
        await self._run_one_cycle(datetime(2026, 10, 3, 10, 0, tzinfo=KST))  # 토요일
        self.trader._run_cycle.assert_not_awaited()


class TestSafetyNetUndeclaredHoliday(unittest.IsolatedAsyncioTestCase):
    """달력은 거래일이라고 하지만 삼성전자 최신 일봉이 오늘이 아니면(=달력에 없는
    휴장일) 사이클을 건너뛰고 1회 알림을 보낸다. 조회 실패 시엔 기존 동작(진행)을 유지."""

    async def asyncSetUp(self):
        self.trader = main.StockTrader()
        self.trader.running = False
        self.trader._run_cycle = AsyncMock()
        self.trader._notify_kis_recovered_if_needed = AsyncMock()

    async def _run_one_cycle(self, fixed_now: datetime):
        async def fake_sleep(_seconds):
            self.trader.running = False

        with patch.object(main, "datetime") as mock_dt, \
             patch("asyncio.sleep", new=fake_sleep), \
             patch.object(main.cache, "set_bot_status", new=AsyncMock()):
            mock_dt.now.side_effect = lambda tz=None: (fixed_now if tz else fixed_now.replace(tzinfo=None))
            self.trader.running = True
            await self.trader._loop()

    async def test_latest_candle_not_today_skips_and_alerts_once(self):
        # 달력상 거래일(10/6 화)인데 삼성전자 최신 일봉이 전날(10/2 금)까지만 있음
        self.trader.trader.get_daily_ohlcv = AsyncMock(
            return_value=[{"date": "20261002", "close": 70000}])
        with patch("common.telegram.send_stock", new=AsyncMock()) as mock_send:
            await self._run_one_cycle(datetime(2026, 10, 6, 10, 0, tzinfo=KST))
        self.trader._run_cycle.assert_not_awaited()
        mock_send.assert_awaited_once()
        self.assertIn("달력에 없는 휴장일", mock_send.await_args.args[0])

    async def test_latest_candle_is_today_runs_cycle_normally(self):
        self.trader.trader.get_daily_ohlcv = AsyncMock(
            return_value=[{"date": "20261006", "close": 70000}])
        await self._run_one_cycle(datetime(2026, 10, 6, 10, 0, tzinfo=KST))
        self.trader._run_cycle.assert_awaited_once()

    async def test_ohlcv_lookup_failure_keeps_existing_behavior(self):
        """일봉 조회가 예외를 던지면 휴장으로 단정하지 않고 사이클을 그대로 진행한다."""
        self.trader.trader.get_daily_ohlcv = AsyncMock(side_effect=Exception("KIS 타임아웃"))
        await self._run_one_cycle(datetime(2026, 10, 6, 10, 0, tzinfo=KST))
        self.trader._run_cycle.assert_awaited_once()

    async def test_empty_candles_keeps_existing_behavior(self):
        """일봉 조회가 빈 리스트를 반환해도 휴장으로 단정하지 않는다."""
        self.trader.trader.get_daily_ohlcv = AsyncMock(return_value=[])
        await self._run_one_cycle(datetime(2026, 10, 6, 10, 0, tzinfo=KST))
        self.trader._run_cycle.assert_awaited_once()

    async def test_checked_only_once_per_day(self):
        """같은 날 두 번째 사이클에서는 안전망 일봉 조회를 다시 하지 않는다."""
        self.trader.trader.get_daily_ohlcv = AsyncMock(
            return_value=[{"date": "20261006", "close": 70000}])
        await self._run_one_cycle(datetime(2026, 10, 6, 10, 0, tzinfo=KST))
        self.trader._run_cycle.assert_awaited_once()
        await self._run_one_cycle(datetime(2026, 10, 6, 10, 1, tzinfo=KST))
        self.assertEqual(self.trader._run_cycle.await_count, 2)
        self.trader.trader.get_daily_ohlcv.assert_awaited_once()

    async def test_holiday_detected_skips_cycle_for_rest_of_day(self):
        """안전망이 휴장으로 판정하면 그 사이클만 건너뛰는 게 아니라 그날 나머지 사이클도
        모두 건너뛴다. 회귀: 예전엔 _safety_net_checked_date가 당일로 설정된 뒤 두 번째
        사이클부터는 판정 블록을 다시 타지 않아 5분만 쉬고 그대로 매매를 돌렸다."""
        self.trader.trader.get_daily_ohlcv = AsyncMock(
            return_value=[{"date": "20261002", "close": 70000}])
        with patch("common.telegram.send_stock", new=AsyncMock()):
            await self._run_one_cycle(datetime(2026, 10, 6, 10, 0, tzinfo=KST))
        self.trader._run_cycle.assert_not_awaited()

        # 같은 날 이후(장 마감 직전) 사이클 — 여전히 건너뛰어야 한다
        with patch("common.telegram.send_stock", new=AsyncMock()):
            await self._run_one_cycle(datetime(2026, 10, 6, 15, 0, tzinfo=KST))
        self.trader._run_cycle.assert_not_awaited()
        # 일봉 조회도 하루 1회만(두 번째 사이클에서 재조회하지 않음)
        self.trader.trader.get_daily_ohlcv.assert_awaited_once()

    async def test_holiday_flag_resets_next_trading_day(self):
        """안전망 휴장 판정은 그날 하루만 유효하고 다음 거래일에는 정상 동작한다."""
        self.trader.trader.get_daily_ohlcv = AsyncMock(
            return_value=[{"date": "20261002", "close": 70000}])
        with patch("common.telegram.send_stock", new=AsyncMock()):
            await self._run_one_cycle(datetime(2026, 10, 6, 10, 0, tzinfo=KST))
        self.trader._run_cycle.assert_not_awaited()

        self.trader.trader.get_daily_ohlcv = AsyncMock(
            return_value=[{"date": "20261007", "close": 70000}])
        await self._run_one_cycle(datetime(2026, 10, 7, 10, 0, tzinfo=KST))
        self.trader._run_cycle.assert_awaited_once()


class TestCalendarStaleWarningLogged(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.trader = main.StockTrader()
        self.trader.running = False
        self.trader._run_cycle = AsyncMock()
        self.trader.trader.get_daily_ohlcv = AsyncMock(return_value=[])
        self.trader._notify_kis_recovered_if_needed = AsyncMock()

    async def test_date_beyond_calendar_coverage_logs_warning_once(self):
        self.assertTrue(date(2027, 1, 4) > main.CALENDAR_COVERS_THROUGH)

        async def fake_sleep(_seconds):
            self.trader.running = False

        fixed_now = datetime(2027, 1, 4, 10, 0, tzinfo=KST)  # 달력 범위 밖의 월요일
        with patch.object(main, "datetime") as mock_dt, \
             patch("asyncio.sleep", new=fake_sleep), \
             patch.object(main.cache, "set_bot_status", new=AsyncMock()):
            mock_dt.now.side_effect = lambda tz=None: (fixed_now if tz else fixed_now.replace(tzinfo=None))
            self.trader.running = True
            with self.assertLogs("stock-trader", level="WARNING") as logs:
                await self.trader._loop()
        self.assertTrue(any("휴장일 달력 범위 초과" in m for m in logs.output))


if __name__ == "__main__":
    unittest.main()
