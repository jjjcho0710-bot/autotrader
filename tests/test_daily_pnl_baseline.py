"""[AT] fix/daily-pnl-baseline — 마감 결산 '오늘 수익' 기준은 직전 거래일 스냅샷만 쓴다."""
import sys
import unittest
from datetime import date
from unittest.mock import MagicMock, patch

for mod_name in ["fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
                 "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai"]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

import dashboard.main as dm  # noqa: E402


class _Conn:
    def __init__(self, snaps, realized=0, raise_exc=False):
        self.snaps, self.realized, self.raise_exc = snaps, realized, raise_exc
        self.snap_dates = []

    async def fetchval(self, q, *args):
        if self.raise_exc:
            raise RuntimeError("db")
        if "balance_snapshot" in q:
            self.snap_dates.append(args[0])
            return self.snaps.get(args[0])
        if "trade_history" in q:
            return self.realized
        return None


class _Acq:
    def __init__(self, c):
        self.c = c

    async def __aenter__(self):
        return self.c

    async def __aexit__(self, *e):
        return False


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _Acq(self.conn)


ACCT = {"total_eval": 9_101_159}


class TestTodayProfitLine(unittest.IsolatedAsyncioTestCase):
    async def _line(self, conn, today=date(2026, 11, 3), acct=ACCT, valid=True):
        with patch.object(dm, "db_pool", _Pool(conn)):
            return await dm._closing_report_today_profit_line(acct, valid, today)

    async def test_uses_previous_trading_day_snapshot(self):
        c = _Conn({date(2026, 11, 2): 9_113_343, date(2026, 10, 30): 9_176_335})
        line = await self._line(c)
        self.assertIn("-12,184원", line)          # 11/2 기준, 10/30 값(-75,176)이 아님
        self.assertEqual(c.snap_dates, [date(2026, 11, 2)])

    async def test_missing_previous_snapshot_does_not_fall_back_to_older(self):
        """11/2 스냅샷 없음 + 10/30 존재: 이틀 전 값을 쓰지 않고 실현손익으로 표시."""
        c = _Conn({date(2026, 10, 30): 9_176_335}, realized=-10_395)
        line = await self._line(c)
        self.assertNotIn("-75,176", line)
        self.assertIn("기준 총자산 없음", line)
        self.assertIn("11/02", line)
        self.assertIn("-10,395원", line)

    async def test_weekend_skipped_monday_uses_friday(self):
        c = _Conn({date(2026, 10, 30): 9_000_000})
        line = await self._line(c, today=date(2026, 11, 2), acct={"total_eval": 9_090_000})
        self.assertIn("+90,000원", line)
        self.assertEqual(c.snap_dates, [date(2026, 10, 30)])

    async def test_today_row_not_used(self):
        c = _Conn({date(2026, 11, 3): 9_101_159})   # 오늘 행만 있음 -> 기준 아님
        line = await self._line(c)
        self.assertIn("기준 총자산 없음", line)

    async def test_zero_snapshot_treated_as_missing(self):
        c = _Conn({date(2026, 11, 2): 0}, realized=0)
        self.assertIn("기준 총자산 없음", await self._line(c))

    async def test_invalid_today_or_db_error(self):
        self.assertIn("집계 시작 전", await self._line(_Conn({}), valid=False))
        self.assertIn("집계 시작 전", await self._line(_Conn({}, raise_exc=True)))


if __name__ == "__main__":
    unittest.main()
