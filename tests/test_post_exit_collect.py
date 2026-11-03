"""[AT] fix/post-exit-collect — 청산 후 10거래일은 watchlist 비활성화에서 제외한다."""
import sys
import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

for mod_name in ["fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
                 "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai"]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

import dashboard.main as dm  # noqa: E402


class Conn:
    """trade_history(최근 거래) / watchlist(scanner 활성 종목) 대역. 실행된 UPDATE를 기록한다."""

    def __init__(self, scanner=("AAA",), recent=(), raise_recent=False):
        self.scanner, self.recent, self.raise_recent = list(scanner), list(recent), raise_recent
        self.executed, self.recent_cutoffs = [], []

    async def fetch(self, query, *args):
        if "FROM trade_history" in query:
            if self.raise_recent:
                raise RuntimeError("db")
            self.recent_cutoffs.append(args[0])
            return [{"symbol": s} for s in self.recent]
        if "added_by='jarvis_scanner'" in query:
            return [{"symbol": s} for s in self.scanner]
        return []

    async def execute(self, query, *args):
        self.executed.append((query, args))

    def deactivated(self):
        return {a[0] for q, a in self.executed if "is_active=FALSE" in q}


class Acq:
    def __init__(self, c):
        self.c = c

    async def __aenter__(self):
        return self.c

    async def __aexit__(self, *e):
        return False


class Pool:
    def __init__(self, c):
        self.c = c

    def acquire(self):
        return Acq(self.c)


class TestCutoff(unittest.TestCase):
    def test_ten_trading_days_skip_weekends(self):
        # 2026-11-03(화) 기준 10거래일 전: 11/2,10/30,29,28,27,26,23,22,21,20 -> 10/20
        self.assertEqual(dm._post_exit_cutoff_date(date(2026, 11, 3), 10), date(2026, 10, 20))

    def test_five_days_over_weekend(self):
        self.assertEqual(dm._post_exit_cutoff_date(date(2026, 11, 2), 1), date(2026, 10, 30))

    def test_holiday_skipped(self):
        # 10/9 한글날은 거래일이 아니다: 10/12(월) 기준 1거래일 전은 10/8(목)
        self.assertEqual(dm._post_exit_cutoff_date(date(2026, 10, 12), 1), date(2026, 10, 8))


class TestRecentlyTraded(unittest.IsolatedAsyncioTestCase):
    async def test_returns_symbols_and_uses_cutoff(self):
        c = Conn(recent=("AAA", "BBB"))
        r = await dm._recently_traded_symbols(c, 10, today=date(2026, 11, 3))
        self.assertEqual(r, {"AAA", "BBB"})
        self.assertEqual(c.recent_cutoffs, [date(2026, 10, 20)])   # 이 날짜(포함) 이후 거래 = 최근

    async def test_failure_returns_none(self):
        self.assertIsNone(await dm._recently_traded_symbols(Conn(raise_recent=True), 10))


class TestCleanupScanner(unittest.IsolatedAsyncioTestCase):
    async def _run(self, conn, held=()):
        pos = {"success": True, "data": [{"symbol": s} for s in held]}
        with patch.object(dm, "db_pool", Pool(conn)), \
                patch.object(dm, "get_stock_positions", AsyncMock(return_value=pos)):
            return await dm._cleanup_scanner_watchlist()

    async def test_recent_symbol_kept_old_one_deactivated(self):
        c = Conn(scanner=("AAA", "OLD"), recent=("AAA",))
        n = await self._run(c)
        self.assertEqual(c.deactivated(), {"OLD"})
        self.assertEqual(n, 1)

    async def test_held_symbol_kept(self):
        c = Conn(scanner=("AAA", "HELD"), recent=())
        await self._run(c, held=("HELD",))
        self.assertEqual(c.deactivated(), {"AAA"})

    async def test_recent_query_failure_skips_all(self):
        c = Conn(scanner=("AAA", "OLD"), raise_recent=True)
        n = await self._run(c)
        self.assertEqual(c.deactivated(), set())
        self.assertEqual(n, 0)


class TestScannerBlock(unittest.IsolatedAsyncioTestCase):
    """_jarvis_stock_scanner 비활성화 블록은 기존 하네스(tests/test_holdings_daily_collect)를 재사용한다."""

    async def _scan(self, conn, positions):
        from tests.test_holdings_daily_collect import TestScannerHoldingsProtection as T
        t = T("test_successful_lookup_still_deactivates_non_held_old_symbols")
        t.setUp()
        try:
            pool = Pool(conn)
            pool.conn = conn
            with patch.object(dm, "db_pool", pool), \
                    patch.object(dm, "get_stock_positions", new=AsyncMock(return_value=positions)):
                await dm._jarvis_stock_scanner()
        finally:
            t.tearDown()

    async def test_recent_trade_symbol_not_deactivated(self):
        c = Conn(scanner=("RECENT", "OLD"), recent=("RECENT",))
        await self._scan(c, {"success": True, "data": []})
        self.assertEqual(c.deactivated(), {"OLD"})

    async def test_recent_query_failure_skips_deactivation(self):
        c = Conn(scanner=("RECENT", "OLD"), raise_recent=True)
        await self._scan(c, {"success": True, "data": []})
        self.assertEqual(c.deactivated(), set())

    async def test_positions_failure_still_skips(self):
        c = Conn(scanner=("OLD",), recent=())
        await self._scan(c, {"success": False, "data": []})
        self.assertEqual(c.deactivated(), set())


if __name__ == "__main__":
    unittest.main()
