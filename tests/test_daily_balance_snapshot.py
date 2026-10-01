"""
tests/test_daily_balance_snapshot.py
  common/database.py Database.insert_balance_snapshot, stock_trader/main.py
  StockTrader._daily_balance_snapshot / _has_trading_activity_today 단위 테스트.

배경: 마감 결산에 "오늘 수익(= 오늘 총자산 − 어제 총자산)"을 넣으려면 일별 총자산 기록이
필요하다. balance_snapshot 테이블(id, bot, total_krw, cash_krw, eval_krw, pnl_today, ts)은
존재했지만 쓰는 코드가 0건이라 계속 비어 있었다. 여기서는
1. insert_balance_snapshot의 정상 기록과 같은 날(KST) 중복 시 INSERT 생략(멱등성),
2. _has_trading_activity_today가 오늘 1분봉(stock_ohlcv) 존재 여부로만 휴장일을 판단하고
   (trade_history/stock_daily_ohlcv는 15:35~16:30 판단 창에서 항상 비어 있어 기준으로 쓸 수
   없음), 조회 예외는 휴장일로 단정하지 않고 그대로 전파하는지,
3. _daily_balance_snapshot의 stale/total=0 스킵, 창(15:35~16:30 KST) 밖 시각 스킵, 주말 스킵,
   1분봉이 아직 없을 때 창이 끝날 때까지 포기하지 않고 재시도한 뒤에야 그날을 생략하는지,
   코루틴 내부 예외가 매매 루프로 전파되지 않음
을 고정한다.
"""
import asyncio
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parent.parent
STOCK_TRADER_DIR = REPO_ROOT / "stock_trader"
for p in (REPO_ROOT, STOCK_TRADER_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _stub_missing_runtime_deps():
    """asyncpg/redis/aiohttp가 없는 테스트 환경에서도 import만 되도록 더미로 대체한다."""
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

import main as stock_main  # noqa: E402  (stock_trader/main.py)
from main import StockTrader  # noqa: E402
from common.database import Database  # noqa: E402

KST = timezone(timedelta(hours=9))


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakeConn:
    """fetchval 호출에 미리 지정한 값을 순서대로 반환하거나 지정한 예외를 던진다.
    execute/fetchval 호출 인자를 기록한다."""

    def __init__(self, fetchval_results=None, fetchval_side_effect=None):
        self._fetchval_results = list(fetchval_results or [])
        self._fetchval_side_effect = fetchval_side_effect
        self.execute_calls = []
        self.fetchval_calls = []

    async def fetchval(self, query, *args):
        self.fetchval_calls.append((query, args))
        if self._fetchval_side_effect:
            raise self._fetchval_side_effect
        if self._fetchval_results:
            return self._fetchval_results.pop(0)
        return False

    async def execute(self, query, *args):
        self.execute_calls.append(args)


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _Acquire(self._conn)


# ── common/database.py: insert_balance_snapshot ──────────────────────
class TestInsertBalanceSnapshot(unittest.TestCase):
    def test_inserts_when_no_record_today(self):
        conn = _FakeConn(fetchval_results=[False])
        db = Database.__new__(Database)
        db.pool = _FakePool(conn)

        inserted = asyncio.run(db.insert_balance_snapshot("stock_trader", 1_000_000, 300_000, 700_000))

        self.assertTrue(inserted)
        self.assertEqual(conn.execute_calls, [("stock_trader", 1_000_000, 300_000, 700_000)])

    def test_skips_insert_when_already_recorded_today(self):
        conn = _FakeConn(fetchval_results=[True])
        db = Database.__new__(Database)
        db.pool = _FakePool(conn)

        inserted = asyncio.run(db.insert_balance_snapshot("stock_trader", 1_000_000, 300_000, 700_000))

        self.assertFalse(inserted)
        self.assertEqual(conn.execute_calls, [])  # 중복이면 INSERT 자체를 하지 않는다


# ── stock_trader/main.py: StockTrader._has_trading_activity_today ────
class TestHasTradingActivityToday(unittest.TestCase):
    """오늘 1분봉(stock_ohlcv) 존재 여부로만 판단해야 한다.
    stock_daily_ohlcv는 16:00 이후에만 채워지므로 15:35~16:30 판단 창에서는 기준으로 쓸 수 없고,
    trade_history도 거래가 없었던 정상 개장일에는 비어 있어 기준으로 쓸 수 없다."""

    @staticmethod
    def _run(conn):
        trader = StockTrader.__new__(StockTrader)
        now = datetime(2026, 9, 29, 15, 40, tzinfo=KST)  # 화요일, 판단 창 안

        async def _drive():
            with mock.patch.object(stock_main.db, "pool", _FakePool(conn), create=True):
                return await StockTrader._has_trading_activity_today(trader, now)

        return asyncio.run(_drive())

    def test_true_when_intraday_bar_exists_even_without_trades(self):
        """핵심 회귀: 거래가 없던 정상 개장일이라도 1분봉 행이 있으면 True여야 한다."""
        conn = _FakeConn(fetchval_results=[True])

        result = self._run(conn)

        self.assertTrue(result)
        self.assertEqual(len(conn.fetchval_calls), 1)
        query = conn.fetchval_calls[0][0]
        self.assertIn("stock_ohlcv", query)
        self.assertNotIn("trade_history", query)
        self.assertNotIn("stock_daily_ohlcv", query)

    def test_false_when_no_intraday_bar(self):
        conn = _FakeConn(fetchval_results=[False])

        result = self._run(conn)

        self.assertFalse(result)

    def test_propagates_exception_from_query(self):
        """조회 자체가 실패하면 휴장일(False)로 단정하지 말고 예외를 그대로 전파해야 한다."""
        conn = _FakeConn(fetchval_side_effect=RuntimeError("DB 연결 끊김"))

        with self.assertRaises(RuntimeError):
            self._run(conn)


# ── stock_trader/main.py: StockTrader._daily_balance_snapshot ────────
class _FrozenNow(datetime):
    """datetime.now(KST) 호출만 고정된 값으로 가로챈다."""
    _fixed = None

    @classmethod
    def now(cls, tz=None):
        return cls._fixed


def _run_daily_balance_snapshot_once(
    now,
    has_activity=True,
    balance=None,
    activity_side_effect=None,
    insert_side_effect=None,
):
    """_daily_balance_snapshot 루프를 한 번만 돈다(첫 sleep 직후 running=False).
    (sleeps, inserts, activity_calls, trader) 를 반환한다. trader는 _balance_snapshot_handled_for
    같은 루프 내부 상태를 검사할 때 쓴다."""
    trader = StockTrader.__new__(StockTrader)
    trader.running = True

    balance_result = balance if balance is not None else {"cash": 300_000, "total": 1_000_000}

    class _Broker:
        async def get_balance(self):
            return balance_result

    trader.trader = _Broker()

    sleep_calls = []

    async def _fake_sleep(secs):
        sleep_calls.append(secs)
        trader.running = False

    insert_calls = []

    async def _fake_insert(bot, total, cash, eval_krw):
        if insert_side_effect:
            raise insert_side_effect
        insert_calls.append((bot, total, cash, eval_krw))
        return True

    activity_calls = []

    async def _fake_has_activity(self, now_arg):
        activity_calls.append(now_arg)
        if activity_side_effect:
            raise activity_side_effect
        return has_activity

    frozen = type("_Frozen", (_FrozenNow,), {"_fixed": now})

    async def _drive():
        with mock.patch.object(stock_main, "datetime", frozen), \
             mock.patch.object(stock_main.asyncio, "sleep", _fake_sleep), \
             mock.patch.object(stock_main.db, "insert_balance_snapshot", _fake_insert, create=True), \
             mock.patch.object(StockTrader, "_has_trading_activity_today", _fake_has_activity):
            await StockTrader._daily_balance_snapshot(trader)

    asyncio.run(_drive())
    return sleep_calls, insert_calls, activity_calls, trader


class TestDailyBalanceSnapshotNormalRecord(unittest.TestCase):
    def test_records_when_in_window_with_activity_and_fresh_balance(self):
        now = datetime(2026, 9, 29, 15, 40, tzinfo=KST)  # 화요일, 창 안
        sleeps, inserts, activity, trader = _run_daily_balance_snapshot_once(
            now, has_activity=True, balance={"cash": 300_000, "total": 1_000_000},
        )

        self.assertEqual(inserts, [("stock_trader", 1_000_000, 300_000, 700_000)])
        self.assertEqual(len(activity), 1)
        self.assertEqual(len(sleeps), 1)  # 성공 후 다음날 창까지 대기
        self.assertEqual(trader._balance_snapshot_handled_for, now.date())


class TestDailyBalanceSnapshotStaleOrZero(unittest.TestCase):
    def test_skips_when_balance_is_stale(self):
        now = datetime(2026, 9, 29, 15, 40, tzinfo=KST)
        sleeps, inserts, _, _ = _run_daily_balance_snapshot_once(
            now, has_activity=True,
            balance={"cash": 300_000, "total": 0, "stale": True},
        )
        self.assertEqual(inserts, [])
        self.assertEqual(sleeps, [60])  # 1분 뒤 재시도

    def test_skips_when_total_is_zero(self):
        now = datetime(2026, 9, 29, 15, 40, tzinfo=KST)
        sleeps, inserts, _, _ = _run_daily_balance_snapshot_once(
            now, has_activity=True,
            balance={"cash": 300_000, "total": 0},
        )
        self.assertEqual(inserts, [])
        self.assertEqual(sleeps, [60])


class TestDailyBalanceSnapshotOutsideWindow(unittest.TestCase):
    def test_before_window_does_not_check_balance_or_activity(self):
        now = datetime(2026, 9, 29, 14, 0, tzinfo=KST)  # 화요일 14:00, 창 전
        sleeps, inserts, activity, trader = _run_daily_balance_snapshot_once(now)

        self.assertEqual(inserts, [])
        self.assertEqual(activity, [])
        self.assertEqual(len(sleeps), 1)
        self.assertIsNone(getattr(trader, "_balance_snapshot_handled_for", None))

    def test_after_window_gives_up_for_the_day_without_checking_activity(self):
        now = datetime(2026, 9, 29, 17, 0, tzinfo=KST)  # 화요일 17:00, 창(16:30) 끝난 뒤
        sleeps, inserts, activity, trader = _run_daily_balance_snapshot_once(now)

        self.assertEqual(inserts, [])
        self.assertEqual(activity, [])
        self.assertEqual(len(sleeps), 1)
        # 창이 완전히 끝난 뒤에만 그날을 포기한 것으로 기록한다.
        self.assertEqual(trader._balance_snapshot_handled_for, now.date())


class TestDailyBalanceSnapshotWeekendSkipped(unittest.TestCase):
    def test_saturday_is_skipped(self):
        now = datetime(2026, 10, 3, 15, 40, tzinfo=KST)  # 토요일
        self.assertEqual(now.weekday(), 5)
        sleeps, inserts, activity, _ = _run_daily_balance_snapshot_once(now)

        self.assertEqual(inserts, [])
        self.assertEqual(activity, [])
        self.assertEqual(len(sleeps), 1)

    def test_sunday_is_skipped(self):
        now = datetime(2026, 10, 4, 15, 40, tzinfo=KST)  # 일요일
        self.assertEqual(now.weekday(), 6)
        sleeps, inserts, activity, _ = _run_daily_balance_snapshot_once(now)

        self.assertEqual(inserts, [])
        self.assertEqual(activity, [])
        self.assertEqual(len(sleeps), 1)


class TestDailyBalanceSnapshotNoIntradayBarYet(unittest.TestCase):
    """오늘 1분봉이 아직 없는 경우: 창 안이면 포기하지 않고 재시도만 한다(그날 생략 처리는
    하지 않음). 판단 창이 끝날 때까지 계속 없을 때에야 비로소 그날을 생략한다
    (TestDailyBalanceSnapshotOutsideWindow.test_after_window_gives_up_for_the_day 참고)."""

    def test_no_activity_yet_inside_window_retries_without_giving_up(self):
        now = datetime(2026, 9, 29, 15, 40, tzinfo=KST)  # 화요일, 창 시작 직후
        sleeps, inserts, activity, trader = _run_daily_balance_snapshot_once(now, has_activity=False)

        self.assertEqual(inserts, [])
        self.assertEqual(len(activity), 1)
        self.assertEqual(sleeps, [60])  # 창 끝(16:30)까지 1분 간격으로 재시도
        # 아직 창 안이므로 그날을 포기한 것으로 기록하면 안 된다.
        self.assertIsNone(getattr(trader, "_balance_snapshot_handled_for", None))


class TestDailyBalanceSnapshotActivityQueryException(unittest.TestCase):
    def test_exception_in_activity_check_does_not_propagate(self):
        now = datetime(2026, 9, 29, 15, 40, tzinfo=KST)
        # 예외가 발생해도 asyncio.run이 끝까지 정상 완료되어야 한다(전파되지 않음).
        sleeps, inserts, _, trader = _run_daily_balance_snapshot_once(
            now, activity_side_effect=RuntimeError("DB 연결 끊김"),
        )
        self.assertEqual(inserts, [])
        self.assertEqual(sleeps, [60])  # 휴장일로 단정하지 않고 1분 뒤 재시도
        # 조회 실패는 휴장일 판단이 아니므로 그날을 포기한 것으로 기록하면 안 된다.
        self.assertIsNone(getattr(trader, "_balance_snapshot_handled_for", None))

    def test_exception_in_insert_does_not_propagate(self):
        now = datetime(2026, 9, 29, 15, 40, tzinfo=KST)
        sleeps, inserts, _, _ = _run_daily_balance_snapshot_once(
            now, has_activity=True, insert_side_effect=RuntimeError("DB 쓰기 실패"),
        )
        self.assertEqual(inserts, [])
        self.assertEqual(len(sleeps), 1)


if __name__ == "__main__":
    unittest.main()
