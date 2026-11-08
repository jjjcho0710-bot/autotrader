"""
tests/test_daily_balance_snapshot.py
  common/database.py Database.insert_balance_snapshot, stock_trader/main.py
  StockTrader._daily_balance_snapshot 단위 테스트.

배경: 마감 결산에 "오늘 수익(= 오늘 총자산 − 어제 총자산)"을 넣으려면 일별 총자산 기록이
필요하다. balance_snapshot 테이블(id, bot, total_krw, cash_krw, eval_krw, pnl_today, ts)은
존재했지만 쓰는 코드가 0건이라 계속 비어 있었다. 여기서는
1. insert_balance_snapshot의 정상 기록과 같은 날(KST) 중복 시 INSERT 생략(멱등성),
2. _daily_balance_snapshot이 1분봉(stock_ohlcv) 존재 여부와 무관하게, 오늘이 거래일
   (common.market_calendar.is_trading_day)이고 KIS 총자산 조회가 성공하면 기록하는지
   ([AT] fix/snapshot-decouple — 분봉 수집이 멈춰도 스냅샷은 기록되어야 함),
3. stale/total=0 스킵 후 창(15:35~16:30 KST) 안 재시도, 창 밖 시각 스킵, 주말·휴장일 스킵,
   창이 끝날 때까지 실패하면 텔레그램 경고를 일 1회만 보내는지(추정값 기록 없음),
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


# ── stock_trader/main.py: StockTrader._daily_balance_snapshot ────────
class _FrozenNow(datetime):
    """datetime.now(KST) 호출만 고정된 값으로 가로챈다."""
    _fixed = None

    @classmethod
    def now(cls, tz=None):
        return cls._fixed


def _run_daily_balance_snapshot_once(
    now,
    balance=None,
    insert_side_effect=None,
    balance_side_effect=None,
    trading_day=None,
    warned_for=None,
    failed_for=None,
):
    """_daily_balance_snapshot 루프를 한 번만 돈다(첫 sleep 직후 running=False).
    (sleeps, inserts, telegram_msgs, trader) 를 반환한다. 분봉(stock_ohlcv)은 어디서도 조회하지 않는다:
    db.pool 이 없으므로 조회를 시도하면 AttributeError 로 테스트가 실패한다.
    trading_day=None 이면 실제 common.market_calendar.is_trading_day 를 쓴다."""
    trader = StockTrader.__new__(StockTrader)
    trader.running = True
    if warned_for is not None:
        trader._balance_snapshot_warned_for = warned_for
    if failed_for is not None:
        trader._balance_snapshot_failed_for = failed_for
        trader._balance_snapshot_last_fail_reason = "이전 실패"

    balance_result = balance if balance is not None else {"cash": 300_000, "total": 1_000_000, "stock_eval": 700_000}

    class _Broker:
        async def get_balance(self):
            if balance_side_effect:
                raise balance_side_effect
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

    telegram_msgs = []

    async def _fake_send_stock(text):
        telegram_msgs.append(text)

    frozen = type("_Frozen", (_FrozenNow,), {"_fixed": now})

    async def _drive():
        patches = [
            mock.patch.object(stock_main, "datetime", frozen),
            mock.patch.object(stock_main.asyncio, "sleep", _fake_sleep),
            mock.patch.object(stock_main.db, "insert_balance_snapshot", _fake_insert, create=True),
            mock.patch("common.telegram.send_stock", _fake_send_stock),
        ]
        if trading_day is not None:
            patches.append(mock.patch.object(stock_main, "is_trading_day", lambda d: trading_day))
        for pt in patches:
            pt.start()
        try:
            await StockTrader._daily_balance_snapshot(trader)
        finally:
            for pt in patches:
                pt.stop()

    asyncio.run(_drive())
    return sleep_calls, insert_calls, telegram_msgs, trader


class TestDailyBalanceSnapshotNormalRecord(unittest.TestCase):
    def test_records_when_trading_day_and_balance_ok_without_any_intraday_bar(self):
        """핵심 회귀: 1분봉 조회 없이(db.pool 없음) 거래일+총자산 조회 성공만으로 기록한다."""
        now = datetime(2026, 9, 29, 15, 40, tzinfo=KST)  # 화요일, 창 안
        sleeps, inserts, msgs, trader = _run_daily_balance_snapshot_once(
            now, balance={"cash": 300_000, "total": 1_000_000, "stock_eval": 650_000},
        )

        # eval_krw는 total-cash(700,000)가 아니라 KIS 주식평가금액(stock_eval) ([AT] fix/balance-fields)
        self.assertEqual(inserts, [("stock_trader", 1_000_000, 300_000, 650_000)])
        self.assertEqual(len(sleeps), 1)  # 성공 후 다음날 창까지 대기
        self.assertEqual(trader._balance_snapshot_handled_for, now.date())
        self.assertEqual(msgs, [])

    def test_no_activity_check_method_remains(self):
        self.assertFalse(hasattr(StockTrader, "_has_trading_activity_today"))


class TestDailyBalanceSnapshotStockEvalMissing(unittest.TestCase):
    def test_skips_when_stock_eval_unknown(self):
        """stock_eval을 알 수 없으면 total-cash로 추정해 기록하지 않고 재시도한다."""
        now = datetime(2026, 9, 29, 15, 40, tzinfo=KST)
        sleeps, inserts, _, _ = _run_daily_balance_snapshot_once(
            now, balance={"cash": 300_000, "total": 1_000_000},
        )
        self.assertEqual(inserts, [])
        self.assertEqual(sleeps, [60])


class TestDailyBalanceSnapshotStaleOrZero(unittest.TestCase):
    def test_skips_when_balance_is_stale(self):
        now = datetime(2026, 9, 29, 15, 40, tzinfo=KST)
        sleeps, inserts, msgs, trader = _run_daily_balance_snapshot_once(
            now, balance={"cash": 300_000, "total": 0, "stale": True},
        )
        self.assertEqual(inserts, [])
        self.assertEqual(sleeps, [60])  # 1분 뒤 재시도
        self.assertEqual(msgs, [])  # 창 안에서는 경고하지 않는다
        self.assertEqual(trader._balance_snapshot_failed_for, now.date())

    def test_skips_when_total_is_zero(self):
        now = datetime(2026, 9, 29, 15, 40, tzinfo=KST)
        sleeps, inserts, _, _ = _run_daily_balance_snapshot_once(
            now, balance={"cash": 300_000, "total": 0},
        )
        self.assertEqual(inserts, [])
        self.assertEqual(sleeps, [60])

    def test_balance_exception_retries_inside_window(self):
        now = datetime(2026, 9, 29, 15, 40, tzinfo=KST)
        sleeps, inserts, _, trader = _run_daily_balance_snapshot_once(
            now, balance_side_effect=RuntimeError("KIS 타임아웃"),
        )
        self.assertEqual(inserts, [])
        self.assertEqual(sleeps, [60])
        self.assertIsNone(getattr(trader, "_balance_snapshot_handled_for", None))


class TestDailyBalanceSnapshotOutsideWindow(unittest.TestCase):
    def test_before_window_does_not_check_balance(self):
        now = datetime(2026, 9, 29, 14, 0, tzinfo=KST)  # 화요일 14:00, 창 전
        sleeps, inserts, msgs, trader = _run_daily_balance_snapshot_once(now)

        self.assertEqual(inserts, [])
        self.assertEqual(msgs, [])
        self.assertEqual(len(sleeps), 1)
        self.assertIsNone(getattr(trader, "_balance_snapshot_handled_for", None))

    def test_after_window_without_attempt_gives_up_silently(self):
        """창 이후에 늦게 들어온 날(재시작 등)은 시도 기록이 없으므로 경고 없이 넘긴다."""
        now = datetime(2026, 9, 29, 17, 0, tzinfo=KST)
        sleeps, inserts, msgs, trader = _run_daily_balance_snapshot_once(now)

        self.assertEqual(inserts, [])
        self.assertEqual(msgs, [])
        self.assertEqual(len(sleeps), 1)
        self.assertEqual(trader._balance_snapshot_handled_for, now.date())


class TestDailyBalanceSnapshotFailureWarning(unittest.TestCase):
    def test_warns_once_when_window_ended_after_failures(self):
        now = datetime(2026, 9, 29, 16, 31, tzinfo=KST)  # 창 종료 직후
        sleeps, inserts, msgs, trader = _run_daily_balance_snapshot_once(now, failed_for=now.date())

        self.assertEqual(inserts, [])
        self.assertEqual(len(msgs), 1)
        self.assertIn("2026-09-29", msgs[0])
        self.assertIn("이전 실패", msgs[0])
        self.assertEqual(trader._balance_snapshot_warned_for, now.date())
        self.assertEqual(trader._balance_snapshot_handled_for, now.date())

    def test_does_not_warn_twice_same_day(self):
        now = datetime(2026, 9, 29, 16, 40, tzinfo=KST)
        _, _, msgs, _ = _run_daily_balance_snapshot_once(
            now, failed_for=now.date(), warned_for=now.date(),
        )
        self.assertEqual(msgs, [])

    def test_no_warning_when_recorded(self):
        now = datetime(2026, 9, 29, 15, 40, tzinfo=KST)
        _, inserts, msgs, trader = _run_daily_balance_snapshot_once(now, failed_for=now.date())
        self.assertEqual(len(inserts), 1)
        self.assertEqual(msgs, [])
        self.assertIsNone(trader._balance_snapshot_failed_for)  # 성공 시 실패 표시 해제


class TestDailyBalanceSnapshotNonTradingDays(unittest.TestCase):
    def test_saturday_is_skipped(self):
        now = datetime(2026, 10, 3, 15, 40, tzinfo=KST)  # 토요일
        self.assertEqual(now.weekday(), 5)
        sleeps, inserts, _, _ = _run_daily_balance_snapshot_once(now)
        self.assertEqual(inserts, [])
        self.assertEqual(len(sleeps), 1)

    def test_sunday_is_skipped(self):
        now = datetime(2026, 10, 4, 15, 40, tzinfo=KST)  # 일요일
        self.assertEqual(now.weekday(), 6)
        sleeps, inserts, _, _ = _run_daily_balance_snapshot_once(now)
        self.assertEqual(inserts, [])
        self.assertEqual(len(sleeps), 1)

    def test_weekday_holiday_is_skipped(self):
        """평일 휴장일(2026-10-09 한글날)은 총자산 조회가 되더라도 기록하지 않는다."""
        now = datetime(2026, 10, 9, 15, 40, tzinfo=KST)  # 금요일, 한글날
        self.assertEqual(now.weekday(), 4)
        sleeps, inserts, msgs, _ = _run_daily_balance_snapshot_once(now)
        self.assertEqual(inserts, [])
        self.assertEqual(msgs, [])
        self.assertEqual(len(sleeps), 1)

    def test_trading_day_flag_is_used(self):
        now = datetime(2026, 9, 29, 15, 40, tzinfo=KST)
        _, inserts, _, _ = _run_daily_balance_snapshot_once(now, trading_day=False)
        self.assertEqual(inserts, [])


class TestDailyBalanceSnapshotExceptions(unittest.TestCase):
    def test_exception_in_insert_does_not_propagate(self):
        now = datetime(2026, 9, 29, 15, 40, tzinfo=KST)
        sleeps, inserts, _, _ = _run_daily_balance_snapshot_once(
            now, insert_side_effect=RuntimeError("DB 쓰기 실패"),
        )
        self.assertEqual(inserts, [])
        self.assertEqual(len(sleeps), 1)


if __name__ == "__main__":
    unittest.main()
