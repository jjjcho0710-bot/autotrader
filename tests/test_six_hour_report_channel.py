"""
stock_trader/main.py StockTrader._six_hour_report / _format_six_hour_report 단위 테스트
([AT] feat/six-hour-report-readable):
  - 제목·DB 조회 구간은 sleep이 끝난 뒤의 시각 기준([전송 시각-6시간, 전송 시각])으로 통일한다
    (이전 버그: sleep 전 루프 시작 시각을 제목에 썼다).
  - 체결 내역에 종목명(없으면 코드)·시각(KST)·수량·체결가를 보여주고, 매도는 손익률을 표시한다
    (pnl이 NULL이면 손익률 생략). 10건을 넘으면 앞 10건만 보이고 "외 N건"을 붙인다.
  - 보유 목록은 손익률 높은 순으로 전체를 보여준다(이름과 %만).
  - 채널 메시지에는 예수금·총자산·평가손익·거래 총액 같은 금액을 넣지 않는다(PM 승인).
  - 누적 수익률은 total이 양수이고 stale이 아닐 때만 표시하고, 아니면 줄 자체를 생략한다.
  - 손절선 줄은 strategy_config.stop_loss를 읽어 쓰고, 보유 중 가장 낮은 손익률이 0 이상이면 생략한다.
  - 거래 0건이고 그 구간에 평일 장시간이 전혀 포함되지 않으면(00:00·06:00 전송분, 주말)
    전송 자체를 건너뛴다(SIX_HOUR_REPORT_SKIP_IDLE로 끌 수 있음).
  - 메시지는 4096자를 넘지 않도록 체결 건수를 줄여서 맞춘다.
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
    """asyncpg/redis/aiohttp 가 없는 테스트 환경에서도 import 만 되도록 더미로 대체한다."""
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
from common.config import config  # noqa: E402
from main import StockTrader  # noqa: E402

KST = timezone(timedelta(hours=9))


def _dt(h, m, day=1):
    """2026-10-<day> h:m KST. 1일=목, 2일=금, 3일=토, 4일=일."""
    return datetime(2026, 10, day, h, m, tzinfo=KST)


# ───────────────────────── _format_six_hour_report (순수 함수) ─────────────────────────

class TestFormatSixHourReport(unittest.TestCase):
    def test_title_window_and_trade_display(self):
        """(a) 제목 구간, 종목명·시각·수량·체결가 표시. 이름이 없으면 코드로 대체."""
        trades = [
            {"side": "SELL", "name": "휴림로봇", "price": 6260, "quantity": 100, "pnl": 62000,
             "created_at": _dt(13, 25)},
            {"side": "BUY", "name": "090710", "price": 160700, "quantity": 2, "pnl": None,
             "created_at": _dt(14, 2)},
        ]
        msg = StockTrader._format_six_hour_report(_dt(12, 0), _dt(18, 0), trades, [], None, None)
        self.assertIn("📊 주식 6시간 리포트 (10/01 12:00~18:00)", msg)
        self.assertIn("체결: 매수 1건 / 매도 1건", msg)
        self.assertIn("13:25 휴림로봇 매도 100주 @ 6,260원", msg)
        self.assertIn("14:02 090710 매수 2주 @ 160,700원", msg)  # 이름 없음(코드) → 코드로 대체

    def test_trades_shown_oldest_first_regardless_of_input_order(self):
        """입력 순서가 섞여 있어도 시간순(오래된 것부터)으로 다시 정렬해 보여준다."""
        trades = [
            {"side": "BUY", "name": "B", "price": 1000, "quantity": 1, "pnl": None,
             "created_at": _dt(14, 0)},
            {"side": "BUY", "name": "A", "price": 1000, "quantity": 1, "pnl": None,
             "created_at": _dt(10, 0)},
        ]
        msg = StockTrader._format_six_hour_report(_dt(6, 0), _dt(18, 0), trades, [], None, None)
        self.assertLess(msg.index("10:00 A"), msg.index("14:00 B"))

    def test_sell_pnl_rate_and_null_omitted(self):
        """(b) 매도 손익률 = pnl/(price*qty-pnl)*100, pnl이 NULL이면 % 생략."""
        trades = [
            {"side": "SELL", "name": "A", "price": 10000, "quantity": 10, "pnl": 10000,
             "created_at": _dt(10, 0)},  # 매도금액 100,000, 원가 90,000 → +11.1%
            {"side": "SELL", "name": "B", "price": 5000, "quantity": 4, "pnl": None,
             "created_at": _dt(10, 5)},
        ]
        msg = StockTrader._format_six_hour_report(_dt(6, 0), _dt(12, 0), trades, [], None, None)
        self.assertIn("A 매도 10주 @ 10,000원 (+11.1%)", msg)
        self.assertIn("B 매도 4주 @ 5,000원", msg)
        self.assertNotIn("B 매도 4주 @ 5,000원 (", msg)  # pnl NULL → % 없음

    def test_more_than_ten_trades_shows_extra_count(self):
        """(c) 11건 이상이면 앞 10건만 보여주고 '외 N건'."""
        trades = [
            {"side": "BUY", "name": f"종목{i}", "price": 1000, "quantity": 1, "pnl": None,
             "created_at": _dt(9, i)}
            for i in range(11)
        ]
        msg = StockTrader._format_six_hour_report(_dt(6, 0), _dt(12, 0), trades, [], None, None)
        self.assertIn("종목0", msg)
        self.assertIn("종목9", msg)
        self.assertNotIn("종목10", msg)
        self.assertIn("외 1건", msg)

    def test_no_money_amount_phrases(self):
        """(d) 예수금·총자산·평가손익·거래 총액 문구/금액이 전혀 없어야 한다."""
        trades = [{"side": "SELL", "name": "A", "price": 10000, "quantity": 10, "pnl": 10000,
                   "created_at": _dt(10, 0)}]
        positions = [{"name": "A", "pnl_rate": 2.0}]
        msg = StockTrader._format_six_hour_report(_dt(6, 0), _dt(12, 0), trades, positions, 1.23, -7.0)
        for forbidden in ("예수금", "총자산", "평가손익", "거래 총액", "100,000원"):
            self.assertNotIn(forbidden, msg)

    def test_cum_rate_omitted_when_none_shown_when_given(self):
        """(e) cum_rate가 None이면(stale/집계 시작 전) '누적 수익률' 줄 자체를 생략."""
        msg_none = StockTrader._format_six_hour_report(_dt(6, 0), _dt(12, 0), [], [], None, None)
        self.assertNotIn("누적 수익률", msg_none)
        msg_val = StockTrader._format_six_hour_report(_dt(6, 0), _dt(12, 0), [], [], -0.91, None)
        self.assertIn("누적 수익률 -0.91%", msg_val)

    def test_stop_loss_line_and_omitted_when_non_negative(self):
        """(f) 최저 손익률 종목·현재 stop_loss 값을 보여주고, 최저 손익률이 0 이상이면 생략."""
        positions_neg = [{"name": "에스엠벡셀", "pnl_rate": -1.7}, {"name": "셀트리온", "pnl_rate": 2.7}]
        msg = StockTrader._format_six_hour_report(_dt(6, 0), _dt(12, 0), [], positions_neg, None, -7.0)
        self.assertIn("⚠️ 손절선(-7%)까지 가장 가까운 종목: 에스엠벡셀 -1.7%", msg)

        positions_pos = [{"name": "A", "pnl_rate": 0.0}, {"name": "B", "pnl_rate": 2.0}]
        msg2 = StockTrader._format_six_hour_report(_dt(6, 0), _dt(12, 0), [], positions_pos, None, -7.0)
        self.assertNotIn("손절선", msg2)

        # 보유 종목이 없으면 stop_loss가 있어도 손절선 줄 자체가 없다
        msg3 = StockTrader._format_six_hour_report(_dt(6, 0), _dt(12, 0), [], [], None, -7.0)
        self.assertNotIn("손절선", msg3)

    def test_positions_sorted_desc_by_pnl_rate(self):
        """(4) 보유 목록은 손익률 높은 순 전체(이름·%만)."""
        positions = [{"name": "C", "pnl_rate": -1.0}, {"name": "A", "pnl_rate": 2.7},
                     {"name": "B", "pnl_rate": 1.5}]
        msg = StockTrader._format_six_hour_report(_dt(6, 0), _dt(12, 0), [], positions, None, None)
        self.assertIn("A +2.7% · B +1.5% · C -1.0%", msg)
        self.assertIn("보유 3종목 (손익률 순)", msg)

    def test_max_trades_param_trims_display(self):
        """req 8: max_trades로 표시 건수를 더 줄일 수 있다(4096자 초과 시 호출부가 사용)."""
        trades = [
            {"side": "BUY", "name": f"종목{i}", "price": 1000, "quantity": 1, "pnl": None,
             "created_at": _dt(9, i % 59)}
            for i in range(5)
        ]
        msg = StockTrader._format_six_hour_report(
            _dt(6, 0), _dt(12, 0), trades, [], None, None, max_trades=2)
        self.assertIn("외 3건", msg)
        self.assertIn("종목0", msg)
        self.assertNotIn("종목2", msg)


# ───────────────────────── _six_hour_window_has_no_market_overlap ─────────────────────────

class TestSixHourWindowHasNoMarketOverlap(unittest.TestCase):
    def test_weekday_slot_without_market_hours(self):
        """00:00~06:00 평일: 장시간 미포함."""
        self.assertTrue(StockTrader._six_hour_window_has_no_market_overlap(_dt(0, 0), _dt(6, 0)))

    def test_weekday_slot_with_market_hours(self):
        """06:00~12:00 평일: 09:00 장시작이 포함됨."""
        self.assertFalse(StockTrader._six_hour_window_has_no_market_overlap(_dt(6, 0), _dt(12, 0)))

    def test_weekend_slot_that_would_normally_contain_market_hours(self):
        """2026-10-03은 토요일. 12:00~18:00 구간이라도 주말이면 장시간 미포함으로 본다."""
        self.assertTrue(StockTrader._six_hour_window_has_no_market_overlap(
            _dt(12, 0, day=3), _dt(18, 0, day=3)))

    def test_midnight_slot_crossing_friday_into_saturday(self):
        """금요일 18:00 ~ 토요일 00:00(00:00 전송분, 요일 경계 포함)도 장시간 미포함."""
        self.assertTrue(StockTrader._six_hour_window_has_no_market_overlap(
            _dt(18, 0, day=2), _dt(0, 0, day=3)))


# ───────────────────────── _six_hour_report (통합) ─────────────────────────

class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, trades):
        self._trades = trades
        self.calls = []

    async def fetch(self, query, *args):
        self.calls.append(args)
        return self._trades


class _FakePool:
    def __init__(self, trades):
        self._conn = _FakeConn(trades)

    def acquire(self):
        return _Acquire(self._conn)


class _FakeNow:
    """datetime.now(tz)를 호출할 때마다 미리 정해둔 값을 순서대로 반환한다(마지막 값은 반복)."""

    def __init__(self, seq):
        self._seq = list(seq)

    def now(self, tz=None):
        if len(self._seq) > 1:
            return self._seq.pop(0)
        return self._seq[0]


def _make_trader(positions=None, strategies=None):
    trader = StockTrader.__new__(StockTrader)
    trader.running = True
    trader.positions = positions if positions is not None else {}
    trader.strategies = strategies if strategies is not None else {
        "MA크로스": {"is_active": True, "params": {"stop_loss": -7}}
    }
    return trader


def _run_six_hour_report(trades, send_report, now_seq, balance=None, positions=None,
                          strategies=None, stop_after_send=True):
    """_six_hour_report 루프를 한 번만 돌린다.
    now_seq: [sleep 이전 now(루프 시작), sleep 이후 now(실제 전송 시각)] — 둘 다 KST-aware.
    stop_after_send=True: send_report 호출 직후 running=False(정상 전송 경로 테스트용).
    stop_after_send=False: sleep 직후 running=False(SKIP_IDLE처럼 전송이 안 될 수도 있는 경로용).
    반환값: 검증용으로 fake pool(= DB 조회 호출 인자 확인 가능)을 돌려준다.
    """
    trader = _make_trader(positions=positions, strategies=strategies)
    balance_result = balance if balance is not None else {"cash": 1_000_000, "total": 11_000_000}

    class _Broker:
        async def get_balance(self):
            return balance_result

    trader.trader = _Broker()
    fake_pool = _FakePool(trades)

    async def _fake_sleep(_):
        if not stop_after_send:
            trader.running = False

    async def _send_report_and_stop(text):
        await send_report(text)
        trader.running = False

    real_report = StockTrader._six_hour_report
    report_fn = _send_report_and_stop if stop_after_send else send_report

    async def _drive():
        with mock.patch.object(stock_main, "datetime", _FakeNow(now_seq), create=False), \
             mock.patch.object(stock_main.db, "pool", fake_pool, create=True), \
             mock.patch.object(stock_main.asyncio, "sleep", _fake_sleep), \
             mock.patch("common.telegram.send_report", report_fn), \
             mock.patch.object(config, "INITIAL_SEED_KRW", 10_000_000):
            await real_report(trader)

    asyncio.run(_drive())
    return fake_pool


class TestSixHourReportChannelOnly(unittest.TestCase):
    def test_sends_to_channel_only(self):
        """send_report(채널)만 호출된다 — send_stock(개인방) 호출은 코드상 존재하지 않는다."""
        report_calls = []

        async def _send_report(text):
            report_calls.append(text)

        _run_six_hour_report(
            [{"side": "BUY", "name": "삼성전자", "price": 70000, "quantity": 5, "pnl": None,
              "created_at": _dt(17, 30)}],
            _send_report, now_seq=[_dt(17, 55), _dt(18, 0)])

        self.assertEqual(len(report_calls), 1)
        self.assertIn("주식 6시간 리포트", report_calls[0])
        self.assertIn("삼성전자", report_calls[0])

    def test_report_has_no_money_amounts_end_to_end(self):
        """(d) 실제 루프를 돌려도 금액 문구/거래총액이 없어야 한다."""
        report_calls = []

        async def _send_report(text):
            report_calls.append(text)

        _run_six_hour_report(
            [{"side": "BUY", "name": "삼성전자", "price": 70000, "quantity": 5, "pnl": None,
              "created_at": _dt(17, 30)}],
            _send_report, now_seq=[_dt(17, 55), _dt(18, 0)],
            balance={"cash": 1_000_000, "total": 11_000_000})

        msg = report_calls[0]
        for forbidden in ("예수금", "총자산", "평가손익", "350,000원"):  # 70,000 × 5
            self.assertNotIn(forbidden, msg)
        self.assertIn("누적 수익률 +10.00%", msg)  # (11,000,000-10,000,000)/10,000,000


class TestSixHourReportStaleBalance(unittest.TestCase):
    def test_stale_balance_omits_cumulative_rate_line(self):
        """(e) stale=True면 누적 수익률 줄을 생략한다."""
        report_calls = []

        async def _send_report(text):
            report_calls.append(text)

        _run_six_hour_report([], _send_report, now_seq=[_dt(17, 55), _dt(18, 0)],
                              balance={"cash": 1_000_000, "total": 0, "stale": True})

        self.assertEqual(len(report_calls), 1)
        self.assertNotIn("누적 수익률", report_calls[0])

    def test_fresh_balance_shows_cumulative_rate(self):
        """(e) total이 양수이고 stale이 아니면 누적 수익률을 표시한다."""
        report_calls = []

        async def _send_report(text):
            report_calls.append(text)

        _run_six_hour_report([], _send_report, now_seq=[_dt(17, 55), _dt(18, 0)],
                              balance={"cash": 1_000_000, "total": 5_000_000})

        self.assertEqual(len(report_calls), 1)
        self.assertIn("누적 수익률", report_calls[0])


class TestSixHourReportWindowTiming(unittest.TestCase):
    def test_window_uses_post_sleep_time_not_pre_sleep(self):
        """(g) 제목 구간이 [전송 시각-6시간, 전송 시각]이고 sleep 이전 시각이 아니어야 한다."""
        pre_sleep = _dt(17, 43)
        post_sleep = _dt(18, 0)
        report_calls = []

        async def _send_report(text):
            report_calls.append(text)

        _run_six_hour_report([], _send_report, now_seq=[pre_sleep, post_sleep])

        self.assertEqual(len(report_calls), 1)
        msg = report_calls[0]
        self.assertIn("10/01 12:00~18:00", msg)
        self.assertNotIn("17:43", msg)

    def test_db_query_window_matches_title_window(self):
        """(g) DB 조회 구간도 제목과 같은 [전송 시각-6시간, 전송 시각]이어야 한다."""
        pre_sleep = _dt(17, 43)
        post_sleep = _dt(18, 0)

        async def _send_report(text):
            pass

        pool = _run_six_hour_report([], _send_report, now_seq=[pre_sleep, post_sleep])

        self.assertEqual(len(pool._conn.calls), 1)
        window_start_arg, window_end_arg = pool._conn.calls[0]
        self.assertEqual(window_end_arg, post_sleep)
        self.assertEqual(window_start_arg, post_sleep - timedelta(hours=6))


class TestSixHourReportSkipIdle(unittest.TestCase):
    def test_skip_idle_window_no_trades_sends_nothing(self):
        """(h) 거래 0건 + 장시간 미포함 구간(00:00 전송분)이면 전송을 건너뛴다."""
        report_calls = []

        async def _send_report(text):
            report_calls.append(text)

        _run_six_hour_report(
            [], _send_report,
            now_seq=[_dt(23, 50, day=2), _dt(0, 0, day=3)],  # 금 23:50 → 토 00:00 전송
            stop_after_send=False)

        self.assertEqual(len(report_calls), 0)

    def test_skip_idle_can_be_disabled(self):
        """SIX_HOUR_REPORT_SKIP_IDLE=False면 거래 0건·장시간 미포함 구간이어도 전송한다."""
        report_calls = []

        async def _send_report(text):
            report_calls.append(text)

        with mock.patch.object(StockTrader, "SIX_HOUR_REPORT_SKIP_IDLE", False):
            _run_six_hour_report(
                [], _send_report,
                now_seq=[_dt(23, 50, day=2), _dt(0, 0, day=3)])

        self.assertEqual(len(report_calls), 1)

    def test_market_hours_window_with_no_trades_still_sends(self):
        """거래 0건이어도 장시간이 포함된 구간(예: 12:00~18:00)이면 그대로 전송한다."""
        report_calls = []

        async def _send_report(text):
            report_calls.append(text)

        _run_six_hour_report([], _send_report, now_seq=[_dt(17, 55), _dt(18, 0)])

        self.assertEqual(len(report_calls), 1)


class TestSixHourReportStopLoss(unittest.TestCase):
    def test_stop_loss_default_when_no_active_strategy(self):
        """활성 전략이 없으면 참고값(DEFAULT_STOP_LOSS_PCT, -7%)으로 손절선 줄을 만든다."""
        report_calls = []

        async def _send_report(text):
            report_calls.append(text)

        _run_six_hour_report(
            [], _send_report, now_seq=[_dt(17, 55), _dt(18, 0)],
            positions={"005930": {"name": "삼성전자", "pnl_rate": -1.0}},
            strategies={})

        self.assertIn("손절선(-7%)", report_calls[0])

    def test_stop_loss_reads_strategy_config_value(self):
        """strategy_config에 설정된 stop_loss(예: -5)를 그대로 읽어 쓴다."""
        report_calls = []

        async def _send_report(text):
            report_calls.append(text)

        _run_six_hour_report(
            [], _send_report, now_seq=[_dt(17, 55), _dt(18, 0)],
            positions={"005930": {"name": "삼성전자", "pnl_rate": -1.0}},
            strategies={"MA크로스": {"is_active": True, "params": {"stop_loss": -5}}})

        self.assertIn("손절선(-5%)", report_calls[0])


class TestSixHourReportMessageLength(unittest.TestCase):
    def test_message_trimmed_under_4096_chars(self):
        """req 8: 체결 건수가 많고 종목명이 길어 4096자를 넘기면 체결 건수를 줄여 맞춘다."""
        long_name = "가" * 500
        trades = [
            {"side": "BUY", "name": f"{long_name}{i}", "price": 1000, "quantity": 1, "pnl": None,
             "created_at": _dt(9, i)}
            for i in range(10)
        ]
        report_calls = []

        async def _send_report(text):
            report_calls.append(text)

        _run_six_hour_report(trades, _send_report, now_seq=[_dt(9, 50), _dt(10, 0)])

        self.assertEqual(len(report_calls), 1)
        self.assertLessEqual(len(report_calls[0]), 4096)


class TestSixHourReportDelayedFillTrades(unittest.TestCase):
    """fix/stock-trader-fill-reconcile(PR #70)에서 지연 체결이 확정되면
    strategy="{원래전략}_지연체결확인"으로 trade_history에 기록된다. 6시간 리포트의 DB
    조회는 strategy 컬럼을 아예 쓰지 않으므로(SELECT에도 없음) 이런 행도 다른 체결과
    동일하게 매수/매도 건수·목록에 집계되어야 한다."""

    def test_delayed_fill_confirmed_trade_counted_and_shown_like_normal_fill(self):
        trades = [
            {"side": "SELL", "name": "휴림로봇", "price": 6260, "quantity": 100, "pnl": 62000,
             "created_at": _dt(13, 25)},  # strategy="손절_지연체결확인"으로 기록된 행과 동일한 모양
        ]
        report_calls = []

        async def _send_report(text):
            report_calls.append(text)

        _run_six_hour_report(trades, _send_report, now_seq=[_dt(17, 55), _dt(18, 0)])

        self.assertEqual(len(report_calls), 1)
        msg = report_calls[0]
        self.assertIn("체결: 매수 0건 / 매도 1건", msg)
        self.assertIn("휴림로봇 매도 100주 @ 6,260원", msg)


if __name__ == "__main__":
    unittest.main()
