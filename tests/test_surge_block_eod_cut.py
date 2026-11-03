"""[AT] feat/protective-rules 7·8 — 급등주 진입 차단(surge_block), 마감 전 손실 정리."""
import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "stock_trader"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from common import protective_rules as pr  # noqa: E402
from common.config import config  # noqa: E402
from stark import execution_guard  # noqa: E402
from tests.test_execution_guard import FakePool, FakeRedis, make_decision, make_signal  # noqa: E402
from tests.test_single_stock_cap import _positions_fn  # noqa: E402
from tests import test_stock_trader_stop_loss  # noqa: F401,E402  (의존성 스텁)
import main as st  # noqa: E402

KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).date()


def dates(n):
    """오늘 이전 n개의 거래일(오름차순). 신선도 검사를 통과하도록 직전 거래일부터 센다."""
    out, d = [], TODAY
    while len(out) < n:
        d -= timedelta(days=1)
        if pr.is_trading_day(d):
            out.append(d)
    return list(reversed(out))


class TestConfig(unittest.TestCase):
    def test_defaults(self):
        self.assertEqual(config.SURGE_5D_BLOCK_PCT, 10.0)
        self.assertEqual(config.SURGE_1D_BLOCK_PCT, 7.0)
        self.assertEqual(config.EOD_CUT_PCT, -3.0)
        self.assertEqual(config.EOD_CUT_TIME_HHMM, "15:20")
        self.assertEqual(pr.eod_cut_time().hour, 15)
        self.assertEqual(pr.eod_cut_time().minute, 20)

    def test_env_override(self):
        import os
        import subprocess
        env = dict(os.environ, SURGE_5D_BLOCK_PCT="12", SURGE_1D_BLOCK_PCT="5",
                   EOD_CUT_PCT="-2.5", EOD_CUT_TIME_HHMM="15:10")
        out = subprocess.run(
            [sys.executable, "-c",
             "from common.config import config as c;"
             "print(c.SURGE_5D_BLOCK_PCT, c.SURGE_1D_BLOCK_PCT, c.EOD_CUT_PCT, c.EOD_CUT_TIME_HHMM)"],
            cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, check=True).stdout.split()
        self.assertEqual(out, ["12.0", "5.0", "-2.5", "15:10"])


class TestSurgePure(unittest.TestCase):
    BASE = [100, 100, 100, 100, 100, 100]  # base[-5]=100, base[-1]=100

    def test_pcts(self):
        self.assertEqual(pr.surge_pcts(self.BASE, 110), (10.0, 10.0))
        p5, p1 = pr.surge_pcts([100, 90, 90, 90, 90, 100], 105)
        self.assertAlmostEqual(p5, 16.6666, places=2)   # base[-5]=90
        self.assertAlmostEqual(p1, 5.0)

    def test_insufficient_data(self):
        self.assertEqual(pr.surge_pcts([], 100), (None, None))
        self.assertEqual(pr.surge_pcts([100, 100, 100], 120), (None, 20.0))   # 5일 값만 없음
        self.assertEqual(pr.surge_pcts(self.BASE, 0), (None, None))
        self.assertEqual(pr.surge_pcts(self.BASE, "x"), (None, None))

    def test_thresholds_boundary(self):
        self.assertIsNotNone(pr.evaluate_surge(10.0, 0.0))        # 경계 포함(>=)
        self.assertIsNone(pr.evaluate_surge(9.99, 6.99))
        self.assertIsNotNone(pr.evaluate_surge(0.0, 7.0))
        self.assertIsNone(pr.evaluate_surge(None, None))          # fail-open
        self.assertIsNotNone(pr.evaluate_surge(None, 8.0))        # 한쪽만 있어도 판정
        self.assertIsNone(pr.evaluate_surge(10.0, 7.0, th5=11, th1=8))  # 인자 우선
        self.assertEqual(pr.evaluate_surge(15, 8)["blocked"], "surge_block")

    def test_prior_closes_drops_today_bar(self):
        self.assertEqual(pr.prior_closes([1, 2, 3], TODAY, TODAY), [1, 2])
        self.assertEqual(pr.prior_closes([1, 2, 3], TODAY - timedelta(days=1), TODAY), [1, 2, 3])
        self.assertEqual(pr.prior_closes([1, 2, 3], None, TODAY), [1, 2, 3])


class TestCheckSurgeBlock(unittest.IsolatedAsyncioTestCase):
    async def _check(self, closes, price, ds=None, **kw):
        n = len(closes)
        pool = FakePool(daily_closes={"005930": closes}, daily_close_dates={"005930": ds or dates(n)} if closes else None)
        return await execution_guard.check_surge_block("005930", price, pool=pool, **kw)

    async def test_blocks_on_5d(self):
        r = await self._check([100, 100, 100, 100, 100, 100, 100], 110)  # 5일 +10%
        self.assertEqual(r["blocked"], "surge_block")
        self.assertTrue(r["reason"].startswith("surge_block"))

    async def test_blocks_on_1d_only(self):
        # 5일 전 대비는 +3%, 직전 종가 대비 +7%
        r = await self._check([104, 104, 104, 104, 104, 104, 100], 107)
        self.assertEqual(r["blocked"], "surge_block")

    async def test_passes_below_thresholds(self):
        self.assertIsNone(await self._check([100] * 7, 106.9))

    async def test_excludes_today_bar(self):
        # 오늘 봉(장중 가격 120)이 DB에 있어도 기준은 어제까지: 현재가 105면 통과해야 한다
        closes = [100] * 6 + [120]
        ds = dates(6) + [TODAY]
        self.assertIsNone(await self._check(closes, 105, ds=ds))

    async def test_fail_open_no_or_short_data(self):
        with patch.object(execution_guard, "_fetch_kis_bars_for_surge", AsyncMock(return_value=([], None))):
            self.assertIsNone(await self._check([], 200))
            self.assertIsNone(await self._check([100, 100], 200))   # 5봉 미만이면 KIS 대체 조회(빈 값) -> 판단 불가 -> 통과

    async def test_fail_open_db_error_and_kis_fallback(self):
        pool = FakePool(raise_on_daily_closes=True)
        with patch.object(execution_guard, "_fetch_kis_bars_for_surge", AsyncMock(return_value=([], None))):
            self.assertIsNone(await execution_guard.check_surge_block("005930", 200, pool=pool))
        with patch.object(execution_guard, "_fetch_kis_bars_for_surge",
                          AsyncMock(return_value=([100] * 7, TODAY - timedelta(days=1)))):
            r = await execution_guard.check_surge_block("005930", 112, pool=pool)
            self.assertEqual(r["blocked"], "surge_block")

    async def test_threshold_override(self):
        self.assertIsNone(await self._check([100] * 7, 110, th5=11, th1=11))


class _Env:
    def __init__(self, closes):
        self.orders, self.sent, self.journal = [], [], []
        self.pool = FakePool(daily_closes={"005930": closes}, daily_close_dates={"005930": dates(len(closes))})
        self.redis = FakeRedis()

    async def run(self, price):
        async def kis_order(sym, p, qty, is_buy):
            self.orders.append(qty)
            return {"success": True}

        async def tg(text):
            self.sent.append(text)

        async def jr(*a, **k):
            self.journal.append(a)

        async def name(sym):
            return "삼성전자"
        execution_guard.reset_buy_lock()
        with patch.object(pr, "realized_pnl_today", AsyncMock(return_value=0.0)):
            return await execution_guard.execute(
                make_signal(price=price, qty=1), make_decision(), pool=self.pool, redis=self.redis,
                kis_order_fn=kis_order, send_telegram_fn=tg, log_journal_fn=jr, save_trade_memory_fn=None,
                code_to_name_fn=name, get_positions_fn=_positions_fn(10_000_000))


class TestExecuteSurge(unittest.IsolatedAsyncioTestCase):
    async def test_surge_blocks_buy_and_records(self):
        e = _Env([100] * 7)
        r = await e.run(112)
        self.assertEqual(r["blocked"], "surge_block")
        self.assertFalse(r["executed"])
        self.assertEqual(e.orders, [])
        self.assertTrue(any(isinstance(a, str) and a.startswith("surge_block") for a in e.pool._conn.decisions[0]))
        self.assertEqual(e.journal[0][6], "SKIP")

    async def test_no_surge_buys(self):
        e = _Env([100] * 7)
        r = await e.run(103)
        self.assertNotEqual(r.get("blocked"), "surge_block")
        self.assertEqual(e.orders, [1])

    async def test_no_data_buys(self):
        e = _Env([])
        with patch.object(execution_guard, "_fetch_kis_bars_for_surge", AsyncMock(return_value=([], None))):
            r = await e.run(500)
        self.assertNotEqual(r.get("blocked"), "surge_block")
        self.assertEqual(e.orders, [1])


class TestEodHelpers(unittest.TestCase):
    def test_bought_within_one_trading_day(self):
        mon = date(2026, 11, 2)   # 월요일
        fri = datetime(2026, 10, 30, 10, 0, tzinfo=KST)
        thu = datetime(2026, 10, 29, 10, 0, tzinfo=KST)
        self.assertTrue(pr.bought_within_one_trading_day(datetime(2026, 11, 2, 9, 30, tzinfo=KST), mon))
        self.assertTrue(pr.bought_within_one_trading_day(fri, mon))        # 주말 건너 직전 거래일
        self.assertFalse(pr.bought_within_one_trading_day(thu, mon))
        self.assertFalse(pr.bought_within_one_trading_day(None, mon))
        # UTC로 저장된 시각도 KST 날짜로 변환해 비교(10/30 01:00 UTC = 10/30 10:00 KST, 직전 거래일)
        self.assertTrue(pr.bought_within_one_trading_day(datetime(2026, 10, 30, 1, 0, tzinfo=timezone.utc), mon))


class FakeRedisClient:
    def __init__(self):
        self.store = {}

    async def get(self, k):
        return self.store.get(k)

    async def setex(self, k, ttl, v):
        self.store[k] = v

    async def delete(self, *ks):
        for k in ks:
            self.store.pop(k, None)


def _pos(sym="005930", avg=10000, cur=9600, qty=10):
    return {"symbol": sym, "name": "테스트", "avg_price": avg, "cur_price": cur, "qty": qty, "sellable_qty": qty}


class TestEodLossCut(unittest.IsolatedAsyncioTestCase):
    NOW = datetime(2026, 11, 2, 15, 21, tzinfo=KST)

    def _bot(self, positions, last_buy, sell_result=None):
        bot = st.StockTrader.__new__(st.StockTrader)
        bot.positions = {p["symbol"]: p for p in positions}
        bot._selling = set()
        bot._eod_cut_date = None
        bot.trader = MagicMock()
        bot.trader.sell = AsyncMock(return_value=sell_result or {"success": True, "order_no": "1", "filled_qty": 10,
                                                                  "avg_fill_price": 9600.0})
        bot._invalidate_position_cache = AsyncMock()

        class Conn:
            async def fetchval(s, q, *a):
                if isinstance(last_buy, Exception):
                    raise last_buy
                return last_buy[a[0]] if isinstance(last_buy, dict) else last_buy

        class Acq:
            async def __aenter__(s):
                return Conn()

            async def __aexit__(s, *e):
                return False

        class Pool:
            def acquire(s):
                return Acq()
        self.pool = Pool()
        return bot

    async def _run(self, bot, now=None):
        self.redis = FakeRedisClient()
        self.sent = AsyncMock()
        self.insert = AsyncMock()
        self.reg = AsyncMock()
        with patch.object(st.db, "pool", self.pool, create=True), \
                patch.object(st.cache, "client", self.redis), \
                patch.object(st.db, "insert_trade", self.insert), \
                patch.object(st, "register_partial_fill", self.reg), \
                patch("common.telegram.send_stock", self.sent):
            await bot._eod_loss_cut(now or self.NOW)

    BOUGHT_TODAY = datetime(2026, 11, 2, 10, 0, tzinfo=KST)

    async def test_sells_loss_within_one_day_with_label(self):
        bot = self._bot([_pos(cur=9700)], self.BOUGHT_TODAY,          # -3.0% 경계
                        {"success": True, "order_no": "1", "filled_qty": 10, "avg_fill_price": 9700.0})
        await self._run(bot)
        bot.trader.sell.assert_awaited_once()
        self.assertEqual(bot.trader.sell.await_args.kwargs["strategy"], "마감전손실정리")
        self.assertEqual(bot.trader.sell.await_args.args[2], 10)       # 전량
        kw = self.insert.await_args.kwargs
        self.assertEqual((kw["side"], kw["strategy"], kw["quantity"]), ("SELL", "마감전손실정리", 10))
        self.assertEqual(kw["pnl"], -3000)
        self.reg.assert_not_awaited()
        self.assertNotIn("005930", bot.positions)

    async def test_not_sold_when_loss_smaller_than_cut(self):
        bot = self._bot([_pos(cur=9701)], self.BOUGHT_TODAY)           # -2.99%
        await self._run(bot)
        bot.trader.sell.assert_not_awaited()

    async def test_not_sold_when_bought_earlier_or_unknown(self):
        old = datetime(2026, 10, 28, 10, 0, tzinfo=KST)
        for lb in (old, None, RuntimeError("db")):
            bot = self._bot([_pos(cur=9000)], lb)
            await self._run(bot)
            bot.trader.sell.assert_not_awaited()

    async def test_not_before_cut_time_and_once_per_day(self):
        bot = self._bot([_pos(cur=9000)], self.BOUGHT_TODAY)
        await self._run(bot, now=datetime(2026, 11, 2, 15, 19, 59, tzinfo=KST))
        bot.trader.sell.assert_not_awaited()
        await self._run(bot)
        self.assertEqual(bot.trader.sell.await_count, 1)
        bot.positions = {"005930": _pos(cur=9000)}
        await bot._eod_loss_cut(self.NOW)       # 같은 날 재호출: 무시
        self.assertEqual(bot.trader.sell.await_count, 1)

    async def test_done_key_in_redis_prevents_rerun_after_restart(self):
        bot = self._bot([_pos(cur=9000)], self.BOUGHT_TODAY)
        self.redis = FakeRedisClient()
        self.redis.store["eod_cut_done:2026-11-02"] = "1"
        with patch.object(st.db, "pool", self.pool, create=True), patch.object(st.cache, "client", self.redis):
            await bot._eod_loss_cut(self.NOW)
        bot.trader.sell.assert_not_awaited()

    async def test_partial_fill_registers_tracker_and_keeps_remainder(self):
        bot = self._bot([_pos(cur=9000)], self.BOUGHT_TODAY,
                        {"success": True, "order_no": "77", "filled_qty": 6, "avg_fill_price": 9000.0, "partial": True})
        await self._run(bot)
        self.reg.assert_awaited_once()
        kw = self.reg.await_args.kwargs
        self.assertEqual((kw["order_no"], kw["side"], kw["requested_qty"], kw["recorded_qty"], kw["strategy"]),
                         ("77", "SELL", 10, 6, "마감전손실정리"))
        self.assertEqual(self.insert.await_args.kwargs["quantity"], 6)
        self.assertEqual(bot.positions["005930"]["qty"], 4)

    async def test_failure_only_alerts_no_retry_no_trade_record(self):
        bot = self._bot([_pos(cur=9000), _pos("000660", cur=9000)], self.BOUGHT_TODAY)
        bot.trader.sell = AsyncMock(return_value={"success": False, "error": "거부"})
        await self._run(bot)
        self.assertEqual(bot.trader.sell.await_count, 2)               # 종목당 1회, 재시도 없음
        self.insert.assert_not_awaited()
        self.assertEqual(self.sent.await_count, 2)
        self.assertIn("실패", self.sent.await_args.args[0])
        self.assertEqual(bot._selling, set())
        self.assertIn("005930", bot.positions)

    async def test_exception_in_sell_alerts_and_continues(self):
        bot = self._bot([_pos(cur=9000), _pos("000660", cur=9000)], self.BOUGHT_TODAY)
        bot.trader.sell = AsyncMock(side_effect=[RuntimeError("boom"), {"success": True, "filled_qty": 10}])
        await self._run(bot)
        self.assertEqual(bot.trader.sell.await_count, 2)

    async def test_pending_is_left_to_reconcile_loop(self):
        bot = self._bot([_pos(cur=9000)], self.BOUGHT_TODAY, {"success": False, "pending": True, "order_no": "9"})
        await self._run(bot)
        self.insert.assert_not_awaited()
        self.sent.assert_not_awaited()                                 # 실패 알림 아님

    async def test_skips_symbol_being_sold_and_zero_qty(self):
        bot = self._bot([_pos(cur=9000), _pos("000660", cur=9000, qty=0)], self.BOUGHT_TODAY)
        bot._selling.add("005930")
        await self._run(bot)
        bot.trader.sell.assert_not_awaited()

    def test_wired_into_run_cycle(self):
        src = (REPO_ROOT / "stock_trader" / "main.py").read_text(encoding="utf-8")
        self.assertIn("await self._eod_loss_cut(", src)

    async def test_enabled_switch_default_false_skips_call(self):
        """EOD_CUT_ENABLED 기본 false -> _run_cycle이 _eod_loss_cut을 호출하지 않고, true면 호출한다."""
        self.assertFalse(config.EOD_CUT_ENABLED)
        for enabled, expected in ((False, 0), (True, 1)):
            bot = st.StockTrader.__new__(st.StockTrader)
            bot.strategies = {}
            bot._load_positions = AsyncMock(return_value=True)
            bot.get_all_active_strategies = lambda: [("MA크로스", {"stop_loss": -7, "max_positions": 0})]
            bot.build_strategy = lambda n, p: None
            bot.positions = {}
            bot._eod_loss_cut = AsyncMock()
            with patch.object(config, "EOD_CUT_ENABLED", enabled):
                await bot._run_cycle()
            self.assertEqual(bot._eod_loss_cut.await_count, expected)


if __name__ == "__main__":
    unittest.main()
