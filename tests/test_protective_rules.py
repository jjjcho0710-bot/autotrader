"""[AT] feat/protective-rules — 일일 손실 한도, 시장 게이트, 섀도 기록, SELL 라벨."""
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "stock_trader"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from common import protective_rules as pr  # noqa: E402
from common.config import config  # noqa: E402
from stark import execution_guard  # noqa: E402
from tests.test_execution_guard import FakePool, FakeRedis, make_decision, make_signal  # noqa: E402
from tests.test_single_stock_cap import _positions_fn, _failed_positions_fn, _zero_equity_positions_fn  # noqa: E402

KST = timezone(timedelta(hours=9))
EQUITY = 10_000_000


def idx(kospi, kosdaq):
    async def _fn():
        return {"success": True, "data": {"KOSPI": {"change_rate": kospi}, "KOSDAQ": {"change_rate": kosdaq}}}
    return _fn


async def idx_fail():
    return {"success": False, "error": "x"}


async def idx_raise():
    raise RuntimeError("down")


class TestConfigDefaults(unittest.TestCase):
    def test_defaults(self):
        self.assertEqual(config.DAILY_LOSS_LIMIT_PCT, -1.0)
        self.assertEqual(config.MARKET_GATE_PCT, -1.0)


class TestDailyLossLimit(unittest.IsolatedAsyncioTestCase):
    async def _check(self, pnl, positions=None):
        with patch.object(pr, "realized_pnl_today", AsyncMock(return_value=pnl)):
            return await pr.check_daily_loss_limit(FakePool(), positions or _positions_fn(EQUITY))

    async def test_blocks_at_and_below_limit(self):
        self.assertEqual((await self._check(-100_000))["blocked"], "daily_loss_limit")   # 경계(-1.0%)
        self.assertEqual((await self._check(-150_000))["blocked"], "daily_loss_limit")

    async def test_passes_above_limit(self):
        self.assertIsNone(await self._check(-99_999))
        self.assertIsNone(await self._check(0))
        self.assertIsNone(await self._check(50_000))

    async def test_fail_open_when_equity_or_pnl_unknown(self):
        self.assertIsNone(await self._check(-500_000, _failed_positions_fn()))
        self.assertIsNone(await self._check(-500_000, _failed_positions_fn(stale=True)))
        self.assertIsNone(await self._check(-500_000, _failed_positions_fn(raise_exc=True)))
        self.assertIsNone(await self._check(None))

    async def test_limit_overridable_by_arg(self):
        with patch.object(pr, "realized_pnl_today", AsyncMock(return_value=-60_000)):
            self.assertIsNone(await pr.check_daily_loss_limit(FakePool(), _positions_fn(EQUITY)))
            self.assertIsNotNone(await pr.check_daily_loss_limit(FakePool(), _positions_fn(EQUITY), limit_pct=-0.5))


class TestMarketGate(unittest.IsolatedAsyncioTestCase):
    async def test_blocks_only_when_both_at_or_below(self):
        self.assertEqual((await pr.check_market_gate(idx(-1.0, -1.0)))["blocked"], "market_gate")
        self.assertEqual((await pr.check_market_gate(idx(-2.5, -3.1)))["blocked"], "market_gate")
        self.assertIsNone(await pr.check_market_gate(idx(-2.5, -0.9)))
        self.assertIsNone(await pr.check_market_gate(idx(-0.5, -2.0)))
        self.assertIsNone(await pr.check_market_gate(idx(0.3, -1.5)))

    async def test_fail_open(self):
        for fn in (idx_fail, idx_raise, None):
            self.assertIsNone(await pr.check_market_gate(fn))

        async def partial():
            return {"success": True, "data": {"KOSPI": {"change_rate": -3.0}}}
        self.assertIsNone(await pr.check_market_gate(partial))


class TestShadowFeatures(unittest.IsolatedAsyncioTestCase):
    async def test_fields_and_kst(self):
        with patch.object(pr, "realized_pnl_today", AsyncMock(return_value=-1234.0)):
            f = await pr.build_shadow_features(
                FakePool(), idx(-1.2, -2.3), now=datetime(2026, 11, 2, 1, 5, tzinfo=timezone.utc))
        self.assertEqual(f["kst_time"], "10:05")
        self.assertEqual(f["kst_hour_band"], "10:00-10:59")
        self.assertEqual((f["kospi_rate"], f["kosdaq_rate"]), (-1.2, -2.3))
        self.assertEqual(f["realized_pnl_today"], -1234.0)

    async def test_partial_failures_become_none(self):
        with patch.object(pr, "realized_pnl_today", AsyncMock(return_value=None)):
            f = await pr.build_shadow_features(FakePool(), idx_raise)
        self.assertIsNone(f["kospi_rate"])
        self.assertIsNone(f["realized_pnl_today"])
        self.assertIn("kst_time", f)


class _Env:
    def __init__(self):
        self.orders, self.sent, self.journal = [], [], []
        self.pool, self.redis = FakePool(), FakeRedis()

    async def run(self, *, pnl=0.0, index=None, positions=None, signal=None):
        async def kis_order(sym, price, qty, is_buy):
            self.orders.append(qty)
            return {"success": True}

        async def tg(text):
            self.sent.append(text)

        async def jr(*a, **k):
            self.journal.append(a)

        async def name(sym):
            return "삼성전자"
        execution_guard.reset_buy_lock()
        with patch.object(pr, "realized_pnl_today", AsyncMock(return_value=pnl)):
            return await execution_guard.execute(
                signal or make_signal(price=50000, qty=1), make_decision(), pool=self.pool, redis=self.redis,
                kis_order_fn=kis_order, send_telegram_fn=tg, log_journal_fn=jr, save_trade_memory_fn=None,
                code_to_name_fn=name, get_positions_fn=positions or _positions_fn(EQUITY),
                get_market_index_fn=index)


class TestExecuteIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_daily_loss_limit_blocks_records_and_alerts_once_per_day(self):
        e = _Env()
        r = await e.run(pnl=-200_000)
        self.assertFalse(r["executed"])
        self.assertEqual(r["blocked"], "daily_loss_limit")
        self.assertEqual(e.orders, [])
        d = e.pool._conn.decisions
        self.assertEqual(len(d), 1)
        self.assertTrue(any(isinstance(a, str) and a.startswith("daily_loss_limit") for a in d[0]))
        self.assertEqual(e.journal[0][6], "SKIP")
        self.assertEqual(len(e.sent), 1)
        r2 = await e.run(pnl=-200_000)          # 같은 날 두 번째: 차단은 하되 알림은 안 보냄
        self.assertEqual(r2["blocked"], "daily_loss_limit")
        self.assertEqual(len(e.sent), 1)

    async def test_market_gate_blocks(self):
        e = _Env()
        r = await e.run(index=idx(-1.5, -2.0))
        self.assertEqual(r["blocked"], "market_gate")
        self.assertEqual(e.orders, [])
        self.assertTrue(any("market_gate" in str(a) for a in e.pool._conn.decisions[0]))

    async def test_loss_limit_checked_before_market_gate(self):
        e = _Env()
        r = await e.run(pnl=-200_000, index=idx(-3, -3))
        self.assertEqual(r["blocked"], "daily_loss_limit")

    async def test_fail_open_orders_proceed(self):
        e = _Env()
        # 총자산 0(보유종목수 확인은 통과, 보호 규칙만 총자산 미확인) + 지수 조회 예외
        r = await e.run(pnl=-999_999, positions=_zero_equity_positions_fn(), index=idx_raise)
        self.assertTrue(r["executed"])
        self.assertEqual(e.orders, [1])

    async def test_normal_day_proceeds(self):
        e = _Env()
        r = await e.run(pnl=-50_000, index=idx(-0.4, -0.2))
        self.assertTrue(r["executed"])

    async def test_sell_and_non_stock_trader_unaffected(self):
        e = _Env()
        r = await e.run(pnl=-500_000, index=idx(-5, -5), signal=make_signal(action="sell", price=50000, qty=1))
        self.assertNotIn(r.get("blocked"), ("daily_loss_limit", "market_gate"))

    async def test_shadow_features_stored_on_block_record(self):
        e = _Env()
        sig = make_signal(price=50000, qty=1)
        sig["shadow_features"] = {"kst_time": "10:05", "kospi_rate": -1.2}
        await e.run(pnl=-200_000, signal=sig)
        stored = e.pool._conn.decisions[0]
        self.assertTrue(any(isinstance(a, str) and "kst_time" in a for a in stored))


class TestDecisionEngineFeatures(unittest.IsolatedAsyncioTestCase):
    async def test_decide_passes_features_to_log(self):
        from stark import decision_engine
        captured = {}

        async def fake_log(pool, symbol, decision, **kw):
            captured.update(kw)

        async def llm(prompt, session_id=None):
            return "EXECUTE: ok"
        with patch.object(decision_engine, "log_decision", fake_log):
            await decision_engine.decide({"symbol": "005930", "action": "buy"}, "p", ask_llm_fn=llm,
                                         pool=None, features={"kst_time": "09:10"})
        self.assertEqual(captured["features"], {"kst_time": "09:10"})


class TestSellLabel(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        from tests import test_stock_trader_stop_loss  # noqa: F401  (의존성 스텁)
        import main as st
        cls.st = st

    def _bot(self):
        return self.st.StockTrader.__new__(self.st.StockTrader)

    async def _run(self, label=None, raise_exc=False):
        class Conn:
            async def fetchval(s, q, *a):
                if raise_exc:
                    raise RuntimeError("db")
                return label

        class Acq:
            async def __aenter__(s):
                return Conn()

            async def __aexit__(s, *e):
                return False

        class Pool:
            def acquire(s):
                return Acq()
        with patch.object(self.st.db, "pool", Pool(), create=True):
            return await self._bot()._sell_label_strategy("211270", "MA크로스")

    async def test_uses_last_buy_strategy(self):
        self.assertEqual(await self._run("RSI반등"), "RSI반등")

    async def test_fallback_when_no_buy_or_error(self):
        self.assertEqual(await self._run(None), "MA크로스")
        self.assertEqual(await self._run(raise_exc=True), "MA크로스")

    def test_all_sell_labels_use_helper(self):
        src = (REPO_ROOT / "stock_trader" / "main.py").read_text(encoding="utf-8")
        self.assertEqual(src.count("{strat_name}_손절"), 0)
        self.assertGreaterEqual(src.count("{sell_strat}_손절"), 3)
        for suffix in ("급등절반익절", "구간익절_신호소멸", "AI익절", "절반확정", "트레일링스탑"):
            self.assertNotIn("{strat_name}_" + suffix, src)


if __name__ == "__main__":
    unittest.main()
