"""[AT] fix/strategy-config-guard 테스트.

배경: 10/26 14:01 KST strategy_config의 stock_trader 활성 전략 3개 stop_loss가 -0.5로 저장돼
사이징 기본금액이 총자산의 150%가 되고 10/27~29 매수가 비중 상한에 줄줄이 막혔다.
검증: (1) stop_loss 저장 범위(-15 ~ -2) 거부, (2) 사용 시 DEFAULT_STOP_LOSS_PCT 폴백,
(3) 변경 시 before/after 감사 기록 + 텔레그램, (4) fix_units 보호(GET 차단, 토큰 필수).
"""
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "stock_trader"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from tests.test_max_positions_setting import FakePool, FakeRedis, _rows  # noqa: E402
from common import position_sizing, strategy_audit  # noqa: E402
from router.handlers import setting_handler  # noqa: E402


class AuditRedis(FakeRedis):
    def __init__(self):
        super().__init__()
        self.lists = {}

    async def lpush(self, key, val):
        self.lists.setdefault(key, []).insert(0, val)

    async def ltrim(self, key, a, b):
        self.lists[key] = self.lists[key][a:b + 1]

    async def expire(self, key, ttl):
        pass


class TestResolveStopLossPct(unittest.TestCase):
    def test_in_range_values(self):
        for raw, want in [(-5, 5.0), (-7, 7.0), (-2, 2.0), (-15, 15.0), ("-5", 5.0), (5, 5.0)]:
            self.assertEqual(position_sizing.resolve_stop_loss_pct(raw), (want, True), raw)

    def test_out_of_range_falls_back_to_default(self):
        for raw in (-0.5, -0.07, -1.99, -15.01, -20, 0, -200):
            pct, ok = position_sizing.resolve_stop_loss_pct(raw)
            self.assertFalse(ok, raw)
            self.assertEqual(pct, position_sizing.DEFAULT_STOP_LOSS_PCT)

    def test_garbage_falls_back(self):
        for raw in ("abc", [], {}, float("nan")):
            self.assertEqual(position_sizing.resolve_stop_loss_pct(raw),
                             (position_sizing.DEFAULT_STOP_LOSS_PCT, False))

    def test_missing_key_is_default_and_ok(self):
        self.assertEqual(position_sizing.resolve_stop_loss_pct(None),
                         (position_sizing.DEFAULT_STOP_LOSS_PCT, True))

    def test_incident_value_no_longer_inflates_base_amount(self):
        pct, _ = position_sizing.resolve_stop_loss_pct(-0.5)
        base = position_sizing.compute_base_amount(9_182_742, pct, 0.75)
        self.assertAlmostEqual(base, 983_865, delta=1)  # 사고 때는 13,774,113원


class TestValidateStopLossRange(unittest.TestCase):
    def test_rejects_below_2_and_above_15(self):
        for v in (-0.5, -1, -1.99, -15.5, -20, 0.5):
            self.assertFalse(setting_handler.validate_setting("stop_loss", v)[0], v)

    def test_accepts_2_to_15_and_normalizes_sign(self):
        for v, want in [(-2, -2.0), (-7, -7.0), (-15, -15.0), (5, -5.0)]:
            self.assertEqual(setting_handler.validate_setting("stop_loss", v), (True, want))

    def test_apply_rejects_out_of_range_defensively(self):
        pool = FakePool(_rows())
        with self.assertRaises(ValueError):
            import asyncio
            asyncio.run(setting_handler.apply_strategy_settings(pool, AuditRedis(), {"stop_loss": -0.5}))
        self.assertEqual(pool.conn.update_count, 0)


class TestChatCommandAudit(unittest.IsolatedAsyncioTestCase):
    async def test_chat_out_of_range_rejected_no_write_no_alert(self):
        pool, sent = FakePool(_rows()), []

        async def tg(text, **kw):
            sent.append(text)
        reply = await setting_handler.handle("손절 -0.5%로 변경해줘", pool, AuditRedis(), tg)
        self.assertIn("허용 범위", reply)
        self.assertEqual(pool.conn.update_count, 0)
        self.assertEqual(sent, [])

    async def test_chat_change_records_before_after_and_alert_has_path(self):
        pool, redis, sent = FakePool(_rows()), AuditRedis(), []

        async def tg(text, **kw):
            sent.append(text)
        await setting_handler.handle("손절 -5%로 변경해줘", pool, redis, tg)
        entries = [json.loads(x) for x in redis.lists[strategy_audit.AUDIT_REDIS_KEY]]
        self.assertEqual(len(entries), 2)
        e = next(x for x in entries if x["name"] == "MA크로스")
        self.assertEqual(e["changes"]["stop_loss"], [-2, -5.0])
        self.assertIn("채팅", e["path"])
        self.assertIn("stop_loss -2→-5.0", sent[0])
        self.assertIn("경로", sent[0])


class TestDashboardSaveGuard(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import dashboard.main as dm
        self.dm = dm
        self.pool, self.redis = FakePool(_rows()), AuditRedis()
        self.sent = []

        async def tg(text, **kw):
            self.sent.append(text)
        for pt in (patch.object(dm, "db_pool", self.pool), patch.object(dm, "redis_client", self.redis),
                   patch.object(dm, "_send_telegram", tg)):
            pt.start()
            self.addCleanup(pt.stop)

    async def test_update_api_rejects_stop_loss_minus_half(self):
        res = await self.dm._update_strategy_raw({"bot": "stock_trader", "name": "MA크로스",
                                                  "is_active": True, "params": {"stop_loss": -0.5}})
        self.assertFalse(res["success"])
        self.assertIn("허용범위", res["error"])
        self.assertEqual(self.pool.conn.update_count, 0)

    async def test_update_api_saves_valid_value_audits_and_alerts(self):
        res = await self.dm._update_strategy_raw({"bot": "stock_trader", "name": "MA크로스",
                                                  "is_active": True,
                                                  "params": {"stop_loss": -7, "max_positions": 5}})
        self.assertTrue(res["success"])
        self.assertTrue(any("전략 설정 변경 감지" in s and "대시보드" in s for s in self.sent))
        e = json.loads(self.redis.lists[strategy_audit.AUDIT_REDIS_KEY][0])
        self.assertEqual(e["changes"]["stop_loss"], [-2, -7.0])

    async def test_add_strategy_rejects_bad_stop_loss(self):
        res = await self.dm._add_strategy_raw({"bot": "stock_trader", "name": "신규", "params": {"stop_loss": -1}})
        self.assertFalse(res["success"])


class TestFixUnitsProtection(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import dashboard.main as dm
        self.dm = dm
        self.pool, self.redis, self.sent = FakePool(_rows()), AuditRedis(), []

        async def tg(text, **kw):
            self.sent.append(text)
        for pt in (patch.object(dm, "db_pool", self.pool), patch.object(dm, "redis_client", self.redis),
                   patch.object(dm, "_send_telegram", tg)):
            pt.start()
            self.addCleanup(pt.stop)

    async def test_get_is_blocked_and_writes_nothing(self):
        with patch.dict(os.environ, {"STRATEGY_FIX_UNITS_TOKEN": "tok"}):
            status, body = await self.dm._fix_strategy_units_raw("GET", "tok", {})
        self.assertEqual(status, 405)
        self.assertEqual(self.pool.conn.update_count, 0)

    async def test_disabled_when_token_env_unset(self):
        env = {k: v for k, v in os.environ.items() if k != "STRATEGY_FIX_UNITS_TOKEN"}
        with patch.dict(os.environ, env, clear=True):
            status, _ = await self.dm._fix_strategy_units_raw("POST", "anything", {})
        self.assertEqual(status, 403)
        self.assertEqual(self.pool.conn.update_count, 0)

    async def test_wrong_token_rejected(self):
        with patch.dict(os.environ, {"STRATEGY_FIX_UNITS_TOKEN": "tok"}):
            status, _ = await self.dm._fix_strategy_units_raw("POST", "bad", {})
        self.assertEqual(status, 403)
        self.assertEqual(self.pool.conn.update_count, 0)

    async def test_post_with_token_uses_default_minus5_keeps_take_profit_and_audits(self):
        with patch.dict(os.environ, {"STRATEGY_FIX_UNITS_TOKEN": "tok"}):
            status, body = await self.dm._fix_strategy_units_raw("POST", "tok", {})
        self.assertEqual(status, 200)
        p = json.loads(next(r for r in self.pool.conn.rows if r["name"] == "MA크로스")["params"])
        self.assertEqual(p["stop_loss"], -5.0)
        self.assertEqual(p["take_profit"], 5)  # 삭제하지 않음
        self.assertEqual(p["max_positions"], 5)
        self.assertTrue(any("fix_units" in s for s in self.sent))
        self.assertEqual(len(self.redis.lists[strategy_audit.AUDIT_REDIS_KEY]), 2)

    async def test_post_out_of_range_target_rejected(self):
        with patch.dict(os.environ, {"STRATEGY_FIX_UNITS_TOKEN": "tok"}):
            status, _ = await self.dm._fix_strategy_units_raw("POST", "tok", {"stop_loss": -0.5})
        self.assertEqual(status, 400)
        self.assertEqual(self.pool.conn.update_count, 0)

    def test_no_hardcoded_minus7_write_left_in_source(self):
        src = (REPO_ROOT / "dashboard" / "main.py").read_text(encoding="utf-8")
        self.assertNotIn('params["stop_loss"] = -7', src)


class TestStockTraderFallback(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from tests.test_stock_trader_stop_loss import StockTrader  # noqa: F401
        import main as st_main
        cls.st = st_main
        cls.StockTrader = st_main.StockTrader

    def test_build_strategy_bad_value_uses_default_7pct(self):
        strat = self.StockTrader.build_strategy(None, "MA크로스", {"stop_loss": -0.5})
        self.assertAlmostEqual(strat.cfg.stop_loss, -0.07)
        self.assertFalse(strat.check_stop_loss(10000, 9900))   # -1%는 손절 아님(사고 때는 손절 트리거 가능)
        self.assertTrue(strat.check_stop_loss(10000, 9290))    # -7.1%

    def test_build_strategy_valid_value_is_respected(self):
        strat = self.StockTrader.build_strategy(None, "RSI반등", {"stop_loss": -5})
        self.assertAlmostEqual(strat.cfg.stop_loss, -0.05)

    def test_build_strategy_missing_key_keeps_legacy_default(self):
        self.assertAlmostEqual(self.StockTrader.build_strategy(None, "MA크로스", {}).cfg.stop_loss, -0.02)

    def test_resolve_warns_once_per_hour_and_returns_default(self):
        self.st._STOP_LOSS_WARN_LAST.clear()
        with patch("asyncio.get_running_loop", side_effect=RuntimeError):
            self.assertEqual(self.st._resolve_stop_loss_pct(-0.5, "t"), position_sizing.DEFAULT_STOP_LOSS_PCT)
        self.assertIn(repr(-0.5), self.st._STOP_LOSS_WARN_LAST)


class TestAuditHelpers(unittest.TestCase):
    def test_diff_and_alert(self):
        e = strategy_audit.build_entry(path="p", actor="a", bot="stock_trader", name="X",
                                       before={"stop_loss": -5, "k": 1}, after={"stop_loss": -0.5, "k": 1})
        self.assertEqual(e["changes"], {"stop_loss": [-5, -0.5]})
        self.assertIn("stop_loss -5 -> -0.5", strategy_audit.build_alert_text([e]))

    def test_no_change_no_alert(self):
        e = strategy_audit.build_entry(path="p", actor="a", bot="b", name="n", before={"a": 1}, after={"a": 1})
        self.assertIsNone(strategy_audit.build_alert_text([e]))

    def test_record_swallows_redis_errors(self):
        import asyncio
        r = MagicMock()
        r.lpush = AsyncMock(side_effect=RuntimeError("down"))
        e = strategy_audit.build_entry(path="p", actor="a", bot="b", name="n", before={}, after={"a": 1})
        asyncio.run(strategy_audit.record(r, e))  # 예외 없이 반환


if __name__ == "__main__":
    unittest.main()
