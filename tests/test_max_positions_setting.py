"""
최대 보유 종목수(max_positions) 화면 설정 테스트.

검증 범위
- 저장: 전략 저장 API(_update_strategy_raw)가 max_positions 를 strategy_config 에 기록하고
  redis 로 알린다. stock_trader 전 전략 행에 같은 값을 맞춘다.
- 범위 검사: 1~100 정수만 허용. 범위 밖/소수/문자/bool 이면 저장(UPDATE)하지 않고 사유를 반환.
- 반영 확인: 저장한 값이 신호 루프(StockTrader.load_strategies)와 주문 직전 검사
  (execution_guard._get_max_positions)에서 실제로 읽힌다 — 한도 검사 코드는 그대로이고
  값이 100이면 보유 5종목에서 신규 매수가 막히지 않는다.
- 화면: strategy.html 에 입력칸(1~100)과 범위 검사·저장 API 호출이 있는지 정적 확인.
"""
import asyncio
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "stock_trader"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

# dashboard.main 로드 전 필요한 외부 모듈 모킹 (test_dashboard_chart_rate_limit.py 와 동일)
for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

from router.handlers import setting_handler  # noqa: E402
from stark import execution_guard  # noqa: E402

STRATEGY_HTML = REPO_ROOT / "dashboard" / "static" / "strategy.html"


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakeStrategyConnection:
    """strategy_config 인메모리 테이블. 대시보드 저장 UPDATE / 공용 적용기 UPDATE / 조회를 모두 지원."""

    def __init__(self, rows):
        self.rows = rows  # [{"id","bot","name","is_active","params"(json str)}]
        self.update_count = 0

    async def fetch(self, query, *args):
        if "strategy_config" not in query:
            return []
        return [dict(r) for r in self.rows if r["bot"] == "stock_trader"]

    async def fetchval(self, query, *args):
        return 0  # execution_guard 의 당일 손절/체결 기록 조회 등

    async def fetchrow(self, query, *args):
        if "strategy_config" not in query:
            return None
        for r in self.rows:
            if r["bot"] == "stock_trader" and r["is_active"]:
                return {"params": r["params"]}
        return None

    async def execute(self, query, *args):
        if "UPDATE strategy_config" not in query:
            return "OK"  # trade_history INSERT 등
        self.update_count += 1
        if "WHERE bot=$3 AND name=$4" in query:
            active, params_json, bot, name = args
            for r in self.rows:
                if r["bot"] == bot and r["name"] == name:
                    r["is_active"], r["params"] = active, params_json
        else:
            params_json, row_id = args
            for r in self.rows:
                if r["id"] == row_id:
                    r["params"] = params_json
        return "UPDATE 1"


class FakePool:
    def __init__(self, rows):
        self.conn = FakeStrategyConnection(rows)

    def acquire(self):
        return _AcquireCtx(self.conn)


class FakeRedis:
    def __init__(self):
        self.published = []
        self.deleted = []

    async def publish(self, channel, message):
        self.published.append((channel, json.loads(message)))

    async def keys(self, pattern):
        return ["cache:strategies:None"]

    async def delete(self, key):
        self.deleted.append(key)


def _rows():
    base = {"short": 5, "long": 20, "stop_loss": -2, "take_profit": 5, "buy_amount": 500000, "max_positions": 5}
    return [
        {"id": 1, "bot": "stock_trader", "name": "MA크로스", "is_active": True, "params": json.dumps(base)},
        {"id": 2, "bot": "stock_trader", "name": "RSI반등", "is_active": False,
         "params": json.dumps({"period": 14, "max_positions": 5})},
    ]


def _params(pool, name):
    return json.loads(next(r for r in pool.conn.rows if r["name"] == name)["params"])


class TestValidateMaxPositions(unittest.TestCase):
    def test_accepts_integers_in_range(self):
        for raw, want in [(1, 1), (5, 5), (100, 100), ("100", 100), (" 7 ", 7), (50.0, 50)]:
            self.assertEqual(setting_handler.validate_max_positions(raw), (True, want), raw)

    def test_rejects_out_of_range_and_non_integers(self):
        for raw in [0, -1, 101, 1000, 5.5, "5.5", "abc", "", "1e1", None, True, False, [], {}]:
            ok, msg = setting_handler.validate_max_positions(raw)
            self.assertFalse(ok, raw)
            self.assertIn("1~100", msg)

    def test_validate_setting_routes_max_positions(self):
        self.assertEqual(setting_handler.validate_setting("max_positions", "100"), (True, 100))
        self.assertFalse(setting_handler.validate_setting("max_positions", 101)[0])


class TestSaveMaxPositionsApi(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import dashboard.main as dm
        self.dm = dm
        self.pool = FakePool(_rows())
        self.redis = FakeRedis()
        for p in (patch.object(dm, "db_pool", self.pool), patch.object(dm, "redis_client", self.redis)):
            p.start()
            self.addCleanup(p.stop)

    async def _save(self, value, name="MA크로스", active=True):
        params = _params(self.pool, name)
        params["max_positions"] = value
        return await self.dm._update_strategy_raw(
            {"bot": "stock_trader", "name": name, "is_active": active, "params": params})

    async def test_save_100_persists_and_syncs_all_stock_trader_rows(self):
        res = await self._save(100)
        self.assertTrue(res["success"], res)
        self.assertEqual(_params(self.pool, "MA크로스")["max_positions"], 100)
        self.assertEqual(_params(self.pool, "RSI반등")["max_positions"], 100)  # 다른 전략 행도 동일 값
        self.assertEqual(_params(self.pool, "MA크로스")["stop_loss"], -2)      # 다른 파라미터 보존
        self.assertTrue(self.pool.conn.rows[0]["is_active"])                  # 활성 상태 보존
        self.assertFalse(self.pool.conn.rows[1]["is_active"])
        # 봇 신호 루프에 변경 알림 + 화면 캐시 무효화
        self.assertTrue(any(ch == "strategy:update" and m["params"]["max_positions"] == 100
                            for ch, m in self.redis.published))
        self.assertTrue(self.redis.deleted)

    async def test_string_value_is_stored_as_int(self):
        res = await self._save("42")
        self.assertTrue(res["success"], res)
        self.assertIs(type(_params(self.pool, "MA크로스")["max_positions"]), int)
        self.assertEqual(_params(self.pool, "MA크로스")["max_positions"], 42)

    async def test_out_of_range_is_rejected_without_saving(self):
        for bad in [0, -3, 101, 5.5, "abc", "", None, True]:
            with self.subTest(value=bad):
                before = json.dumps(self.pool.conn.rows, sort_keys=True)
                res = await self._save(bad)
                self.assertFalse(res["success"])
                self.assertIn("1~100", res["error"])
                self.assertEqual(self.pool.conn.update_count, 0)
                self.assertEqual(json.dumps(self.pool.conn.rows, sort_keys=True), before)
                self.assertEqual(self.redis.published, [])

    async def test_saving_without_max_positions_leaves_other_rows_alone(self):
        params = _params(self.pool, "MA크로스")
        del params["max_positions"]
        params["buy_amount"] = 300000
        res = await self.dm._update_strategy_raw(
            {"bot": "stock_trader", "name": "MA크로스", "is_active": True, "params": params})
        self.assertTrue(res["success"], res)
        self.assertEqual(_params(self.pool, "RSI반등")["max_positions"], 5)


class TestSavedValueIsAppliedByBotAndGuard(unittest.IsolatedAsyncioTestCase):
    """저장한 값이 신호 루프와 execution_guard 에 실제로 읽히는지 (DB 재조회 → 캐시 없음)."""

    def setUp(self):
        import dashboard.main as dm
        self.dm = dm
        self.pool = FakePool(_rows())
        self.redis = FakeRedis()
        for p in (patch.object(dm, "db_pool", self.pool), patch.object(dm, "redis_client", self.redis)):
            p.start()
            self.addCleanup(p.stop)

    async def _save_100(self):
        params = _params(self.pool, "MA크로스")
        params["max_positions"] = 100
        res = await self.dm._update_strategy_raw(
            {"bot": "stock_trader", "name": "MA크로스", "is_active": True, "params": params})
        self.assertTrue(res["success"], res)

    async def test_execution_guard_reads_new_limit_immediately(self):
        self.assertEqual(await execution_guard._get_max_positions(self.pool), 5)
        await self._save_100()
        self.assertEqual(await execution_guard._get_max_positions(self.pool), 100)

    async def test_stock_trader_signal_loop_reloads_new_limit_on_strategy_update(self):
        try:
            from main import StockTrader  # stock_trader/main.py
        except ImportError as e:  # 런타임 의존성 없는 환경 방어
            self.skipTest(f"stock_trader.main import 불가: {e}")
        import main as st_main

        trader = StockTrader.__new__(StockTrader)
        trader.strategies = {}
        fake_db = types.SimpleNamespace(pool=self.pool)
        with patch.object(st_main, "db", fake_db):
            await StockTrader.load_strategies(trader)
            self.assertEqual(int(trader.get_all_active_strategies()[0][1]["max_positions"]), 5)
            await self._save_100()
            # 화면 저장 시 publish 된 strategy:update → 봇이 load_strategies 를 다시 호출
            self.assertTrue(any(ch == "strategy:update" for ch, _ in self.redis.published))
            await StockTrader.load_strategies(trader)
            name, params = trader.get_all_active_strategies()[0]
            self.assertEqual(int(params["max_positions"]), 100)

    async def test_guard_limit_check_still_enforced_and_100_lets_sixth_buy_through(self):
        """한도 검사는 그대로: 5종목 보유 + 한도 5 → 6번째 차단, 한도 100 → 통과."""
        execution_guard.reset_buy_lock()
        held = [{"symbol": f"0000{i}", "name": f"종목{i}", "qty": 10} for i in range(1, 6)]

        async def get_positions():
            return {"success": True, "data": held}

        async def run(limit):
            execution_guard.reset_buy_lock()
            orders = []

            async def kis_order(symbol, price, qty, is_buy):
                orders.append(symbol)
                return {"success": True}

            async def noop(*a, **k):
                return None

            async def code_to_name(symbol):
                return "신규종목"

            signal = {"symbol": "000099", "name": "신규종목", "action": "buy", "price": 10000,
                      "qty": 1, "strategy": "MA크로스", "bot": "stock_trader", "reason": "test"}
            decision = {"is_small": False, "reply": "EXECUTE: 매수 추천"}
            res = await execution_guard.execute(
                signal, decision, pool=self.pool, redis=_GuardRedis(),
                kis_order_fn=kis_order, send_telegram_fn=noop, log_journal_fn=noop,
                save_trade_memory_fn=None, code_to_name_fn=code_to_name,
                get_positions_fn=get_positions,
                max_positions=limit)
            return res, orders

        res, orders = await run(await execution_guard._get_max_positions(self.pool))  # 저장 전: 5
        self.assertEqual(res.get("blocked"), "max_positions_limit")
        self.assertEqual(orders, [])

        await self._save_100()
        limit = await execution_guard._get_max_positions(self.pool)
        self.assertEqual(limit, 100)
        res, _ = await run(limit)
        self.assertNotEqual(res.get("blocked"), "max_positions_limit")


class _GuardRedis:
    """execution_guard 가 쓰는 redis 호출을 전부 받아 주는 최소 대역 (in-flight 키 없음)."""

    async def keys(self, pattern):
        return []

    def __getattr__(self, name):
        async def _noop(*a, **k):
            return None
        return _noop


class TestStrategyHtml(unittest.TestCase):
    """strategy.html(모바일 + PC 임베드 공용) 정적 확인 — 브라우저 실행 검증은 아님."""

    @classmethod
    def setUpClass(cls):
        cls.html = STRATEGY_HTML.read_text(encoding="utf-8")

    def test_has_max_positions_input_with_range(self):
        self.assertIn("최대 보유 종목수", self.html)
        self.assertIn('data-key="max_positions"', self.html)
        self.assertIn('min="1" max="100"', self.html)

    def test_range_check_blocks_save_and_shows_message(self):
        self.assertIn("function validateMaxPositions", self.html)
        self.assertIn("1~100", self.html)
        self.assertIn("param-msg", self.html)
        # 검증 실패 분기에서 saveParams 호출 전에 return 해야 한다
        body = self.html[self.html.index("async function onSaveClick"):]
        self.assertLess(body.index("return;"), body.index("await saveParams"))

    def test_saves_via_existing_strategy_update_api(self):
        self.assertIn("/api/strategies/update", self.html)
        self.assertNotIn("/api/strategy/params", self.html)  # 존재하지 않는 엔드포인트


if __name__ == "__main__":
    unittest.main()
