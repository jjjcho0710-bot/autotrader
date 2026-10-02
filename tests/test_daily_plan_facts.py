"""
[AT] fix/daily-plan-facts 회귀 테스트.

배경: dashboard/main.py._jarvis_daily_plan("오늘의 작전")이 보유 종목수·최대 보유 한도·
현금·종목당 매수 상한을 AI에게 안 주면서도 작전 문장은 이를 단정했다(10/2 실측: 보유
10종목이 한도 9를 넘었는데 "예수금 여력 충분"·신규 종목 "진입 1순위" 제시, 실제 현금은
약 111만원뿐이었는데 "예수금 70%까지 매수 가능"이라고 썼음).

검증 범위:
(a) 보유 종목수가 한도에 도달하면 [현재 상태]에 "신규 매수 불가" 문구가 들어간다.
(b) KIS 조회·max_positions 조회가 실패하면 해당 항목은 '확인 불가'로 표시한다(지어내지 않음).
(c) [현재 상태]의 현금값이 KIS 잔고 조회(get_stock_positions)의 account.cash와 같다.
(d) _jarvis_daily_plan() 실제 호출 경로에서 AI에게 보내는 프롬프트에 [현재 상태] 블록이
    들어가고, 지시사항/지식에 붙은 내부 관리번호((#29), K83)는 보이지 않는다.
"""
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()


def _load_dashboard_main():
    import dashboard.main as dm
    return dm


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _StrategyConfigConn:
    """strategy_config에서 max_positions만 돌려주는 최소 커넥션. raise_on_fetchrow=True면
    조회 실패를 흉내낸다(값 확인 불가 경로 검증용)."""

    def __init__(self, max_positions=None, raise_on_fetchrow=False):
        self.max_positions = max_positions
        self.raise_on_fetchrow = raise_on_fetchrow

    async def fetchrow(self, query, *args):
        if self.raise_on_fetchrow:
            raise RuntimeError("DB 조회 실패(테스트)")
        if self.max_positions is None:
            return None
        return {"params": '{"max_positions": %d}' % self.max_positions}

    async def fetch(self, query, *args):
        return []


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _AcquireCtx(self.conn)


def _positions_result(held: int, cash: int, equity: int, success: bool = True):
    return {
        "success": success,
        "data": [{"symbol": f"00{i}", "name": f"종목{i}", "qty": 1} for i in range(held)],
        "account": {"cash": cash, "total_eval": equity},
    }


class TestDailyPlanCurrentState(unittest.IsolatedAsyncioTestCase):
    """_jarvis_daily_plan_current_state()가 [현재 상태] 블록을 코드 계산값만으로 채우는지 확인."""

    async def test_held_at_limit_shows_blocked_new_buy(self):
        dm = _load_dashboard_main()
        with patch.object(dm, "get_stock_positions",
                           new=AsyncMock(return_value=_positions_result(10, 1_110_000, 10_000_000))), \
             patch.object(dm, "db_pool", _Pool(_StrategyConfigConn(max_positions=9)), create=True):
            state = await dm._jarvis_daily_plan_current_state()

        self.assertIn("10/9", state)
        self.assertIn("신규 매수 불가", state)

    async def test_held_below_limit_has_no_block_line(self):
        dm = _load_dashboard_main()
        with patch.object(dm, "get_stock_positions",
                           new=AsyncMock(return_value=_positions_result(3, 5_000_000, 10_000_000))), \
             patch.object(dm, "db_pool", _Pool(_StrategyConfigConn(max_positions=9)), create=True):
            state = await dm._jarvis_daily_plan_current_state()

        self.assertIn("3/9", state)
        self.assertNotIn("신규 매수 불가", state)

    async def test_cash_matches_account_cash_field(self):
        dm = _load_dashboard_main()
        with patch.object(dm, "get_stock_positions",
                           new=AsyncMock(return_value=_positions_result(1, 1_110_000, 10_000_000))), \
             patch.object(dm, "db_pool", _Pool(_StrategyConfigConn(max_positions=9)), create=True):
            state = await dm._jarvis_daily_plan_current_state()

        self.assertIn("1,110,000원", state)

    async def test_positions_fetch_failure_shows_unavailable_not_fabricated(self):
        dm = _load_dashboard_main()
        with patch.object(dm, "get_stock_positions",
                           new=AsyncMock(return_value={"success": False, "error": "KIS 실패", "data": []})), \
             patch.object(dm, "db_pool", _Pool(_StrategyConfigConn(max_positions=9)), create=True):
            state = await dm._jarvis_daily_plan_current_state()

        self.assertIn("확인 불가", state)
        self.assertNotIn("0/9", state)  # 보유 0종목으로 지어내면 안 됨

    async def test_positions_fetch_raises_shows_unavailable(self):
        dm = _load_dashboard_main()
        with patch.object(dm, "get_stock_positions", new=AsyncMock(side_effect=RuntimeError("timeout"))), \
             patch.object(dm, "db_pool", _Pool(_StrategyConfigConn(max_positions=9)), create=True):
            state = await dm._jarvis_daily_plan_current_state()

        self.assertIn("확인 불가", state)

    async def test_max_positions_query_failure_shows_unavailable_for_held(self):
        dm = _load_dashboard_main()
        with patch.object(dm, "get_stock_positions",
                           new=AsyncMock(return_value=_positions_result(10, 1_110_000, 10_000_000))), \
             patch.object(dm, "db_pool", _Pool(_StrategyConfigConn(raise_on_fetchrow=True)), create=True):
            state = await dm._jarvis_daily_plan_current_state()

        # 보유 종목수는 알아도 한도를 모르면 한도 도달 여부를 단정할 수 없으므로 전체를 확인 불가로.
        self.assertIn("확인 불가", state)
        self.assertNotIn("신규 매수 불가", state)

    async def test_base_amount_computed_from_equity_risk_and_stop_loss(self):
        dm = _load_dashboard_main()
        equity = 10_000_000
        with patch.object(dm, "get_stock_positions",
                           new=AsyncMock(return_value=_positions_result(1, 1_000_000, equity))), \
             patch.object(dm, "db_pool", _Pool(_StrategyConfigConn(max_positions=9)), create=True):
            state = await dm._jarvis_daily_plan_current_state()

        # 자산 1,000만원 × 0.75% ÷ 7.0% = 1,071,429원(변동성 배율 반영 전 기본 상한).
        self.assertIn("1,071,429원", state)

    async def test_equity_unavailable_shows_unavailable_cap(self):
        dm = _load_dashboard_main()
        with patch.object(dm, "get_stock_positions",
                           new=AsyncMock(return_value={"success": False, "error": "x", "data": []})), \
             patch.object(dm, "db_pool", _Pool(_StrategyConfigConn(max_positions=9)), create=True):
            state = await dm._jarvis_daily_plan_current_state()

        self.assertIn("종목당 매수 상한(기본): 확인 불가", state)


class TestStripInternalIds(unittest.TestCase):
    """_strip_internal_ids()가 지시/지식 번호를 걷어내는지 확인(사람이 못 알아듣는 번호 노출 방지)."""

    def test_strips_directive_id_prefix(self):
        dm = _load_dashboard_main()
        out = dm._strip_internal_ids("- (#29) 손절 기준을 -5%로 바꿔라")
        self.assertNotIn("#29", out)
        self.assertIn("손절 기준을 -5%로 바꿔라", out)

    def test_strips_knowledge_core_id_prefix(self):
        dm = _load_dashboard_main()
        out = dm._strip_internal_ids("- K83 골든크로스+거래량급증은 스윙 허용")
        self.assertNotIn("K83", out)
        self.assertIn("골든크로스+거래량급증은 스윙 허용", out)

    def test_strips_learning_rule_id_prefix(self):
        dm = _load_dashboard_main()
        out = dm._strip_internal_ids("- R12 하락장에서는 강한 신호만 매수")
        self.assertNotIn("R12", out)
        self.assertIn("하락장에서는 강한 신호만 매수", out)

    def test_leaves_plain_lines_untouched(self):
        dm = _load_dashboard_main()
        text = "- 삼성전자(005930): 실적 기대"
        self.assertEqual(dm._strip_internal_ids(text), text)


class TestDailyPlanEndToEnd(unittest.IsolatedAsyncioTestCase):
    """_jarvis_daily_plan() 실제 호출 경로에서 프롬프트에 [현재 상태]가 들어가고,
    내부 관리번호는 사라지는지 end-to-end로 확인한다."""

    async def _run(self, *, held=10, max_positions=9, cash=1_110_000, equity=10_000_000,
                   positions_success=True):
        dm = _load_dashboard_main()

        class _WatchlistConn:
            async def fetch(self, query, *args):
                return []

        captured = {}

        async def fake_ask_openwebui(prompt, **kw):
            captured["prompt"] = prompt
            return "오늘의 작전 내용"

        class _FakeRedisNoop:
            async def get(self, key):
                return None

            async def setex(self, *a, **kw):
                pass

        pos_res = (_positions_result(held, cash, equity) if positions_success
                   else {"success": False, "error": "KIS 실패", "data": []})

        with patch.object(dm, "db_pool",
                           _MultiQueryPool(_WatchlistConn(), _StrategyConfigConn(max_positions=max_positions)),
                           create=True), \
             patch.object(dm, "redis_client", _FakeRedisNoop(), create=True), \
             patch.object(dm, "get_stock_positions", new=AsyncMock(return_value=pos_res)), \
             patch.object(dm, "_get_jarvis_lessons", new=AsyncMock(return_value="(없음)")), \
             patch.object(dm, "_get_jarvis_knowledge", new=AsyncMock(return_value="- K83 골든크로스는 스윙 허용")), \
             patch.object(dm, "_get_active_directives",
                           new=AsyncMock(return_value="- (#29) 손절 기준을 -5%로 바꿔라")), \
             patch.object(dm, "_ask_openwebui", new=fake_ask_openwebui), \
             patch.object(dm, "_send_telegram", new=AsyncMock()):
            await dm._jarvis_daily_plan()

        return captured.get("prompt", "")

    async def test_prompt_contains_current_state_block(self):
        prompt = await self._run()
        self.assertIn("[현재 상태", prompt)
        self.assertIn("10/9", prompt)
        self.assertIn("1,110,000원", prompt)

    async def test_prompt_blocks_new_entry_when_at_limit(self):
        prompt = await self._run(held=10, max_positions=9)
        self.assertIn("신규 매수 불가", prompt)

    async def test_prompt_rule_tells_ai_to_prefer_current_state(self):
        prompt = await self._run()
        self.assertIn("[현재 상태]가 항상 우선", prompt)
        self.assertIn("'진입 1순위'로 단정하지 말라", prompt)

    async def test_prompt_hides_internal_ids_from_directives_and_knowledge(self):
        prompt = await self._run()
        self.assertNotIn("#29", prompt)
        self.assertNotIn("K83", prompt)
        self.assertIn("손절 기준을 -5%로 바꿔라", prompt)
        self.assertIn("골든크로스는 스윙 허용", prompt)

    async def test_prompt_shows_unavailable_when_positions_fetch_fails(self):
        prompt = await self._run(positions_success=False)
        self.assertIn("확인 불가", prompt)


class _MultiQueryPool:
    """워치리스트 조회(_WatchlistConn)와 strategy_config 조회(_StrategyConfigConn)를
    한 db_pool.acquire()로 둘 다 받아야 하는 _jarvis_daily_plan 호출을 위한 합성 커넥션."""

    def __init__(self, watchlist_conn, strategy_conn):
        self._watchlist_conn = watchlist_conn
        self._strategy_conn = strategy_conn

    def acquire(self):
        return _AcquireCtx(self)

    async def fetch(self, query, *args):
        return await self._watchlist_conn.fetch(query, *args)

    async def fetchrow(self, query, *args):
        return await self._strategy_conn.fetchrow(query, *args)


if __name__ == "__main__":
    unittest.main()
