"""
[AT] fix/stoploss-text-from-config 회귀 테스트.

배경: 활성 전략 3개 모두 stop_loss=-5(%)인데 손절선을 말하는 알림·AI 프롬프트 문구가
"-7%"로 하드코딩되어 있었다(stock_trader/main.py의 가격 모니터 알림, dashboard/main.py의
포트폴리오 컨텍스트·채팅 프롬프트). strategy_config의 활성 전략 stop_loss를 읽어 표시값을
만들도록 바꾸고, 활성 전략이 없거나 조회에 실패하면 참고용 폴백
(position_sizing.DEFAULT_STOP_LOSS_PCT=7.0)을 쓴다.

검증 범위:
(a) stock_trader/main.py.StockTrader._active_stop_loss_display() — 활성 전략 값 반영/폴백.
(b) dashboard/main.py._get_active_stop_loss_display() — strategy_config 조회/조회 실패 시 폴백.
(c) dashboard/main.py.get_portfolio_context()의 "손절선(...) 도달·초과" 문구가 (b)의
    표시값을 그대로 반영한다(하드코딩된 "-7%"가 남지 않는다).
"""
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from tests.test_stock_trader_stop_loss import StockTrader  # noqa: E402 (stub 설정 + sys.path 재사용)
import main as stock_main  # noqa: E402 (stock_trader/main.py)


def _trader():
    return StockTrader.__new__(StockTrader)  # __init__(DB/KIS 연결) 없이 순수 메서드만 사용


class TestStockTraderActiveStopLossDisplay(unittest.TestCase):
    def test_reads_active_strategy_stop_loss(self):
        bot = _trader()
        bot.strategies = {"MA크로스": {"is_active": True, "params": {"stop_loss": -5}}}
        self.assertEqual(bot._active_stop_loss_display(), "-5%")

    def test_falls_back_to_default_when_no_active_strategy(self):
        bot = _trader()
        bot.strategies = {}
        self.assertEqual(bot._active_stop_loss_display(), "-7%")

    def test_falls_back_when_active_strategy_missing_stop_loss_key(self):
        bot = _trader()
        bot.strategies = {"MA크로스": {"is_active": True, "params": {}}}
        self.assertEqual(bot._active_stop_loss_display(), "-7%")


for _mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai",
]:
    if _mod_name not in sys.modules:
        sys.modules[_mod_name] = MagicMock()


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


class _StopLossConn:
    """strategy_config에서 stop_loss만 돌려주는 최소 커넥션.
    raise_on_fetchrow=True면 조회 실패(DB 에러)를 흉내낸다."""

    def __init__(self, stop_loss=None, raise_on_fetchrow=False):
        self.stop_loss = stop_loss
        self.raise_on_fetchrow = raise_on_fetchrow

    async def fetchrow(self, query, *args):
        if self.raise_on_fetchrow:
            raise RuntimeError("DB 조회 실패(테스트)")
        if self.stop_loss is None:
            return None
        return {"params": '{"stop_loss": %s}' % self.stop_loss}

    async def fetch(self, query, *args):
        return []


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _AcquireCtx(self.conn)


class TestDashboardActiveStopLossDisplay(unittest.IsolatedAsyncioTestCase):
    async def test_reads_active_strategy_stop_loss(self):
        dm = _load_dashboard_main()
        with patch.object(dm, "db_pool", _Pool(_StopLossConn(stop_loss=-5)), create=True):
            display = await dm._get_active_stop_loss_display()
        self.assertEqual(display, "-5%")

    async def test_falls_back_when_no_active_strategy_row(self):
        dm = _load_dashboard_main()
        with patch.object(dm, "db_pool", _Pool(_StopLossConn(stop_loss=None)), create=True):
            display = await dm._get_active_stop_loss_display()
        self.assertEqual(display, "-7%")

    async def test_falls_back_on_db_error(self):
        dm = _load_dashboard_main()
        with patch.object(dm, "db_pool", _Pool(_StopLossConn(raise_on_fetchrow=True)), create=True):
            display = await dm._get_active_stop_loss_display()
        self.assertEqual(display, "-7%")


class TestDashboardPortfolioContextStopLossText(unittest.IsolatedAsyncioTestCase):
    """get_portfolio_context()의 "손절선(-7%) 도달·초과" 문구가 활성 전략 값을 반영하는지."""

    async def test_status_text_uses_active_stop_loss_value(self):
        dm = _load_dashboard_main()
        positions_result = {
            "success": True,
            "data": [{"symbol": "005930", "name": "삼성전자", "qty": 10,
                       "avg_price": 70000, "cur_price": 65100, "pnl": -49000, "pnl_rate": -7.0}],
            "account": {"cash": 0, "total_eval": 0},
        }
        with patch.object(dm, "get_stock_positions", new=AsyncMock(return_value=positions_result)), \
             patch.object(dm, "_get_active_stop_loss_display", new=AsyncMock(return_value="-5%")), \
             patch.object(dm, "_get_market_index_ctx", new=AsyncMock(return_value="")), \
             patch.object(dm, "_get_jarvis_knowledge", new=AsyncMock(return_value="")), \
             patch.object(dm, "db_pool", None, create=True):
            ctx = await dm.get_portfolio_context()

        self.assertIn("손절선(-5%) 도달·초과", ctx)
        self.assertNotIn("-7%", ctx)


if __name__ == "__main__":
    unittest.main()
