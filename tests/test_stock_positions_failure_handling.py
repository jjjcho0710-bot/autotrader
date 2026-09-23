"""
_get_stock_positions_raw 및 get_portfolio_context 실패 처리 단위 테스트
"""
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# dashboard.main 로드 전 필요한 외부 모듈 모킹
for mod_name in [
    "fastapi",
    "fastapi.staticfiles",
    "fastapi.responses",
    "fastapi.middleware.cors",
    "asyncpg",
    "redis",
    "redis.asyncio",
    "aiohttp",
    "google",
    "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()


class TestStockPositionsFailureHandling(unittest.IsolatedAsyncioTestCase):
    async def test_get_portfolio_context_failure_shows_explicit_warning(self):
        """get_stock_positions가 실패(success: False)하면 프롬프트에 '예수금 조회 실패(계좌 확인 필요)'가 명시되어야 함"""
        import dashboard.main as dm

        mock_stock_pos = {
            "success": False,
            "error": "모의투자 주문이 불가한 계좌입니다.",
            "data": [],
        }

        # get_crypto_positions가 모듈에 동적으로 존재할 수 있으므로 mock 주입
        dm.get_crypto_positions = AsyncMock(return_value={"success": True, "data": []})

        with patch("dashboard.main.get_stock_positions", new_callable=AsyncMock) as mock_get_pos, \
             patch("dashboard.main._get_market_index_ctx", new_callable=AsyncMock) as mock_market, \
             patch("dashboard.main._get_jarvis_knowledge", new_callable=AsyncMock) as mock_knowledge:

            mock_get_pos.return_value = mock_stock_pos
            mock_market.return_value = ""
            mock_knowledge.return_value = ""

            ctx = await dm.get_portfolio_context()

            self.assertIn("예수금 조회 실패(계좌 확인 필요)", ctx)
            self.assertIn("모의투자 주문이 불가한 계좌입니다.", ctx)
            self.assertIn("예수금이 0원인 것이 아니므로", ctx)

    async def test_get_portfolio_context_success_shows_account_details(self):
        """get_stock_positions가 성공(success: True)하면 계좌 상세(예수금, 총평가)가 프롬프트에 포함되어야 함"""
        import dashboard.main as dm

        mock_stock_pos = {
            "success": True,
            "data": [
                {
                    "symbol": "005930",
                    "name": "삼성전자",
                    "qty": 10,
                    "avg_price": 70000,
                    "cur_price": 72000,
                    "pnl": 20000,
                    "pnl_rate": 2.8,
                }
            ],
            "account": {
                "total_eval": 5000000,
                "stock_eval": 720000,
                "cash": 4280000,
                "pnl": 20000,
                "pnl_rate": 0.4,
            }
        }

        dm.get_crypto_positions = AsyncMock(return_value={"success": True, "data": []})

        with patch("dashboard.main.get_stock_positions", new_callable=AsyncMock) as mock_get_pos, \
             patch("dashboard.main._get_market_index_ctx", new_callable=AsyncMock) as mock_market, \
             patch("dashboard.main._get_jarvis_knowledge", new_callable=AsyncMock) as mock_knowledge:

            mock_get_pos.return_value = mock_stock_pos
            mock_market.return_value = ""
            mock_knowledge.return_value = ""

            ctx = await dm.get_portfolio_context()

            self.assertIn("예수금: 4,280,000원", ctx)
            self.assertIn("총평가금액: 5,000,000원", ctx)
            self.assertNotIn("예수금 조회 실패(계좌 확인 필요)", ctx)


if __name__ == "__main__":
    unittest.main()
