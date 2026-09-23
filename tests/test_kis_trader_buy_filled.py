"""
stock_trader/kis_trader.py의 buy() 체결 확인 로직 회귀 테스트.

검증 항목:
1. KISTrader.buy() 호출 시 주문 접수(rt_cd=0) 후 _get_filled_qty(order_no, symbol, side="02")를 호출한다.
2. 체결수량이 0 이하(미체결)이면 success=False 및 적절한 에러 메시지를 반환한다.
3. 체결수량이 정상 확인(양수)되면 success=True 및 filled_qty를 반환한다.
4. 체결 확인 조회가 실패(None)하면 접수 결과만으로 보수적 성공(fill_unconfirmed=True)을 반환한다.
"""
import asyncio
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "stock_trader"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _stub_missing_runtime_deps():
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
        stub.TCPConnector = lambda *a, **kw: None
        stub.ClientTimeout = lambda *a, **kw: None
        sys.modules["aiohttp"] = stub


_stub_missing_runtime_deps()

from common.config import config  # noqa: E402
from stock_trader.kis_trader import KISTrader  # noqa: E402


class DummyResponse:
    def __init__(self, data):
        self._data = data

    async def json(self):
        return self._data

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class DummySession:
    def __init__(self, resp_data):
        self.resp_data = resp_data
        self.closed = False

    def post(self, url, headers=None, json=None):
        return DummyResponse(self.resp_data)

    async def close(self):
        self.closed = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class TestKISTraderBuyFilled(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.trader = KISTrader()
        config.KIS_ACCOUNT_NO = "50193041"

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_buy_fails_when_unfilled(self, mock_sleep):
        """주문 접수는 성공(rt_cd=0)했으나 체결수량이 0주이면 success=False 반환"""
        order_resp = {
            "rt_cd": "0",
            "output": {"ODNO": "0001234567"},
            "msg1": "주문접수완료",
        }
        self.trader._new_session = lambda: DummySession(order_resp)
        self.trader._get_filled_qty = AsyncMock(return_value=0)

        res = await self.trader.buy("005930", 70000, 10)

        self.assertFalse(res["success"])
        self.assertIn("미체결", res.get("error", ""))
        self.trader._get_filled_qty.assert_awaited_once_with("0001234567", "005930", side="02")

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_buy_succeeds_when_filled(self, mock_sleep):
        """주문 접수 성공 후 체결수량이 정상 확인되면 success=True 및 filled_qty 반환"""
        order_resp = {
            "rt_cd": "0",
            "output": {"ODNO": "0001234568"},
            "msg1": "주문접수완료",
        }
        self.trader._new_session = lambda: DummySession(order_resp)
        self.trader._get_filled_qty = AsyncMock(return_value=10)

        res = await self.trader.buy("005930", 70000, 10)

        self.assertTrue(res["success"])
        self.assertEqual(res["filled_qty"], 10)
        self.assertEqual(res["order_no"], "0001234568")
        self.trader._get_filled_qty.assert_awaited_once_with("0001234568", "005930", side="02")

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_buy_fill_unconfirmed_fallback(self, mock_sleep):
        """체결 확인 조회가 실패(None)한 경우 보수적 성공(fill_unconfirmed=True) 반환"""
        order_resp = {
            "rt_cd": "0",
            "output": {"ODNO": "0001234569"},
            "msg1": "주문접수완료",
        }
        self.trader._new_session = lambda: DummySession(order_resp)
        self.trader._get_filled_qty = AsyncMock(return_value=None)

        res = await self.trader.buy("005930", 70000, 10)

        self.assertTrue(res["success"])
        self.assertTrue(res.get("fill_unconfirmed"))
        self.assertEqual(res["order_no"], "0001234569")


if __name__ == "__main__":
    unittest.main()
