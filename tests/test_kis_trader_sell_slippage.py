"""
stock_trader/kis_trader.py sell() 매도 슬리피지 감지/알림 회귀 테스트.

배경: 매도는 시장가(01)로 나가기 때문에 급락 갭에서 신호 발생 시점 가격보다
과도하게 나쁜 가격에 체결될 수 있다(쓰리빌리언 -11.8% 체결 사례). 주문 자체를
막을 수는 없으므로(시장가라 이미 체결된 뒤에만 확인 가능) 체결가가 신호가 대비
3% 이상 낮으면 경고 로그와 텔레그램 알림만 남기는지 검증한다. 실제 하한선으로
주문을 제한하는 기능은 이 작업 범위 밖이다.
"""
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

    def post(self, url, headers=None, json=None):
        return DummyResponse(self.resp_data)

    async def close(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class TestCheckSellSlippage(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.trader = KISTrader()
        config.KIS_ACCOUNT_NO = "50193041"

    async def test_alerts_when_fill_price_far_below_signal_price(self):
        """체결가가 신호가 대비 3% 이상 낮으면(쓰리빌리언 -11.8% 사례) 경고+알림"""
        with patch("common.telegram.send_stock", new_callable=AsyncMock) as mock_send:
            await self.trader._check_sell_slippage("900070", 10000, 8820)  # -11.8%
            mock_send.assert_awaited_once()
            self.assertIn("슬리피지", mock_send.await_args.args[0])

    async def test_alerts_exactly_at_threshold(self):
        """정확히 -3%인 경계값에서도 알림이 발송되어야 한다"""
        with patch("common.telegram.send_stock", new_callable=AsyncMock) as mock_send:
            await self.trader._check_sell_slippage("005930", 10000, 9700)  # -3.0%
            mock_send.assert_awaited_once()

    async def test_no_alert_within_threshold(self):
        """체결가가 신호가 대비 3% 미만 차이면 알림을 보내지 않는다"""
        with patch("common.telegram.send_stock", new_callable=AsyncMock) as mock_send:
            await self.trader._check_sell_slippage("005930", 10000, 9800)  # -2%
            mock_send.assert_not_awaited()

    async def test_no_alert_when_fill_price_better_than_signal(self):
        """체결가가 신호가보다 오히려 좋으면 알림을 보내지 않는다"""
        with patch("common.telegram.send_stock", new_callable=AsyncMock) as mock_send:
            await self.trader._check_sell_slippage("005930", 10000, 10500)
            mock_send.assert_not_awaited()

    async def test_no_alert_when_avg_fill_price_missing(self):
        """평균체결가 조회 실패(0/None)면 알림을 시도하지 않는다"""
        with patch("common.telegram.send_stock", new_callable=AsyncMock) as mock_send:
            await self.trader._check_sell_slippage("005930", 10000, 0)
            await self.trader._check_sell_slippage("005930", 10000, None)
            mock_send.assert_not_awaited()


class TestSellIntegratesSlippageCheck(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.trader = KISTrader()
        config.KIS_ACCOUNT_NO = "50193041"

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_sell_triggers_slippage_alert_on_bad_fill(self, mock_sleep):
        order_resp = {"rt_cd": "0", "output": {"ODNO": "0009999999"}, "msg1": "주문접수완료"}
        self.trader._new_session = lambda: DummySession(order_resp)
        self.trader._get_filled_qty = AsyncMock(return_value=(10, 8820.0))

        with patch("common.telegram.send_stock", new_callable=AsyncMock) as mock_send:
            res = await self.trader.sell("900070", 10000, 10)

        self.assertTrue(res["success"])
        self.trader._get_filled_qty.assert_awaited_once_with("0009999999", "900070", with_price=True)
        mock_send.assert_awaited_once()

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_sell_no_alert_on_good_fill(self, mock_sleep):
        order_resp = {"rt_cd": "0", "output": {"ODNO": "0009999998"}, "msg1": "주문접수완료"}
        self.trader._new_session = lambda: DummySession(order_resp)
        self.trader._get_filled_qty = AsyncMock(return_value=(10, 9990.0))

        with patch("common.telegram.send_stock", new_callable=AsyncMock) as mock_send:
            res = await self.trader.sell("005930", 10000, 10)

        self.assertTrue(res["success"])
        mock_send.assert_not_awaited()

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_sell_does_not_block_order_on_bad_slippage(self, mock_sleep):
        """슬리피지가 심해도 주문 자체(성공 판정)는 막지 않는다 — 감지·기록·알림까지만"""
        order_resp = {"rt_cd": "0", "output": {"ODNO": "0009999997"}, "msg1": "주문접수완료"}
        self.trader._new_session = lambda: DummySession(order_resp)
        self.trader._get_filled_qty = AsyncMock(return_value=(10, 5000.0))  # -50% 체결

        with patch("common.telegram.send_stock", new_callable=AsyncMock):
            res = await self.trader.sell("900070", 10000, 10)

        self.assertTrue(res["success"])
        self.assertEqual(res["filled_qty"], 10)


if __name__ == "__main__":
    unittest.main()
