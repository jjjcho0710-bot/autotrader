"""
stock_trader/kis_trader.py의 buy() 체결 확인 로직 회귀 테스트.

검증 항목:
1. KISTrader.buy() 호출 시 주문 접수(rt_cd=0) 후 _get_filled_qty(order_no, symbol, side="02")를 호출한다.
2. 체결수량이 0 이하(미체결)이면 즉시 실패로 단정하지 않고 +3초, +7초 두 번 더 재확인한다.
   재확인 중 체결이 확인되면 기존 성공 형태로 반환한다.
3. 체결수량이 정상 확인(양수)되면 success=True 및 filled_qty를 반환한다.
4. 체결 확인 조회가 실패(None)하면 접수 결과만으로 보수적 성공(fill_unconfirmed=True)을 반환한다.
5. 재확인(+3초, +7초) 끝까지 0/None이면 success=False, pending=True를 반환하고 Redis에
   주문 맥락(order_pending:{order_no})과 중복 주문 방지 락(order_pending_lock:{symbol}:buy)을 저장한다.
6. pending 락이 걸려 있는 동안 같은 종목·방향 재주문은 즉시 차단된다([AT] fix/stock-trader-fill-reconcile).
"""
import asyncio
import json
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
from common.database import cache  # noqa: E402
from stock_trader.kis_trader import KISTrader  # noqa: E402


class FakeRedis:
    """cache.client를 흉내내는 최소 인메모리 Redis 대역(get/setex/delete만 지원)"""

    def __init__(self, store=None):
        self.store = store or {}

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, value):
        self.store[key] = value

    async def delete(self, key):
        self.store.pop(key, None)


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

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_buy_retry_succeeds_on_second_confirm(self, mock_sleep):
        """첫 확인(1.5초)은 0주였지만 +3초 재확인에서 체결되면 성공으로 반환해야 한다
        (모의투자 서버 지연으로 TCC스틸 등 매수 기록이 누락된 문제 재현/수정 검증)."""
        order_resp = {
            "rt_cd": "0",
            "output": {"ODNO": "0001234570"},
            "msg1": "주문접수완료",
        }
        self.trader._new_session = lambda: DummySession(order_resp)
        self.trader._get_filled_qty = AsyncMock(side_effect=[0, 10])

        res = await self.trader.buy("005930", 70000, 10)

        self.assertTrue(res["success"])
        self.assertEqual(res["filled_qty"], 10)
        self.assertEqual(self.trader._get_filled_qty.await_count, 2)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_buy_pending_when_still_unfilled_after_retries(self, mock_sleep):
        """1.5초 확인 + 재확인 2회(+3초, +7초) 모두 0주면 실패 단정 대신 pending을 반환하고
        Redis에 주문 맥락과 중복 주문 방지 락을 저장해야 한다."""
        order_resp = {
            "rt_cd": "0",
            "output": {"ODNO": "0001234571"},
            "msg1": "주문접수완료",
        }
        self.trader._new_session = lambda: DummySession(order_resp)
        self.trader._get_filled_qty = AsyncMock(return_value=0)
        fake_redis = FakeRedis()

        with patch.object(cache, "client", fake_redis):
            res = await self.trader.buy("005930", 70000, 10, strategy="MA크로스_매수")

        self.assertFalse(res["success"])
        self.assertTrue(res.get("pending"))
        self.assertEqual(res["order_no"], "0001234571")
        self.assertEqual(self.trader._get_filled_qty.await_count, 3)  # 1.5초 + 3초 + 7초

        ctx = json.loads(fake_redis.store["order_pending:0001234571"])
        self.assertEqual(ctx["symbol"], "005930")
        self.assertEqual(ctx["side"], "buy")
        self.assertEqual(ctx["qty"], 10)
        self.assertEqual(ctx["strategy"], "MA크로스_매수")
        self.assertEqual(fake_redis.store["order_pending_lock:005930:buy"], "0001234571")

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_buy_blocked_while_pending_lock_held(self, mock_sleep):
        """같은 종목·방향으로 pending 락이 걸려 있으면 새 매수 주문 자체를 내지 않고 즉시 차단한다."""
        fake_redis = FakeRedis({"order_pending_lock:005930:buy": "0001234571"})
        self.trader._new_session = AsyncMock(side_effect=AssertionError("주문 API가 호출되면 안 됨"))

        with patch.object(cache, "client", fake_redis):
            res = await self.trader.buy("005930", 70000, 10)

        self.assertFalse(res["success"])
        self.assertTrue(res.get("pending"))


if __name__ == "__main__":
    unittest.main()
