"""
stock_trader/kis_trader.py sell() 지연 체결(pending) 재확인 회귀 테스트.

배경: [AT] fix/stock-trader-fill-reconcile — 모의투자 서버 지연으로 매도 직후(1.5초) 체결이
아직 반영되지 않은 주문을 즉시 미체결(실패)로 단정해 trade_history 기록이 빠지는 문제
(피엠티·이뮨온시아·쓰리빌리언·동일스틸럭스·휴림로봇 매도 기록 누락)를 재현/검증한다.

검증 항목:
1. 첫 확인이 0주여도 +3초, +7초 재확인에서 체결되면 success=True를 반환한다.
2. 재확인 끝까지 0주면 success=False, pending=True를 반환하고 Redis에 주문 맥락
   (symbol/side/qty/price/strategy/avg_price=평단가/accepted_at)과 중복 주문 방지 락을 저장한다.
3. 같은 종목·방향으로 pending 락이 걸려 있으면 새 매도 주문 자체를 내지 않고 즉시 차단한다.
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

    def post(self, url, headers=None, json=None):
        return DummyResponse(self.resp_data)

    async def close(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class TestKISTraderSellPendingReconcile(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.trader = KISTrader()
        config.KIS_ACCOUNT_NO = "50193041"

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_sell_retry_succeeds_on_third_confirm(self, mock_sleep):
        """1.5초/+3초 확인은 0주였지만 +7초 재확인에서 체결되면 성공으로 반환해야 한다."""
        order_resp = {"rt_cd": "0", "output": {"ODNO": "0007000001"}, "msg1": "주문접수완료"}
        self.trader._new_session = lambda: DummySession(order_resp)
        self.trader._get_filled_qty = AsyncMock(
            side_effect=[(0, 0.0), (0, 0.0), (10, 9800.0)]
        )

        res = await self.trader.sell("900070", 10000, 10)

        self.assertTrue(res["success"])
        self.assertEqual(res["filled_qty"], 10)
        self.assertEqual(self.trader._get_filled_qty.await_count, 3)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_sell_pending_when_still_unfilled_after_retries(self, mock_sleep):
        """재확인 2회(+3초, +7초)에도 0주면 pending을 반환하고 Redis에 전략명·평단가를
        포함한 주문 맥락과 중복 주문 방지 락을 저장해야 한다."""
        order_resp = {"rt_cd": "0", "output": {"ODNO": "0007000002"}, "msg1": "주문접수완료"}
        self.trader._new_session = lambda: DummySession(order_resp)
        self.trader._get_filled_qty = AsyncMock(return_value=(0, 0.0))
        fake_redis = FakeRedis()

        with patch.object(cache, "client", fake_redis):
            res = await self.trader.sell(
                "005930", 60000, 10, strategy="MA크로스_손절", avg_price=70000.0)

        self.assertFalse(res["success"])
        self.assertTrue(res.get("pending"))
        self.assertEqual(res["order_no"], "0007000002")
        self.assertEqual(self.trader._get_filled_qty.await_count, 3)

        ctx = json.loads(fake_redis.store["order_pending:0007000002"])
        self.assertEqual(ctx["symbol"], "005930")
        self.assertEqual(ctx["side"], "sell")
        self.assertEqual(ctx["qty"], 10)
        self.assertEqual(ctx["strategy"], "MA크로스_손절")
        self.assertEqual(ctx["avg_price"], 70000.0)
        self.assertEqual(fake_redis.store["order_pending_lock:005930:sell"], "0007000002")

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_sell_blocked_while_pending_lock_held(self, mock_sleep):
        """같은 종목·방향으로 pending 락이 걸려 있으면 새 매도 주문 자체를 내지 않고 즉시 차단한다."""
        fake_redis = FakeRedis({"order_pending_lock:005930:sell": "0007000002"})
        self.trader._new_session = AsyncMock(side_effect=AssertionError("주문 API가 호출되면 안 됨"))

        with patch.object(cache, "client", fake_redis):
            res = await self.trader.sell("005930", 60000, 10)

        self.assertFalse(res["success"])
        self.assertTrue(res.get("pending"))


if __name__ == "__main__":
    unittest.main()
