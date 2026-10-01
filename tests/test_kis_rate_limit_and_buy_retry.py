"""
tests/test_kis_rate_limit_and_buy_retry.py - [AT] fix/kis-rate-limit-and-buy-retry 테스트

배경: 10/1 실측 — 매수 사이클이 종목마다 KIS를 연속 호출해 "초당 거래건수 초과"
(KIS 속도제한)에 걸렸고, 30분 고정 억제로 매수 기회가 날아갔다. 이 파일은 수정 B
(KISTrader 레벨 공용 최소 호출 간격 + EGW00201 자동 재시도)를 검증한다.
수정 A(execution_guard.py 매수 실패 억제 시간 차등화)는 tests/test_execution_guard.py에서
검증한다.

검증 항목:
1. KISTrader 공용 최소 호출 간격(_throttle_kis_call, KIS_MIN_CALL_INTERVAL_SEC)이
   직전 호출 시각 기준으로 남은 대기 시간만큼만 sleep한다.
2. _refresh_token_if_expired()는 EGW00201(속도제한)을 토큰 만료로 오인해 강제
   재발급하지 않는다 ("EGW00" 접두사가 겹쳐 오매칭되던 버그 수정).
3. buy()/get_balance()/get_positions()가 KIS 속도제한(EGW00201) 응답을 받으면
   1.5초 후 1회 자동 재시도해 성공을 반환한다.
4. sell()은 공용 최소 호출 간격 적용에서는 제외되지만(체결 지연이 손실 확대로
   이어질 수 있어서), 속도제한(EGW00201) 응답은 buy()와 동일하게 1회 자동 재시도한다.
5. get_balance()는 실제 KIS가 속도제한(EGW00201)을 HTTP 500으로 반환하는 경우에도
   (10/1 10:58:48, 9/30 11:18:20 로그 실측) status==200 여부와 무관하게 재시도한다
   (fix/kis-rate-limit-http500 — status==200 조건에 걸려 재시도가 발동하지 않던 버그 수정).
"""
import sys
import time
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "stock_trader"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _stub_missing_runtime_deps():
    """requirements.txt의 asyncpg/redis/aiohttp가 테스트 환경에 없어도
    import만 되도록 더미로 대체한다. 실제 패키지가 설치되어 있으면 아무 것도 하지 않는다."""
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


class _Resp:
    def __init__(self, data, status=200):
        self._data = data
        self.status = status

    async def json(self):
        return self._data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _QueueSession:
    """호출될 때마다 큐에서 다음 응답을 꺼내는 세션 더블(순차적 재시도 시나리오 검증용)."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.call_count = 0

    def _next(self):
        self.call_count += 1
        item = self._responses.pop(0)
        if isinstance(item, tuple):
            data, status = item
        else:
            data, status = item, 200
        return _Resp(data, status=status)

    def get(self, url, *args, **kwargs):
        return self._next()

    def post(self, url, *args, **kwargs):
        return self._next()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class TestThrottleKisCall(unittest.IsolatedAsyncioTestCase):
    async def test_waits_remaining_interval_since_last_call(self):
        """직전 호출로부터 0.2초만 지났으면 KIS_MIN_CALL_INTERVAL_SEC(0.55) - 0.2 = 0.35초 대기한다."""
        trader = KISTrader()
        with patch("time.time", return_value=100.2), \
             patch("stock_trader.kis_trader.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            trader._last_kis_call_ts = 100.0
            await trader._throttle_kis_call()
            mock_sleep.assert_awaited_once()
            waited = mock_sleep.await_args.args[0]
            self.assertAlmostEqual(waited, 0.35, places=3)

    async def test_no_wait_when_interval_already_elapsed(self):
        trader = KISTrader()
        with patch("time.time", return_value=200.0), \
             patch("stock_trader.kis_trader.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            trader._last_kis_call_ts = 100.0
            await trader._throttle_kis_call()
            mock_sleep.assert_not_awaited()


class TestRefreshTokenIgnoresRateLimit(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.trader = KISTrader()
        self.trader._get_token = AsyncMock()

    async def test_rate_limit_code_does_not_trigger_token_reissue(self):
        """EGW00201(초당 거래건수 초과)은 'EGW00' 접두사가 겹치지만 토큰 문제가
        아니므로 강제 재발급을 하면 안 된다."""
        result = await self.trader._refresh_token_if_expired(
            {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다"})
        self.assertFalse(result)
        self.trader._get_token.assert_not_awaited()

    async def test_real_token_expiry_still_triggers_reissue(self):
        """실제 토큰 만료(EGW00123 등)는 기존대로 재발급을 트리거해야 한다(회귀 방지)."""
        result = await self.trader._refresh_token_if_expired(
            {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "기간이 만료된 token 입니다"})
        self.assertTrue(result)
        self.trader._get_token.assert_awaited_once()


class TestBuyRateLimitRetry(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        config.KIS_ACCOUNT_NO = "50193041"

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_buy_retries_once_on_rate_limit_then_succeeds(self, mock_sleep):
        trader = KISTrader()
        limited_resp = {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다"}
        success_resp = {"rt_cd": "0", "output": {"ODNO": "0001234567"}, "msg1": "주문접수완료"}
        session = _QueueSession([limited_resp, success_resp])
        trader._new_session = lambda: session
        trader._get_filled_qty = AsyncMock(return_value=10)

        res = await trader.buy("005930", 70000, 10)

        self.assertTrue(res["success"])
        self.assertEqual(res["filled_qty"], 10)
        self.assertEqual(session.call_count, 2)


class TestGetBalanceRateLimitRetry(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        config.KIS_ACCOUNT_NO = "50193041"

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_get_balance_retries_once_on_rate_limit_then_succeeds(self, mock_sleep):
        trader = KISTrader()
        # 총평가금액(tot_evlu_amt)은 inquire-psbl-order 응답에 없는 필드다(실제 KIS 키 목록
        # — [AT] fix/balance-total-source). get_positions()가 이미 채워 둔 신선한 캐시가
        # 있다고 가정해 get_balance()가 추가 KIS 호출 없이 재사용하도록 한다.
        trader._last_total = 2_000_000
        trader._last_total_ts = time.time()
        limited_resp = {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다"}
        success_resp = {
            "rt_cd": "0",
            "output": {"ord_psbl_cash": "1000000"},
        }
        session = _QueueSession([limited_resp, success_resp])
        trader._new_session = lambda: session

        res = await trader.get_balance()

        self.assertEqual(res["cash"], 1000000)
        self.assertEqual(res["total"], 2_000_000)
        self.assertNotIn("stale", res)
        self.assertEqual(session.call_count, 2)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_get_balance_retries_on_http_500_rate_limit_then_succeeds(self, mock_sleep):
        """실제 KIS는 속도제한(EGW00201)을 HTTP 500으로 반환한다(10/1 10:58:48,
        9/30 11:18:20 로그). status==200 조건에 걸려 재시도가 발동하지 않던 버그
        회귀 테스트."""
        trader = KISTrader()
        trader._last_total = 2_000_000
        trader._last_total_ts = time.time()
        limited_resp = ({"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다"}, 500)
        success_resp = ({
            "rt_cd": "0",
            "output": {"ord_psbl_cash": "1000000"},
        }, 200)
        session = _QueueSession([limited_resp, success_resp])
        trader._new_session = lambda: session

        res = await trader.get_balance()

        self.assertEqual(res["cash"], 1000000)
        self.assertNotIn("stale", res)
        self.assertEqual(session.call_count, 2)


class TestGetPositionsRateLimitRetry(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        config.KIS_ACCOUNT_NO = "50193041"

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_get_positions_retries_once_on_rate_limit_without_token_reissue(self, mock_sleep):
        trader = KISTrader()
        limited_resp = {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다"}
        success_resp = {"rt_cd": "0", "output1": [], "output2": [{"dnca_tot_amt": "500000"}]}
        session = _QueueSession([limited_resp, success_resp])
        trader._new_session = lambda: session
        trader._get_token = AsyncMock()

        res = await trader.get_positions()

        self.assertEqual(res, [])
        self.assertEqual(session.call_count, 2)
        # 속도제한을 토큰만료로 오인해 재발급하면 안 된다(수정 B의 전제조건 버그 수정 검증)
        trader._get_token.assert_not_awaited()


class TestSellRateLimitRetryWithoutThrottle(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        config.KIS_ACCOUNT_NO = "50193041"

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_sell_retries_once_on_rate_limit_then_succeeds(self, mock_sleep):
        trader = KISTrader()
        limited_resp = {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다"}
        success_resp = {"rt_cd": "0", "output": {"ODNO": "0009999999"}, "msg1": "주문접수완료"}
        session = _QueueSession([limited_resp, success_resp])
        trader._new_session = lambda: session
        trader._get_filled_qty = AsyncMock(return_value=(10, 70000.0))

        res = await trader.sell("005930", 70000, 10)

        self.assertTrue(res["success"])
        self.assertEqual(session.call_count, 2)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_sell_does_not_apply_shared_min_call_interval(self, mock_sleep):
        """매도는 체결 지연이 손실 확대로 이어질 수 있어 공용 최소 호출 간격
        (_throttle_kis_call)을 적용하지 않는다."""
        trader = KISTrader()
        trader._throttle_kis_call = AsyncMock()
        success_resp = {"rt_cd": "0", "output": {"ODNO": "0009999998"}, "msg1": "주문접수완료"}
        trader._new_session = lambda: _QueueSession([success_resp])
        trader._get_filled_qty = AsyncMock(return_value=(10, 70000.0))

        await trader.sell("005930", 70000, 10)

        trader._throttle_kis_call.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
