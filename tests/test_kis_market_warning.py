"""
stock_trader/kis_trader.py의 get_market_warning() (투자경고/VI 상태 조회) 회귀 테스트.

검증 항목:
1. 정상 종목(mrkt_warn_cls_code="00")이면 mrkt_warn_cls_code/vi_cls_code를 그대로 반환한다.
2. 투자경고 종목(mrkt_warn_cls_code="02")이면 해당 코드를 그대로 반환한다(호출부가 판단).
3. API 실패(rt_cd != "0")면 get_current_price()처럼 값을 조작하지 않고 None을 반환한다.
4. HTTP status != 200 이면 None을 반환한다.
5. 응답 output이 비어있으면 None을 반환한다.
6. 네트워크 예외 발생 시 None을 반환한다.
7. 연속 호출 시 최소 호출 간격(MARKET_WARNING_MIN_INTERVAL_SEC)만큼 대기한다(속도제한 방지).
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
    def __init__(self, data, status=200):
        self._data = data
        self.status = status

    async def json(self):
        return self._data

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class DummySession:
    def __init__(self, resp_data=None, status=200, raise_exc=None):
        self.resp_data = resp_data
        self.status = status
        self.raise_exc = raise_exc

    def get(self, url, headers=None, params=None):
        if self.raise_exc:
            raise self.raise_exc
        return DummyResponse(self.resp_data, status=self.status)

    async def close(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class TestGetMarketWarning(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.trader = KISTrader()
        config.KIS_ACCOUNT_NO = "50193041"

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_normal_stock_returns_codes(self, mock_sleep):
        """정상 종목: mrkt_warn_cls_code=00, vi_cls_code=N 그대로 반환"""
        resp = {
            "rt_cd": "0",
            "output": {"mrkt_warn_cls_code": "00", "vi_cls_code": "N"},
        }
        self.trader._new_session = lambda: DummySession(resp)

        result = await self.trader.get_market_warning("005930")

        self.assertEqual(result, {"mrkt_warn_cls_code": "00", "vi_cls_code": "N"})

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_warning_stock_returns_flagged_code(self, mock_sleep):
        """투자경고 종목: mrkt_warn_cls_code=02 그대로 반환(스킵 여부는 호출부 책임)"""
        resp = {
            "rt_cd": "0",
            "output": {"mrkt_warn_cls_code": "02", "vi_cls_code": "N"},
        }
        self.trader._new_session = lambda: DummySession(resp)

        result = await self.trader.get_market_warning("109670")

        self.assertEqual(result["mrkt_warn_cls_code"], "02")

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_api_error_rt_cd_returns_none(self, mock_sleep):
        """rt_cd != '0' (예: 초당 거래건수 초과)이면 값을 조작하지 않고 None 반환"""
        resp = {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다"}
        self.trader._new_session = lambda: DummySession(resp)

        result = await self.trader.get_market_warning("147760")

        self.assertIsNone(result)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_http_status_error_returns_none(self, mock_sleep):
        """HTTP status != 200 이면 None 반환"""
        resp = {"rt_cd": "0", "output": {"mrkt_warn_cls_code": "00"}}
        self.trader._new_session = lambda: DummySession(resp, status=500)

        result = await self.trader.get_market_warning("147760")

        self.assertIsNone(result)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_empty_output_returns_none(self, mock_sleep):
        """output이 빈 딕셔너리(147760 사례)면 None 반환 — 0/정상으로 오인하지 않음"""
        resp = {"rt_cd": "0", "output": {}}
        self.trader._new_session = lambda: DummySession(resp)

        result = await self.trader.get_market_warning("147760")

        self.assertIsNone(result)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_network_exception_returns_none(self, mock_sleep):
        """네트워크 예외 발생 시 None 반환"""
        self.trader._new_session = lambda: DummySession(raise_exc=ConnectionError("boom"))

        result = await self.trader.get_market_warning("147760")

        self.assertIsNone(result)

    async def test_enforces_min_interval_between_calls(self):
        """연속 호출 시 최소 호출 간격만큼 sleep으로 대기한다(속도제한 회피)"""
        resp = {"rt_cd": "0", "output": {"mrkt_warn_cls_code": "00", "vi_cls_code": "N"}}
        self.trader._new_session = lambda: DummySession(resp)
        self.trader._last_market_warning_call_ts = 0.0

        with patch("time.time", return_value=100.2), \
             patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            # 직전 호출 시각을 100.0으로 세팅한 뒤 100.2초에 재호출 → 0.3초 대기 필요
            self.trader._last_market_warning_call_ts = 100.0
            await self.trader.get_market_warning("005930")

            mock_sleep.assert_awaited_once()
            waited = mock_sleep.await_args.args[0]
            self.assertAlmostEqual(waited, 0.3, places=3)


if __name__ == "__main__":
    unittest.main()
