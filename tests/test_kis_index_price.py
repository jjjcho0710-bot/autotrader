"""
stock_trader/kis_trader.py의 get_index_price() (국내업종 현재지수 조회, FHPUP02100000)
단위 테스트([AT] feat/entry-risk-shadow).

검증 항목:
1. 정상 조회 시 {"close", "change_rate"}를 bstp_nmix_prpr/bstp_nmix_prdy_ctrt에서 그대로 뽑아 반환.
2. API 실패(rt_cd != "0")면 None 반환.
3. 현재지수가 0 이하이면(조회 실패로 간주) None 반환.
4. 응답 파싱 실패(숫자 변환 불가)면 None 반환.
5. 네트워크 예외 발생 시 None 반환.
6. 속도제한(EGW00201) 1회 재시도 후 성공.
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


class QueueDummySession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.call_count = 0

    def get(self, url, headers=None, params=None):
        self.call_count += 1
        data, status = self._responses.pop(0)
        return DummyResponse(data, status=status)

    async def close(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class TestGetIndexPrice(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.trader = KISTrader()
        config.KIS_ACCOUNT_NO = "50193041"

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_normal_kospi_returns_close_and_change_rate(self, mock_sleep):
        resp = {
            "rt_cd": "0",
            "output": {"bstp_nmix_prpr": "3123.45", "bstp_nmix_prdy_ctrt": "-0.52"},
        }
        self.trader._new_session = lambda: DummySession(resp)

        result = await self.trader.get_index_price("0001")

        self.assertEqual(result, {"close": 3123.45, "change_rate": -0.52})

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_normal_kosdaq_returns_close_and_change_rate(self, mock_sleep):
        resp = {
            "rt_cd": "0",
            "output": {"bstp_nmix_prpr": "812.1", "bstp_nmix_prdy_ctrt": "1.03"},
        }
        self.trader._new_session = lambda: DummySession(resp)

        result = await self.trader.get_index_price("1001")

        self.assertEqual(result, {"close": 812.1, "change_rate": 1.03})

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_api_error_rt_cd_returns_none(self, mock_sleep):
        resp = {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다"}
        self.trader._new_session = lambda: DummySession(resp)

        result = await self.trader.get_index_price("0001")

        self.assertIsNone(result)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_zero_close_returns_none(self, mock_sleep):
        """현재지수 0 이하는 정상 응답이라도 조회 실패로 간주해 None(저장 스킵)."""
        resp = {"rt_cd": "0", "output": {"bstp_nmix_prpr": "0", "bstp_nmix_prdy_ctrt": "0"}}
        self.trader._new_session = lambda: DummySession(resp)

        result = await self.trader.get_index_price("0001")

        self.assertIsNone(result)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_unparseable_output_returns_none(self, mock_sleep):
        resp = {"rt_cd": "0", "output": {"bstp_nmix_prpr": "N/A", "bstp_nmix_prdy_ctrt": "N/A"}}
        self.trader._new_session = lambda: DummySession(resp)

        result = await self.trader.get_index_price("0001")

        self.assertIsNone(result)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_network_exception_returns_none(self, mock_sleep):
        self.trader._new_session = lambda: DummySession(raise_exc=ConnectionError("boom"))

        result = await self.trader.get_index_price("0001")

        self.assertIsNone(result)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_rate_limit_retries_then_succeeds(self, mock_sleep):
        limited = ({"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다"}, 200)
        success = ({"rt_cd": "0", "output": {"bstp_nmix_prpr": "3100.0", "bstp_nmix_prdy_ctrt": "0.1"}}, 200)
        session = QueueDummySession([limited, success])
        self.trader._new_session = lambda: session

        result = await self.trader.get_index_price("0001")

        self.assertEqual(result, {"close": 3100.0, "change_rate": 0.1})
        self.assertEqual(session.call_count, 2)


if __name__ == "__main__":
    unittest.main()
