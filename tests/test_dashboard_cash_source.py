"""
tests/test_dashboard_cash_source.py - [AT] fix/dashboard-cash-source 회귀 테스트

확정된 버그: dashboard/main.py _get_stock_positions_raw()의 예수금(cash) 계산이
prvs_rcdl_excc_amt(D+2) → nxdy_excc_amt(D+1) → dnca_tot_amt 순으로 첫 0이 아닌 값을
썼다. 모의투자 응답에서 앞의 두 값이 0이라 dnca_tot_amt(매수해도 D+2 결제 전까지 안
줄어드는 값)로 내려가, 실제 가용 현금(총평가-주식평가)보다 과대 표시됐다(10/1 실측:
tot_evlu_amt=9,909,068, scts_evlu_amt=8,796,200, dnca_tot_amt=6,360,689 → 실제 현금은
1,112,868인데 6,360,689로 표시).

수정: 총평가(tot_evlu_amt) - 주식평가(scts_evlu_amt)가 0보다 크고 총평가보다 작으면
그 값을 최우선으로 쓴다. 그 다음 prvs_rcdl_excc_amt, nxdy_excc_amt 순이고,
dnca_tot_amt는 위 값이 모두 없을 때의 최후 폴백이다.
"""
import asyncio
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "stock_trader"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _stub_missing_runtime_deps():
    """requirements.txt의 asyncpg/redis/aiohttp/fastapi가 테스트 환경에 없어도
    임포트할 수 있도록 더미로 대체한다."""
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

        class ServerTimeoutError(Exception):
            pass

        stub.ServerTimeoutError = ServerTimeoutError
        sys.modules["aiohttp"] = stub

    for mod_name in [
        "fastapi",
        "fastapi.staticfiles",
        "fastapi.responses",
        "fastapi.middleware.cors",
        "google",
        "google.generativeai",
    ]:
        if mod_name not in sys.modules:
            sys.modules[mod_name] = MagicMock()


_stub_missing_runtime_deps()


class FakeResponse:
    def __init__(self, data: dict, status: int = 200):
        self._data = data
        self.status = status

    def __await__(self):
        async def _coro():
            return self
        return _coro().__await__()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._data


class FakeSession:
    def __init__(self, output2: dict, output1=None):
        self._output2 = output2
        self._output1 = output1 or []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, *args, **kwargs):
        return FakeResponse({
            "rt_cd": "0",
            "output1": self._output1,
            "output2": [self._output2],
        })


class TestDashboardCashSource(unittest.IsolatedAsyncioTestCase):
    async def _run_with_summary(self, summary: dict):
        import dashboard.main as dm

        dm._stock_positions_cache = None
        dm._stock_positions_cache_ts = 0.0

        with patch("dashboard.main.get_kis_token", AsyncMock(return_value="FAKE_TOKEN")), \
             patch("aiohttp.ClientSession", return_value=FakeSession(summary)):
            return await dm._get_stock_positions_raw()

    async def test_real_values_derive_cash_from_total_minus_stock_eval(self):
        """10/1 실측 값(prvs_rcdl_excc_amt=0, nxdy_excc_amt=0)에서 cash는
        총평가-주식평가(1,112,868)가 나와야 한다 — dnca_tot_amt(6,360,689)가 아니다."""
        result = await self._run_with_summary({
            "tot_evlu_amt": "9909068",
            "scts_evlu_amt": "8796200",
            "dnca_tot_amt": "6360689",
            "prvs_rcdl_excc_amt": "0",
            "nxdy_excc_amt": "0",
        })

        self.assertTrue(result["success"])
        self.assertEqual(result["account"]["cash"], 1112868)
        self.assertEqual(result["account"]["total_eval"], 9909068)
        self.assertEqual(result["account"]["stock_eval"], 8796200)

    async def test_total_minus_stock_eval_takes_priority_over_prvs_rcdl_excc_amt(self):
        """prvs_rcdl_excc_amt가 0이 아니어도, 총평가-주식평가가 유효(0보다 크고 총평가보다
        작음)하면 그 값이 우선이어야 한다."""
        result = await self._run_with_summary({
            "tot_evlu_amt": "9909068",
            "scts_evlu_amt": "8796200",
            "dnca_tot_amt": "6360689",
            "prvs_rcdl_excc_amt": "500000",
            "nxdy_excc_amt": "0",
        })

        self.assertEqual(result["account"]["cash"], 1112868)

    async def test_fallback_order_when_total_eval_missing(self):
        """총평가(tot_evlu_amt) 필드가 없어 총평가-주식평가를 신뢰할 수 없으면
        prvs_rcdl_excc_amt → nxdy_excc_amt → dnca_tot_amt 순 폴백을 그대로 따라야 한다."""
        result = await self._run_with_summary({
            "scts_evlu_amt": "8796200",
            "dnca_tot_amt": "6360689",
            "prvs_rcdl_excc_amt": "700000",
            "nxdy_excc_amt": "0",
        })

        self.assertEqual(result["account"]["cash"], 700000)

    async def test_fallback_reaches_dnca_tot_amt_when_all_else_zero(self):
        """총평가가 없고 prvs_rcdl_excc_amt/nxdy_excc_amt도 0이면 최후 폴백인
        dnca_tot_amt를 써야 한다."""
        result = await self._run_with_summary({
            "scts_evlu_amt": "8796200",
            "dnca_tot_amt": "6360689",
            "prvs_rcdl_excc_amt": "0",
            "nxdy_excc_amt": "0",
        })

        self.assertEqual(result["account"]["cash"], 6360689)


if __name__ == "__main__":
    unittest.main()
