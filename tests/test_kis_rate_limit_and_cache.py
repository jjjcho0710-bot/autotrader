"""
tests/test_kis_rate_limit_and_cache.py - KIS 잔고/포지션 동시 호출 직렬화 및 캐시 검증 테스트

검증 항목:
1. KISTrader.get_balance 동시 10건 호출 시 KIS 실제 호출 1회 (직렬화 + 20초 캐시)
2. 주문 체결(buy/sell) 시 잔고 캐시 즉시 무효화 및 재조회 검증
3. 잔고 조회 실패(HTTP 에러/rt_cd!=0/타임아웃) 시 사유 로그 기록 및 계좌번호 미노출 검증
4. dashboard/main.py _get_stock_positions_raw 동시 10건 호출 시 KIS 실제 호출 1회 (직렬화 + 10초 캐시)
"""
import asyncio
import logging
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

from common.config import config
from stock_trader.kis_trader import KISTrader


class FakeResponse:
    def __init__(self, data: dict, status: int = 200, delay: float = 0.0, error: Exception = None):
        self._data = data
        self.status = status
        self._delay = delay
        self._error = error

    def __await__(self):
        async def _coro():
            if self._delay > 0:
                await asyncio.sleep(self._delay)
            if self._error:
                raise self._error
            return self
        return _coro().__await__()

    async def __aenter__(self):
        if self._delay > 0:
            await asyncio.sleep(self._delay)
        if self._error:
            raise self._error
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._data


class FakeSession:
    def __init__(self, handler):
        self.handler = handler

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, *args, **kwargs):
        return self.handler(url, *args, **kwargs)

    def post(self, url, *args, **kwargs):
        return self.handler(url, *args, **kwargs)


class TestKISTraderBalanceRateLimitAndCache(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.trader = KISTrader()
        self.trader.access_token = "TEST_TOKEN"
        self.call_count = 0

    async def test_concurrent_10_calls_triggers_single_kis_request(self):
        """동시 10건 잔고 조회 호출 시 실제 KIS HTTP 요청은 1회만 발생하고 결과는 모두 동일해야 함"""
        def fake_get(url, *args, **kwargs):
            self.call_count += 1
            return FakeResponse({
                "rt_cd": "0",
                "msg_cd": "MCA00000",
                "msg1": "정상조회",
                "output": {
                    "ord_psbl_cash": "1500000",
                    "tot_evlu_amt": "3500000",
                }
            }, delay=0.05)

        self.trader._new_session = lambda: FakeSession(fake_get)

        # 10개 비동기 요청 동시 발송
        tasks = [self.trader.get_balance() for _ in range(10)]
        results = await asyncio.gather(*tasks)

        self.assertEqual(len(results), 10)
        self.assertEqual(self.call_count, 1)  # KIS 호출 단 1회
        for r in results:
            self.assertEqual(r["cash"], 1500000)
            self.assertEqual(r["total"], 3500000)
            self.assertFalse(r.get("stale", False))  # 정상 조회 시 stale 없음/False

    async def test_cache_invalidation_on_order_fill(self):
        """주문 체결 성공 시 잔고 캐시가 무효화되어 다음 get_balance 시 KIS를 재호출해야 함"""
        def fake_get(url, *args, **kwargs):
            self.call_count += 1
            return FakeResponse({
                "rt_cd": "0",
                "msg_cd": "MCA00000",
                "msg1": "정상조회",
                "output": {
                    "ord_psbl_cash": "2000000",
                    "tot_evlu_amt": "5000000",
                }
            })

        self.trader._new_session = lambda: FakeSession(fake_get)

        # 1차 조회 -> KIS 1회 호출
        r1 = await self.trader.get_balance()
        self.assertEqual(r1["cash"], 2000000)
        self.assertEqual(self.call_count, 1)

        # 2차 조회 (캐시 유효) -> KIS 호출 없이 캐시 반환
        r2 = await self.trader.get_balance()
        self.assertEqual(r2["cash"], 2000000)
        self.assertEqual(self.call_count, 1)

        # 주문 체결 등으로 캐시 무효화 발생
        self.trader.invalidate_balance_cache()

        # 3차 조회 -> 캐시가 무효화되었으므로 KIS 재호출 (총 2회)
        r3 = await self.trader.get_balance()
        self.assertEqual(r3["cash"], 2000000)
        self.assertEqual(self.call_count, 2)

    async def test_failure_logs_status_and_codes_without_account_number(self):
        """KIS 실패 응답 시 HTTP 상태, rt_cd, msg_cd, msg1이 로그에 기록되고 계좌번호는 마스킹되어야 함"""
        acct_no = config.kis_account_no
        raw_msg = f"계좌번호 {acct_no}에 대한 초당 거래건수 초과입니다"

        def fake_fail_get(url, *args, **kwargs):
            return FakeResponse({
                "rt_cd": "1",
                "msg_cd": "EGW00123",
                "msg1": raw_msg,
            }, status=429)

        self.trader._new_session = lambda: FakeSession(fake_fail_get)
        self.trader._last_cash = 700000

        with self.assertLogs("stock_trader.kis_trader", level="ERROR") as cm:
            res = await self.trader.get_balance()

        self.assertIn("error", res)
        self.assertTrue(res.get("stale"))  # 실패 시 stale=True
        self.assertEqual(res["cash"], 700000)  # 캐시된 옛 예수금 반환
        # 로그 검증
        log_output = "\n".join(cm.output)
        self.assertIn("HTTP 429", log_output)
        self.assertIn("rt_cd=1", log_output)
        self.assertIn("msg_cd=EGW00123", log_output)
        self.assertIn("초당 거래건수 초과", log_output)
        # 계좌번호가 평문으로 노출되지 않는지 확인
        if acct_no:
            cano = self.trader._cano
            if cano:
                self.assertNotIn(cano, log_output)

    async def test_timeout_logs_and_returns_fallback(self):
        """10초 타임아웃 발생 시 에러 로그가 기록되고 폴백 결과를 반환해야 함"""
        def fake_timeout_get(url, *args, **kwargs):
            return FakeResponse({}, error=asyncio.TimeoutError())

        self.trader._new_session = lambda: FakeSession(fake_timeout_get)
        self.trader._last_cash = 500000

        with self.assertLogs("stock_trader.kis_trader", level="ERROR") as cm:
            res = await self.trader.get_balance()

        self.assertEqual(res["cash"], 500000)
        self.assertIn("타임아웃", res["error"])
        self.assertTrue(res.get("stale"))  # 실패 시 stale=True
        log_output = "\n".join(cm.output)
        self.assertIn("타임아웃", log_output)

    async def test_generic_exception_returns_stale_fallback(self):
        """타임아웃이 아닌 일반 예외(예: 연결 오류) 발생 시에도 stale=True와 캐시된 예수금을 반환해야 함"""
        def fake_error_get(url, *args, **kwargs):
            return FakeResponse({}, error=ConnectionError("연결 거부"))

        self.trader._new_session = lambda: FakeSession(fake_error_get)
        self.trader._last_cash = 300000

        with self.assertLogs("stock_trader.kis_trader", level="ERROR") as cm:
            res = await self.trader.get_balance()

        self.assertEqual(res["cash"], 300000)
        self.assertTrue(res.get("stale"))
        log_output = "\n".join(cm.output)
        self.assertIn("예외 발생", log_output)


class TestDashboardPositionsRateLimitAndCache(unittest.IsolatedAsyncioTestCase):
    async def test_dashboard_concurrent_10_calls_triggers_single_kis_request(self):
        """dashboard _get_stock_positions_raw 동시 10건 호출 시 KIS 실제 호출은 1회만 발생해야 함"""
        import dashboard.main as dm

        call_count = 0

        # dashboard main의 캐시 초기화
        dm._stock_positions_cache = None
        dm._stock_positions_cache_ts = 0.0

        def delayed_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return FakeResponse({
                "rt_cd": "0",
                "output1": [{"pdno": "005930", "prdt_name": "삼성전자", "hldg_qty": "10"}],
                "output2": [{"tot_evlu_amt": "2000000", "dnca_tot_amt": "1000000"}],
            }, delay=0.05)

        mock_sess = FakeSession(delayed_get)

        with patch("dashboard.main.get_kis_token", AsyncMock(return_value="FAKE_TOKEN")), \
             patch("aiohttp.ClientSession", return_value=mock_sess):

            tasks = [dm._get_stock_positions_raw() for _ in range(10)]
            results = await asyncio.gather(*tasks)

            self.assertEqual(len(results), 10)
            self.assertEqual(call_count, 1)  # KIS 호출은 1회만 발생
            for r in results:
                self.assertTrue(r["success"])
                self.assertEqual(r["account"]["total_eval"], 2000000)


if __name__ == "__main__":
    unittest.main()
