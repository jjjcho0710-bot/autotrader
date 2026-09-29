"""
stock_trader/main.py StockTrader._loop() 의 KIS 연결 오류 처리 단위 테스트:
  "Cannot connect to host ..." 류 오류는 DB 재연결 분기("connection" 문자열 매칭)에 걸리지 않는데,
  이 오류가 연속으로 발생하면 KIS 토큰을 강제 재발급하고, 알림 빈도를 5분에 1회로 줄이되
  재시도는 계속 유지해야 한다. 연결이 복구되면 복구 알림을 1회 보내야 한다.
"""
import asyncio
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parent.parent
STOCK_TRADER_DIR = REPO_ROOT / "stock_trader"
for p in (REPO_ROOT, STOCK_TRADER_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _stub_missing_runtime_deps():
    """asyncpg/redis/aiohttp가 테스트 환경에 없어도 import만 되도록 더미로 대체한다."""
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
        stub.ClientTimeout = lambda *a, **kw: None

        class ClientConnectorError(Exception):
            pass

        class ClientConnectionError(Exception):
            pass

        stub.ClientConnectorError = ClientConnectorError
        stub.ClientConnectionError = ClientConnectionError
        sys.modules["aiohttp"] = stub


_stub_missing_runtime_deps()

import main as stock_main  # noqa: E402  (stock_trader/main.py)
from main import StockTrader  # noqa: E402


def _new_trader():
    trader = StockTrader.__new__(StockTrader)
    trader.running = True
    trader.positions = {}
    return trader


class TestIsKisConnectError(unittest.TestCase):
    def test_cannot_connect_to_host_string_is_detected(self):
        """'Cannot connect to host ...'엔 'connect'만 있고 'connection'은 없어
        DB 재연결 분기 문자열 매칭에 안 걸린다 — 그래도 KIS 연결 오류로는 잡혀야 한다."""
        err = Exception("Cannot connect to host openapi.koreainvestment.com:9443 ssl:default [Try again]")
        self.assertTrue(StockTrader._is_kis_connect_error(err, str(err)))

    def test_db_connection_string_is_not_treated_as_kis_error(self):
        err = Exception("connection to server was lost")
        self.assertFalse(StockTrader._is_kis_connect_error(err, str(err)))

    def test_unrelated_error_is_not_detected(self):
        err = Exception("division by zero")
        self.assertFalse(StockTrader._is_kis_connect_error(err, str(err)))


class TestHandleKisConnectError(unittest.IsolatedAsyncioTestCase):
    async def test_first_two_failures_only_notify_no_reissue(self):
        trader = _new_trader()
        reissued = []
        trader.trader = types.SimpleNamespace(
            force_reissue_token=lambda: reissued.append(1) or asyncio.sleep(0))
        notified = []

        async def _fake_notify(msg):
            notified.append(msg)

        trader._notify_error = _fake_notify

        await trader._handle_kis_connect_error("Cannot connect to host x:443")
        await trader._handle_kis_connect_error("Cannot connect to host x:443")

        self.assertEqual(trader._kis_conn_err_count, 2)
        self.assertEqual(len(reissued), 0)  # 3회 미만은 토큰 재발급 안 함
        self.assertEqual(len(notified), 2)  # 매 회 알림

    async def test_third_consecutive_failure_forces_token_reissue(self):
        trader = _new_trader()
        reissue_calls = []

        async def _reissue():
            reissue_calls.append(1)

        trader.trader = types.SimpleNamespace(force_reissue_token=_reissue)
        notified = []

        async def _fake_notify(msg):
            notified.append(msg)

        trader._notify_error = _fake_notify

        for _ in range(3):
            await trader._handle_kis_connect_error("Cannot connect to host x:443")

        self.assertEqual(trader._kis_conn_err_count, 3)
        self.assertEqual(len(reissue_calls), 1)  # 3회차에 강제 재발급 1회
        self.assertEqual(len(notified), 3)  # 1,2회차 알림 + 3회차 알림(첫 알림이라 쿨다운 미적용)

    async def test_alert_frequency_throttled_to_once_per_5min_after_third_failure(self):
        trader = _new_trader()
        trader.trader = types.SimpleNamespace(force_reissue_token=lambda: asyncio.sleep(0))
        notified = []

        async def _fake_notify(msg):
            notified.append(msg)

        trader._notify_error = _fake_notify

        for _ in range(5):  # 3,4,5회차 모두 연속 실패
            await trader._handle_kis_connect_error("Cannot connect to host x:443")

        # 1,2회차는 매번 알림 + 3회차 최초 알림 = 3건. 4,5회차는 5분 쿨다운에 걸려 추가 알림 없음.
        self.assertEqual(len(notified), 3)

    async def test_token_reissue_failure_does_not_raise(self):
        trader = _new_trader()

        async def _reissue_fail():
            raise RuntimeError("발급 실패")

        trader.trader = types.SimpleNamespace(force_reissue_token=_reissue_fail)

        async def _fake_notify(msg):
            pass

        trader._notify_error = _fake_notify

        for _ in range(3):
            await trader._handle_kis_connect_error("Cannot connect to host x:443")  # 예외 없이 통과해야 함


class TestKisRecoveryNotification(unittest.IsolatedAsyncioTestCase):
    async def test_sends_recovery_notice_once_after_prior_failures(self):
        trader = _new_trader()
        trader._kis_conn_err_count = 3

        sent = []

        async def _fake_send_stock(text):
            sent.append(text)

        with mock.patch("common.telegram.send_stock", _fake_send_stock):
            await trader._notify_kis_recovered_if_needed()

        self.assertEqual(len(sent), 1)
        self.assertIn("KIS 연결 복구됨", sent[0])
        self.assertEqual(trader._kis_conn_err_count, 0)

    async def test_no_notice_when_no_prior_failures(self):
        trader = _new_trader()
        trader._kis_conn_err_count = 0

        sent = []

        async def _fake_send_stock(text):
            sent.append(text)

        with mock.patch("common.telegram.send_stock", _fake_send_stock):
            await trader._notify_kis_recovered_if_needed()

        self.assertEqual(len(sent), 0)


if __name__ == "__main__":
    unittest.main()
