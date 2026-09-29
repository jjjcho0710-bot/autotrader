"""
stock_trader/main.py StockTrader._six_hour_report 단위 테스트:
  6시간 리포트는 개인방(send_stock)에 그대로 보내고, 이어서 채널용 send_report 도 호출해야 한다.
  send_report 는 TELEGRAM_CHANNEL_ID 가 있으면 채널로, 없으면 내부적으로 send_stock 으로 폴백한다.
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
    """asyncpg/redis/aiohttp 가 없는 테스트 환경에서도 import 만 되도록 더미로 대체한다."""
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
        sys.modules["aiohttp"] = stub


_stub_missing_runtime_deps()

import main as stock_main  # noqa: E402  (stock_trader/main.py)
from main import StockTrader  # noqa: E402


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, trades):
        self._trades = trades

    async def fetch(self, query, *args):
        return self._trades


class _FakePool:
    def __init__(self, trades):
        self._conn = _FakeConn(trades)

    def acquire(self):
        return _Acquire(self._conn)


def _run_one_report(trades, send_stock, send_report, balance=None):
    """_six_hour_report 루프를 한 번만 돌린다(sleep 즉시 반환, 첫 sleep 이후 running=False)."""
    trader = StockTrader.__new__(StockTrader)
    trader.running = True
    trader.positions = {"005930": {"name": "삼성전자", "pnl_rate": 1.5}}

    balance_result = balance if balance is not None else {"cash": 1_000_000}

    class _Broker:
        async def get_balance(self):
            return balance_result

    trader.trader = _Broker()

    async def _fake_sleep(_):
        pass

    real_report = StockTrader._six_hour_report

    async def _drive():
        # send_stock 호출 직후 running=False 로 바꿔, 이번 회차 본문이 끝나면 루프가 종료되게 한다
        async def _send_stock_and_stop(text):
            await send_stock(text)
            trader.running = False

        with mock.patch.object(stock_main.db, "pool", _FakePool(trades), create=True), \
             mock.patch.object(stock_main.asyncio, "sleep", _fake_sleep), \
             mock.patch("common.telegram.send_stock", _send_stock_and_stop), \
             mock.patch("common.telegram.send_report", send_report):
            await real_report(trader)

    asyncio.run(_drive())


class TestSixHourReportChannel(unittest.TestCase):
    def _trades(self):
        return [{"side": "BUY", "symbol": "005930", "amount": 500000, "pnl": 0,
                 "strategy": "MA크로스", "created_at": None}]

    def test_sends_to_personal_room_and_channel_with_same_text(self):
        stock_calls, report_calls = [], []

        async def _send_stock(text):
            stock_calls.append(text)

        async def _send_report(text):
            report_calls.append(text)

        _run_one_report(self._trades(), _send_stock, _send_report)

        self.assertEqual(len(stock_calls), 1)   # 개인방 전송 유지
        self.assertEqual(len(report_calls), 1)  # 채널 전송 추가
        self.assertEqual(stock_calls[0], report_calls[0])
        self.assertIn("주식 6시간 리포트", report_calls[0])
        self.assertIn("삼성전자", report_calls[0])

    def test_send_stock_called_before_send_report(self):
        order = []

        async def _send_stock(text):
            order.append("stock")

        async def _send_report(text):
            order.append("report")

        _run_one_report(self._trades(), _send_stock, _send_report)
        self.assertEqual(order, ["stock", "report"])


class TestSixHourReportStaleCashIndicator(unittest.TestCase):
    """get_balance()가 stale=True(KIS 조회 실패 → 캐시된 옛 예수금)를 반환할 때
    리포트의 예수금 표기에 지연 표시가 붙어야 하고, 정상 조회 시에는 붙지 않아야 한다."""

    def _trades(self):
        return [{"side": "BUY", "symbol": "005930", "amount": 500000, "pnl": 0,
                 "strategy": "MA크로스", "created_at": None}]

    def test_stale_balance_shows_delay_indicator(self):
        report_calls = []

        async def _send_stock(text):
            pass

        async def _send_report(text):
            report_calls.append(text)

        _run_one_report(
            self._trades(), _send_stock, _send_report,
            balance={"cash": 1_000_000, "total": 0, "stale": True},
        )

        self.assertEqual(len(report_calls), 1)
        self.assertIn("예수금: 1,000,000원 ⚠️(마지막 확인: 지연됨)", report_calls[0])

    def test_fresh_balance_has_no_delay_indicator(self):
        report_calls = []

        async def _send_stock(text):
            pass

        async def _send_report(text):
            report_calls.append(text)

        _run_one_report(
            self._trades(), _send_stock, _send_report,
            balance={"cash": 1_000_000, "total": 5_000_000},
        )

        self.assertEqual(len(report_calls), 1)
        self.assertIn("예수금: 1,000,000원", report_calls[0])
        self.assertNotIn("지연됨", report_calls[0])


if __name__ == "__main__":
    unittest.main()
