"""
stock_trader/main.py StockTrader._six_hour_report 단위 테스트 ([AT] feat/telegram-routing):
  6시간 리포트는 채널만의 "읽는 기록"이다(개인방 중복 발송 제거) — send_report만 호출하고
  send_stock은 호출하지 않는다. 채널 메시지에는 예수금·손익 금액 같은 계좌 잔고 규모를
  드러내는 금액을 쓰지 않고 수량·%만 쓴다.
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
from common.config import config  # noqa: E402
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
    """_six_hour_report 루프를 한 번만 돌린다(sleep 즉시 반환, send_report 호출 직후 running=False)."""
    trader = StockTrader.__new__(StockTrader)
    trader.running = True
    trader.positions = {"005930": {"name": "삼성전자", "pnl_rate": 1.5}}

    balance_result = balance if balance is not None else {"cash": 1_000_000, "total": 11_000_000}

    class _Broker:
        async def get_balance(self):
            return balance_result

    trader.trader = _Broker()

    async def _fake_sleep(_):
        pass

    real_report = StockTrader._six_hour_report

    async def _drive():
        # send_report 호출 직후 running=False 로 바꿔, 이번 회차 본문이 끝나면 루프가 종료되게 한다
        async def _send_report_and_stop(text):
            await send_report(text)
            trader.running = False

        with mock.patch.object(stock_main.db, "pool", _FakePool(trades), create=True), \
             mock.patch.object(stock_main.asyncio, "sleep", _fake_sleep), \
             mock.patch("common.telegram.send_stock", send_stock), \
             mock.patch("common.telegram.send_report", _send_report_and_stop), \
             mock.patch.object(config, "INITIAL_SEED_KRW", 10_000_000):
            await real_report(trader)

    asyncio.run(_drive())


class TestSixHourReportChannel(unittest.TestCase):
    def _trades(self):
        return [{"side": "BUY", "symbol": "005930", "amount": 500000, "pnl": 0,
                 "strategy": "MA크로스", "created_at": None}]

    def test_sends_to_channel_only_not_personal(self):
        """채널 전용 — send_stock(개인방)은 호출하지 않고 send_report(채널)만 호출한다."""
        stock_calls, report_calls = [], []

        async def _send_stock(text):
            stock_calls.append(text)

        async def _send_report(text):
            report_calls.append(text)

        _run_one_report(self._trades(), _send_stock, _send_report)

        self.assertEqual(len(stock_calls), 0)   # 개인방 중복 발송 없음
        self.assertEqual(len(report_calls), 1)  # 채널 전송만
        self.assertIn("주식 6시간 리포트", report_calls[0])
        self.assertIn("삼성전자", report_calls[0])

    def test_report_has_no_money_amounts(self):
        """채널 메시지에는 예수금·금액 손익 등 계좌 잔고 규모를 드러내는 금액이 없어야 한다
        (누적손익은 %만, 최근 매매는 종목명만)."""
        report_calls = []

        async def _send_stock(text):
            pass

        async def _send_report(text):
            report_calls.append(text)

        _run_one_report(self._trades(), _send_stock, _send_report,
                         balance={"cash": 1_000_000, "total": 11_000_000})

        self.assertEqual(len(report_calls), 1)
        msg = report_calls[0]
        self.assertNotIn("예수금", msg)
        self.assertNotIn("500,000원", msg)  # 개별 매매 금액 없음
        self.assertIn("누적손익(원금대비)", msg)
        self.assertIn("+10.00%", msg)  # (11,000,000 - 10,000,000)/10,000,000


class TestSixHourReportStaleBalance(unittest.TestCase):
    """get_balance()가 stale=True(KIS 조회 실패)를 반환하면 누적손익 줄 자체를 생략해야 한다
    (예수금 표시는 더 이상 하지 않으므로 지연 표시가 아니라 생략으로 처리)."""

    def _trades(self):
        return [{"side": "BUY", "symbol": "005930", "amount": 500000, "pnl": 0,
                 "strategy": "MA크로스", "created_at": None}]

    def test_stale_balance_omits_cumulative_pnl_line(self):
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
        self.assertNotIn("누적손익", report_calls[0])

    def test_fresh_balance_shows_cumulative_pnl_rate(self):
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
        self.assertIn("누적손익(원금대비)", report_calls[0])
        self.assertNotIn("예수금", report_calls[0])


if __name__ == "__main__":
    unittest.main()
