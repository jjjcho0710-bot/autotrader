"""
tests/test_total_pnl_display.py - "누적 손익(원금 대비)" 지표 테스트

검증 범위
- common.config.compute_total_pnl: 시작 자금(INITIAL_SEED_KRW) 대비 손익(원, %) 계산
- dashboard/main.py _get_stock_positions_raw: account에 total_pnl/total_pnl_rate 포함
- dashboard/main.py /api/account/stock (account_stock_summary): total_pnl/total_pnl_rate 반환
- dashboard/main.py _jarvis_closing_report: 마감 결산 메시지에 "누적손익(원금대비)" 한 줄 포함
- stock_trader/main.py StockTrader._six_hour_report: 6시간 리포트에 "누적 수익률" 한 줄 포함
  (리포트 형식 상세 테스트는 tests/test_six_hour_report_channel.py)
- 대시보드 홈 화면(home.html, pc/pages/home.html) 정적 마크업: 새 지표 표시 요소 존재
"""
import asyncio
import inspect
import re
import sys
import types
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
STOCK_TRADER_DIR = REPO_ROOT / "stock_trader"
for p in (REPO_ROOT, STOCK_TRADER_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _stub_missing_runtime_deps():
    """asyncpg/redis/aiohttp/fastapi가 없는 테스트 환경에서도 import 되도록 더미로 대체한다."""
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
    for mod_name in [
        "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
        "google", "google.generativeai",
    ]:
        if mod_name not in sys.modules:
            sys.modules[mod_name] = MagicMock()


_stub_missing_runtime_deps()

from common.config import config, compute_total_pnl  # noqa: E402

HOME_HTML = REPO_ROOT / "dashboard" / "static" / "home.html"
PC_HOME_HTML = REPO_ROOT / "dashboard" / "static" / "pc" / "pages" / "home.html"


class TestComputeTotalPnl(unittest.TestCase):
    def test_default_seed_from_config(self):
        pnl, rate = compute_total_pnl(config.INITIAL_SEED_KRW + 500_000)
        self.assertEqual(pnl, 500_000)
        self.assertAlmostEqual(rate, 500_000 / config.INITIAL_SEED_KRW * 100)

    def test_negative_pnl_below_seed(self):
        pnl, rate = compute_total_pnl(9_000_000, seed=10_000_000)
        self.assertEqual(pnl, -1_000_000)
        self.assertAlmostEqual(rate, -10.0)

    def test_zero_seed_does_not_divide_by_zero(self):
        pnl, rate = compute_total_pnl(1_000_000, seed=0)
        self.assertEqual((pnl, rate), (0, 0.0))


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
    def __init__(self, handler):
        self.handler = handler

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, *args, **kwargs):
        return self.handler(url, *args, **kwargs)


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, trades, wl_count=0, prev_total_krw=None):
        self._trades = trades
        self._wl_count = wl_count
        self._prev_total_krw = prev_total_krw

    async def fetch(self, query, *args):
        return self._trades

    async def fetchval(self, query, *args):
        if "balance_snapshot" in query:
            return self._prev_total_krw
        return self._wl_count


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _Acquire(self._conn)


class TestDashboardAccountTotalPnl(unittest.IsolatedAsyncioTestCase):
    async def test_get_stock_positions_raw_includes_total_pnl(self):
        """_get_stock_positions_raw의 account dict에 시작 자금 대비 누적 손익이 포함돼야 함"""
        import dashboard.main as dm

        dm._stock_positions_cache = None
        dm._stock_positions_cache_ts = 0.0

        def fake_get(*args, **kwargs):
            return FakeResponse({
                "rt_cd": "0",
                "output1": [],
                "output2": [{"tot_evlu_amt": "12000000", "dnca_tot_amt": "12000000"}],
            })

        mock_sess = FakeSession(fake_get)

        with patch("dashboard.main.get_kis_token", AsyncMock(return_value="FAKE_TOKEN")), \
             patch("aiohttp.ClientSession", return_value=mock_sess), \
             patch.object(config, "INITIAL_SEED_KRW", 10_000_000):
            result = await dm._get_stock_positions_raw()

        self.assertTrue(result["success"])
        acct = result["account"]
        self.assertEqual(acct["total_eval"], 12_000_000)
        self.assertEqual(acct["total_pnl"], 2_000_000)
        self.assertAlmostEqual(acct["total_pnl_rate"], 20.0)

    def test_account_stock_summary_source_forwards_total_pnl(self):
        """/api/account/stock 핸들러(account_stock_summary)가 total_pnl/total_pnl_rate를 응답에 포함해야 함
        (핸들러가 @app.get 데코레이터로 감싸여 있어 테스트 환경의 fastapi 목(MagicMock)에서는
        직접 호출이 불가능하므로, 이 저장소의 다른 라우트 테스트(test_max_positions_setting.py 등)와
        동일하게 정적 소스 검사로 검증한다)"""
        import dashboard.main as dm

        src = inspect.getsource(dm)
        m = re.search(r'async def account_stock_summary.*?(?=\n@app\.|\nasync def |\Z)', src, re.S)
        self.assertIsNotNone(m, "account_stock_summary 함수를 찾을 수 없음")
        body = m.group(0)
        self.assertIn('"total_pnl": acct.get("total_pnl"', body)
        self.assertIn('"total_pnl_rate": acct.get("total_pnl_rate"', body)

    async def test_closing_report_includes_cumulative_pnl_line(self):
        """마감 결산 채널용 마지막 블록에 '누적 수익' 한 줄이 포함돼야 함
        ([AT] feat/telegram-routing — 본문은 금액 없이 마지막 블록만 금액 허용)"""
        import dashboard.main as dm

        trades = [
            {"symbol": "005930", "side": "BUY", "price": 70000, "quantity": 10,
             "amount": 700000, "pnl": 0, "strategy": "MA크로스", "ts": None},
        ]
        mock_positions = {
            "success": True,
            "data": [{"name": "삼성전자", "symbol": "005930", "qty": 10, "pnl_rate": 1.2}],
            "account": {"total_eval": 10_500_000, "total_pnl": 500_000, "total_pnl_rate": 5.0},
        }

        sent = []
        dests = []

        async def _fake_send_telegram(msg, *a, **kw):
            sent.append(msg)
            dests.append(kw.get("dest"))

        with mock.patch.object(dm, "db_pool", _FakePool(_FakeConn(trades, wl_count=3)), create=True), \
             patch("dashboard.main.get_stock_positions", AsyncMock(return_value=mock_positions)), \
             patch("dashboard.main._code_to_name", AsyncMock(return_value="삼성전자")), \
             patch("dashboard.main._send_telegram", _fake_send_telegram):
            await dm._jarvis_closing_report()

        self.assertEqual(len(sent), 1)
        self.assertEqual(dests, ["channel"])  # 마감 결산은 채널 전용
        self.assertIn("누적 수익", sent[0])
        self.assertIn("+500,000원", sent[0])
        self.assertIn("+5.00%", sent[0])
        self.assertIn("총자산 10,500,000원", sent[0])
        # 이전 balance_snapshot 기록이 없으므로(prev_total_krw 미지정) 추정값 없이 집계 시작 전 표시
        self.assertIn("오늘 수익: 집계 시작 전", sent[0])


def _stub_stock_trader_deps():
    _stub_missing_runtime_deps()


_stub_stock_trader_deps()

import main as stock_main  # noqa: E402  (stock_trader/main.py)
from main import StockTrader  # noqa: E402


def _run_one_report(trades, total_eval, send_stock, send_report):
    """6시간 리포트는 채널 전용이다([AT] feat/telegram-routing) — send_report 호출 직후
    루프를 멈춘다. send_stock은 더 이상 호출되지 않아야 한다."""
    trader = StockTrader.__new__(StockTrader)
    trader.running = True
    trader.positions = {}
    trader.strategies = {}  # get_all_active_strategies()가 참조 — 활성 전략 없음(참고 손절값 사용)

    class _Broker:
        async def get_balance(self):
            return {"cash": 1_000_000, "total": total_eval}

    trader.trader = _Broker()

    async def _fake_sleep(_):
        pass

    real_report = StockTrader._six_hour_report

    async def _drive():
        async def _send_report_and_stop(text):
            await send_report(text)
            trader.running = False

        # SIX_HOUR_REPORT_SKIP_IDLE은 이 테스트의 관심사가 아니다(실제 벽시계 시각에 따라
        # 거래 0건 + 장시간 미포함 구간으로 판정되면 전송이 생략돼 테스트가 들떠서(flaky) 실패할
        # 수 있어 꺼둔다).
        with mock.patch.object(stock_main.db, "pool", _FakePool(_FakeConn(trades)), create=True), \
             mock.patch.object(stock_main.asyncio, "sleep", _fake_sleep), \
             mock.patch.object(StockTrader, "SIX_HOUR_REPORT_SKIP_IDLE", False), \
             mock.patch("common.telegram.send_stock", send_stock), \
             mock.patch("common.telegram.send_report", _send_report_and_stop), \
             mock.patch.object(config, "INITIAL_SEED_KRW", 10_000_000):
            await real_report(trader)

    asyncio.run(_drive())


class TestSixHourReportTotalPnl(unittest.TestCase):
    def test_report_includes_cumulative_pnl_rate_when_total_eval_available(self):
        """채널 메시지는 금액 없이 누적손익률(%)만 보여준다(계좌 규모를 드러내는 금액 제외)."""
        report_calls = []

        async def _send_stock(text):
            pass

        async def _send_report(text):
            report_calls.append(text)

        _run_one_report([], total_eval=11_000_000, send_stock=_send_stock, send_report=_send_report)

        self.assertEqual(len(report_calls), 1)
        self.assertIn("누적 수익률", report_calls[0])
        self.assertIn("+10.00%", report_calls[0])
        self.assertNotIn("1,000,000원", report_calls[0])

    def test_report_omits_cumulative_pnl_when_balance_unavailable(self):
        """total_eval이 0(조회 실패 등)이면 잘못된 -100% 표시 대신 줄을 생략해야 함"""
        report_calls = []

        async def _send_stock(text):
            pass

        async def _send_report(text):
            report_calls.append(text)

        _run_one_report([], total_eval=0, send_stock=_send_stock, send_report=_send_report)

        self.assertEqual(len(report_calls), 1)
        self.assertNotIn("누적 수익률", report_calls[0])


class TestHomeHtmlMarkup(unittest.TestCase):
    def test_mobile_home_has_cumulative_pnl_element(self):
        html = HOME_HTML.read_text(encoding="utf-8")
        self.assertIn('id="heroTotalPnl"', html)
        self.assertIn("누적 손익", html)
        self.assertIn("acct.total_pnl", html)

    def test_pc_home_has_cumulative_pnl_kpi(self):
        html = PC_HOME_HTML.read_text(encoding="utf-8")
        self.assertIn('id="kTotalPnl"', html)
        self.assertIn("누적 손익", html)
        self.assertIn("a.total_pnl", html)


if __name__ == "__main__":
    unittest.main()
