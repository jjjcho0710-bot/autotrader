"""
tests/test_change_rate_calc.py - [AT] fix/change-rate-calc 회귀 테스트 (PM 승인)

배경: stock_daily_ohlcv.change_rate가 전부 0.00으로 저장됐다. 일봉 조회(KIS
FHKST03010100, inquire-daily-itemchartprice) 응답에는 prdy_ctrt(전일대비율) 키가
없는데 세 곳(data_collector/collectors/daily_collector.py,
stock_trader/kis_trader.py, data_collector/bulk_fetch.py)이 전부 그 키를 읽고
있었다. 공용 함수 common/change_rate.py로 인접 일봉 종가 기준 직접 계산하도록
고쳤다 — 이 테스트는:

1. common/change_rate.py 자체(정상 계산/가장 오래된 행/종가 0·None).
2. 세 호출부가 (호출부마다 반환 순서가 서로 달라도) 같은 입력에 대해 동일한
   change_rate를 내는지(날짜별로 비교).
"""
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "stock_trader"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from common.change_rate import compute_change_rate, fill_change_rates  # noqa: E402
from data_collector.collectors.daily_collector import DailyCollector  # noqa: E402
from stock_trader.kis_trader import KISTrader  # noqa: E402
from data_collector import bulk_fetch  # noqa: E402

# 공통 합성 입력: 날짜 오름차순 (date_str, close). KIS 응답은 보통 최신순이므로
# 각 테스트에서 호출부가 기대하는 순서(최신순/오래된순)로 뒤집어서 넣는다.
_ASCENDING = [
    ("20261005", 1000),
    ("20261006", 1050),   # (1050-1000)/1000*100 = +5.00
    ("20261007", 1029),   # (1029-1050)/1050*100 = -2.00
    ("20261008", 1080),   # (1080-1029)/1029*100 = +4.96
]
_EXPECTED_BY_DATE = {
    "20261005": 0.0,   # 가장 오래된 행 — 전일 없음
    "20261006": 5.0,
    "20261007": -2.0,
    "20261008": 4.96,
}


def _kis_output2_row(date_str, close):
    """KIS inquire-daily-itemchartprice output2 한 행 — prdy_ctrt 키가 없다(실제 버그 재현)."""
    return {
        "stck_bsop_date": date_str,
        "stck_oprc": str(close), "stck_hgpr": str(close), "stck_lwpr": str(close),
        "stck_clpr": str(close), "acml_vol": "1000",
        # prdy_ctrt 없음 — 실제 응답과 동일하게 재현
    }


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakeConn:
    async def execute(self, *a, **kw):
        return "INSERT 0"


class _FakePool:
    def acquire(self):
        return _AcquireCtx(_FakeConn())


class _FakeJsonResponse:
    def __init__(self, payload):
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._payload


class _FakeGetSession:
    """get_daily_ohlcv()류가 기대하는 aiohttp 세션 대역 — async with sess.get(...) 지원."""

    def __init__(self, output2):
        self._output2 = output2

    def get(self, url, headers=None, params=None, timeout=None):
        return _FakeJsonResponse({"output2": self._output2})

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _AwaitableJsonResponse(_FakeJsonResponse):
    """bulk_fetch.py처럼 `await session.get(...)` 뒤에 `await res.json()`을 쓰는
    경로용 — __await__를 지원해 async with 없이도 쓸 수 있다."""

    def __await__(self):
        async def _coro():
            return self
        return _coro().__await__()


class _FakeAwaitGetSession:
    def __init__(self, output2):
        self._output2 = output2

    def get(self, url, headers=None, params=None):
        return _AwaitableJsonResponse({"output2": self._output2})


# ── 1) 공용 함수 자체 ────────────────────────────────────────────────────
class TestComputeChangeRate(unittest.TestCase):
    def test_normal_case(self):
        self.assertEqual(compute_change_rate(1000, 1050), 5.0)
        self.assertEqual(compute_change_rate(1050, 1029), -2.0)

    def test_rounds_to_two_decimals(self):
        # (1003-1000)/1000*100 = 0.3 → 반올림해도 0.3이지만 비정수 비율 케이스 확인
        self.assertEqual(compute_change_rate(1000, 1007), 0.7)
        self.assertEqual(compute_change_rate(3, 10), round((10 - 3) / 3 * 100, 2))

    def test_prev_close_none_returns_zero(self):
        """가장 오래된 행(전일 없음)은 0.0을 유지한다."""
        self.assertEqual(compute_change_rate(None, 1000), 0.0)

    def test_prev_close_zero_returns_zero(self):
        self.assertEqual(compute_change_rate(0, 1000), 0.0)

    def test_close_zero_returns_zero(self):
        self.assertEqual(compute_change_rate(1000, 0), 0.0)

    def test_close_none_returns_zero(self):
        self.assertEqual(compute_change_rate(1000, None), 0.0)


class TestFillChangeRates(unittest.TestCase):
    def test_ascending_input(self):
        candles = [{"date": d, "close": c} for d, c in _ASCENDING]
        fill_change_rates(candles)
        for c in candles:
            self.assertEqual(c["change_rate"], _EXPECTED_BY_DATE[c["date"]])

    def test_descending_input_same_result(self):
        """호출부가 최신순으로 들고 있어도(KIS 응답 그대로) 날짜 기준으로 올바르게 계산된다."""
        candles = [{"date": d, "close": c} for d, c in reversed(_ASCENDING)]
        fill_change_rates(candles)
        for c in candles:
            self.assertEqual(c["change_rate"], _EXPECTED_BY_DATE[c["date"]])
        # 입력 리스트의 순서 자체는 바꾸지 않아야 한다(호출부의 기존 반환 순서 유지)
        self.assertEqual([c["date"] for c in candles], [d for d, _ in reversed(_ASCENDING)])

    def test_close_zero_or_none_row_defensive(self):
        candles = [
            {"date": "20261005", "close": 1000},
            {"date": "20261006", "close": 0},
            {"date": "20261007", "close": None},
        ]
        fill_change_rates(candles)
        self.assertEqual(candles[0]["change_rate"], 0.0)  # 가장 오래된 행
        self.assertEqual(candles[1]["change_rate"], 0.0)  # close=0
        self.assertEqual(candles[2]["change_rate"], 0.0)  # close=None


# ── 2) 세 호출부가 같은 입력에서 같은 결과를 내는지 ──────────────────────
class TestThreeCallSitesAgree(unittest.IsolatedAsyncioTestCase):
    async def test_daily_collector_matches_expected(self):
        output2 = [_kis_output2_row(d, c) for d, c in reversed(_ASCENDING)]  # KIS는 최신순
        collector = DailyCollector.__new__(DailyCollector)
        collector.session = _FakeGetSession(output2)
        collector.access_token = "TOK"
        candles = await collector.get_daily_ohlcv("005930", days=10)

        by_date = {c["date"]: c["change_rate"] for c in candles}
        self.assertEqual(by_date, _EXPECTED_BY_DATE)

    async def test_kis_trader_matches_expected(self):
        output2 = [_kis_output2_row(d, c) for d, c in reversed(_ASCENDING)]
        trader = KISTrader.__new__(KISTrader)
        trader.access_token = "TOK"
        trader._new_session = lambda: _FakeGetSession(output2)

        async def _no_throttle():
            return None
        trader._throttle_kis_call = _no_throttle

        candles = await trader.get_daily_ohlcv("005930", "20261001", "20261010")

        by_date = {c["date"]: c["change_rate"] for c in candles}
        self.assertEqual(by_date, _EXPECTED_BY_DATE)

    async def test_bulk_fetch_matches_expected(self):
        output2 = [_kis_output2_row(d, c) for d, c in reversed(_ASCENDING)]
        session = _FakeAwaitGetSession(output2)

        with patch.object(bulk_fetch, "START_DATE", "20260101"), \
             patch.object(bulk_fetch, "END_DATE", "20261231"):
            candles = await bulk_fetch.fetch_ohlcv_kis(session, "TOK", "005930")

        # 튜플: (symbol, ts, open, high, low, close, volume, change_rate)
        by_date = {c[1].strftime("%Y%m%d"): c[7] for c in candles}
        self.assertEqual(by_date, _EXPECTED_BY_DATE)

    async def test_all_three_agree_with_each_other(self):
        """세 호출부를 같은 입력으로 돌려 서로 완전히 같은 날짜별 change_rate를 내는지."""
        output2 = [_kis_output2_row(d, c) for d, c in reversed(_ASCENDING)]

        collector = DailyCollector.__new__(DailyCollector)
        collector.session = _FakeGetSession(output2)
        collector.access_token = "TOK"
        daily_candles = await collector.get_daily_ohlcv("005930", days=10)
        daily_by_date = {c["date"]: c["change_rate"] for c in daily_candles}

        trader = KISTrader.__new__(KISTrader)
        trader.access_token = "TOK"
        trader._new_session = lambda: _FakeGetSession(output2)

        async def _no_throttle():
            return None
        trader._throttle_kis_call = _no_throttle
        trader_candles = await trader.get_daily_ohlcv("005930", "20261001", "20261010")
        trader_by_date = {c["date"]: c["change_rate"] for c in trader_candles}

        session2 = _FakeAwaitGetSession(output2)
        with patch.object(bulk_fetch, "START_DATE", "20260101"), \
             patch.object(bulk_fetch, "END_DATE", "20261231"):
            bulk_candles = await bulk_fetch.fetch_ohlcv_kis(session2, "TOK", "005930")
        bulk_by_date = {c[1].strftime("%Y%m%d"): c[7] for c in bulk_candles}

        self.assertEqual(daily_by_date, trader_by_date)
        self.assertEqual(trader_by_date, bulk_by_date)


if __name__ == "__main__":
    unittest.main()
