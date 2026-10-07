"""
tests/test_morning_scan_fallback.py - [AT] fix/morning-scan-fallback

배경: 08:30 아침 스캔(_jarvis_stock_scanner)에서 KRX(pykrx) 코스피/코스닥 종목
목록 조회가 간헐적으로 실패하면, 그 시장은 조용히 스킵되고 후보가 비어 매매
기회를 놓칠 수 있었다. 기존에도 티커 목록 조회에 3회 재시도가 있었지만, 재시도
후에도 실패하면 그 시장은 완전히 스킵되고 다른 시장 결과만으로 "정상 완료"
처리돼 아무 알림도 남지 않는 문제가 있었다.

이 테스트가 검증하는 수정:
1. 재시도(3회) 후에도 한 시장의 티커 목록 조회가 끝까지 실패하면, DB 종목
   유니버스(stocks 테이블)를 대신 스캔 대상으로 사용한다(빈 목록으로 조용히
   진행하지 않음).
2. 폴백(DB 유니버스 또는 KIS API 폴백 스캔)을 쓴 경우 개인방 텔레그램 알림에
   그 사실이 표시된다.
3. 정상 평일에 양쪽 시장 조회가 모두 성공하면 폴백 문구 없이 기존과 동일하게
   동작한다(회귀 아님).
4. DB 유니버스조차 없고 KIS 폴백도 실패하면 기존 watchlist를 보존하고 실패를
   알린다(기존 동작 유지 확인).

이 되돌림 테스트는 `dashboard/main.py`의 `_scan()` 안 "DB 종목 유니버스로
대체" 분기를 제거하면(원래 `continue`로 시장을 그냥 스킵하던 코드로 되돌리면)
`test_ticker_list_failure_falls_back_to_db_universe`가 깨진다.
"""
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

import dashboard.main as dm  # noqa: E402


class _FakeSeries(list):
    """pykrx가 반환하는 pandas Series 중 코드가 쓰는 부분(iloc 슬라이싱/인덱싱,
    tolist, mean)만 흉내 내는 최소 대역. pandas가 설치되어 있지 않은 테스트
    환경이라 실제 DataFrame을 쓸 수 없다."""

    @property
    def iloc(self):
        return self

    def __getitem__(self, key):
        if isinstance(key, slice):
            return _FakeSeries(list.__getitem__(self, key))
        return list.__getitem__(self, key)

    def mean(self):
        return sum(self) / len(self)

    def tolist(self):
        return list(self)


class _FakeDF:
    """pykrx_stock.get_market_ohlcv()가 반환하는 DataFrame 중 _scan()이 실제로
    쓰는 컬럼(종가/거래량/등락률)과 len()만 흉내 낸다."""

    def __init__(self, closes, vols, changes):
        self._cols = {
            "종가": _FakeSeries(closes),
            "거래량": _FakeSeries(vols),
            "등락률": _FakeSeries(changes),
        }
        self._len = len(closes)

    def __getitem__(self, key):
        return self._cols[key]

    def __len__(self):
        return self._len


def _winning_df():
    """golden_cross는 아니지만 ma_trend_ok+거래량급증+등락률로 score>=4를
    만드는 25일치 가짜 OHLCV (상세 설계는 모듈 docstring 참고)."""
    closes = [9000 + 100 * i for i in range(25)]
    vols = [10] * 24 + [100]
    changes = [0] * 24 + [5.0]
    return _FakeDF(closes, vols, changes)


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakeConn:
    """stocks(DB 종목 유니버스 폴백 조회)와 watchlist(스캐너 결과 반영) 양쪽
    쿼리를 모두 받는 최소 대역."""

    def __init__(self, stock_symbols=()):
        self.stock_symbols = list(stock_symbols)
        self.executed = []

    async def fetch(self, query, *args):
        if "FROM stocks" in query:
            return [{"symbol": s} for s in self.stock_symbols]
        return []  # watchlist 조회(기존 scanner 종목/활성 종목) — 빈 상태로 가정

    async def execute(self, query, *args):
        self.executed.append((query, args))
        return "OK"


class FakePool:
    def __init__(self, stock_symbols=()):
        self.conn = FakeConn(stock_symbols)

    def acquire(self):
        return _AcquireCtx(self.conn)


class TestMorningScanFallback(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._patches = []

        def start(p):
            m = p.start()
            self._patches.append(p)
            return m

        self.sent = []

        async def fake_send_telegram(text, **kw):
            self.sent.append((text, kw))

        start(patch.object(dm, "_send_telegram", side_effect=fake_send_telegram))
        start(patch.object(dm, "get_stock_positions",
                            new=AsyncMock(return_value={"success": True, "data": []})))
        # 티커 목록 재시도 간 3초 대기를 실제로 하면 테스트가 느려진다.
        start(patch("time.sleep", new=MagicMock()))

        self._pykrx = MagicMock()
        start(patch.dict(sys.modules, {"pykrx": MagicMock(stock=self._pykrx), "pykrx.stock": self._pykrx}))

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()

    def _set_pykrx(self, *, ticker_list_by_market, ohlcv_by_ticker, name_by_ticker=None):
        def get_market_ticker_list(date, market=None):
            value = ticker_list_by_market[market]
            if isinstance(value, Exception):
                raise value
            return value

        def get_market_ohlcv(d30, today, ticker):
            return ohlcv_by_ticker.get(ticker)

        def get_market_ticker_name(ticker):
            return (name_by_ticker or {}).get(ticker, f"종목{ticker}")

        self._pykrx.get_market_ticker_list = MagicMock(side_effect=get_market_ticker_list)
        self._pykrx.get_market_ohlcv = MagicMock(side_effect=get_market_ohlcv)
        self._pykrx.get_market_ticker_name = MagicMock(side_effect=get_market_ticker_name)

    async def test_ticker_list_failure_falls_back_to_db_universe(self):
        """KOSPI 티커 목록 조회가 3회 모두 실패하면 DB 종목 유니버스(stocks)로
        대체해 스캔을 계속하고, 개인방 알림에 그 사실을 남긴다."""
        kospi_fail = RuntimeError("KRX 연결 끊김")
        self._set_pykrx(
            ticker_list_by_market={
                "KOSPI": kospi_fail,  # 항상 예외 → 3회 재시도 모두 실패
                "KOSDAQ": ["000020"],
            },
            ohlcv_by_ticker={
                "005930": None,  # 거래일 프로브 — 실패해도 기본 날짜로 진행
                "000020": _winning_df(),
                "000030": _winning_df(),  # DB 유니버스 폴백으로만 등장하는 종목
            },
        )
        pool = FakePool(stock_symbols=["000030"])
        with patch.object(dm, "db_pool", pool):
            await dm._jarvis_stock_scanner()

        # 3회 모두 재시도했는지 확인(기존 재시도 로직 회귀 아님)
        kospi_calls = [c for c in self._pykrx.get_market_ticker_list.call_args_list
                        if c.kwargs.get("market") == "KOSPI"]
        self.assertEqual(len(kospi_calls), 3)

        self.assertEqual(len(self.sent), 1)
        msg, kw = self.sent[0]
        self.assertEqual(kw.get("dest"), "personal")
        self.assertIn("KOSPI", msg)
        self.assertIn("DB 유니버스", msg)

        inserted_symbols = {
            args[0] for query, args in pool.conn.executed if "INSERT INTO watchlist" in query
        }
        self.assertIn("000020", inserted_symbols)  # 정상 조회된 KOSDAQ 종목
        self.assertIn("000030", inserted_symbols)  # DB 유니버스 폴백으로 추가된 종목

    async def test_both_markets_succeed_no_fallback_note(self):
        """양쪽 시장 모두 정상 조회되면 폴백 문구 없이 기존과 동일하게 동작한다."""
        self._set_pykrx(
            ticker_list_by_market={
                "KOSPI": ["000020"],
                "KOSDAQ": ["000040"],
            },
            ohlcv_by_ticker={
                "005930": None,
                "000020": _winning_df(),
                "000040": _winning_df(),
            },
        )
        pool = FakePool(stock_symbols=["999999"])  # 폴백 상황이 아니므로 쓰이지 않아야 함
        with patch.object(dm, "db_pool", pool):
            await dm._jarvis_stock_scanner()

        self.assertEqual(len(self.sent), 1)
        msg, kw = self.sent[0]
        self.assertNotIn("DB 유니버스", msg)
        self.assertNotIn("KIS API 폴백", msg)
        inserted_symbols = {
            args[0] for query, args in pool.conn.executed if "INSERT INTO watchlist" in query
        }
        self.assertIn("000020", inserted_symbols)
        self.assertIn("000040", inserted_symbols)
        self.assertNotIn("999999", inserted_symbols)

    async def test_retry_succeeds_on_third_attempt_without_fallback(self):
        """처음 두 번은 빈 응답, 세 번째 시도에 성공하면 폴백 없이 정상 처리된다."""
        attempts = {"n": 0}

        def flaky_kospi(date, market=None):
            if market == "KOSDAQ":
                return ["000040"]
            attempts["n"] += 1
            if attempts["n"] < 3:
                return []
            return ["000020"]

        self._pykrx.get_market_ticker_list = MagicMock(side_effect=flaky_kospi)
        self._pykrx.get_market_ohlcv = MagicMock(
            side_effect=lambda d30, today, ticker: {
                "005930": None, "000020": _winning_df(), "000040": _winning_df(),
            }.get(ticker))
        self._pykrx.get_market_ticker_name = MagicMock(side_effect=lambda t: f"종목{t}")

        pool = FakePool(stock_symbols=[])
        with patch.object(dm, "db_pool", pool):
            await dm._jarvis_stock_scanner()

        self.assertEqual(attempts["n"], 3)
        msg, kw = self.sent[0]
        self.assertNotIn("DB 유니버스", msg)
        inserted_symbols = {
            args[0] for query, args in pool.conn.executed if "INSERT INTO watchlist" in query
        }
        self.assertIn("000020", inserted_symbols)

    async def test_both_markets_fail_and_db_universe_empty_falls_back_to_kis(self):
        """양쪽 시장 티커 목록이 모두 실패하고 DB 유니버스도 비어 있으면 KIS API
        폴백 스캔으로 넘어가고, 그 사실도 알림에 남는다."""
        fail = RuntimeError("KRX 전체 장애")
        self._set_pykrx(
            ticker_list_by_market={"KOSPI": fail, "KOSDAQ": fail},
            ohlcv_by_ticker={"005930": None},
        )
        pool = FakePool(stock_symbols=[])  # DB 유니버스도 없음
        kis_candidates = [{
            "symbol": "000099", "name": "KIS폴백종목", "change": 2.0, "vol_ratio": 2.0,
            "golden_cross": False, "score": 5, "close": 12000, "rsi": 55.0, "momentum_5d": 1.0,
        }]
        with patch.object(dm, "db_pool", pool), \
             patch.object(dm, "_kis_scan_candidates", new=AsyncMock(return_value=kis_candidates)):
            await dm._jarvis_stock_scanner()

        self.assertEqual(len(self.sent), 1)
        msg, kw = self.sent[0]
        self.assertEqual(kw.get("dest"), "personal")
        self.assertIn("KIS API 폴백", msg)
        inserted_symbols = {
            args[0] for query, args in pool.conn.executed if "INSERT INTO watchlist" in query
        }
        self.assertIn("000099", inserted_symbols)

    async def test_total_failure_preserves_watchlist_and_alerts(self):
        """pykrx도 DB 유니버스도 KIS 폴백도 모두 실패하면 기존 watchlist를 건드리지
        않고 실패를 개인방에 알린다(기존 동작 유지)."""
        fail = RuntimeError("KRX 전체 장애")
        self._set_pykrx(
            ticker_list_by_market={"KOSPI": fail, "KOSDAQ": fail},
            ohlcv_by_ticker={"005930": None},
        )
        pool = FakePool(stock_symbols=[])
        with patch.object(dm, "db_pool", pool), \
             patch.object(dm, "_kis_scan_candidates", new=AsyncMock(return_value=None)):
            await dm._jarvis_stock_scanner()

        self.assertEqual(len(self.sent), 1)
        msg, kw = self.sent[0]
        self.assertEqual(kw.get("dest"), "personal")
        self.assertIn("스캔 실패", msg)
        self.assertIn("기존 감시종목 유지", msg)
        # 실패 시 watchlist에 아무 것도 쓰지 않아야 한다(빈 목록으로 조용히 덮어쓰지 않음)
        self.assertEqual([q for q, a in pool.conn.executed if "watchlist" in q], [])


if __name__ == "__main__":
    unittest.main()
