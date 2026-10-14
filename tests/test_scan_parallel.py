"""
tests/test_scan_parallel.py - [AT] fix/scan-parallel (PM 승인)

배경: 2026-10-14 아침스캔이 2173.5초 걸려 morning_routine이 스케줄러를 36분
블로킹했다. 원인은 dashboard/main.py `_jarvis_stock_scanner`의 `_scan()` 내부
종목별 `pykrx.get_market_ohlcv` 순차 조회(종목 수에 선형, sleep 없음)다.

이 테스트가 검증하는 것:
1. `_scan_fetch_workers()` 환경변수 SCAN_FETCH_WORKERS 클램프(기본 5, 1~8).
2. 종목별 일봉 조회를 병렬화해도 동시 요청 수(1/5/8)와 무관하게 후보 선정
   결과(점수·순서·watchlist 신규 목록)가 동일하다 — 후보 선정 기준·점수 계산·
   정렬은 절대 바뀌지 않았음을 보증한다.
3. 종목 단위 예외는 해당 종목만 스킵하고 나머지 스캔은 계속된다(기존과 동일
   의미).
4. 한 시장의 병렬 조회 실패율이 30%를 넘으면 WARNING을 남기고 그 시장 전체를
   순차 방식으로 재조회한다(KRX 동시요청 차단 대비 안전장치).
5. 조회 종목수(N)·실패 종목수·fetch_elapsed·score_elapsed가
   health:scan_detail:YYYY-MM-DD(7일 TTL)에 JSON으로 저장된다.
"""
import json
import logging
import os
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


# ── 대역(tests/test_scan_timing.py와 동일한 패턴) ──────────────────────────
class _FakeSeries(list):
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
    closes = [9000 + 100 * i for i in range(25)]
    vols = [10] * 24 + [100]
    changes = [0] * 24 + [5.0]
    return _FakeDF(closes, vols, changes)


def _losing_df():
    """score < 4 이므로 후보에 들지 않는 평범한 종목."""
    closes = [9000] * 25
    vols = [10] * 25
    changes = [0] * 25
    return _FakeDF(closes, vols, changes)


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakeConn:
    def __init__(self, stock_symbols=()):
        self.stock_symbols = list(stock_symbols)
        self.executed = []

    async def fetch(self, query, *args):
        if "FROM stocks" in query:
            return [{"symbol": s} for s in self.stock_symbols]
        return []

    async def execute(self, query, *args):
        self.executed.append((query, args))
        return "OK"


class FakePool:
    def __init__(self, stock_symbols=()):
        self.conn = FakeConn(stock_symbols)

    def acquire(self):
        return _AcquireCtx(self.conn)


class FakeRedis:
    """get/setex만 지원 — health:scan_detail:* 저장 검증용."""

    def __init__(self):
        self.store = {}

    async def get(self, key):
        v = self.store.get(key)
        return v

    async def setex(self, key, ttl, value):
        self.store[key] = value


class TestScanFetchWorkersClamp(unittest.TestCase):
    """_scan_fetch_workers() — SCAN_FETCH_WORKERS 환경변수 클램프(기본 5, 1~8)."""

    def _with_env(self, value):
        env = dict(os.environ)
        if value is None:
            env.pop("SCAN_FETCH_WORKERS", None)
        else:
            env["SCAN_FETCH_WORKERS"] = value
        return patch.dict(os.environ, env, clear=True)

    def test_default_when_unset(self):
        with self._with_env(None):
            self.assertEqual(dm._scan_fetch_workers(), 5)

    def test_within_range_is_kept(self):
        with self._with_env("3"):
            self.assertEqual(dm._scan_fetch_workers(), 3)

    def test_clamped_to_max_8(self):
        with self._with_env("20"):
            self.assertEqual(dm._scan_fetch_workers(), 8)

    def test_clamped_to_min_1(self):
        with self._with_env("0"):
            self.assertEqual(dm._scan_fetch_workers(), 1)
        with self._with_env("-5"):
            self.assertEqual(dm._scan_fetch_workers(), 1)

    def test_invalid_value_falls_back_to_default(self):
        with self._with_env("not-a-number"):
            self.assertEqual(dm._scan_fetch_workers(), 5)


class TestScanParallelEquivalence(unittest.IsolatedAsyncioTestCase):
    """동시 조회 수(1/5/8)와 무관하게 후보 선정 결과(점수·순서)가 동일해야 한다."""

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
        start(patch("time.sleep", new=MagicMock()))

        self._pykrx = MagicMock()
        start(patch.dict(sys.modules, {"pykrx": MagicMock(stock=self._pykrx), "pykrx.stock": self._pykrx}))

        # KOSPI 4종목(3승 + 1패) + KOSDAQ 2종목(1승 + 1패) — 동점 처리 순서까지 확인
        kospi = ["010000", "010010", "010020", "010030"]
        kosdaq = ["020000", "020010"]
        winners = {"010000", "010020", "010030", "020000"}

        def get_market_ticker_list(date, market=None):
            return {"KOSPI": kospi, "KOSDAQ": kosdaq}[market]

        def get_market_ohlcv(d30, today, ticker):
            return _winning_df() if ticker in winners else _losing_df()

        self._pykrx.get_market_ticker_list = MagicMock(side_effect=get_market_ticker_list)
        self._pykrx.get_market_ohlcv = MagicMock(side_effect=get_market_ohlcv)
        self._pykrx.get_market_ticker_name = MagicMock(side_effect=lambda t: f"종목{t}")

        self.redis = FakeRedis()
        start(patch.object(dm, "redis_client", self.redis))

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()

    async def _run_once(self, workers_env):
        self.sent.clear()
        pool = FakePool(stock_symbols=[])
        env = dict(os.environ)
        if workers_env is None:
            env.pop("SCAN_FETCH_WORKERS", None)
        else:
            env["SCAN_FETCH_WORKERS"] = workers_env
        with patch.dict(os.environ, env, clear=True):
            with patch.object(dm, "db_pool", pool):
                await dm._jarvis_stock_scanner()
        self.assertEqual(len(self.sent), 1)
        msg, _ = self.sent[0]
        inserted = [args[0] for q, args in pool.conn.executed if "INSERT INTO watchlist" in q]
        return msg, inserted

    async def test_result_identical_across_worker_counts(self):
        msg_1, inserted_1 = await self._run_once("1")
        msg_5, inserted_5 = await self._run_once("5")
        msg_8, inserted_8 = await self._run_once("8")

        # watchlist INSERT 순서(= 후보 선정·정렬 순서)가 동시 요청 수와 무관하게 동일
        self.assertEqual(inserted_1, inserted_5)
        self.assertEqual(inserted_5, inserted_8)
        self.assertEqual(set(inserted_1), {"010000", "010020", "010030", "020000"})

        # 텔레그램 메시지(신규 종목 목록 포함) 내용도 동일
        self.assertEqual(msg_1, msg_5)
        self.assertEqual(msg_5, msg_8)

    async def test_scan_detail_recorded_to_redis(self):
        await self._run_once("5")
        keys = [k for k in self.redis.store if k.startswith("health:scan_detail:")]
        self.assertEqual(len(keys), 1)
        detail = json.loads(self.redis.store[keys[0]])
        self.assertEqual(detail["total"], 6)  # KOSPI 4 + KOSDAQ 2
        self.assertEqual(detail["fail"], 0)
        self.assertIn("fetch_elapsed", detail)
        self.assertIn("score_elapsed", detail)


class TestScanParallelFallback(unittest.IsolatedAsyncioTestCase):
    """한 시장의 조회 실패율이 30%를 넘으면 WARNING + 순차 재조회로 폴백한다."""

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
        start(patch("time.sleep", new=MagicMock()))

        self._pykrx = MagicMock()
        start(patch.dict(sys.modules, {"pykrx": MagicMock(stock=self._pykrx), "pykrx.stock": self._pykrx}))

        # KOSPI: 5종목 중 2종목이 항상 실패(40% > 30% 임계값) → 폴백 트리거
        # KOSDAQ: 1종목, 실패 없음 → 폴백 트리거 안 됨
        self.kospi = ["030000", "030010", "030020", "030030", "030040"]
        self.kosdaq = ["040000"]
        self.fail_tickers = {"030000", "030010"}

        def get_market_ticker_list(date, market=None):
            return {"KOSPI": self.kospi, "KOSDAQ": self.kosdaq}[market]

        def get_market_ohlcv(d30, today, ticker):
            if ticker in self.fail_tickers:
                raise RuntimeError(f"KRX 조회 실패: {ticker}")
            return _winning_df()

        self._pykrx.get_market_ticker_list = MagicMock(side_effect=get_market_ticker_list)
        self._pykrx.get_market_ohlcv = MagicMock(side_effect=get_market_ohlcv)
        self._pykrx.get_market_ticker_name = MagicMock(side_effect=lambda t: f"종목{t}")

        self.redis = FakeRedis()
        start(patch.object(dm, "redis_client", self.redis))

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()

    async def test_fallback_warning_and_surviving_candidates(self):
        pool = FakePool(stock_symbols=[])
        with patch.dict(os.environ, {"SCAN_FETCH_WORKERS": "5"}, clear=False):
            with patch.object(dm, "db_pool", pool):
                with self.assertLogs(dm.logger, level="INFO") as cm:
                    await dm._jarvis_stock_scanner()

        warnings = [r.getMessage() for r in cm.records if r.levelno >= logging.WARNING]
        fallback_warnings = [w for w in warnings if "순차 조회로 폴백" in w]
        self.assertEqual(len(fallback_warnings), 1)
        self.assertIn("2/5", fallback_warnings[0])

        # 실패한 2종목은 스킵되고 나머지 4종목(KOSPI 3 + KOSDAQ 1)은 정상 통과
        inserted = {args[0] for q, args in pool.conn.executed if "INSERT INTO watchlist" in q}
        self.assertEqual(inserted, {"030020", "030030", "030040", "040000"})
        self.assertFalse(inserted & self.fail_tickers)

        keys = [k for k in self.redis.store if k.startswith("health:scan_detail:")]
        detail = json.loads(self.redis.store[keys[0]])
        self.assertEqual(detail["total"], 6)
        self.assertEqual(detail["fail"], 2)


if __name__ == "__main__":
    unittest.main()
