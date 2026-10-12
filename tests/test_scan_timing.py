"""
tests/test_scan_timing.py - [AT] feat/scan-timing

배경: 2026-10-12 아침 스캔 알림이 08:30 예정인데 09:06에 도착해 원인 조사가
필요했다. 기존 로직·주문·알림 내용은 바꾸지 않고 타이밍 로깅만 추가한다
(dashboard/main.py `_run_scheduled_job` 공통 래퍼 + `_jarvis_stock_scanner`
단계별 로그). 이 테스트가 검증하는 것:

1. `_run_scheduled_job`는 잡의 반환값과 예외를 그대로 전달한다(동작 불변).
2. 예정 시각 대비 실제 시작 지연이 30초 미만이면 WARNING을 남기지 않는다.
3. 지연이 30초 이상이면 WARNING 로그를 남긴다.
4. `_jarvis_stock_scanner`에 데이터조회/스코어링/알림발송 단계별 소요시간
   로그가 추가되어도, 기존 텔레그램 메시지 내용과 watchlist 갱신 결과는
   바뀌지 않는다(회귀 아님 — test_morning_scan_fallback.py와 동일한 대역 재사용).
"""
import logging
import sys
import unittest
from datetime import datetime as _real_datetime
from unittest.mock import AsyncMock, MagicMock, patch

for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

import dashboard.main as dm  # noqa: E402
from datetime import time as dtime  # noqa: E402


def _frozen_datetime_at(year, month, day, hour, minute, second=0):
    class _Frozen(_real_datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(year, month, day, hour, minute, second, tzinfo=dm.KST)
    return _Frozen


class TestRunScheduledJobTiming(unittest.IsolatedAsyncioTestCase):
    """_run_scheduled_job 공통 래퍼 — 시작/종료 로그, 지연 경고, 동작 불변 검증."""

    async def test_passes_through_return_value_unchanged(self):
        """잡의 반환값을 그대로 전달한다(동작 변경 없음)."""
        job = AsyncMock(return_value={"ok": True})
        with patch.object(dm, "datetime", _frozen_datetime_at(2026, 10, 6, 8, 30)):
            result = await dm._run_scheduled_job("test_job", dtime(8, 30), job)
        self.assertEqual(result, {"ok": True})
        job.assert_awaited_once_with()

    async def test_passes_through_exception_unchanged(self):
        """잡이 던진 예외를 삼키지 않고 그대로 전파한다(동작 변경 없음)."""
        async def boom():
            raise RuntimeError("작업 실패")

        with patch.object(dm, "datetime", _frozen_datetime_at(2026, 10, 6, 8, 30)):
            with self.assertRaises(RuntimeError):
                await dm._run_scheduled_job("boom_job", dtime(8, 30), boom)

    async def test_on_time_start_logs_no_delay_warning(self):
        """예정 시각과 실제 시작 시각이 같으면(지연 0초) WARNING을 남기지 않는다."""
        job = AsyncMock(return_value=None)
        with patch.object(dm, "datetime", _frozen_datetime_at(2026, 10, 6, 8, 30, 0)):
            with self.assertLogs(dm.logger, level="INFO") as cm:
                await dm._run_scheduled_job("on_time_job", dtime(8, 30), job)
        warnings = [r for r in cm.records if r.levelno >= logging.WARNING]
        self.assertEqual(warnings, [])
        infos = "\n".join(cm.output)
        self.assertIn("[on_time_job] 시작", infos)
        self.assertIn("[on_time_job] 종료", infos)

    async def test_small_delay_under_30s_logs_no_warning(self):
        """지연이 30초 미만이면 WARNING을 남기지 않는다(경계 미달)."""
        job = AsyncMock(return_value=None)
        with patch.object(dm, "datetime", _frozen_datetime_at(2026, 10, 6, 8, 30, 10)):
            with self.assertLogs(dm.logger, level="INFO") as cm:
                await dm._run_scheduled_job("slightly_late_job", dtime(8, 30), job)
        warnings = [r for r in cm.records if r.levelno >= logging.WARNING]
        self.assertEqual(warnings, [])

    async def test_delay_30s_or_more_logs_warning(self):
        """예정 시각 대비 실제 시작 지연이 30초 이상이면 WARNING 로그를 남긴다."""
        job = AsyncMock(return_value=None)
        with patch.object(dm, "datetime", _frozen_datetime_at(2026, 10, 6, 8, 31, 0)):
            with self.assertLogs(dm.logger, level="INFO") as cm:
                await dm._run_scheduled_job("late_job", dtime(8, 30), job)
        warnings = [r for r in cm.records if r.levelno >= logging.WARNING]
        self.assertEqual(len(warnings), 1)
        self.assertIn("late_job", warnings[0].getMessage())
        self.assertIn("지연", warnings[0].getMessage())

    async def test_delay_exactly_30s_logs_warning(self):
        """지연이 정확히 30초면(경계값) 경고 조건을 만족한다."""
        job = AsyncMock(return_value=None)
        with patch.object(dm, "datetime", _frozen_datetime_at(2026, 10, 6, 8, 30, 30)):
            with self.assertLogs(dm.logger, level="INFO") as cm:
                await dm._run_scheduled_job("boundary_job", dtime(8, 30), job)
        warnings = [r for r in cm.records if r.levelno >= logging.WARNING]
        self.assertEqual(len(warnings), 1)


# ── 아래는 tests/test_morning_scan_fallback.py의 대역을 그대로 재사용 ──
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


class TestMorningScanTimingLogsDoNotChangeBehavior(unittest.IsolatedAsyncioTestCase):
    """_jarvis_stock_scanner에 추가한 단계별 타이밍 로그가 기존 텔레그램 알림
    내용과 watchlist 갱신 결과를 바꾸지 않음을 확인한다(회귀 아님)."""

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

        def get_market_ticker_list(date, market=None):
            return {"KOSPI": ["000020"], "KOSDAQ": ["000040"]}[market]

        def get_market_ohlcv(d30, today, ticker):
            return {"005930": None, "000020": _winning_df(), "000040": _winning_df()}.get(ticker)

        self._pykrx.get_market_ticker_list = MagicMock(side_effect=get_market_ticker_list)
        self._pykrx.get_market_ohlcv = MagicMock(side_effect=get_market_ohlcv)
        self._pykrx.get_market_ticker_name = MagicMock(side_effect=lambda t: f"종목{t}")

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()

    async def test_telegram_message_and_watchlist_unchanged_with_timing_logs(self):
        pool = FakePool(stock_symbols=["999999"])
        with patch.object(dm, "db_pool", pool):
            with self.assertLogs(dm.logger, level="INFO") as cm:
                await dm._jarvis_stock_scanner()

        # 기존 동작: 폴백 문구 없는 정상 알림 1건, watchlist에 두 종목 INSERT
        self.assertEqual(len(self.sent), 1)
        msg, kw = self.sent[0]
        self.assertEqual(kw.get("dest"), "personal")
        self.assertNotIn("DB 유니버스", msg)
        self.assertNotIn("KIS API 폴백", msg)
        inserted_symbols = {
            args[0] for query, args in pool.conn.executed if "INSERT INTO watchlist" in query
        }
        self.assertEqual(inserted_symbols, {"000020", "000040"})

        # 새로 추가된 타이밍 로그가 실제로 남는다
        log_text = "\n".join(cm.output)
        self.assertIn("데이터조회", log_text)
        self.assertIn("스코어링", log_text)
        self.assertIn("알림발송", log_text)
        self.assertIn("총 소요", log_text)


if __name__ == "__main__":
    unittest.main()
