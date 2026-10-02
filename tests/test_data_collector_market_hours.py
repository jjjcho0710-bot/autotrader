"""
data_collector/main.py 장 시간 수집 윈도우 회귀 테스트(fix/collector-market-hours).

배경: _loop()가 60초마다 collect_all()을 장 시간 검사 없이 무조건 호출해
장외 시간에도 KIS 속도제한(EGW00201)/SESSION FULL이 반복되고 1분봉 테이블에
00:00~23:59 전 시간대·주말까지 행이 쌓였다(10/2 dashboard 로그).

검증 항목:
1. 평일 10:00 — 장중이므로 수집한다.
2. 평일 07:00, 평일 16:30 — 장외이므로 건너뛴다.
3. 토요일, 일요일 — 건너뛴다.
4. 평일 08:55/15:35 경계값 — 포함(수집)이다.
5. 평일 08:54/15:36 경계값 바로 밖 — 건너뛴다.
6. COLLECT_ALWAYS=1 이면 장 시간 검사와 무관하게 항상 수집한다.
7. 장외 시간에도 16시 이후 일봉(+ 수급/공시/뉴스) 수집 경로는 그대로 호출된다.
8. 장중/장외 전환 시에만 로그 1줄, 같은 상태가 이어지는 사이클에는 추가 로그 없음.
"""
import importlib.util
import sys
import types
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "data_collector"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _stub_missing_runtime_deps():
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

    try:
        import fastapi  # noqa: F401
    except ImportError:
        stub = types.ModuleType("fastapi")

        class _FastAPI:
            def __init__(self, *a, **kw):
                pass

            def post(self, *a, **kw):
                return lambda fn: fn

            def get(self, *a, **kw):
                return lambda fn: fn

        class _Request:
            pass

        stub.FastAPI = _FastAPI
        stub.Request = _Request
        sys.modules["fastapi"] = stub

    try:
        import uvicorn  # noqa: F401
    except ImportError:
        stub = types.ModuleType("uvicorn")
        stub.Server = lambda *a, **kw: None
        stub.Config = lambda *a, **kw: None
        sys.modules["uvicorn"] = stub


_stub_missing_runtime_deps()

from common.config import config  # noqa: E402

# data_collector/main.py를 "main"이 아닌 고유 이름으로 로드한다.
# stock_trader/main.py 등 다른 테스트가 같은 이름 "main"을 sys.modules에
# 캐싱해 두므로, 그대로 `import main`을 쓰면 전체 pytest 세션에서
# 어느 테스트가 먼저 수입되느냐에 따라 서로 다른 모듈을 덮어써 충돌한다.
_main_path = REPO_ROOT / "data_collector" / "main.py"
_spec = importlib.util.spec_from_file_location("data_collector_main_module", _main_path)
data_collector_main = importlib.util.module_from_spec(_spec)
sys.modules["data_collector_main_module"] = data_collector_main
_spec.loader.exec_module(data_collector_main)

DataCollector = data_collector_main.DataCollector
_in_collect_window = data_collector_main._in_collect_window


class TestInCollectWindow(unittest.TestCase):
    """_in_collect_window() 순수 함수 단위 테스트."""

    def test_weekday_10am_collects(self):
        # 2026-10-05 월요일 10:00
        self.assertTrue(_in_collect_window(datetime(2026, 10, 5, 10, 0, 0)))

    def test_weekday_before_open_skips(self):
        # 2026-10-05 월요일 07:00
        self.assertFalse(_in_collect_window(datetime(2026, 10, 5, 7, 0, 0)))

    def test_weekday_after_close_skips(self):
        # 2026-10-05 월요일 16:30
        self.assertFalse(_in_collect_window(datetime(2026, 10, 5, 16, 30, 0)))

    def test_saturday_skips(self):
        # 2026-10-03 토요일 10:00
        self.assertFalse(_in_collect_window(datetime(2026, 10, 3, 10, 0, 0)))

    def test_sunday_skips(self):
        # 2026-10-04 일요일 10:00
        self.assertFalse(_in_collect_window(datetime(2026, 10, 4, 10, 0, 0)))

    def test_window_start_boundary_inclusive(self):
        # 월요일 08:55:00 — 포함
        self.assertTrue(_in_collect_window(datetime(2026, 10, 5, 8, 55, 0)))

    def test_just_before_window_start_excluded(self):
        # 월요일 08:54:59 — 제외
        self.assertFalse(_in_collect_window(datetime(2026, 10, 5, 8, 54, 59)))

    def test_window_end_boundary_inclusive(self):
        # 월요일 15:35:00 — 포함(마지막 분봉 수집 보장)
        self.assertTrue(_in_collect_window(datetime(2026, 10, 5, 15, 35, 0)))

    def test_just_after_window_end_excluded(self):
        # 월요일 15:35:01 — 제외
        self.assertFalse(_in_collect_window(datetime(2026, 10, 5, 15, 35, 1)))


class _AsyncGatherNoop:
    """asyncio.gather를 대체해 kis.collect_all() 호출 여부만 기록."""


class TestLoopMarketHoursGating(unittest.IsolatedAsyncioTestCase):
    """DataCollector._loop() 한 사이클 동작 — collect_all 호출 여부·로그 검증."""

    async def asyncSetUp(self):
        self.collector = DataCollector()
        self.collector.running = False  # 한 사이클만 돌리고 직접 종료
        self.collector.kis = AsyncMock()
        self.collector.daily = AsyncMock()
        config.COLLECT_ALWAYS = False

    async def _run_one_cycle(self, now):
        """_loop()를 한 바퀴만 돌리기 위해 두 번째 반복에서 running=False로 멈춘다."""
        call_count = {"n": 0}

        async def fake_sleep(_seconds):
            call_count["n"] += 1
            self.collector.running = False

        with patch.object(data_collector_main, "datetime") as mock_dt, \
             patch("asyncio.sleep", new=fake_sleep), \
             patch.object(data_collector_main.cache, "set_bot_status", new=AsyncMock()):
            mock_dt.now.return_value = now
            self.collector.running = True
            await self.collector._loop()

    async def test_weekday_market_hours_collects(self):
        await self._run_one_cycle(datetime(2026, 10, 5, 10, 0, 0))
        self.collector.kis.collect_all.assert_awaited_once()

    async def test_weekday_before_open_skips_collect(self):
        await self._run_one_cycle(datetime(2026, 10, 5, 7, 0, 0))
        self.collector.kis.collect_all.assert_not_awaited()

    async def test_weekend_skips_collect(self):
        await self._run_one_cycle(datetime(2026, 10, 3, 10, 0, 0))  # 토요일
        self.collector.kis.collect_all.assert_not_awaited()

    async def test_collect_always_overrides_window(self):
        config.COLLECT_ALWAYS = True
        try:
            await self._run_one_cycle(datetime(2026, 10, 5, 7, 0, 0))
        finally:
            config.COLLECT_ALWAYS = False
        self.collector.kis.collect_all.assert_awaited_once()

    async def test_after_hours_daily_collect_path_still_runs(self):
        """16시 이후면 장외라도 일봉(daily.collect_all) 경로는 그대로 호출된다."""
        self.collector.last_daily_collect = None
        await self._run_one_cycle(datetime(2026, 10, 5, 16, 30, 0))
        self.collector.kis.collect_all.assert_not_awaited()
        self.collector.daily.collect_all.assert_awaited_once()

    async def test_state_change_logs_once(self):
        """장외→장중 전환 시 1줄만 로그, 동일 상태가 이어지면 전환 로그가 또 찍히지 않는다."""
        with self.assertLogs("data-collector", level="INFO") as logs:
            await self._run_one_cycle(datetime(2026, 10, 5, 10, 0, 0))
        transition_logs = [m for m in logs.output if "장중 수집 시작" in m or "수집 건너뜀" in m]
        self.assertEqual(len(transition_logs), 1)

        # 이미 "수집 중" 상태이므로 같은 상태의 다음 사이클에는 전환 로그가 없어야 한다.
        with self.assertLogs("data-collector", level="INFO") as logs:
            await self._run_one_cycle(datetime(2026, 10, 5, 10, 1, 0))
        transition_logs = [m for m in logs.output if "장중 수집 시작" in m or "수집 건너뜀" in m]
        self.assertEqual(len(transition_logs), 0)


if __name__ == "__main__":
    unittest.main()
