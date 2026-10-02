"""
data_collector/main.py 장 시간 수집 윈도우 회귀 테스트(fix/collector-market-hours).

배경 1: _loop()가 60초마다 collect_all()을 장 시간 검사 없이 무조건 호출해
장외 시간에도 KIS 속도제한(EGW00201)/SESSION FULL이 반복되고 1분봉 테이블에
00:00~23:59 전 시간대·주말까지 행이 쌓였다(10/2 dashboard 로그).

배경 2(실측): data-collector 서버 시간대는 UTC다(10/2 data-collector Console:
TZ 환경변수 없음, datetime.now()=05:53인데 실제 KST는 14:53). 서버 로컬
datetime.now()로 장 시간을 비교하면 장중(KST 09:00~15:30 = UTC 00:00~06:30)에
수집이 꺼지고 장 마감 후 KST 17:55~00:35에 켜지는 거꾸로 된 동작이 된다.
평일 판단도 UTC 기준이면 월요일 KST 08:55~09:00(= 일요일 UTC 23:55)을
주말로 오판한다. 이를 막기 위해 _in_collect_window는 KST timezone-aware
datetime을 받고, _loop()는 datetime.now(KST)로 구해서 넘긴다(서버 TZ와 무관).

검증 항목:
1. _in_collect_window(KST-aware datetime) 자체: 평일/주말, 08:55·15:35 경계.
2. _loop() 통합: 서버 시계가 UTC라고 가정하고 datetime.now()(naive)·
   datetime.now(KST)를 동시에 모킹해 (a)~(g) 시나리오를 검증한다.
   (a) 서버 UTC 01:00 평일(=KST 10:00) → 수집함
   (b) 서버 UTC 08:55~09:00 평일(=KST 17:55~18:00) → 건너뜀
   (c) 일요일 UTC 23:55(=월요일 KST 08:55) → 수집함(경계 포함, 요일 전환)
   (d) 금요일 UTC 15:00(=토요일 KST 00:00) → 건너뜀(요일 전환)
   (e) 토요일 UTC 01:00(=토요일 KST 10:00) → 건너뜀
   (f) KST 15:35 경계 포함, 15:36 제외
   (g) COLLECT_ALWAYS=1 이면 서버 시계·창과 무관하게 항상 수집
3. 장외 시간에도 일봉(+ 수급/공시/뉴스) 수집 경로(서버 시간 16시 이후)는
   그대로 호출된다 — 이 조건은 서버 시간(UTC) 기준이라 KST 01시에 동작한다
   (이번 수정 범위 밖, 기존 동작 유지만 확인).
4. 장중/장외 전환 시에만 로그 1줄, 같은 상태가 이어지는 사이클에는 추가 로그 없음.
"""
import importlib.util
import sys
import types
import unittest
from datetime import datetime, timezone as dt_timezone
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
KST = data_collector_main.KST


def _kst(*args):
    """KST timezone-aware datetime 생성 헬퍼."""
    return datetime(*args, tzinfo=KST)


class TestInCollectWindow(unittest.TestCase):
    """_in_collect_window() 순수 함수 단위 테스트 — 인자는 KST-aware datetime."""

    def test_weekday_10am_collects(self):
        # 2026-10-05 월요일 KST 10:00
        self.assertTrue(_in_collect_window(_kst(2026, 10, 5, 10, 0, 0)))

    def test_weekday_before_open_skips(self):
        # 2026-10-05 월요일 KST 07:00
        self.assertFalse(_in_collect_window(_kst(2026, 10, 5, 7, 0, 0)))

    def test_weekday_after_close_skips(self):
        # 2026-10-05 월요일 KST 16:30
        self.assertFalse(_in_collect_window(_kst(2026, 10, 5, 16, 30, 0)))

    def test_saturday_skips(self):
        # 2026-10-03 토요일 KST 10:00
        self.assertFalse(_in_collect_window(_kst(2026, 10, 3, 10, 0, 0)))

    def test_sunday_skips(self):
        # 2026-10-04 일요일 KST 10:00
        self.assertFalse(_in_collect_window(_kst(2026, 10, 4, 10, 0, 0)))

    def test_window_start_boundary_inclusive(self):
        # 월요일 KST 08:55:00 — 포함
        self.assertTrue(_in_collect_window(_kst(2026, 10, 5, 8, 55, 0)))

    def test_just_before_window_start_excluded(self):
        # 월요일 KST 08:54:59 — 제외
        self.assertFalse(_in_collect_window(_kst(2026, 10, 5, 8, 54, 59)))

    def test_window_end_boundary_inclusive(self):
        # 월요일 KST 15:35:00 — 포함(마지막 분봉 수집 보장)
        self.assertTrue(_in_collect_window(_kst(2026, 10, 5, 15, 35, 0)))

    def test_just_after_window_end_excluded(self):
        # 월요일 KST 15:35:01 — 제외
        self.assertFalse(_in_collect_window(_kst(2026, 10, 5, 15, 35, 1)))


class TestLoopMarketHoursGating(unittest.IsolatedAsyncioTestCase):
    """DataCollector._loop() 한 사이클 동작 — 서버 시계가 UTC라고 가정하고
    datetime.now()(naive)/datetime.now(KST)를 함께 모킹해 검증한다."""

    async def asyncSetUp(self):
        self.collector = DataCollector()
        self.collector.running = False  # 한 사이클만 돌리고 직접 종료
        self.collector.kis = AsyncMock()
        self.collector.daily = AsyncMock()
        config.COLLECT_ALWAYS = False

    async def _run_one_cycle(self, utc_now):
        """utc_now: 서버가 실제로 관측하는 UTC 벽시계 값(naive).

        서버 TZ가 UTC(TZ 환경변수 미설정)인 상황을 재현한다: 인자 없는
        datetime.now()는 이 값 그대로를 반환하고(서버 로컬=UTC), tz를 넘긴
        datetime.now(KST)는 실제 Python 동작과 동일하게 이 값을 KST로
        변환한 timezone-aware datetime을 반환해야 한다.
        """

        def fake_now(tz=None):
            if tz is None:
                return utc_now
            return utc_now.replace(tzinfo=dt_timezone.utc).astimezone(tz)

        async def fake_sleep(_seconds):
            self.collector.running = False

        with patch.object(data_collector_main, "datetime") as mock_dt, \
             patch("asyncio.sleep", new=fake_sleep), \
             patch.object(data_collector_main.cache, "set_bot_status", new=AsyncMock()):
            mock_dt.now.side_effect = fake_now
            self.collector.running = True
            await self.collector._loop()

    # (a) 서버 UTC 01:00 평일(=KST 10:00) → 수집함
    async def test_a_server_utc_morning_weekday_collects(self):
        await self._run_one_cycle(datetime(2026, 10, 5, 1, 0, 0))  # 월요일 UTC 01:00
        self.collector.kis.collect_all.assert_awaited_once()

    # (b) 서버 UTC 08:55~09:00 평일(=KST 17:55~18:00) → 건너뜀
    async def test_b_server_utc_evening_weekday_skips(self):
        await self._run_one_cycle(datetime(2026, 10, 5, 8, 55, 0))  # 월요일 UTC 08:55
        self.collector.kis.collect_all.assert_not_awaited()

    async def test_b_server_utc_evening_weekday_skips_upper_bound(self):
        await self._run_one_cycle(datetime(2026, 10, 5, 9, 0, 0))  # 월요일 UTC 09:00
        self.collector.kis.collect_all.assert_not_awaited()

    # (c) 일요일 UTC 23:55(=월요일 KST 08:55) → 수집함(경계 포함, 요일 전환)
    async def test_c_sunday_utc_late_night_rolls_into_monday_kst_collects(self):
        await self._run_one_cycle(datetime(2026, 10, 4, 23, 55, 0))  # 일요일 UTC 23:55
        self.collector.kis.collect_all.assert_awaited_once()

    # (d) 금요일 UTC 15:00(=토요일 KST 00:00) → 건너뜀(요일 전환)
    async def test_d_friday_utc_evening_rolls_into_saturday_kst_skips(self):
        await self._run_one_cycle(datetime(2026, 10, 2, 15, 0, 0))  # 금요일 UTC 15:00
        self.collector.kis.collect_all.assert_not_awaited()

    # (e) 토요일 UTC 01:00(=토요일 KST 10:00) → 건너뜀
    async def test_e_saturday_utc_morning_skips(self):
        await self._run_one_cycle(datetime(2026, 10, 3, 1, 0, 0))  # 토요일 UTC 01:00
        self.collector.kis.collect_all.assert_not_awaited()

    # (f) KST 15:35 경계 포함, 15:36 제외 (UTC로 표현: 06:35 포함, 06:36 제외)
    async def test_f_kst_window_end_boundary_inclusive(self):
        await self._run_one_cycle(datetime(2026, 10, 5, 6, 35, 0))  # 월요일 UTC 06:35 = KST 15:35
        self.collector.kis.collect_all.assert_awaited_once()

    async def test_f_kst_window_end_boundary_excluded(self):
        await self._run_one_cycle(datetime(2026, 10, 5, 6, 36, 0))  # 월요일 UTC 06:36 = KST 15:36
        self.collector.kis.collect_all.assert_not_awaited()

    # (g) COLLECT_ALWAYS=1 이면 서버 시계·창과 무관하게 항상 수집
    async def test_g_collect_always_overrides_window_even_off_hours(self):
        config.COLLECT_ALWAYS = True
        try:
            await self._run_one_cycle(datetime(2026, 10, 2, 15, 0, 0))  # 금요일 UTC 15:00 = 토요일 KST 00:00
        finally:
            config.COLLECT_ALWAYS = False
        self.collector.kis.collect_all.assert_awaited_once()

    async def test_after_hours_daily_collect_path_still_runs(self):
        """일봉(daily.collect_all) 경로는 서버 시간(UTC) 16시 이후 기준으로,
        KST 장 시간 검사와 무관하게 그대로 호출된다(이번 수정 범위 밖, 기존 유지 확인).
        UTC 16:30 = KST 다음날(화) 01:30 → 장외라 kis.collect_all은 건너뛴다."""
        self.collector.last_daily_collect = None
        await self._run_one_cycle(datetime(2026, 10, 5, 16, 30, 0))  # 월요일 UTC 16:30
        self.collector.kis.collect_all.assert_not_awaited()
        self.collector.daily.collect_all.assert_awaited_once()

    async def test_state_change_logs_once(self):
        """장외→장중 전환 시 1줄만 로그, 동일 상태가 이어지면 전환 로그가 또 찍히지 않는다."""
        with self.assertLogs("data-collector", level="INFO") as logs:
            await self._run_one_cycle(datetime(2026, 10, 5, 1, 0, 0))  # 월요일 UTC 01:00 = KST 10:00
        transition_logs = [m for m in logs.output if "장중 수집 시작" in m or "수집 건너뜀" in m]
        self.assertEqual(len(transition_logs), 1)
        self.assertIn("KST", transition_logs[0])

        # 이미 "수집 중" 상태이므로 같은 상태의 다음 사이클에는 전환 로그가 없어야 한다.
        with self.assertLogs("data-collector", level="INFO") as logs:
            await self._run_one_cycle(datetime(2026, 10, 5, 1, 1, 0))  # 월요일 UTC 01:01 = KST 10:01
        transition_logs = [m for m in logs.output if "장중 수집 시작" in m or "수집 건너뜀" in m]
        self.assertEqual(len(transition_logs), 0)


if __name__ == "__main__":
    unittest.main()
