"""
dashboard/main.py _cache_warmer 회귀 테스트.

배경: hot 계산 줄(dtime 사용)에서 예외가 나면 except 뒤 `asyncio.sleep(18 if hot else 60)` 에서
UnboundLocalError 가 나 워머 루프 전체가 죽었다. 원인은 dtime 이 모듈 전역에 없어 매 사이클
NameError 가 났기 때문. 여기서는 (1) hot 계산 전에 예외가 나도 루프가 계속 도는지,
(2) dtime 이 전역에 있어 정상 사이클이 실제로 데이터를 선적재하는지,
(3) 차트 선적재가 종목 사이 간격을 두고 한 종목 실패에 안 멈추며 info 로그를 남기는지 검증한다.
"""
import asyncio
import sys
import unittest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()


class _Stop(BaseException):
    """루프를 끊기 위한 신호 (except Exception 에 잡히지 않도록 BaseException)"""


class TestCacheWarmer(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import dashboard.main as dm
        self.dm = dm

    def _patch_sleep(self, max_loop_sleeps):
        """워머 sleep 을 기록하고, 루프 주기 sleep(18/60초)이 max_loop_sleeps 회가 되면 끊는다.
        시작 대기(5초)와 차트 종목 간격 sleep 은 카운트하지 않는다."""
        sleeps = []

        async def fake_sleep(sec):
            sleeps.append(sec)
            if len([x for x in sleeps if x in (18, 60)]) >= max_loop_sleeps:
                raise _Stop()

        p = patch.object(self.dm.asyncio, "sleep", fake_sleep)
        p.start()
        self.addCleanup(p.stop)
        return sleeps

    async def test_module_has_dtime(self):
        self.assertTrue(hasattr(self.dm, "dtime"))

    async def test_exception_before_hot_computed_does_not_kill_loop(self):
        """hot 계산 전(datetime.now)에서 매 사이클 예외 → UnboundLocalError 없이 계속 돌아야 함"""
        sleeps = self._patch_sleep(3)
        fake_dt = MagicMock()
        fake_dt.now.side_effect = RuntimeError("boom-before-hot")
        with patch.object(self.dm, "datetime", fake_dt), \
                self.assertLogs(self.dm.logger, level="WARNING") as cm:
            with self.assertRaises(_Stop):
                await self.dm._cache_warmer()
        loop_sleeps = [x for x in sleeps if x in (18, 60)]
        self.assertEqual(loop_sleeps, [60, 60, 60])  # hot 기본값 False → 60초, 3사이클 모두 도달
        warn = "\n".join(cm.output)
        self.assertIn("캐시 워머 사이클 오류", warn)
        self.assertIn("boom-before-hot", warn)  # 사유 포함

    async def test_exception_after_hot_still_continues_and_uses_hot_interval(self):
        sleeps = self._patch_sleep(2)
        weekday_noon = datetime(2026, 9, 28, 12, 0, tzinfo=self.dm.KST)  # 월요일 장중
        fake_dt = MagicMock()
        fake_dt.now.return_value = weekday_noon
        with patch.object(self.dm, "datetime", fake_dt), \
                patch.object(self.dm, "get_market_index", AsyncMock(side_effect=RuntimeError("kis down"))), \
                self.assertLogs(self.dm.logger, level="WARNING"):
            with self.assertRaises(_Stop):
                await self.dm._cache_warmer()
        self.assertEqual([x for x in sleeps if x in (18, 60)], [18, 18])

    async def test_warmer_actually_preloads_charts_and_logs_summary(self):
        """정상 사이클: dtime 이 있어 NameError 없이 진행하고 첫 사이클에 차트를 선적재 + info 로그"""
        self._patch_sleep(1)
        fake_dt = MagicMock()
        fake_dt.now.return_value = datetime(2026, 9, 28, 12, 0, tzinfo=self.dm.KST)
        chart = AsyncMock(return_value={"success": True, "data": [{"c": 1}]})
        pos = {"data": [{"symbol": f"00{i:04d}"} for i in range(8)]}
        with patch.object(self.dm, "datetime", fake_dt), \
                patch.object(self.dm, "get_market_index", AsyncMock()), \
                patch.object(self.dm, "get_stock_positions", AsyncMock(return_value=pos)), \
                patch.object(self.dm, "get_stock_prices", AsyncMock()), \
                patch.object(self.dm, "get_chart_data", chart), \
                self.assertLogs(self.dm.logger, level="INFO") as cm:
            with self.assertRaises(_Stop):
                await self.dm._cache_warmer()
        self.assertEqual(chart.await_count, 8)  # 보유 8개 전부
        for c in chart.await_args_list:
            self.assertEqual(c.args[1:], (90, "D"))
        out = "\n".join(cm.output)
        self.assertIn("캐시 워머 차트 선적재", out)
        self.assertIn("성공 8/8종목", out)
        self.assertIn("종목당", out)

    async def test_chart_warm_spaces_calls_and_survives_single_failure(self):
        sleeps = []

        async def fake_sleep(sec):
            sleeps.append(sec)

        results = [{"success": True, "data": [1]}, RuntimeError("KIS 500"),
                   {"success": False, "error": "x", "data": []}]
        chart = AsyncMock(side_effect=results)
        with patch.object(self.dm.asyncio, "sleep", fake_sleep), \
                patch.object(self.dm, "get_chart_data", chart), \
                self.assertLogs(self.dm.logger, level="INFO") as cm:
            await self.dm._warm_position_charts([{"symbol": "000001"}, {"symbol": "000002"}, {"symbol": "000003"}])
        self.assertEqual(chart.await_count, 3)                      # 한 종목 예외에도 나머지 진행
        self.assertEqual(sleeps, [self.dm._WARM_CHART_GAP] * 2)     # 종목 사이 간격
        out = "\n".join(cm.output)
        self.assertIn("성공 1/3종목 실패 2", out)

    # ── 경과 시간 기준 선적재 (가짜 monotonic 시계) ────────────────────
    def _run_loop_with_fake_clock(self, when, loop_sleeps, n_positions=3):
        """가짜 시계로 워머 루프를 loop_sleeps 사이클 돌리고 (chart mock, 사이클별 시각) 반환.
        루프 주기 sleep(18/60초)만 시계를 그만큼 전진시키고, 종목 간격 sleep 은 시계를 움직이지 않는다."""
        clock = {"t": 1000.0}
        cycle_times = []
        chart = AsyncMock(return_value={"success": True, "data": [1]})
        pos = {"data": [{"symbol": f"00{i:04d}"} for i in range(n_positions)]}
        fake_time = MagicMock()
        fake_time.monotonic = lambda: clock["t"]
        n = {"loop": 0}

        async def fake_sleep(sec):
            if sec in (18, 60):
                n["loop"] += 1
                if n["loop"] >= loop_sleeps:
                    raise _Stop()
                clock["t"] += sec

        async def index():
            cycle_times.append(clock["t"] - 1000.0)

        fake_dt = MagicMock()
        fake_dt.now.return_value = when
        return clock, cycle_times, chart, pos, fake_time, fake_sleep, index, fake_dt

    async def _drive(self, when, loop_sleeps, n_positions=3):
        clock, cycle_times, chart, pos, fake_time, fake_sleep, index, fake_dt = \
            self._run_loop_with_fake_clock(when, loop_sleeps, n_positions)
        with patch.object(self.dm, "_time", fake_time), \
                patch.object(self.dm.asyncio, "sleep", fake_sleep), \
                patch.object(self.dm, "datetime", fake_dt), \
                patch.object(self.dm, "get_market_index", index), \
                patch.object(self.dm, "get_stock_positions", AsyncMock(return_value=pos)), \
                patch.object(self.dm, "get_stock_prices", AsyncMock()), \
                patch.object(self.dm, "get_chart_data", chart), \
                self.assertLogs(self.dm.logger, level="INFO"):
            with self.assertRaises(_Stop):
                await self.dm._cache_warmer()
        return chart, cycle_times

    async def test_hot_interval_is_time_based_240s(self):
        """장중(sleep 18초): 240초 미만 사이클(t≤234)에는 첫 선적재 1회뿐, 252초 사이클에서 다시 선적재"""
        hot = datetime(2026, 9, 28, 12, 0, tzinfo=self.dm.KST)  # 월요일 장중
        chart, times = await self._drive(hot, 14)                # 사이클 t=0..234
        self.assertEqual(times[-1], 234.0)
        self.assertEqual(chart.await_count, 3)                    # 첫 사이클의 3종목만
        chart, times = await self._drive(hot, 15)                # 사이클 t=252 추가
        self.assertEqual(times[-1], 252.0)
        self.assertEqual(chart.await_count, 6)                    # 두 번째 선적재 3종목 추가

    async def test_cold_interval_is_same_240s_rule(self):
        """장외/주말(sleep 60초)도 같은 240초 기준: t=180 까지는 1회, t=240 에서 다시"""
        cold = datetime(2026, 9, 26, 12, 0, tzinfo=self.dm.KST)  # 토요일
        chart, times = await self._drive(cold, 4)                # 사이클 t=0,60,120,180
        self.assertEqual(times[-1], 180.0)
        self.assertEqual(chart.await_count, 3)
        chart, times = await self._drive(cold, 5)                # 사이클 t=240 → 경계값 포함
        self.assertEqual(times[-1], 240.0)
        self.assertEqual(chart.await_count, 6)

    async def test_no_preload_below_240s_boundary(self):
        """마지막 선적재 후 239초에는 하지 않는다 (경계: 239 미실행 / 240 실행)"""
        hot = datetime(2026, 9, 28, 12, 0, tzinfo=self.dm.KST)
        clock = {"t": 0.0}
        fake_time = MagicMock()
        fake_time.monotonic = lambda: clock["t"]
        chart = AsyncMock(return_value={"success": True, "data": [1]})
        n = {"loop": 0}

        async def fake_sleep(sec):
            if sec in (18, 60):
                n["loop"] += 1
                if n["loop"] >= 3:
                    raise _Stop()
                clock["t"] = next(seq_after)

        seq_after = iter([239.0, 240.0])
        fake_dt = MagicMock()
        fake_dt.now.return_value = hot
        with patch.object(self.dm, "_time", fake_time), \
                patch.object(self.dm.asyncio, "sleep", fake_sleep), \
                patch.object(self.dm, "datetime", fake_dt), \
                patch.object(self.dm, "get_market_index", AsyncMock()), \
                patch.object(self.dm, "get_stock_positions", AsyncMock(return_value={"data": [{"symbol": "000001"}]})), \
                patch.object(self.dm, "get_stock_prices", AsyncMock()), \
                patch.object(self.dm, "get_chart_data", chart), \
                self.assertLogs(self.dm.logger, level="INFO"):
            with self.assertRaises(_Stop):
                await self.dm._cache_warmer()
        # 사이클: t=0(선적재) → t=239(안 함) → t=240(선적재)
        self.assertEqual(chart.await_count, 2)

    async def _warm_n(self, n):
        chart = AsyncMock(return_value={"success": True, "data": [1]})
        sleeps = []

        async def fake_sleep(sec):
            sleeps.append(sec)

        with patch.object(self.dm.asyncio, "sleep", fake_sleep), \
                patch.object(self.dm, "get_chart_data", chart), \
                self.assertLogs(self.dm.logger, level="INFO"):
            await self.dm._warm_position_charts([{"symbol": f"{i:06d}"} for i in range(n)])
        return chart, sleeps

    async def test_nine_positions_all_preloaded(self):
        chart, sleeps = await self._warm_n(9)
        self.assertEqual(chart.await_count, 9)
        self.assertEqual([c.args[0] for c in chart.await_args_list], [f"{i:06d}" for i in range(9)])
        self.assertEqual(sleeps, [self.dm._WARM_CHART_GAP] * 8)  # 순차 + 종목 사이 간격 유지

    async def test_thirteen_positions_capped_at_twelve(self):
        chart, sleeps = await self._warm_n(13)
        self.assertEqual(self.dm._WARM_CHART_MAX, 12)
        self.assertEqual(chart.await_count, 12)
        self.assertNotIn("000012", [c.args[0] for c in chart.await_args_list])  # 13번째는 제외
        self.assertEqual(sleeps, [self.dm._WARM_CHART_GAP] * 11)

    async def test_interval_is_shorter_than_chart_cache_ttl(self):
        self.assertEqual(self.dm._WARM_CHART_INTERVAL, 240)
        self.assertLess(self.dm._WARM_CHART_INTERVAL, self.dm.ttl_for("D"))

    async def test_warm_log_never_contains_account_or_keys(self):
        chart = AsyncMock(side_effect=RuntimeError("acct 12345678-01 appkey APPKEY-SECRET-VALUE"))
        with patch.object(self.dm.config, "KIS_APP_KEY", "APPKEY-SECRET-VALUE"), \
                patch.object(self.dm.asyncio, "sleep", AsyncMock()), \
                patch.object(self.dm, "get_chart_data", chart), \
                self.assertLogs(self.dm.logger, level="INFO") as cm:
            await self.dm._warm_position_charts([{"symbol": "000001"}])
        out = "\n".join(cm.output)
        self.assertNotIn("APPKEY-SECRET-VALUE", out)
        self.assertNotIn("12345678", out)


if __name__ == "__main__":
    unittest.main()
