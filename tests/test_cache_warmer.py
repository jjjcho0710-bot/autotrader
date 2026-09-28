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
        """정상 사이클: dtime 이 있어 NameError 없이 진행하고 차트를 선적재 + info 로그"""
        self._patch_sleep(50)  # hot 장중은 50사이클(≈5분)마다 차트 선적재
        weekday_noon = datetime(2026, 9, 28, 12, 0, tzinfo=self.dm.KST)
        fake_dt = MagicMock()
        fake_dt.now.return_value = weekday_noon
        chart = AsyncMock(return_value={"success": True, "data": [{"c": 1}]})
        pos = {"data": [{"symbol": f"00{i:04d}"} for i in range(8)]}  # 8종목 → 6개만
        with patch.object(self.dm, "datetime", fake_dt), \
                patch.object(self.dm, "get_market_index", AsyncMock()), \
                patch.object(self.dm, "get_stock_positions", AsyncMock(return_value=pos)), \
                patch.object(self.dm, "get_stock_prices", AsyncMock()), \
                patch.object(self.dm, "get_chart_data", chart), \
                self.assertLogs(self.dm.logger, level="INFO") as cm:
            with self.assertRaises(_Stop):
                await self.dm._cache_warmer()
        self.assertEqual(chart.await_count, 6)
        for c in chart.await_args_list:
            self.assertEqual(c.args[1:], (90, "D"))
        out = "\n".join(cm.output)
        self.assertIn("성공 6/6종목", out)
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
