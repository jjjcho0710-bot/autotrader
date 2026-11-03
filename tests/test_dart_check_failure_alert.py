"""[AT] fix/dockerfile-data-collector — _morning_routine의 DART 공시 확인 실패를 하루 1회 텔레그램으로 알린다."""
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
for p in (REPO_ROOT, REPO_ROOT / "stock_trader"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))
for mod_name in ["fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
                 "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai"]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()


class _Acq:
    async def __aenter__(self):
        c = MagicMock()
        c.fetch = AsyncMock(return_value=[{"symbol": "005930"}])
        return c

    async def __aexit__(self, *a):
        return False


class _Pool:
    def acquire(self):
        return _Acq()


class TestDartFailureAlert(unittest.IsolatedAsyncioTestCase):
    def _patches(self, dm, sent, gate):
        async def tg(text, **kw):
            sent.append(text)
        return [patch.object(dm, n, AsyncMock()) for n in
                ("_jarvis_stock_scanner", "_jarvis_auto_analysis", "_jarvis_daily_plan")] + [
            patch.object(dm, "_manual_collect", AsyncMock()),
            patch.object(dm, "_trigger_ohlcv_collect", AsyncMock()),
            patch.object(dm, "db_pool", _Pool()),
            patch.object(dm, "_send_telegram", tg),
            patch.object(dm, "_daily_gate", gate),
        ]

    async def _run(self, gate_result):
        import dashboard.main as dm
        sent = []
        gate = AsyncMock(return_value=gate_result)
        ps = self._patches(dm, sent, gate)
        ps.append(patch("data_collector.collectors.dart_collector.DARTCollector.collect_and_alert",
                        AsyncMock(side_effect=ImportError("No module named 'data_collector'"))))
        for p in ps:
            p.start()
        try:
            await dm._morning_routine()
        finally:
            for p in ps:
                p.stop()
        return sent, gate

    async def test_failure_sends_one_alert_when_gate_open(self):
        sent, gate = await self._run(True)
        self.assertEqual(len(sent), 1)
        self.assertIn("공시 확인 실패", sent[0])
        self.assertIn("ImportError", sent[0])
        self.assertEqual(gate.await_args.args[0], "dart_check_fail_alert")

    async def test_second_failure_same_day_is_silent(self):
        sent, _ = await self._run(False)
        self.assertEqual(sent, [])

    async def test_alert_send_failure_does_not_break_routine(self):
        import dashboard.main as dm
        gate = AsyncMock(return_value=True)
        boom = AsyncMock(side_effect=RuntimeError("telegram down"))
        ps = self._patches(dm, [], gate)
        ps[6] = patch.object(dm, "_send_telegram", boom)
        ps.append(patch("data_collector.collectors.dart_collector.DARTCollector.collect_and_alert",
                        AsyncMock(side_effect=RuntimeError("x"))))
        for p in ps:
            p.start()
        try:
            await dm._morning_routine()  # 예외 없이 끝나야 한다
        finally:
            for p in ps:
                p.stop()


if __name__ == "__main__":
    unittest.main()
