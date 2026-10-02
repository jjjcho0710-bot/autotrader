"""
[AT] dashboard/main.py._jarvis_auto_analysis: ML 예측 확률이 동전 던지기 수준이라
AUTO_ANALYSIS_ENABLED=False(기본)일 때는 계산·텔레그램 전송을 모두 건너뛴다.
"""
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "google", "google.generativeai", "aiohttp",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()


def _load_dashboard_main():
    import dashboard.main as dm
    return dm


class TestAutoAnalysisDisabledByDefault(unittest.IsolatedAsyncioTestCase):
    def test_default_is_disabled(self):
        dm = _load_dashboard_main()
        self.assertFalse(dm.AUTO_ANALYSIS_ENABLED)

    async def test_skips_db_and_telegram_when_disabled(self):
        dm = _load_dashboard_main()
        raising_pool = MagicMock()
        raising_pool.acquire.side_effect = AssertionError("db_pool에 접근하면 안 됨")
        with patch.object(dm, "AUTO_ANALYSIS_ENABLED", False), \
             patch.object(dm, "db_pool", raising_pool), \
             patch.object(dm, "_send_telegram", new=AsyncMock()) as send_mock:
            await dm._jarvis_auto_analysis()
        send_mock.assert_not_called()

    async def test_runs_when_enabled(self):
        dm = _load_dashboard_main()

        class FakeConn:
            async def fetch(self, query, *a):
                return []

        class FakeAcquire:
            async def __aenter__(self):
                return FakeConn()

            async def __aexit__(self, *exc):
                return False

        fake_pool = MagicMock()
        fake_pool.acquire.return_value = FakeAcquire()
        with patch.object(dm, "AUTO_ANALYSIS_ENABLED", True), \
             patch.object(dm, "db_pool", fake_pool), \
             patch.object(dm, "_send_telegram", new=AsyncMock()) as send_mock:
            await dm._jarvis_auto_analysis()
        # 감시 종목이 없으면 조회 직후 조용히 반환 — 적어도 db_pool엔 접근해야 한다
        fake_pool.acquire.assert_called()
        send_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
