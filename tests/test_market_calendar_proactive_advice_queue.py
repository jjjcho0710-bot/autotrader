"""
tests/test_market_calendar_proactive_advice_queue.py - [AT] feat/market-calendar

dashboard/main.py _jarvis_proactive_advice()의 _market_open 계산(장 시간 즉시 실행
vs 장외 예약) 실제 호출부 검증. 휴장일(10/5 개천절 대체공휴일)에 평일·장시간 조건만
보고 "장중"으로 오판해 매도 제안을 즉시 실행해 버리던 문제를 막는다 — 휴장일에는
장이 서지 않으므로 즉시 체결 시도가 아니라 다음 개장일 예약(advice:queue)으로 가야
한다. is_trading_day() 적용을 되돌리면 이 테스트가 깨진다.
"""
import json
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


class _FrozenHolidayDatetime(_real_datetime):
    """datetime.now(KST)를 2026-10-05(월, 개천절 대체공휴일) 11:00으로 고정한다."""

    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 5, 11, 0, tzinfo=dm.KST)


class _FrozenWeekdayDatetime(_real_datetime):
    """datetime.now(KST)를 2026-10-06(화, 휴장일 아님) 11:00으로 고정한다."""

    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 6, 11, 0, tzinfo=dm.KST)


class FakeAdviceRedis:
    def __init__(self):
        self.store = {}
        self.queue = []

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, value):
        self.store[key] = value

    async def rpush(self, key, value):
        self.queue.append(value)


class TestMarketOpenHolidayAwareness(unittest.IsolatedAsyncioTestCase):
    async def _run(self, frozen_datetime):
        redis = FakeAdviceRedis()
        jarvis_chat_calls = []

        async def fake_jarvis_chat(body):
            jarvis_chat_calls.append(body["message"])
            return {"reply": f"처리됨: {body['message']}"}

        llm_output = json.dumps([
            {"title": "익절 매도", "reason": "목표가 도달", "command": "삼성전자 전량 매도"},
        ], ensure_ascii=False)

        patches = [
            patch.object(dm, "get_stock_positions", new=AsyncMock(return_value={
                "data": [{"symbol": "005930", "name": "삼성전자", "qty": 10,
                          "avg_price": 70000, "cur_price": 75000, "pnl_rate": 7.1}],
            })),
            patch.object(dm, "_analyze_chart", new=AsyncMock(return_value="[차트 요약]")),
            patch.object(dm, "_get_jarvis_knowledge", new=AsyncMock(return_value="")),
            patch.object(dm, "_get_jarvis_lessons", new=AsyncMock(return_value="")),
            patch.object(dm, "_get_active_directives", new=AsyncMock(return_value="")),
            patch.object(dm, "_get_market_index_ctx", new=AsyncMock(return_value="")),
            patch.object(dm, "redis_client", redis),
            patch.object(dm, "_ask_openwebui", new=AsyncMock(return_value=llm_output)),
            patch.object(dm, "jarvis_chat", new=fake_jarvis_chat),
            patch.object(dm, "_send_telegram", new=AsyncMock()),
            patch.object(dm, "_save_chat_history", new=AsyncMock()),
            patch.object(dm, "datetime", frozen_datetime),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        await dm._jarvis_proactive_advice("auto")
        return jarvis_chat_calls, redis.queue

    async def test_confirmed_holiday_queues_instead_of_executing_immediately(self):
        jarvis_chat_calls, queued = await self._run(_FrozenHolidayDatetime)
        self.assertEqual(jarvis_chat_calls, [])
        self.assertEqual(len(queued), 1)
        self.assertIn("삼성전자 전량 매도", queued[0])

    async def test_plain_weekday_still_executes_immediately(self):
        """휴장일 처리를 추가해도 정상 평일(장중)의 즉시 실행 동작은 그대로다(회귀 아님)."""
        jarvis_chat_calls, queued = await self._run(_FrozenWeekdayDatetime)
        self.assertIn("삼성전자 전량 매도", jarvis_chat_calls)
        self.assertEqual(queued, [])


if __name__ == "__main__":
    unittest.main()
