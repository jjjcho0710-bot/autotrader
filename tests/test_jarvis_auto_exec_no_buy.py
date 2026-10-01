"""
dashboard/main.py _jarvis_proactive_advice / _run_advice_queue 단위 테스트
([AT] buy-gate-unification).

배경: AI 자동 실행(_jarvis_proactive_advice)이 신호 경로(stark/execution_guard)를 거치지
않고 근거 없는 "ML 100%" 문구로 신규 매수 3건을 시도한 사고(10/1)가 있었다. PM 정책:
AI 자동 실행은 매수를 절대 하지 않는다(매도·익절·손절 조정·지시 저장만 허용). 이 테스트는
1. LLM이 매수 command를 제안해도 검증 단계에서 거부되고(로그만 남기고) 다른 유효한
   제안(매도)은 정상 실행되는지,
2. 이 fix 이전에 이미 장외 예약 큐(redis "advice:queue")에 쌓여 있던 매수 command도
   재생 시(_run_advice_queue) 건너뛰는지
를 검증한다.
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

import json  # noqa: E402
from datetime import datetime as _real_datetime  # noqa: E402

import dashboard.main as dm  # noqa: E402


class _FrozenDatetime(_real_datetime):
    """datetime.now(KST)만 장중(11:00) 시각으로 고정해 즉시 실행 분기를 타게 한다."""

    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 1, 11, 0, tzinfo=dm.KST)


class FakeAdviceRedis:
    """advice:recent_titles(get/setex)·advice:queue(rpush/lpop) 전용 최소 더블."""

    def __init__(self, queue=None):
        self.store = {}
        self.queue = list(queue or [])
        self.rpushed = []

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, value):
        self.store[key] = value

    async def rpush(self, key, value):
        self.rpushed.append((key, value))
        self.queue.append(value)

    async def lpop(self, key):
        return self.queue.pop(0) if self.queue else None


class TestProactiveAdviceRejectsBuy(unittest.IsolatedAsyncioTestCase):
    async def test_buy_command_rejected_but_sell_command_still_executes(self):
        """LLM이 매수+매도 두 건을 제안하면, 매수는 거부(로그만)되고 매도만 실행된다."""
        redis = FakeAdviceRedis()
        jarvis_chat_calls = []

        async def fake_jarvis_chat(body):
            jarvis_chat_calls.append(body["message"])
            return {"reply": f"처리됨: {body['message']}"}

        llm_output = json.dumps([
            {"title": "근거 없는 매수", "reason": "ML 100%", "command": "삼성전자 1주 매수"},
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
            # 장중으로 고정 — 큐잉이 아니라 즉시 실행 분기를 타게 한다.
            patch.object(dm, "datetime", _FrozenDatetime),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        msg = await dm._jarvis_proactive_advice("auto")

        # 매수 command는 어떤 경로로도 jarvis_chat에 전달되지 않는다.
        self.assertFalse(any("매수" in c for c in jarvis_chat_calls),
                          f"매수 command가 실행 경로로 전달됨: {jarvis_chat_calls}")
        # 매도 제안은 정상적으로 실행된다.
        self.assertIn("삼성전자 전량 매도", jarvis_chat_calls)
        self.assertIn("삼성전자 전량 매도", msg)
        self.assertNotIn("매수", msg)


class TestAdviceQueueSkipsBuy(unittest.IsolatedAsyncioTestCase):
    async def test_queue_replay_skips_buy_command_but_runs_others(self):
        """이 fix 이전에 이미 advice:queue에 쌓여 있던 매수 command도 재생 시 건너뛴다."""
        queued = [
            json.dumps({"command": "삼성전자 1주 매수", "title": "근거 없는 매수"}, ensure_ascii=False),
            json.dumps({"command": "삼성전자 전량 매도", "title": "익절 매도"}, ensure_ascii=False),
        ]
        redis = FakeAdviceRedis(queue=queued)
        jarvis_chat_calls = []

        async def fake_jarvis_chat(body):
            jarvis_chat_calls.append(body["message"])
            return {"reply": f"처리됨: {body['message']}"}

        patches = [
            patch.object(dm, "redis_client", redis),
            patch.object(dm, "jarvis_chat", new=fake_jarvis_chat),
            patch.object(dm, "_send_telegram", new=AsyncMock()),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        await dm._run_advice_queue()

        self.assertFalse(any("매수" in c for c in jarvis_chat_calls),
                          f"큐에 쌓여 있던 매수 command가 재생됨: {jarvis_chat_calls}")
        self.assertEqual(jarvis_chat_calls, ["삼성전자 전량 매도"])
        self.assertEqual(redis.queue, [])  # 큐는 끝까지 비워져야 함(매수도 소비는 됨, 실행만 안 함)


if __name__ == "__main__":
    unittest.main()
