"""
bot/telegram_bot.py 단위 테스트: 명령어 라우팅, callback_query 처리, STARK_BOT_TOKEN
우선순위 폴백을 검증한다.
"""
import asyncio
import os
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from bot import telegram_bot  # noqa: E402


class FakeConfig:
    JARVIS_ANALYST_TOKEN = "jarvis-token"
    TELEGRAM_TOKEN = "default-token"


def make_ctx(**overrides):
    calls = {"sent": [], "typing": [], "manual_collect": 0}

    async def send_telegram(text, chat_id=None, token=None):
        calls["sent"].append((text, chat_id, token))

    async def typing_action(chat_id, token=None):
        calls["typing"].append((chat_id, token))

    async def ask_openwebui(message, session_id=None):
        return f"AI: {message}"

    async def jarvis_chat(body):
        return {"reply": f"라우터응답: {body['message']}"}

    async def get_trades(limit=10):
        return {"data": []}

    def trigger_manual_collect():
        calls["manual_collect"] += 1

    defaults = dict(
        config=FakeConfig(),
        send_telegram=send_telegram,
        typing_action=typing_action,
        ask_openwebui=ask_openwebui,
        jarvis_chat=jarvis_chat,
        get_trades=get_trades,
        trigger_manual_collect=trigger_manual_collect,
        session_id="jarvis_main",
    )
    defaults.update(overrides)
    return telegram_bot.BotContext(**defaults), calls


class TestResolveToken(unittest.TestCase):
    def test_stark_bot_token_wins_when_set(self):
        os.environ["STARK_BOT_TOKEN"] = "stark-token"
        try:
            self.assertEqual(telegram_bot.resolve_token(FakeConfig()), "stark-token")
        finally:
            del os.environ["STARK_BOT_TOKEN"]

    def test_falls_back_to_jarvis_analyst_token(self):
        os.environ.pop("STARK_BOT_TOKEN", None)
        self.assertEqual(telegram_bot.resolve_token(FakeConfig()), "jarvis-token")


class TestHandleUpdate(unittest.IsolatedAsyncioTestCase):
    async def test_channel_post_is_ignored(self):
        ctx, calls = make_ctx()
        result = await telegram_bot.handle_update({"channel_post": {}}, ctx)
        self.assertEqual(result, {"ok": True})
        self.assertEqual(calls["sent"], [])

    async def test_empty_text_is_ignored(self):
        ctx, calls = make_ctx()
        body = {"message": {"chat": {"id": 1}, "text": ""}}
        result = await telegram_bot.handle_update(body, ctx)
        self.assertEqual(result, {"ok": True})
        self.assertEqual(calls["sent"], [])

    async def test_start_command_sends_intro(self):
        ctx, calls = make_ctx()
        body = {"message": {"chat": {"id": 1}, "text": "/start"}}
        await telegram_bot.handle_update(body, ctx)
        self.assertEqual(len(calls["sent"]), 1)
        self.assertIn("STARK", calls["sent"][0][0])

    async def test_history_with_no_trades(self):
        ctx, calls = make_ctx()
        body = {"message": {"chat": {"id": 1}, "text": "/history"}}
        await telegram_bot.handle_update(body, ctx)
        self.assertIn("이력 없음", calls["sent"][0][0])

    async def test_free_text_routes_through_jarvis_chat_with_channel_tag(self):
        ctx, calls = make_ctx()
        captured = {}

        async def jarvis_chat(body):
            captured.update(body)
            return {"reply": "네 알겠습니다"}

        ctx.jarvis_chat = jarvis_chat
        body = {"message": {"chat": {"id": 42}, "text": "삼성전자 어때?"}}
        await telegram_bot.handle_update(body, ctx)

        self.assertEqual(captured["channel"], "telegram")
        self.assertEqual(captured["message"], "삼성전자 어때?")
        self.assertIn("네 알겠습니다", calls["sent"][0][0])

    async def test_manual_collect_keyword_triggers_background_task(self):
        ctx, calls = make_ctx()
        body = {"message": {"chat": {"id": 1}, "text": "뉴스 수집해줘"}}
        await telegram_bot.handle_update(body, ctx)
        self.assertEqual(calls["manual_collect"], 1)


if __name__ == "__main__":
    unittest.main()
