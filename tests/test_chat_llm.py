"""
tests/test_chat_llm.py - STARK v2 LLM 순수 호출 및 프로토콜 필터링 단위 테스트
"""
import sys
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

import chat.llm
from chat.llm import ask_openwebui


class TestChatLLM(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # aiohttp mock 구성
        self.mock_aiohttp = MagicMock()
        self.orig_aiohttp = getattr(chat.llm, "aiohttp", None)
        chat.llm.aiohttp = self.mock_aiohttp

    def tearDown(self):
        chat.llm.aiohttp = self.orig_aiohttp

    @patch("chat.llm.get_chat_history", new_callable=AsyncMock)
    async def test_ask_openwebui_does_not_save_memory(self, mock_get_history):
        """
        [STARK v2 핵심 검증]
        ask_openwebui는 순수 LLM 호출만 전담하며, 내부에서 _save_chat_history를 호출하지 않음
        """
        mock_get_history.return_value = [{"role": "user", "content": "이전 대화"}]

        # Open-WebUI 응답 모의
        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "답변입니다."}}]
        }

        mock_post_cm = MagicMock()
        mock_post_cm.__aenter__.return_value = mock_response
        mock_post_cm.__aexit__.return_value = None

        mock_session = MagicMock()
        mock_session.post.return_value = mock_post_cm

        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__.return_value = mock_session
        mock_session_cm.__aexit__.return_value = None

        self.mock_aiohttp.ClientSession.return_value = mock_session_cm

        with patch.dict("os.environ", {"OPENWEBUI_API_TOKEN": "mock_token"}):
            reply = await ask_openwebui("현재 상태 알려줘", session_id="daily_plan")

        self.assertEqual(reply, "답변입니다.")

    @patch("chat.llm.get_chat_history", new_callable=AsyncMock)
    async def test_ask_openwebui_final_protocol(self, mock_get_history):
        """
        [[FINAL]] 태그가 포함된 경우 이전 사고 과정을 정상 제거하는지 검증
        """
        mock_get_history.return_value = []

        mock_response = AsyncMock()
        mock_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": "생각 중... 사용자가 물어본 것은 주가다.\n[[FINAL]]\n삼성전자 현재가는 75,000원입니다."
                    }
                }
            ]
        }

        mock_post_cm = MagicMock()
        mock_post_cm.__aenter__.return_value = mock_response
        mock_post_cm.__aexit__.return_value = None

        mock_session = MagicMock()
        mock_session.post.return_value = mock_post_cm

        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__.return_value = mock_session
        mock_session_cm.__aexit__.return_value = None

        self.mock_aiohttp.ClientSession.return_value = mock_session_cm

        with patch.dict("os.environ", {"OPENWEBUI_API_TOKEN": "mock_token"}):
            reply = await ask_openwebui("삼성전자 얼마야?", session_id="telegram")

        self.assertEqual(reply, "삼성전자 현재가는 75,000원입니다.")


if __name__ == "__main__":
    unittest.main()
