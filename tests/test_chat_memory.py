"""
tests/test_chat_memory.py - STARK v2 대화 메모리 및 순수 발화 격리 단위 테스트
"""
import unittest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone, timedelta

from chat.memory import (
    save_chat_history,
    get_chat_history,
    summarize_old_chats,
    V006_MIGRATION_CUTOFF,
)


class TestChatMemory(unittest.IsolatedAsyncioTestCase):
    async def test_save_chat_history_db_and_redis(self):
        """DB와 Redis에 정상 저장되는지 검증"""
        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        mock_redis = AsyncMock()
        mock_redis.get.return_value = None

        await save_chat_history(
            chat_id="test_session",
            role="user",
            content="삼성전자 매수해줘",
            channel="telegram",
            is_pure_user=True,
            pool=mock_pool,
            redis=mock_redis,
        )

        # 1. DB INSERT 확인
        mock_conn.execute.assert_called_once()
        args = mock_conn.execute.call_args[0]
        self.assertIn("INSERT INTO jarvis_memory", args[0])
        self.assertEqual(args[1], "test_session")
        self.assertEqual(args[2], "user")
        self.assertEqual(args[3], "삼성전자 매수해줘")
        self.assertEqual(args[4], "telegram")
        self.assertTrue(args[5])  # is_pure_user

        # 2. Redis setex 확인
        mock_redis.setex.assert_called_once()
        redis_args = mock_redis.setex.call_args[0]
        self.assertEqual(redis_args[0], "jarvis:history:test_session")
        self.assertEqual(redis_args[1], 604800)
        self.assertIn("삼성전자 매수해줘", redis_args[2])

    async def test_get_chat_history_pure_user_filter(self):
        """
        [PM 핵심 지시사항 검증]
        only_pure_user=True 시, V006 마이그레이션(2026-09-22 05:44:00 UTC) 이전 데이터는
        is_pure_user=TRUE라도 배제하고, 마이그레이션 이후 신규 순수 발화만 조회하는지 검증
        """
        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        # DB 반환 모의 데이터 (마이그레이션 이후 신규 발화 2건)
        mock_conn.fetch.return_value = [
            {"role": "user", "content": "신규 순수 발화 2"},
            {"role": "user", "content": "신규 순수 발화 1"},
        ]

        history = await get_chat_history(
            chat_id="test_session",
            max_turns=5,
            only_pure_user=True,
            pool=mock_pool,
            redis=None,
        )

        # DB 쿼리에 cutoff 시점이 정상 바인딩되었는지 확인
        mock_conn.fetch.assert_called_once()
        query, session_id, cutoff, limit = mock_conn.fetch.call_args[0]
        self.assertIn("created_at >= $2", query)
        self.assertIn("is_pure_user = TRUE", query)
        self.assertEqual(session_id, "test_session")
        self.assertEqual(cutoff, V006_MIGRATION_CUTOFF)
        self.assertEqual(limit, 10)

        # 결과가 시간순(reversed)으로 정렬되었는지 확인
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["content"], "신규 순수 발화 1")
        self.assertEqual(history[1]["content"], "신규 순수 발화 2")

    async def test_summarize_old_chats_retains_raw_memory(self):
        """
        [STARK v2 원칙 검증]
        7일 지난 대화를 요약하되 jarvis_memory 원문 DELETE는 실행하지 않고 보존함
        """
        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        mock_conn.fetchval.return_value = "2026-09-10"
        mock_conn.fetch.return_value = [
            {"role": "user", "content": "전략 회의"},
            {"role": "assistant", "content": "손절 -7% 준수"},
        ]

        mock_llm = AsyncMock(return_value="2026-09-10 회의 요약 완료")

        await summarize_old_chats(pool=mock_pool, ask_llm_fn=mock_llm)

        # 요약이 jarvis_notes에 INSERT 되었는지 확인
        executed_statements = [call[0][0] for call in mock_conn.execute.call_args_list]
        self.assertTrue(any("INSERT INTO jarvis_notes" in stmt for stmt in executed_statements))
        # [핵심] DELETE FROM jarvis_memory가 호출되지 않았는지 확인
        self.assertFalse(any("DELETE FROM jarvis_memory" in stmt for stmt in executed_statements))


if __name__ == "__main__":
    unittest.main()
