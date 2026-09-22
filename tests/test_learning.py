"""
tests/test_learning.py - STARK v2 학습 원문 전문 보존 및 원칙 추출 파이프라인 단위 테스트
"""
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

from learning.repository import (
    save_learning_source,
    save_learning_rules,
    get_active_rules,
    search_learning_sources,
)
from learning.collector import learn_from_url
from learning.curator import get_jarvis_knowledge


class TestLearningPipeline(unittest.IsolatedAsyncioTestCase):
    async def test_save_learning_source_and_rules(self):
        """learning_sources 및 learning_rules CRUD 검증"""
        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        mock_conn.fetchval.side_effect = [101, 1001, 1002]

        # 1. 원문 전문 저장
        source_id = await save_learning_source(
            source_type="YOUTUBE",
            url="https://youtube.com/watch?v=abcd1234efg",
            content_raw="이것은 유튜브 자막 전문입니다. 아주 길고 상세한 내용이 들어있습니다.",
            title="워뇨띠 매매법 분석",
            author="주식초보",
            hint="손절 라인 위주",
            pool=mock_pool,
        )
        self.assertEqual(source_id, 101)

        # 2. 파생 원칙 저장 (source_id FK)
        rules = ["원칙: 손절선은 -7% 엄격 준수", "원칙: 거래량 폭증 시에만 분할 진입"]
        rule_ids = await save_learning_rules(
            source_id=source_id,
            rules=rules,
            importance_score=5,
            pool=mock_pool,
        )
        self.assertEqual(rule_ids, [1001, 1002])

    async def test_get_active_rules_and_search(self):
        """활성 원칙 조회 및 전문 검색 인터페이스 검증"""
        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        mock_conn.fetch.return_value = [
            {
                "rule_id": 1,
                "source_id": 10,
                "rule_text": "원칙: 분할 매수 3단계",
                "importance_score": 3,
                "created_at": "2026-09-22",
                "title": "단타의 신",
                "url": "https://example.com/art1",
                "source_type": "WEB_ARTICLE",
            }
        ]

        active_rules = await get_active_rules(limit=5, pool=mock_pool)
        self.assertEqual(len(active_rules), 1)
        self.assertEqual(active_rules[0]["rule_text"], "원칙: 분할 매수 3단계")

        # 전문 검색
        mock_conn.fetch.return_value = [
            {
                "source_id": 10,
                "source_type": "WEB_ARTICLE",
                "url": "https://example.com/art1",
                "title": "단타의 신",
                "author": "홍길동",
                "hint": "단타",
                "created_at": "2026-09-22",
                "snippet": "단타 매매에서 가장 중요한 것은...",
            }
        ]
        results = await search_learning_sources("단타", limit=5, pool=mock_pool)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["title"], "단타의 신")

    @patch("learning.collector.fetch_learning_text", new_callable=AsyncMock)
    async def test_learn_from_url_saves_raw_content(self, mock_fetch):
        """
        [STARK v2 핵심 검증]
        learn_from_url 실행 시 원문 전문이 learning_sources에 저장되고
        LLM 추출 원칙이 learning_rules에 저장되는 파이프라인 검증
        """
        raw_article = "전설의 트레이더 인터뷰: 첫째, 손절은 목숨이다. 둘째, 추세를 거스르지 마라." * 20
        mock_fetch.return_value = ("트레이더 인터뷰", raw_article)

        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        mock_conn.fetchval.side_effect = [55, 501, 502]

        llm_reply = "원칙: 손절은 목숨처럼 지켜라\n원칙: 추세를 거스르지 않는 매매를 하라"
        mock_llm = AsyncMock(return_value=llm_reply)

        result = await learn_from_url(
            url="https://finance.naver.com/news/123",
            hint="손절 중심",
            pool=mock_pool,
            ask_llm_fn=mock_llm,
        )

        self.assertIn("학습 완료", result)
        self.assertIn("지식 2건 저장", result)

        # 원문 전문 저장이 호출되었는지 확인
        executed_stmts = [call[0][0] for call in mock_conn.fetchval.call_args_list]
        self.assertTrue(any("INSERT INTO learning_sources" in stmt for stmt in executed_stmts))
        self.assertTrue(any("INSERT INTO learning_rules" in stmt for stmt in executed_stmts))

    async def test_get_jarvis_knowledge_prefers_learning_rules(self):
        """get_jarvis_knowledge가 신규 learning_rules를 우선 반환하는지 검증"""
        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        # learning_rules에 활성 규칙이 있는 경우
        mock_conn.fetch.side_effect = [
            [{"rule_id": 99, "rule_text": "원칙: 뇌동매매 절대 금지"}],
        ]

        res = await get_jarvis_knowledge(limit=5, pool=mock_pool)
        self.assertIn("- R99 원칙: 뇌동매매 절대 금지", res)


if __name__ == "__main__":
    unittest.main()
