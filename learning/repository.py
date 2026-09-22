"""
learning/repository.py - STARK v2 학습 원문 및 원칙 CRUD 리포지토리

연계 테이블:
- learning_sources (V005): 자막 및 웹 본문 전문 영구 보존
- learning_rules (V005): 추출된 원칙 구조화 저장 (source_id FK)
"""
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger("learning.repository")


async def save_learning_source(
    source_type: str,
    url: str,
    content_raw: str,
    title: str = "",
    author: str = "",
    hint: str = "",
    pool=None,
) -> Optional[int]:
    """
    학습 원문 전문(자막/본문)을 learning_sources 테이블에 영구 저장.
    저장 성공 시 source_id를 반환한다.
    """
    if not pool:
        logger.warning("DB pool 없음: learning_sources 저장 스킵")
        return None

    try:
        async with pool.acquire() as conn:
            source_id = await conn.fetchval(
                """
                INSERT INTO learning_sources (source_type, url, title, author, content_raw, hint, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, NOW())
                RETURNING source_id
                """,
                source_type[:20],
                url[:1000],
                title[:255] if title else None,
                author[:100] if author else None,
                content_raw,
                hint[:255] if hint else None,
            )
            return source_id
    except Exception as e:
        logger.error(f"learning_sources 저장 실패: {e}")
        return None


async def save_learning_rules(
    source_id: int,
    rules: List[str],
    importance_score: int = 1,
    pool=None,
) -> List[int]:
    """
    추출된 원칙 목록을 learning_rules 테이블에 저장 (source_id FK 연계).
    생성된 rule_id 리스트를 반환한다.
    """
    if not pool or not source_id or not rules:
        return []

    created_ids = []
    try:
        async with pool.acquire() as conn:
            for rule_text in rules:
                rule_text = rule_text.strip()
                if not rule_text:
                    continue
                rule_id = await conn.fetchval(
                    """
                    INSERT INTO learning_rules (source_id, rule_text, importance_score, is_active, created_at)
                    VALUES ($1, $2, $3, TRUE, NOW())
                    RETURNING rule_id
                    """,
                    source_id,
                    rule_text,
                    importance_score,
                )
                if rule_id:
                    created_ids.append(rule_id)
        return created_ids
    except Exception as e:
        logger.error(f"learning_rules 저장 실패: {e}")
        return created_ids


async def get_active_rules(limit: int = 20, pool=None) -> List[Dict[str, Any]]:
    """
    활성화된 학습 원칙 목록 조회 (소스 메타데이터 JOIN)
    """
    if not pool:
        return []

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT r.rule_id, r.source_id, r.rule_text, r.importance_score, r.created_at,
                       s.title, s.url, s.source_type
                FROM learning_rules r
                JOIN learning_sources s ON r.source_id = s.source_id
                WHERE r.is_active = TRUE
                ORDER BY r.created_at DESC
                LIMIT $1
                """,
                limit,
            )
            return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"active learning_rules 조회 실패: {e}")
        return []


async def search_learning_sources(query: str, limit: int = 5, pool=None) -> List[Dict[str, Any]]:
    """
    학습 원문 전문 및 메타데이터 검색
    """
    if not pool or not query:
        return []

    pattern = f"%{query.strip()}%"
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT source_id, source_type, url, title, author, hint, created_at,
                       SUBSTRING(content_raw FROM 1 FOR 300) AS snippet
                FROM learning_sources
                WHERE title ILIKE $1 OR content_raw ILIKE $1 OR hint ILIKE $1
                ORDER BY created_at DESC
                LIMIT $2
                """,
                pattern,
                limit,
            )
            return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"learning_sources 검색 실패: {e}")
        return []
