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


async def get_knowledge_entries(limit: int = 20, pool=None) -> List[Dict[str, Any]]:
    """
    대화("지식 보여줘" 등)에서 쓰는 지식 목록 — 구조화 규칙(learning_rules)과
    레거시 메모(jarvis_notes, category='knowledge')를 하나로 합쳐 최신순으로 반환한다.

    표시용 id 형식은 curator.get_jarvis_knowledge와 동일하게 규칙은 'R{rule_id}',
    레거시 메모는 '#{id}'를 쓴다 — 삭제 시(deactivate_knowledge_entry) 이 접두사로
    어느 테이블을 지울지 구분한다.
    """
    if not pool:
        return []
    entries: List[Dict[str, Any]] = []
    try:
        async with pool.acquire() as conn:
            rules = await conn.fetch(
                """
                SELECT rule_id, rule_text, created_at FROM learning_rules
                WHERE is_active = TRUE ORDER BY created_at DESC LIMIT $1
                """,
                limit,
            )
            for r in rules:
                entries.append({
                    "display_id": f"R{r['rule_id']}", "kind": "rule",
                    "id": r["rule_id"], "text": r["rule_text"], "created_at": r["created_at"],
                })

            notes = await conn.fetch(
                """
                SELECT id, content, created_at FROM jarvis_notes
                WHERE category='knowledge' AND is_active=TRUE
                ORDER BY created_at DESC LIMIT $1
                """,
                limit,
            )
            for n in notes:
                entries.append({
                    "display_id": f"#{n['id']}", "kind": "note",
                    "id": n["id"], "text": n["content"], "created_at": n["created_at"],
                })
    except Exception as e:
        logger.error(f"지식 목록 통합 조회 실패: {e}")
        return entries

    entries.sort(key=lambda e: e["created_at"] or 0, reverse=True)
    return entries[:limit]


async def deactivate_knowledge_entry(display_id: str, pool=None) -> bool:
    """
    get_knowledge_entries가 내려준 표시용 id('R12' 또는 '#12'/'12')로 지식 항목 하나를
    비활성화한다. 'R' 접두사면 learning_rules, 아니면 jarvis_notes(knowledge)를 지운다.
    실제로 한 건이 지워졌을 때만 True.
    """
    if not pool or not display_id:
        return False
    display_id = display_id.strip()
    try:
        async with pool.acquire() as conn:
            if display_id.upper().startswith("R"):
                rule_id = int(display_id[1:])
                result = await conn.execute(
                    "UPDATE learning_rules SET is_active=FALSE WHERE rule_id=$1 AND is_active=TRUE",
                    rule_id,
                )
            else:
                note_id = int(display_id.lstrip("#"))
                result = await conn.execute(
                    "UPDATE jarvis_notes SET is_active=FALSE WHERE id=$1 AND category='knowledge' AND is_active=TRUE",
                    note_id,
                )
        return isinstance(result, str) and not result.endswith(" 0")
    except Exception as e:
        logger.error(f"지식 항목 비활성화 실패({display_id}): {e}")
        return False


async def get_learning_sources(limit: int = 50, pool=None) -> List[Dict[str, Any]]:
    """학습 소스 목록 (추출된 활성 원칙 개수 포함) 조회"""
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT s.source_id, s.source_type, s.url, s.title, s.author, s.created_at,
                       COUNT(r.rule_id) FILTER (WHERE r.is_active) AS rule_count
                FROM learning_sources s
                LEFT JOIN learning_rules r ON r.source_id = s.source_id
                GROUP BY s.source_id
                ORDER BY s.created_at DESC
                LIMIT $1
                """,
                limit,
            )
            return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"learning_sources 목록 조회 실패: {e}")
        return []


async def get_learning_source_detail(source_id: int, pool=None) -> Optional[Dict[str, Any]]:
    """학습 소스 원문 전문 + 메타데이터 단건 조회"""
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM learning_sources WHERE source_id=$1", source_id
            )
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"learning_source 상세 조회 실패({source_id}): {e}")
        return None


async def get_rules_by_source(source_id: int, pool=None) -> List[Dict[str, Any]]:
    """특정 소스에서 추출된 활성 원칙 목록"""
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT rule_id, rule_text, importance_score, created_at
                FROM learning_rules
                WHERE source_id=$1 AND is_active=TRUE
                ORDER BY created_at DESC
                """,
                source_id,
            )
            return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"소스별 원칙 조회 실패({source_id}): {e}")
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
