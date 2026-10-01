"""
chat/memory.py - STARK v2 대화 메모리 관리 모듈

기능:
1. save_chat_history: 대화 히스토리 영구 저장(PostgreSQL) 및 최근 캐시(Redis)
   - V006 컬럼(channel, is_pure_user) 지원
2. get_chat_history: 대화 히스토리 조회
   - only_pure_user=True 시, V006 마이그레이션(2026-09-22 05:44:00 UTC) 이후 생성된
     신규 순수 발화(is_pure_user=TRUE)만 신뢰하여 조회(과거 오염 데이터 배제)
3. summarize_old_chats: 7일 지난 대화 일 단위 요약 후 jarvis_notes에 기록
   - STARK v2 원칙에 따라 jarvis_memory 원문 DELETE는 수행하지 않고 영구 보존
"""
import json
import logging
import re
from datetime import datetime, timedelta, timezone

logger = logging.getLogger("chat.memory")

_KST = timezone(timedelta(hours=9))

# 신뢰 가능한 매매 데이터 기준선 (data_baseline 테이블 'reliable_trading_data_from' 행, V009).
# 테이블이 없거나 조회에 실패하면 V009가 넣는 값과 같은 이 상수로 폴백한다.
RELIABLE_BASELINE_KEY = "reliable_trading_data_from"
RELIABLE_BASELINE_FALLBACK = datetime(2026, 9, 28, 11, 0, 0, tzinfo=_KST)

# _save_trade_memory(dashboard/main.py)가 jarvis_memory에 남기는 행: "[매매기록 2026-09-10 13:05] ..."
_TRADE_MEMORY_RE = re.compile(r"^\s*\[매매기록 (\d{4}-\d{2}-\d{2} \d{2}:\d{2})\]")
# 대화 요약(jarvis_notes chat_summary)의 접두어: "[2026-09-10] ..."
_SUMMARY_DAY_RE = re.compile(r"^\s*\[(\d{4}-\d{2}-\d{2})\]")


async def get_reliable_baseline(pool=None) -> datetime:
    """data_baseline.reliable_trading_data_from 의 effective_at. 조회 실패/행 없음이면 상수 폴백."""
    if pool:
        try:
            async with pool.acquire() as conn:
                val = await conn.fetchval(
                    "SELECT effective_at FROM data_baseline WHERE key = $1", RELIABLE_BASELINE_KEY)
            if isinstance(val, datetime):
                return val if val.tzinfo else val.replace(tzinfo=_KST)
        except Exception as e:
            logger.debug(f"data_baseline 조회 실패 — 폴백 사용: {e}")
    return RELIABLE_BASELINE_FALLBACK


def is_pre_baseline_trade_memory(content, since: datetime) -> bool:
    """'[매매기록 ...]' 행이고 기록 시각이 기준선 이전이면 True (시각을 못 읽으면 보수적으로 True)"""
    m = _TRADE_MEMORY_RE.match(content or "")
    if not m:
        return False
    try:
        ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M").replace(tzinfo=_KST)
    except ValueError:
        return True
    return ts < since


def filter_trade_memory(items: list, since) -> list:
    """대화 히스토리에서 기준선 이전 매매기록 행 제거 (since=None이면 그대로)"""
    if since is None:
        return items
    return [it for it in items if not is_pre_baseline_trade_memory(it.get("content"), since)]


def is_summary_before_baseline(content, since: datetime) -> bool:
    """대화 요약이 기준선 당일 이전 하루치면 True. 기준선 당일 요약도 기준선 이전 시간대가 섞여 제외한다.
    날짜 접두어를 못 읽으면 보수적으로 True."""
    m = _SUMMARY_DAY_RE.match(content or "")
    if not m:
        return True
    return m.group(1) <= since.astimezone(_KST).strftime("%Y-%m-%d")

# V006 마이그레이션 완료 시점 (과거 행은 is_pure_user=TRUE로 채워졌으나 오염 데이터임)
# 이 시점 이후 생성된 행만 is_pure_user=TRUE 플래그를 신뢰한다.
V006_MIGRATION_CUTOFF = datetime(2026, 9, 22, 5, 44, 0, tzinfo=timezone.utc)


async def save_chat_history(
    chat_id: str,
    role: str,
    content: str,
    channel: str = "web",
    is_pure_user: bool = True,
    pool=None,
    redis=None,
):
    """
    대화 히스토리 저장 — PostgreSQL(영구) + Redis(캐시)
    """
    # 1. PostgreSQL 영구 저장
    if pool:
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO jarvis_memory (session_id, role, content, channel, is_pure_user, created_at)
                    VALUES ($1, $2, $3, $4, $5, NOW())
                    """,
                    chat_id,
                    role,
                    content,
                    channel,
                    is_pure_user,
                )
        except Exception as e:
            logger.debug(f"메모리 DB 저장 실패: {e}")

    # 2. Redis 캐시 (최근 40턴, 빠른 조회용)
    if redis:
        try:
            key = f"jarvis:history:{chat_id}"
            raw = await redis.get(key)
            history = json.loads(raw if isinstance(raw, str) else raw.decode()) if raw else []
            history.append({
                "role": role,
                "content": content,
                "channel": channel,
                "is_pure_user": is_pure_user,
            })
            if len(history) > 40:
                history = history[-40:]
            await redis.setex(key, 604800, json.dumps(history, ensure_ascii=False))  # 7일 보관
        except Exception as e:
            logger.warning(f"히스토리 Redis 저장 실패: {e}")


async def get_chat_history(
    chat_id: str,
    max_turns: int = 8,
    only_pure_user: bool = False,
    pool=None,
    redis=None,
    trade_memory_since=None,
) -> list:
    """
    대화 히스토리 로드 — 순수 사용자 발화 격리 및 Redis/DB 조회

    :param chat_id: 세션/채팅 ID
    :param max_turns: 최대 대화 턴 수 (1턴 = 2메시지 기준)
    :param only_pure_user: True일 경우 V006 마이그레이션 이후 생성된 신규 순수 발화만 반환
    :param pool: DB 커넥션 풀
    :param redis: Redis 클라이언트
    :param trade_memory_since: 주어지면 이 시각 이전의 '[매매기록 ...]' 행을 결과에서 제외
    :return: [{"role": ..., "content": ...}, ...]
    """
    if trade_memory_since is not None:
        history = await get_chat_history(
            chat_id, max_turns=max_turns, only_pure_user=only_pure_user, pool=pool, redis=redis)
        return filter_trade_memory(history, trade_memory_since)

    limit_count = max_turns * 2

    # 순수 발화 필터링이 필요한 경우: 과거 오염 데이터 배제하고 마이그레이션 이후 신규 행만 신뢰
    if only_pure_user:
        if pool:
            try:
                async with pool.acquire() as conn:
                    rows = await conn.fetch(
                        """
                        SELECT role, content FROM jarvis_memory
                        WHERE session_id = $1
                          AND is_pure_user = TRUE
                          AND created_at >= $2
                        ORDER BY created_at DESC
                        LIMIT $3
                        """,
                        chat_id,
                        V006_MIGRATION_CUTOFF,
                        limit_count,
                    )
                return [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]
            except Exception as e:
                logger.debug(f"순수 사용자 히스토리 DB 로드 실패: {e}")
        return []

    # 일반 대화 히스토리: Redis 캐시 우선
    if redis:
        try:
            key = f"jarvis:history:{chat_id}"
            raw = await redis.get(key)
            if raw:
                data = json.loads(raw if isinstance(raw, str) else raw.decode())
                # role, content만 추출
                clean_history = [{"role": item["role"], "content": item["content"]} for item in data]
                return clean_history[-limit_count:]
        except Exception as e:
            logger.debug(f"Redis 히스토리 로드 예외: {e}")

    # Redis 미사용 또는 캐시 미존재 시 DB 조회
    if pool:
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT role, content FROM jarvis_memory
                    WHERE session_id = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                    """,
                    chat_id,
                    limit_count,
                )
            history = [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]
            # Redis에 캐시 복원
            if redis and history:
                key = f"jarvis:history:{chat_id}"
                await redis.setex(key, 604800, json.dumps(history, ensure_ascii=False))
            return history
        except Exception as e:
            logger.debug(f"PostgreSQL 히스토리 로드 실패: {e}")

    return []


async def summarize_old_chats(pool=None, ask_llm_fn=None):
    """
    7일 지난 대화를 일 단위로 요약해 장기 기억(jarvis_notes)으로 이관.
    STARK v2 원칙: 원문(jarvis_memory)은 삭제(DELETE)하지 않고 영구 보존한다.
    """
    if not pool:
        return

    try:
        async with pool.acquire() as conn:
            day = await conn.fetchval(
                """
                SELECT DATE(created_at AT TIME ZONE 'Asia/Seoul') FROM jarvis_memory
                WHERE created_at < NOW() - INTERVAL '7 days'
                ORDER BY created_at LIMIT 1
                """
            )
            if not day:
                return
            rows = await conn.fetch(
                """
                SELECT role, content FROM jarvis_memory
                WHERE DATE(created_at AT TIME ZONE 'Asia/Seoul') = $1
                ORDER BY created_at LIMIT 200
                """,
                day,
            )
        if not rows:
            return

        convo = "\n".join(
            f"{'주인' if r['role'] == 'user' else '한강뷰매니저'}: {r['content'][:200]}"
            for r in rows
        )[:6000]

        summary_prompt = (
            f"다음은 {day} 하루의 주인-한강뷰매니저 대화다. 나중에 참조할 핵심(결정사항, 지시, 전략 논의, 중요 사실)만 "
            f"500자 이내로 요약하라. 잡담은 제외.\n\n{convo}"
        )

        summary = ""
        if ask_llm_fn:
            summary = await ask_llm_fn(summary_prompt, session_id="summarizer")

        if summary and not summary.startswith("❌"):
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO jarvis_notes (category, content) VALUES ('chat_summary', $1)",
                    f"[{day}] {summary.strip()[:600]}",
                )
                # STARK v2: DELETE FROM jarvis_memory 로직은 제거 (원문 보존)
            logger.info(f"🧠 대화 요약 이관 완료: {day} (원문 보존 유지)")
    except Exception as e:
        logger.error(f"대화 요약 오류: {e}")
