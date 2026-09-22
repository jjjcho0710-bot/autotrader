"""
STARK v2 판단 로그 — stark_decisions 테이블 적재/조회.

docs/STARK_PLAN.md 2번·6번 원칙:
  - "판단(AI)과 실행(룰)을 코드 레벨로 분리" — 판단 레이어는 반드시 실제 호출 로그를 남긴다.
  - "모든 사이클은 무언가를 기록한다" — 매수/매도/보류(SKIP) 어느 쪽이든, 심지어
    모델 호출 자체가 실패한 경우까지도 사유와 함께 기록해서 "0건인 날"의 원인을
    사후에 조회할 수 있어야 한다.

DB 접근 방식은 이 저장소의 기존 관례를 그대로 따른다: asyncpg 커넥션 풀을
`pool.acquire()` 비동기 컨텍스트 매니저로 열어 쓰는 방식이며, 특정 전역 풀에
의존하지 않고 pool을 인자로 주입받는다 (ml/model.py `MLModelManager`가
`db_pool`을 생성자에서 주입받는 것과 동일한 패턴). 이 모듈은 asyncpg를 직접
import하지 않고 pool을 덕타이핑으로만 사용하므로, 호출부(dashboard/main.py의
`db_pool`, common/database.py의 `db.pool`, stock_trader/main.py 등)가 이미
들고 있는 풀을 그대로 넘기면 된다.

스키마 정본: migrations/V003__create_stark_decisions.sql
"""
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


async def log_decision(
    pool: Any,
    symbol: str,
    decision: str,
    *,
    name: Optional[str] = None,
    confidence: Optional[float] = None,
    reason: Optional[str] = None,
    rationale: Optional[str] = None,
    strategy: Optional[str] = None,
    source: str = "ai",
    model_name: Optional[str] = None,
    executed: bool = False,
    order_success: Optional[bool] = None,
    price: Optional[float] = None,
    quantity: Optional[float] = None,
) -> Optional[int]:
    """STARK 판단 1건을 stark_decisions에 기록.

    decision은 자유 문자열이지만 관례상 BUY/SELL/HOLD/HALF/ALL/SKIP 등을 사용한다.
    판단이 SKIP/HOLD로 끝나거나 모델 호출 자체가 실패한 경우에도 반드시 호출해서
    사유(reason)와 함께 기록을 남겨야 한다 — 침묵 금지.

    반환값: 삽입된 행의 id. 실패 시 None (예외를 삼키고 로그만 남김 — 판단 로그
    기록 실패가 실제 매매 실행 흐름을 막아서는 안 된다).
    """
    if pool is None:
        logger.error(f"stark_decisions 기록 실패({symbol}): DB pool이 없음")
        return None
    try:
        async with pool.acquire() as conn:
            decision_id = await conn.fetchval(
                """
                INSERT INTO stark_decisions
                    (symbol, name, decision, confidence, reason, rationale,
                     strategy, source, model_name, executed, order_success,
                     price, quantity)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                RETURNING id
                """,
                symbol, name, decision, confidence, reason, rationale,
                strategy, source, model_name, executed, order_success,
                price, quantity,
            )
        return decision_id
    except Exception as e:
        logger.error(f"stark_decisions 기록 실패({symbol}, {decision}): {e}")
        return None


async def get_recent_decisions(pool: Any, limit: int = 20) -> List[Dict[str, Any]]:
    """가장 최근 판단 N건 조회 (판단시각 역순)."""
    if pool is None:
        return []
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM stark_decisions ORDER BY decided_at DESC LIMIT $1",
                limit,
            )
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"최근 판단 조회 실패: {e}")
        return []


async def get_decisions_by_symbol(
    pool: Any, symbol: str, limit: int = 20
) -> List[Dict[str, Any]]:
    """특정 종목의 판단 이력 조회 (판단시각 역순)."""
    if pool is None:
        return []
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM stark_decisions WHERE symbol=$1 "
                "ORDER BY decided_at DESC LIMIT $2",
                symbol, limit,
            )
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"종목별 판단 조회 실패({symbol}): {e}")
        return []
