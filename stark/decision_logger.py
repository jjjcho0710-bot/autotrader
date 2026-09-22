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

스키마 정본: migrations/V003__create_stark_decisions.sql, V008(features_json 추가)

features_json 직렬화 방식은 이 저장소의 기존 JSONB 컬럼 관례를 그대로 따른다
(dashboard/main.py의 strategy_config.params 처리와 동일): asyncpg는 json/jsonb를
기본적으로 텍스트로 인코딩/디코딩하므로, 쓸 때는 json.dumps()한 문자열을 그대로
파라미터로 넘기고 읽을 때는 값이 str이면 json.loads()로 되돌린다.
"""
import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _decode_features_json(row: Dict[str, Any]) -> Dict[str, Any]:
    """조회 결과 row의 features_json을 dict로 역직렬화 (문자열로 온 경우만)."""
    raw = row.get("features_json")
    if isinstance(raw, str):
        try:
            row["features_json"] = json.loads(raw)
        except (TypeError, ValueError) as e:
            logger.error(f"features_json 역직렬화 실패(id={row.get('id')}): {e}")
    return row


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
    features: Optional[Dict[str, Any]] = None,
) -> Optional[int]:
    """STARK 판단 1건을 stark_decisions에 기록.

    decision은 자유 문자열이지만 관례상 BUY/SELL/HOLD/HALF/ALL/SKIP 등을 사용한다.
    판단이 SKIP/HOLD로 끝나거나 모델 호출 자체가 실패한 경우에도 반드시 호출해서
    사유(reason)와 함께 기록을 남겨야 한다 — 침묵 금지.

    features: 판단 시점에 모델에 입력된 피처 스냅샷(학습·복기용, 선택). dict로
    받아 JSONB 컬럼(features_json)에 저장한다.

    반환값: 삽입된 행의 id. 실패 시 None (예외를 삼키고 로그만 남김 — 판단 로그
    기록 실패가 실제 매매 실행 흐름을 막아서는 안 된다). symbol/decision처럼
    테이블에서 NOT NULL인 필수 컬럼이 비어 있으면 DB 왕복 없이 바로 None을 반환한다.
    """
    if pool is None:
        logger.error(f"stark_decisions 기록 실패({symbol}): DB pool이 없음")
        return None
    if not symbol or not decision:
        logger.error(
            f"stark_decisions 기록 실패: 필수 컬럼 누락(symbol={symbol!r}, decision={decision!r})"
        )
        return None
    features_json = json.dumps(features, ensure_ascii=False) if features is not None else None
    try:
        async with pool.acquire() as conn:
            decision_id = await conn.fetchval(
                """
                INSERT INTO stark_decisions
                    (symbol, name, decision, confidence, reason, rationale,
                     strategy, source, model_name, executed, order_success,
                     price, quantity, features_json)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                RETURNING id
                """,
                symbol, name, decision, confidence, reason, rationale,
                strategy, source, model_name, executed, order_success,
                price, quantity, features_json,
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
        return [_decode_features_json(dict(r)) for r in rows]
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
        return [_decode_features_json(dict(r)) for r in rows]
    except Exception as e:
        logger.error(f"종목별 판단 조회 실패({symbol}): {e}")
        return []
