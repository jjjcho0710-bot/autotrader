"""
common/alert_throttle.py - 텔레그램 알림 스팸 방지 및 상태 기반 스로틀링 모듈

규칙:
1. 동일 종목·동일 종류 알림은 상태가 바뀔 때만 1회 즉시 발송
2. 동일 상태가 계속 유지·반복될 때는 최소 1시간(3600초) 간격으로만 발송
3. 정상 상태(NORMAL)로 복귀 시에는 알림을 발송하지 않고 상태만 갱신(다음 악화 시 즉시 1회 발송 보장)
4. Redis를 우선 사용하며, Redis 장애 시 인메모리 딕셔너리로 자동 폴백
"""
import json
import logging
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)

# 인메모리 폴백 캐시: {key: {"state": str, "ts": float}}
_MEMORY_CACHE = {}


def _make_key(symbol: str, alert_type: str) -> str:
    return f"alert:throttle:{symbol}:{alert_type}"


async def should_send_symbol_alert(
    symbol: str,
    alert_type: str,
    new_state: str,
    min_interval_sec: int = 3600,
    redis_client: Optional[Any] = None,
    now_ts: Optional[float] = None,
) -> bool:
    """동일 종목·동일 알림 종류의 상태 변화 및 시간 간격을 검사하여 발송 여부를 반환.

    - new_state == "NORMAL": 상태를 NORMAL로 갱신하고 False 반환 (정상 복귀는 무알림)
    - 이전 상태와 다름 (상태 변경): 상태 갱신 + True 반환 (즉시 1회 발송)
    - 이전 상태와 같음 (동일 상태 반복):
        - 마지막 발송 후 min_interval_sec(기본 3600초) 경과 시: 타임스탬프 갱신 + True 반환
        - 경과하지 않았으면: False 반환 (억제)
    """
    if now_ts is None:
        now_ts = time.time()

    key = _make_key(symbol, alert_type)
    prev_state = None
    last_ts = 0.0

    # 1. 이전 상태 조회 (Redis -> 인메모리 폴백)
    loaded = False
    if redis_client is not None:
        try:
            raw = await redis_client.get(key)
            if raw:
                data = json.loads(raw)
                prev_state = data.get("state")
                last_ts = float(data.get("ts", 0.0))
                loaded = True
        except Exception as e:
            logger.debug(f"Redis throttle get 실패, 인메모리 폴백 사용: {e}")

    if not loaded:
        entry = _MEMORY_CACHE.get(key)
        if entry:
            prev_state = entry.get("state")
            last_ts = float(entry.get("ts", 0.0))

    # 2. 상태 판단
    # 정상 복귀 (NORMAL)인 경우: 상태만 기록하고 알림은 보내지 않음
    if new_state == "NORMAL":
        await _save_throttle(key, "NORMAL", now_ts, redis_client)
        return False

    # 상태가 변경된 경우: 즉시 1회 발송 허용
    if prev_state != new_state:
        await _save_throttle(key, new_state, now_ts, redis_client)
        logger.info(f"🔔 [{symbol}:{alert_type}] 상태 변경 감지: '{prev_state}' -> '{new_state}' (발송 허용)")
        return True

    # 동일 상태인 경우: 1시간 간격 체크
    elapsed = now_ts - last_ts
    if elapsed >= min_interval_sec:
        await _save_throttle(key, new_state, now_ts, redis_client)
        logger.info(f"⏳ [{symbol}:{alert_type}] 동일 상태 '{new_state}' {elapsed:.0f}초 경과 -> 주기 발송 허용")
        return True

    # 1시간 미경과 동일 상태: 억제
    logger.debug(f"⏸️ [{symbol}:{alert_type}] 동일 상태 '{new_state}' 스로틀 억제 (경과: {elapsed:.0f}s < {min_interval_sec}s)")
    return False


async def _save_throttle(key: str, state: str, ts: float, redis_client: Optional[Any] = None):
    """상태 및 타임스탬프 저장 (TTL: 24시간)"""
    data = {"state": state, "ts": ts}
    _MEMORY_CACHE[key] = data

    if redis_client is not None:
        try:
            val = json.dumps(data, ensure_ascii=False)
            await redis_client.setex(key, 86400, val)
        except Exception as e:
            logger.debug(f"Redis throttle setex 실패: {e}")


async def reset_symbol_alert(symbol: str, alert_type: Optional[str] = None, redis_client: Optional[Any] = None):
    """종목 청산 시 알림 스로틀 초기화"""
    if alert_type:
        keys = [_make_key(symbol, alert_type)]
    else:
        # 주요 alert_type 모두 초기화
        keys = [
            _make_key(symbol, "price_monitor"),
            _make_key(symbol, "sell_fail"),
            _make_key(symbol, "buy_fail"),
        ]

    for k in keys:
        _MEMORY_CACHE.pop(k, None)
        if redis_client is not None:
            try:
                await redis_client.delete(k)
            except Exception:
                pass
