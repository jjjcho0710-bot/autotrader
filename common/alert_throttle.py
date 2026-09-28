"""
common/alert_throttle.py - 텔레그램 알림 스팸 방지 및 상태/원인 기반 스로틀링 모듈

규칙:
1. 동일 종목·동일 종류 알림은 상태가 바뀔 때만 1회 즉시 발송
2. 동일 상태가 계속 유지·반복될 때는 최소 1시간(3600초) 간격으로만 발송
3. 정상 상태(NORMAL)로 복귀 시에는 알림을 발송하지 않고 상태만 갱신(다음 악화 시 즉시 1회 발송 보장)
4. 매수·매도 실패 알림은 원인(에러 메시지) 기준 스로틀:
   - 종목이 달라도 같은 원인이면 1시간에 1건으로 묶고 "N종목" 요약
   - 원인이 바뀌면 즉시 발송
5. 체결 알림 및 상태 전이 알림은 절대 억제하지 않음
6. Redis를 우선 사용하며, Redis 장애 시 인메모리 딕셔너리로 자동 폴백
"""
import json
import logging
import time
from typing import Any, List, Optional, Tuple

logger = logging.getLogger(__name__)

# 인메모리 폴백 캐시
_MEMORY_CACHE = {}
_MEMORY_CAUSE_CACHE = {}


def _make_key(symbol: str, alert_type: str) -> str:
    return f"alert:throttle:{symbol}:{alert_type}"


def _make_cause_key(action_kr: str) -> str:
    return f"alert:cause_throttle:{action_kr}"


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
    - 이전 상태와 다름 (상태 전이): 상태 갱신 + True 반환 (즉시 1회 발송, 절대 억제 안 함)
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

    # 상태가 변경된 경우 (상태 전이): 즉시 1회 발송 허용 (절대 억제 안 함)
    if prev_state != new_state:
        await _save_throttle(key, new_state, now_ts, redis_client)
        logger.info(f"🔔 [{symbol}:{alert_type}] 상태 전이 감지: '{prev_state}' -> '{new_state}' (즉시 발송 허용)")
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
        keys = [
            _make_key(symbol, "price_monitor"),
            _make_key(symbol, "sell_fail"),
            _make_key(symbol, "exit_ai_fail"),
        ]

    for k in keys:
        _MEMORY_CACHE.pop(k, None)
        if redis_client is not None:
            try:
                await redis_client.delete(k)
            except Exception:
                pass


# ── 원인(에러 메시지) 기준 실패 알림 스로틀 ────────────────────────
async def check_cause_fail_throttle(
    action_kr: str,
    symbol: str,
    name: str,
    err: str,
    min_interval_sec: int = 3600,
    redis_client: Optional[Any] = None,
    now_ts: Optional[float] = None,
) -> Tuple[bool, str]:
    """매수·매도 실패 알림 원인(에러 메시지) 기준 스로틀:
    - 종목이 달라도 같은 원인이면 1시간에 1건으로 묶고 "N종목" 요약
    - 원인 바뀌면 즉시 발송

    반환: (should_send: bool, summary_disp: str)
      - should_send: True이면 알림 발송, False이면 스로틀 억제
      - summary_disp: 알림에 표시할 종목명 또는 요약 문구 (예: "서전기전" 또는 "서전기전 외 2종목 (총 3종목)")
    """
    if now_ts is None:
        now_ts = time.time()

    key = _make_cause_key(action_kr)
    clean_err = str(err or "").strip()

    prev_cause = None
    symbols: List[str] = []
    last_sent_ts = 0.0

    # 1. 이전 기록 조회
    loaded = False
    if redis_client is not None:
        try:
            raw = await redis_client.get(key)
            if raw:
                data = json.loads(raw)
                prev_cause = data.get("cause")
                symbols = data.get("symbols", [])
                last_sent_ts = float(data.get("last_sent_ts", 0.0))
                loaded = True
        except Exception as e:
            logger.debug(f"Redis cause throttle get 실패: {e}")

    if not loaded:
        entry = _MEMORY_CAUSE_CACHE.get(key)
        if entry:
            prev_cause = entry.get("cause")
            symbols = list(entry.get("symbols", []))
            last_sent_ts = float(entry.get("last_sent_ts", 0.0))

    # 2. 원인 변경 시: 즉시 1회 발송 및 초기화
    if prev_cause != clean_err:
        new_symbols = [name]
        await _save_cause_throttle(key, clean_err, new_symbols, now_ts, redis_client)
        logger.info(f"🔔 [{action_kr} 실패] 원인 변경 감지: '{prev_cause}' -> '{clean_err}' (종목: {name}) -> 즉시 발송")
        return True, name

    # 3. 동일 원인 지속 시: 종목 목록에 추가 (중복 방지)
    if name not in symbols:
        symbols.append(name)

    elapsed = now_ts - last_sent_ts

    # 1시간 미경과: 1시간에 1건으로 묶어 발송 억제
    if elapsed < min_interval_sec:
        await _save_cause_throttle(key, clean_err, symbols, last_sent_ts, redis_client)
        logger.info(f"⏸️ [{action_kr} 실패] 동일 원인 스로틀 억제: '{clean_err}' ({len(symbols)}종목 누적: {symbols})")
        return False, ""

    # 1시간 경과: N종목 요약 문구 생성 후 주기적 1회 발송
    count = len(symbols)
    if count > 1:
        summary_disp = f"{symbols[0]} 외 {count - 1}종목 (총 {count}종목)"
    else:
        summary_disp = symbols[0] if symbols else name

    # 발송 후 현재 종목 기준으로 새 윈도우 시작
    await _save_cause_throttle(key, clean_err, [name], now_ts, redis_client)
    logger.info(f"⏳ [{action_kr} 실패] 동일 원인 1시간 경과 -> 요약 발송: {summary_disp}")
    return True, summary_disp


async def _save_cause_throttle(
    key: str, cause: str, symbols: List[str], last_sent_ts: float, redis_client: Optional[Any] = None
):
    data = {"cause": cause, "symbols": symbols, "last_sent_ts": last_sent_ts}
    _MEMORY_CAUSE_CACHE[key] = data

    if redis_client is not None:
        try:
            val = json.dumps(data, ensure_ascii=False)
            await redis_client.setex(key, 86400, val)
        except Exception as e:
            logger.debug(f"Redis cause throttle setex 실패: {e}")


def reset_cause_fail_throttle(action_kr: Optional[str] = None):
    """원인별 실패 스로틀 초기화 (테스트용)"""
    if action_kr:
        _MEMORY_CAUSE_CACHE.pop(_make_cause_key(action_kr), None)
    else:
        _MEMORY_CAUSE_CACHE.clear()
