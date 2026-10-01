"""
common/alert_throttle.py - 텔레그램 알림 스팸 방지 및 상태/원인 기반 스로틀링 모듈

규칙:
1. 동일 종목·동일 종류 알림은 상태가 바뀔 때만 1회 즉시 발송
2. 동일 상태가 계속 유지·반복될 때는 최소 1시간(3600초) 간격으로만 발송
3. 정상 상태(NORMAL)로 복귀 시에는 알림을 발송하지 않고 상태만 갱신(다음 악화 시 즉시 1회 발송 보장)
4. 매수·매도 실패 알림은 원인(에러 메시지) 기준 정규화 및 원인별 개별 키 스로틀:
   - 에러 문구에서 숫자와 종목명·종목코드를 제거해 정규화한 값을 원인 키로 사용
   - 원인별 개별 키로 저장: alert:cause:{액션}:{정규화원인}
   - 각 원인이 독립적으로 1시간 1건 발송
   - 새 원인은 즉시 발송하고, 이후 동일 원인은 1시간 동안 묶음 ("N종목" 요약)
5. 체결 알림 및 상태 전이 알림은 절대 억제하지 않음
6. Redis를 우선 사용하며, Redis 장애 시 인메모리 딕셔너리로 자동 폴백
"""
import json
import logging
import re
import time
from typing import Any, List, Optional, Tuple

logger = logging.getLogger(__name__)

# 인메모리 폴백 캐시
_MEMORY_CACHE = {}
_MEMORY_CAUSE_CACHE = {}
_MEMORY_DROP_CACHE = {}


def _make_key(symbol: str, alert_type: str) -> str:
    return f"alert:throttle:{symbol}:{alert_type}"


def normalize_cause(err: str, name: str = "", symbol: str = "") -> str:
    """에러 문구에서 숫자와 종목명·종목코드를 제거하여 정규화된 원인 키를 반환"""
    text = str(err or "").strip()
    if name:
        text = text.replace(name, "")
    if symbol:
        text = text.replace(symbol, "")
    # 숫자 및 콤마 제거 (금액, 수량, 코드 등 가변 숫자 정규화)
    text = re.sub(r"[\d,]+", "", text)
    # 연속 공백 정리
    text = re.sub(r"\s+", " ", text).strip()
    return text or "기타오류"


def _make_cause_key(action_kr: str, norm_cause: str) -> str:
    return f"alert:cause:{action_kr}:{norm_cause}"


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
    if new_state == "NORMAL":
        await _save_throttle(key, "NORMAL", now_ts, redis_client)
        return False

    if prev_state != new_state:
        await _save_throttle(key, new_state, now_ts, redis_client)
        logger.info(f"🔔 [{symbol}:{alert_type}] 상태 전이 감지: '{prev_state}' -> '{new_state}' (즉시 발송 허용)")
        return True

    elapsed = now_ts - last_ts
    if elapsed >= min_interval_sec:
        await _save_throttle(key, new_state, now_ts, redis_client)
        logger.info(f"⏳ [{symbol}:{alert_type}] 동일 상태 '{new_state}' {elapsed:.0f}초 경과 -> 주기 발송 허용")
        return True

    logger.debug(f"⏸️ [{symbol}:{alert_type}] 동일 상태 '{new_state}' 스로틀 억제 (경과: {elapsed:.0f}s < {min_interval_sec}s)")
    return False


async def _save_throttle(key: str, state: str, ts: float, redis_client: Optional[Any] = None):
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

    if not alert_type:
        drop_key = _make_drop_key(symbol)
        _MEMORY_DROP_CACHE.pop(drop_key, None)
        if redis_client is not None:
            try:
                await redis_client.delete(drop_key)
            except Exception:
                pass


def _make_drop_key(symbol: str) -> str:
    return f"drop_alert_last:{symbol}"


async def should_send_drop_alert(
    symbol: str,
    pnl_rate: float,
    min_drop_delta: float = 1.0,
    redis_client: Optional[Any] = None,
) -> Tuple[bool, Optional[float]]:
    """급락(DROP) 알림 전용 스로틀: 직전에 알린 손익률보다 min_drop_delta(%p) 이상
    추가로 하락했을 때만 재알림.

    - 처음 알리는 경우(직전 기록 없음): 즉시 발송 허용
    - 정상 범위로 복귀했다가 다시 급락에 진입해도 이 기록은 초기화되지 않음
      (복귀만으로 즉시 재알림하지 않음 — 호출부에서 NORMAL 구간에 이 함수를 호출하지 않으면 됨)
    - STOP_LOSS 알림/손절 실패 알림에는 사용하지 않음(별도 스로틀 유지)

    반환: (should_send: bool, 직전에 알린 손익률 또는 None)
    """
    key = _make_drop_key(symbol)
    prev: Optional[float] = None

    raw = None
    if redis_client is not None:
        try:
            raw = await redis_client.get(key)
        except Exception as e:
            logger.debug(f"Redis drop-alert get 실패, 인메모리 폴백 사용: {e}")

    if raw is not None:
        try:
            prev = float(raw)
        except Exception:
            prev = None
    else:
        prev = _MEMORY_DROP_CACHE.get(key)

    should_send = prev is None or pnl_rate <= prev - min_drop_delta

    if should_send:
        _MEMORY_DROP_CACHE[key] = pnl_rate
        if redis_client is not None:
            try:
                await redis_client.setex(key, 86400, str(pnl_rate))
            except Exception as e:
                logger.debug(f"Redis drop-alert setex 실패: {e}")

    return should_send, prev


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
    - (1) 에러 문구에서 숫자와 종목명을 제거해 정규화한 값을 원인 키로 사용
    - (2) 원인별 개별 키로 저장: alert:cause:{액션}:{정규화원인}
    - (3) 각 원인이 독립적으로 1시간 1건 스로틀
    - (4) 새 원인은 즉시 발송하고 이후 동일 원인은 묶음 ("N종목" 요약)

    반환: (should_send: bool, summary_disp: str)
    """
    if now_ts is None:
        now_ts = time.time()

    clean_err = str(err or "").strip()
    norm_cause = normalize_cause(clean_err, name, symbol)
    key = _make_cause_key(action_kr, norm_cause)

    data = None
    if redis_client is not None:
        try:
            raw = await redis_client.get(key)
            if raw:
                data = json.loads(raw)
        except Exception as e:
            logger.debug(f"Redis cause throttle get 실패: {e}")

    if data is None:
        data = _MEMORY_CAUSE_CACHE.get(key)

    # 1. 새 원인인 경우: 즉시 발송
    if not data:
        new_symbols = [name]
        await _save_cause_throttle(key, norm_cause, clean_err, new_symbols, now_ts, redis_client)
        logger.info(f"🔔 [{action_kr} 실패] 새 원인 감지: '{norm_cause}' (종목: {name}) -> 즉시 발송")
        return True, name

    # 2. 기존 원인인 경우: 종목 누적 및 1시간 간격 체크
    symbols = list(data.get("symbols", []))
    last_sent_ts = float(data.get("last_sent_ts", 0.0))
    if name not in symbols:
        symbols.append(name)

    elapsed = now_ts - last_sent_ts

    # 1시간 미경과: 동일 원인 묶음으로 발송 억제
    if elapsed < min_interval_sec:
        await _save_cause_throttle(key, norm_cause, clean_err, symbols, last_sent_ts, redis_client)
        logger.info(f"⏸️ [{action_kr} 실패] 동일 원인 스로틀 억제: '{norm_cause}' ({len(symbols)}종목 누적: {symbols})")
        return False, ""

    # 1시간 경과: N종목 요약 문구 생성 후 주기적 1회 발송
    count = len(symbols)
    if count > 1:
        summary_disp = f"{symbols[0]} 외 {count - 1}종목 (총 {count}종목)"
    else:
        summary_disp = symbols[0] if symbols else name

    # 발송 후 현재 종목 기준으로 새 윈도우 시작
    await _save_cause_throttle(key, norm_cause, clean_err, [name], now_ts, redis_client)
    logger.info(f"⏳ [{action_kr} 실패] 동일 원인 1시간 경과 -> 요약 발송: {summary_disp}")
    return True, summary_disp


async def _save_cause_throttle(
    key: str, norm_cause: str, raw_err: str, symbols: List[str], last_sent_ts: float, redis_client: Optional[Any] = None
):
    data = {"norm_cause": norm_cause, "raw_err": raw_err, "symbols": symbols, "last_sent_ts": last_sent_ts}
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
        prefix = f"alert:cause:{action_kr}:"
        to_del = [k for k in _MEMORY_CAUSE_CACHE if k.startswith(prefix)]
        for k in to_del:
            _MEMORY_CAUSE_CACHE.pop(k, None)
    else:
        _MEMORY_CAUSE_CACHE.clear()
