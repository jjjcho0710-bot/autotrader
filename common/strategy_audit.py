"""strategy_config 변경 감사 기록 ([AT] fix/strategy-config-guard).

모든 변경 경로(대시보드 설정 화면 API, 채팅 설정 명령, 대화 [[ACTION]] 설정, 전략 추가,
fix_units)가 before/after params와 경로·행위자를 남기도록 공용으로 쓴다. DB 스키마 변경 없이
로그 + Redis 최근 기록 리스트(audit:strategy_config, 최대 500건, 90일 TTL)에 남긴다.
감사 기록 실패가 설정 변경 자체를 막아서는 안 되므로 모든 예외를 삼킨다."""
import json
import logging
import time as _time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("common.strategy_audit")

KST = timezone(timedelta(hours=9))
AUDIT_REDIS_KEY = "audit:strategy_config"
AUDIT_MAX_ENTRIES = 500
AUDIT_TTL_SEC = 90 * 86400


def diff_params(before: Optional[dict], after: Optional[dict]) -> Dict[str, tuple]:
    """키별 (before, after) 변경 목록. 값이 같으면 포함하지 않는다."""
    b, a = before or {}, after or {}
    out = {}
    for k in sorted(set(b) | set(a)):
        if b.get(k) != a.get(k):
            out[k] = (b.get(k), a.get(k))
    return out


def format_diff(changes: Dict[str, tuple]) -> str:
    return ", ".join(f"{k}: {bv} -> {av}" for k, (bv, av) in changes.items()) or "변경 없음"


def build_entry(*, path: str, actor: str, bot: str, name: str,
                before: Optional[dict], after: Optional[dict]) -> Dict[str, Any]:
    return {
        "at": datetime.now(KST).isoformat(), "path": path, "actor": actor,
        "bot": bot, "name": name, "before": before, "after": after,
        "changes": {k: [bv, av] for k, (bv, av) in diff_params(before, after).items()},
    }


async def record(redis: Any, entry: Dict[str, Any]) -> None:
    """감사 항목을 로그와 Redis에 남긴다(실패해도 예외를 던지지 않는다)."""
    try:
        logger.warning(
            f"[strategy_config 변경] 경로={entry['path']} 행위자={entry['actor']} "
            f"{entry['bot']}/{entry['name']} 변경={json.dumps(entry['changes'], ensure_ascii=False, default=str)}")
    except Exception:
        pass
    if redis is None:
        return
    try:
        await redis.lpush(AUDIT_REDIS_KEY, json.dumps(entry, ensure_ascii=False, default=str))
        await redis.ltrim(AUDIT_REDIS_KEY, 0, AUDIT_MAX_ENTRIES - 1)
        await redis.expire(AUDIT_REDIS_KEY, AUDIT_TTL_SEC)
    except Exception as e:
        logger.warning(f"strategy_config 감사 기록 Redis 저장 실패(무시): {e}")


def build_alert_text(entries: List[Dict[str, Any]]) -> Optional[str]:
    """텔레그램 알림 문구. 실제 변경이 있는 항목이 없으면 None."""
    real = [e for e in entries if e.get("changes")]
    if not real:
        return None
    head = real[0]
    lines = [f"🛠 <b>전략 설정 변경 감지</b>", f"경로: {head['path']}", f"행위자: {head['actor']}"]
    for e in real:
        chg = ", ".join(f"{k} {bv} -> {av}" for k, (bv, av) in e["changes"].items())
        lines.append(f"- {e['bot']}/{e['name']}: {chg}")
    return "\n".join(lines)
