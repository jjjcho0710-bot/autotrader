"""
router/handlers/setting_handler.py - 전략 설정(손절/익절/매수금액) 실시간 변경 처리기

dashboard/main.py의 _handle_setting_command(구 4169행)·_apply_strategy_settings(구 4232행)·
_validate_setting(구 4256행)를 이관. strategy_config 테이블을 직접 갱신하고 redis pub/sub로
봇에 즉시 반영한다(배포 없이 1분 내 반영) — 단위는 항상 '퍼센트 숫자 그대로'로 저장한다는
기존 관례를 그대로 유지한다(과거 단위 추측 로직이 값이 계속 줄어드는 연쇄 버그를 낸 적이 있어
추측 로직은 완전히 제거된 상태).
"""
import json
import logging
import re
from typing import Any, Optional, Tuple

logger = logging.getLogger("router.handlers.setting")


def validate_setting(k: str, v) -> Tuple[bool, Any]:
    """(ok, normalized_value or 오류메시지)"""
    try:
        v = float(v)
    except Exception:
        return False, f"{k} 값이 숫자가 아님"
    if k == "stop_loss":
        v = -abs(v)
        return ((-15 <= v <= -0.5), v if -15 <= v <= -0.5 else "손절 허용범위 -0.5~-15%")
    if k == "take_profit":
        v = abs(v)
        return ((0.5 <= v <= 20), v if 0.5 <= v <= 20 else "익절 허용범위 0.5~20%")
    if k == "buy_amount":
        return ((50000 <= v <= 5000000), int(v) if 50000 <= v <= 5000000 else "매수금액 허용범위 5만~500만원")
    if k == "max_num_stocks":
        return ((1 <= v <= 20), int(v) if 1 <= v <= 20 else "보유종목 허용범위 1~20개")
    if k == "max_buy_percent_of_cash":
        v = v / 100 if v > 1 else v  # "70" 또는 "0.7" 둘 다 허용
        return ((0.05 <= v <= 1.0), v if 0.05 <= v <= 1.0 else "예수금 비율 허용범위 5~100%")
    return False, f"알 수 없는 설정 {k}"


async def apply_strategy_settings(pool: Any, redis: Any, changes: dict) -> list:
    """전략 설정 변경 공용 적용기 (검증된 changes만 받음). 반환: 적용 전략명"""
    applied = []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, is_active, params FROM strategy_config WHERE bot='stock_trader'")
        for r in rows:
            params = r["params"] if isinstance(r["params"], dict) else json.loads(r["params"] or "{}")
            for k, v in changes.items():
                params[k] = v
            await conn.execute(
                "UPDATE strategy_config SET params=$1, updated_at=NOW() WHERE id=$2",
                json.dumps(params), r["id"])
            try:
                await redis.publish("strategy:update", json.dumps({
                    "bot": "stock_trader", "name": r["name"],
                    "is_active": r["is_active"], "params": params}))
            except Exception:
                pass
            applied.append(r["name"])
    return applied


async def handle(user_msg: str, pool: Any, redis: Any, send_telegram_fn) -> Optional[str]:
    """전략 설정 실시간 변경 (배포 없음). 해당 없으면 None"""
    msg = user_msg.replace(",", "").strip()

    # 패턴: 손절 -7% / 익절 3% / 매수금액 100만원(또는 1000000원) + 변경/바꿔/설정/해줘
    if not re.search(r"(변경|바꿔|바꾸|설정|해줘|올려|내려|조정)", msg):
        return None
    m_sl = re.search(r"손절[을를]?\s*(-?\d+(?:\.\d+)?)\s*%", msg)
    m_tp = re.search(r"익절[을를]?\s*(\+?\d+(?:\.\d+)?)\s*%", msg)
    m_amt = re.search(r"매수\s*금액[을를]?\s*(\d+(?:\.\d+)?)\s*(만원|원)", msg)
    if not (m_sl or m_tp or m_amt):
        return None

    changes = {}
    if m_sl:
        v = -abs(float(m_sl.group(1)))
        if not (-15 <= v <= -0.5):
            return f"⚠️ 손절 {v}%는 허용 범위(-0.5% ~ -15%)를 벗어나 적용하지 않았습니다."
        changes["stop_loss"] = v
    if m_tp:
        v = abs(float(m_tp.group(1)))
        if not (0.5 <= v <= 20):
            return f"⚠️ 익절 {v}%는 허용 범위(0.5% ~ 20%)를 벗어나 적용하지 않았습니다."
        changes["take_profit"] = v
    if m_amt:
        v = float(m_amt.group(1)) * (10000 if m_amt.group(2) == "만원" else 1)
        if not (50000 <= v <= 5000000):
            return f"⚠️ 매수금액 {v:,.0f}원은 허용 범위(5만~500만원)를 벗어나 적용하지 않았습니다."
        changes["buy_amount"] = int(v)

    applied = await apply_strategy_settings(pool, redis, changes)

    desc = " · ".join(
        [f"손절 {changes['stop_loss']}%" if "stop_loss" in changes else "",
         f"익절 {changes['take_profit']}%" if "take_profit" in changes else "",
         f"매수금액 {changes['buy_amount']:,}원" if "buy_amount" in changes else ""])
    desc = " · ".join([d for d in desc.split(" · ") if d])
    await send_telegram_fn(f"⚙️ 전략 설정 변경 (채팅 지시)\n{desc}\n적용 전략: {', '.join(applied)}", broadcast=True)
    return (f"⚙️ 설정 변경 완료 — {desc}\n"
            f"적용: {', '.join(applied)} (봇이 1분 내 자동 반영, 배포 없음)")
