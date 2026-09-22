"""
stark/decision_engine.py - STARK v2 순수 AI 판단 레이어

STARK_PLAN 4번·6번 원칙("판단(AI)과 실행(룰)을 코드 레벨로 분리", "모든 사이클은 무언가를
기록한다")의 핵심 구현. dashboard/main.py의 jarvis_signal(구 6231~6261행)에서
LLM 호출 → 판정(EXECUTE/EXECUTE_SMALL/PROPOSE/SKIP) 파싱 → ML 폴백까지를 그대로 이관했다.

기존 코드와의 중요한 차이 한 가지: 이 모듈은 판단 결과가 나올 때마다(SKIP·ML폴백 포함)
반드시 stark/decision_logger.log_decision을 호출해 stark_decisions에 적재한다.
원래 jarvis_signal에는 이 호출이 전혀 없었다 — trade_journal(매매일지)에만 기록되고
STARK v2용 stark_decisions 테이블(EDITH가 만든 스키마)은 아무도 쓰지 않고 있었다.
이 연결이 이번 변경의 실질적인 "물리적 분리 + 판단 로그 필수화" 부분이다.

이 모듈은 절대 주문을 실행하지 않는다 — 실행은 stark/execution_guard.py 전담.
"""
import logging
import re
from typing import Any, Dict

from stark.decision_logger import log_decision

logger = logging.getLogger("stark.decision_engine")

_VERDICT_RE = re.compile(r"\b(EXECUTE_SMALL|EXECUTE|PROPOSE|SKIP)\b")
_ML_PROB_RE = re.compile(r"ML매수확률[:\s]*([0-9]+)%")

# 판정별 확신도 휴리스틱. 원본 코드는 EXECUTE/EXECUTE_SMALL/PROPOSE/SKIP 라벨만 파싱했을 뿐
# 수치 확신도가 전혀 없었다 — stark_decisions.confidence를 의미 있게 채우기 위해 도입한
# 고정 매핑이다(모델이 직접 수치를 말하게 하지 않는 이유: JARVIS_SYSTEM_PROMPT가 정한 4개
# 판정 라벨 형식을 흔들지 않기 위함). PROPOSE는 규칙 밖 신호라 EXECUTE_SMALL보다 낮게 잡는다.
_VERDICT_CONFIDENCE = {"EXECUTE": 0.85, "EXECUTE_SMALL": 0.65, "PROPOSE": 0.70, "SKIP": 0.30}


async def decide(signal: Dict[str, Any], prompt: str, *, ask_llm_fn, pool: Any = None) -> Dict[str, Any]:
    """AI 판단 1회 수행 + stark_decisions 기록. 실행 여부만 판단하고 실행은 하지 않는다.

    반환: {"verdict","should_execute","is_small","reply","ai_failed","source","confidence"}
    """
    symbol = signal["symbol"]
    name = signal.get("name") or symbol
    action = signal.get("action", "buy")

    reply = await ask_llm_fn(prompt, session_id="signal")
    logger.info(f"🤖 Jarvis 판단 [{symbol}]: {(reply or '')[:150]}")

    m = _VERDICT_RE.search((reply or "").upper())
    verdict = m.group(1) if m else ""
    is_small = verdict == "EXECUTE_SMALL"
    should_execute = verdict in ("EXECUTE", "EXECUTE_SMALL")
    source = "ai"
    confidence = None

    # PROPOSE: 규칙(가격상한·분산 등) 밖 강신호 — 승인 없이 자동 실행(주인 지시: 완전 자동화).
    # 안전을 위해 소액(절반 수량)으로 진입.
    if verdict == "PROPOSE" and action in ("buy", "BUY"):
        is_small = True
        should_execute = True
        reply = (reply or "") + "\n[자동 실행: 규칙 외 강신호 — 소액 자동 진입]"
        confidence = _VERDICT_CONFIDENCE["PROPOSE"]

    # 429/오류 폴백: AI 응답 불가 시 신호의 ML 확률로 규칙 판단 (봇 생존)
    ai_failed = (not reply) or reply.startswith("❌") or "429" in (reply or "")[:200]
    if ai_failed:
        ml_m = _ML_PROB_RE.search(signal.get("reason") or "")
        ml_prob = int(ml_m.group(1)) if ml_m else 0
        # 원본 코드 그대로: 소문자 "buy"만 확인한다("BUY" 대문자는 폴백 매수를 안 함) —
        # 의도적 동작인지 확인되지 않은 기존 동작이라 그대로 보존.
        should_execute = (action == "buy" and ml_prob >= 70)
        reply = (f"[AI폴백] ML확률 {ml_prob}% 기준 "
                 f"{'EXECUTE' if should_execute else 'SKIP'} (Gemini 응답 불가)")
        source = "rule"
        confidence = round(ml_prob / 100.0, 2)  # ML 폴백은 실제 수치가 있으니 휴리스틱 대신 그대로 사용
        logger.warning(f"🤖 AI 폴백 판단 [{symbol}]: {reply}")
        decision_label = "EXECUTE" if should_execute else "SKIP"
    else:
        decision_label = "EXECUTE_SMALL" if is_small else (verdict or "SKIP")

    if confidence is None:
        confidence = _VERDICT_CONFIDENCE.get(decision_label, _VERDICT_CONFIDENCE["SKIP"])

    await log_decision(
        pool, symbol, decision_label,
        name=name,
        confidence=confidence,
        reason=(signal.get("reason") or "")[:300],
        rationale=(reply or "")[:500],
        strategy=signal.get("strategy"),
        source=source,
        executed=False,  # 실행 여부는 execution_guard가 결정 후 별도로 기록하지 않음(이 레코드는 판단 시점 기록)
    )

    return {
        "verdict": verdict,
        "should_execute": should_execute,
        "is_small": is_small,
        "reply": reply,
        "ai_failed": ai_failed,
        "source": source,
        "confidence": confidence,
    }
