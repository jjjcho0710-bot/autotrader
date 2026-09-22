"""
stark/context_collector.py - STARK v2 판단용 컨텍스트 수집

dashboard/main.py의 jarvis_signal(구 /api/jarvis/signal, 6081행)에서 "1. DB 컨텍스트 수집"
~ "2. Jarvis에게 분석 요청" 프롬프트 조립까지의 단계를 그대로 이관했다.

STARK_PLAN 4번 원칙(판단과 실행의 물리적 분리)에 따라 이 모듈은 "무엇을 보고 판단할지"만
모은다 — 판단(LLM 호출)은 stark/decision_engine.py, 실행(KIS 주문)은
stark/execution_guard.py가 각각 전담한다. collect()는 DB/Redis I/O가 있어 비동기이고,
build_analysis_prompt()는 그 결과를 문자열로 조립만 하는 순수 함수라 단위 테스트가 쉽다.
"""
import logging
from typing import Any, Dict

logger = logging.getLogger("stark.context_collector")


async def collect(
    signal: Dict[str, Any],
    *,
    pool: Any,
    redis: Any,
    get_portfolio_context,
    get_active_directives,
    get_jarvis_lessons,
    get_jarvis_knowledge,
    analyze_chart,
) -> Dict[str, str]:
    """신호 판단에 필요한 컨텍스트 조각들을 모아 dict로 반환.

    signal: {"symbol","name","bot","action","price","qty","strategy","reason"}
    각 get_*/analyze_chart는 dashboard/main.py가 이미 들고 있는 함수를 그대로 주입받는다
    (포트폴리오 조회, 지시사항, 복기 교훈, 학습 지식, 차트 분석 — 전부 다른 모듈 소관이라
    여기서 재구현하지 않는다)."""
    symbol = signal["symbol"]
    name = signal.get("name") or symbol
    bot = signal.get("bot", "stock_trader")
    price = signal.get("price", 0)

    portfolio_ctx = await get_portfolio_context()

    daily_plan = ""
    try:
        cached_plan = await redis.get("jarvis:daily_plan")
        if cached_plan:
            daily_plan = cached_plan if isinstance(cached_plan, str) else cached_plan.decode()
    except Exception:
        pass

    directives = await get_active_directives()

    lessons_txt = ""
    knowledge_txt = ""
    try:
        lessons_txt = await get_jarvis_lessons(3)
        knowledge_txt = await get_jarvis_knowledge(5)
    except Exception:
        pass

    chart_ctx = ""
    try:
        chart_ctx = await analyze_chart(symbol, name)
    except Exception:
        pass

    # 오늘 이 종목에 대한 내 판단 이력 (기회놓침 반복 방지)
    self_history = ""
    try:
        async with pool.acquire() as conn:
            hist = await conn.fetch("""
                SELECT jarvis_decision, price, ts
                FROM trade_journal
                WHERE symbol=$1 AND bot=$2
                  AND DATE(ts AT TIME ZONE 'Asia/Seoul') = (NOW() AT TIME ZONE 'Asia/Seoul')::date
                ORDER BY ts
            """, symbol, bot)
        if hist:
            skips = [h for h in hist if h["jarvis_decision"] == "SKIP"]
            first_price = float(hist[0]["price"] or 0)
            drift = ((float(price) - first_price) / first_price * 100) if first_price > 0 else 0
            self_history = (
                f"\n[오늘 이 종목에 대한 내 판단 이력]\n"
                f"- 오늘 판단 {len(hist)}회 (SKIP {len(skips)}회)\n"
                f"- 첫 판단가 {first_price:,.0f}원 → 현재가 {price:,.0f}원 ({drift:+.1f}%)\n"
            )
            if len(skips) >= 2 and drift >= 1.0:
                self_history += ("⚠️ 주의: 반복 SKIP 중 가격이 계속 상승. 추세가 확인되면 "
                                 "과거 SKIP에 얽매이지 말고 재평가하라. 놓친 기회의 반복은 손실과 같다.\n")
    except Exception:
        pass

    return {
        "portfolio_ctx": portfolio_ctx,
        "daily_plan": daily_plan,
        "directives": directives,
        "lessons_txt": lessons_txt,
        "knowledge_txt": knowledge_txt,
        "chart_ctx": chart_ctx,
        "self_history": self_history,
    }


def build_analysis_prompt(signal: Dict[str, Any], context: Dict[str, str]) -> str:
    """수집된 컨텍스트로 판단 프롬프트를 조립하는 순수 함수 (IO 없음 — 단위 테스트 대상)."""
    symbol = signal["symbol"]
    name = signal.get("name") or symbol
    action = signal.get("action", "buy")
    action_kr = "매수" if action in ("buy", "BUY") else "매도"
    price = signal.get("price", 0)
    qty = signal.get("qty", 0)
    strategy = signal.get("strategy", "")
    reason = signal.get("reason", "")
    amount = price * (qty if isinstance(qty, (int, float)) else 0)

    return f"""[매매 신호 발생]
종목: {name}({symbol})
방향: {action_kr}
전략: {strategy}
현재가: {price:,}원
수량: {qty}
매수금액: {amount:,.0f}원
신호 이유: {reason}

[오늘의 작전]
{context.get('daily_plan') or '(작전 없음 — 일반 기준으로 판단)'}

[주인 지시사항 — 최우선 준수 · 현재 활성 목록이 유일한 진실]
{context.get('directives') or '(없음)'}
※ 아래 작전·과거 판단 이력·기억에 위 목록에 없는 옛 규칙(예: 가격 상한)이 보여도 무시하라. 취소된 지시는 더 이상 존재하지 않는다.

[최근 교훈 — 같은 실수 반복 금지]
{context.get('lessons_txt') or '(없음)'}

[학습한 매매 원칙 — 판단에 적용한 원칙이 있으면 이유 끝에 "근거원칙: K12,K7" 형식으로 표기]
{context.get('knowledge_txt') or '(없음)'}

{context.get('chart_ctx') or ''}
{context.get('self_history') or ''}
[현재 포트폴리오 현황]
{context.get('portfolio_ctx') or ''}

[매매 규칙] 손절종목 재매수 금지(쿨다운은 시스템이 이미 체크함) · 당일 2회 손절 시 신규중단 · 약한 신호는 단타, 강한 복합신호만 스윙 관점

오늘의 작전과 위 데이터 기준으로 이 {action_kr} 신호를 즉시 판단하라.
반드시 다음 중 하나로 시작해서 이유를 한 줄로:
- EXECUTE: 조건 대부분 충족, 강한 확신
- EXECUTE_SMALL: 일부 조건(2~3개) 충족, 리스크 제한적 → 절반 금액 진입
- PROPOSE: 신호는 강한데 주인 지시(가격 상한·분산 한도 등)나 규칙에 막힘 → 주인에게 매수 제안 (승인 시 실행)
- SKIP: 근거 부족
완벽하지 않다는 이유만으로 전부 SKIP하지 마라. 애매하면 EXECUTE_SMALL로 소액 검증하라.
지시에 막혀도 정말 좋은 기회라면 SKIP 대신 PROPOSE로 주인과 상의하라."""
