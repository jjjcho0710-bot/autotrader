"""
router/intent_router.py - STARK v2 2-tier 인텐트 라우터

Tier 1 (고속 패턴 매칭): 지시/설정/감시종목/지식/차트/매매승인/직접매매처럼
정규식·키워드로 즉시 확정할 수 있는 명령을 router/handlers/*로 순서대로 시도한다.
하나라도 응답을 내면(문자열을 반환하면) 그 즉시 확정, LLM을 호출하지 않는다.

Tier 2 (AI 분류): 위 어느 패턴에도 안 걸리면 route()는 None을 반환한다.
호출부(dashboard/main.py의 _jarvis_chat_impl)는 이 경우에만 포트폴리오/차트/기억을
컨텍스트로 채운 뒤 LLM(Open-WebUI)을 호출해 자유 대화로 응답한다 — LLM 응답에 실린
[[ACTION]] 태그가 사실상 "AI가 분류한 의도"이며, 그 실행도 route_action()이 같은
handlers를 재사용해 처리한다.

이 라우터는 이전 dashboard/main.py의 레거시 정규식 9분기(직접 하드코딩되어 있던 지시·설정·
감시종목·지식·차트·제안승인·매매지시 판별 순서)를 그대로 옮긴 것이다 — 우선순위가 바뀌면
"손절 -5%로 바꿔"류 문장이 다른 분기에 먼저 걸릴 수 있으므로 순서를 임의로 바꾸지 않는다.
"""
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from router.handlers import (
    chart_handler,
    directive_handler,
    knowledge_handler,
    order_handler,
    setting_handler,
    watchlist_handler,
)

logger = logging.getLogger("router.intent_router")


@dataclass
class RouterContext:
    """핸들러들이 공유하는 의존성 묶음 — market/universe.py, stark/decision_logger.py와
    동일하게 pool/redis 등을 생성자(여기서는 dataclass 필드)로 주입받는 방식을 따른다."""

    pool: Any
    redis: Any
    universe: Any
    config: Any
    stock_name_map: dict
    session_id: str
    send_telegram: Callable[..., Any]
    get_kis_token: Callable[..., Any]
    kis_order: Callable[..., Any]
    analyze_chart: Callable[..., Any]
    get_stock_positions: Callable[..., Any]
    log_journal: Callable[..., Any]
    web_research: Callable[..., Any]
    jarvis_chat: Callable[..., Any]
    channel: str = "web"


async def route_early(user_msg: str, ctx: RouterContext) -> Optional[str]:
    """Tier 1 앞부분: 지시 → 설정 → 감시종목(명령/확정추가) → 지식(목록/삭제).

    dashboard/main.py의 _jarvis_chat_impl은 이 함수와 route_late() 사이에서 원본 순서를
    지키기 위해 학습 URL 블록(router가 담당하지 않는 백그라운드 작업)을 그대로 검사한다 —
    그래서 route()를 하나로 합치지 않고 early/late로 나눴다."""

    # 지시사항 저장/목록/취소
    reply = await directive_handler.handle(user_msg, ctx.pool, ctx.redis)
    if reply is not None:
        return reply

    # 전략 설정 실시간 변경 (손절/익절/매수금액)
    reply = await setting_handler.handle(user_msg, ctx.pool, ctx.redis, ctx.send_telegram)
    if reply is not None:
        return reply

    # 감시 종목 추가/삭제/조회 (Open-WebUI 거치지 않고 직접 처리)
    reply = await watchlist_handler.handle_command(user_msg, ctx.pool, ctx.universe, ctx.stock_name_map)
    if reply is not None:
        return reply

    # 감시/관심 추가 확정 명령: "OO 감시 추가해줘"
    reply = await watchlist_handler.handle_quick_add(user_msg, ctx.pool, ctx.universe, ctx.web_research)
    if reply is not None:
        return reply

    # 학습 지식 목록 (확정 명령, AI 미경유)
    reply = await knowledge_handler.handle_list(user_msg, ctx.pool)
    if reply is not None:
        return reply

    # 지식 삭제 N / RN
    reply = await knowledge_handler.handle_delete(user_msg, ctx.pool)
    if reply is not None:
        return reply

    return None


async def route_late(user_msg: str, ctx: RouterContext) -> Optional[str]:
    """Tier 1 뒷부분: 차트 → 능동제안 승인/거절 → 매수제안 승인/거절 → 직접 매매 지시.
    호출부는 학습 URL 블록 다음에 이 함수를 호출한다(원본 순서 그대로)."""

    # 차트 리서치: "차트 OO" / "OO 차트 어때"
    reply = await chart_handler.handle(user_msg, ctx.universe, ctx.analyze_chart)
    if reply is not None:
        return reply

    # 능동 제안 승인/거절: "승인 2" / "거절 1"
    reply = await order_handler.handle_advice_response(
        user_msg, redis=ctx.redis, jarvis_chat_fn=ctx.jarvis_chat,
        send_telegram_fn=ctx.send_telegram, session_id=ctx.session_id)
    if reply is not None:
        return reply

    # 매수 제안(proposal) 승인/거절
    reply = await order_handler.handle_proposal_response(
        user_msg, redis=ctx.redis, kis_order_fn=ctx.kis_order,
        log_journal_fn=ctx.log_journal, send_telegram_fn=ctx.send_telegram)
    if reply is not None:
        return reply

    # 채팅 직접 매매 지시 → 실제 KIS 주문 실행 (자비스 경유 X)
    reply = await order_handler.handle_trade_command(
        user_msg, pool=ctx.pool, redis=ctx.redis, universe=ctx.universe,
        get_kis_token_fn=ctx.get_kis_token, config=ctx.config, kis_order_fn=ctx.kis_order,
        get_stock_positions_fn=ctx.get_stock_positions, send_telegram_fn=ctx.send_telegram,
        log_journal_fn=ctx.log_journal)
    if reply is not None:
        return reply

    return None
