"""
router/handlers/chart_handler.py - 차트 분석 대화 처리기

dashboard/main.py의 _jarvis_chat_impl 내 "차트" 키워드 블록(구 4428행)을 이관.
차트 데이터 산출 자체(추세·지지/저항·캔들·거래량 계산)는 dashboard/main.py의 _analyze_chart가
여러 곳(판단 프롬프트, 종목 자동 첨부 등)에서 공유하는 인프라라 그대로 두고,
analyze_chart_fn으로 주입받아 호출만 한다.
"""
from typing import Any, Optional


async def handle(user_msg: str, universe: Any, analyze_chart_fn) -> Optional[str]:
    """차트 리서치 명령: "차트 OO" / "OO 차트 어때". 종목을 특정 못하면 None(일반 대화로 넘김)"""
    if "차트" not in user_msg:
        return None
    symbol, name = await universe.resolve_symbol(user_msg)
    if not symbol:
        return None
    chart_txt = await analyze_chart_fn(symbol, name)
    if chart_txt:
        return chart_txt
    return f"⚠️ {name}({symbol}) 차트 데이터를 가져오지 못했어요."
