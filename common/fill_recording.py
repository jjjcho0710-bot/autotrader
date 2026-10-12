"""여러 주문 경로(stark/execution_guard.py, router/handlers/order_handler.py,
stock_trader/main.py)가 공유하는 체결 수량·체결가 판정·표시 로직.

배경: trade_history에 주문 수량이 그대로 기록되고 "체결 완료"로 보고되는 사고가 반복됐다
(부분체결인데 주문 수량 그대로 기록, 10/8 부국철강 100주 주문·34주 체결 사례 등). 원인은
주문 실행 함수(kis_order_fn)가 돌려준 결과 dict를 쓰는 쪽마다 제각각 "주문 수량"을 그대로
썼기 때문이다. 이 모듈은 그 판정을 한 곳으로 모은다 — trade_history에는 항상 실제 체결
수량만 남아야 한다.

같은 이유로 trade_history.price도 주문가(신호가/지정가)가 그대로 기록되어 실제 체결평균가와
달랐다(009830 9/28 KIS 체결 8주 @32,178원 vs DB 32,200원 — 매도 pnl·손절 실행가 분석에
오차 발생). resolve_fill_price()가 그 판정도 한 곳으로 모은다."""
from typing import Any, Dict, Optional, Tuple


def resolve_filled_quantity(order: Dict[str, Any], requested_qty: float) -> Optional[float]:
    """kis_order 실행 결과 dict에서 trade_history에 기록할 실제 체결 수량을 정한다.

    - order["filled_qty"]가 있으면 그 값(완전·부분체결 모두 포함, 신뢰할 수 있는 실체결량).
    - order["fill_unconfirmed"]=True(체결조회 API 자체가 실패해 판정 불가)면 None을 반환해
      호출부가 보유수량 재조회(reconcile_uncertain_order 등 기존 UNCERTAIN 처리 경로)로
      넘기게 한다 — 체결 수량을 모르는 채로 주문 수량을 그대로 기록하면 안 된다.
    - 둘 다 없으면(체결 확인 로직이 아직 없는 레거시 호출부) requested_qty를 그대로 쓴다."""
    if order.get("fill_unconfirmed"):
        return None
    filled = order.get("filled_qty")
    if filled is not None:
        return filled
    return requested_qty


def partial_fill_note(filled_qty: Optional[float], requested_qty: Optional[float]) -> str:
    """부분체결이면 "일부 체결 N/M주 (미체결 K주)", 전량체결이거나 판정 불가면 빈 문자열."""
    if filled_qty is None or requested_qty is None or filled_qty >= requested_qty:
        return ""
    remain = requested_qty - filled_qty
    return f"일부 체결 {filled_qty:.0f}/{requested_qty:.0f}주 (미체결 {remain:.0f}주)"


def resolve_fill_price(avg_fill_price: Optional[float], order_price: float) -> Tuple[float, bool]:
    """trade_history에 기록할 가격을 정한다 — (기록값, 주문가로 대체했는지 여부).

    avg_fill_price(KIS 체결평균가 조회 결과)가 있으면 그 값을 쓰고, 조회 자체가 실패했거나
    호출부가 체결가를 못 구한 경우(0/None)에는 기존처럼 주문가(order_price)로 대체한다 —
    trade_history.price는 항상 값이 있어야 하므로 대체는 유지하되, 대체 여부를 반환해
    호출부가 로그에 "주문가 대체"를 남길 수 있게 한다."""
    if avg_fill_price:
        return float(avg_fill_price), False
    return float(order_price), True
