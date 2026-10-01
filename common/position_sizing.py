"""
common/position_sizing.py - 리스크 기반 포지션 사이징 공용 로직 (PM 승인, 2026-09-30)

stock_trader(자동매매 스캔)와 router/handlers/order_handler.py(채팅 직접 매매) 양쪽이
동일한 기준으로 종목당 매수 금액 상한을 계산하도록 stock_trader/main.py에 있던 구현을
이곳으로 옮겼다(중복 구현 금지).

ML 정확도(42~64%)가 확신도 기반 사이징의 근거가 되기엔 약해 "종목당 최대 손실 고정"
방식을 쓴다: 기본 매수금액 = 자산 × RISK_PER_TRADE_PCT(%) ÷ |손절률(%)|, 여기에
ATR(20일) 변동성 구간별 배율을 곱해 최종 상한을 정한다.
"""

DEFAULT_STOP_LOSS_PCT = 7.0
# ATR(20일, stock_daily_ohlcv)을 현재가 대비 %로 환산한 변동성 구간별 배율 — (상한%, 배율) 오름차순.
VOLATILITY_BANDS = ((2.0, 1.00), (4.0, 0.75), (6.0, 0.50))
VOLATILITY_MULT_HIGH = 0.30        # 6% 초과
VOLATILITY_MULT_FALLBACK = 0.75    # ATR 계산 불가(데이터 부족) 시 보수적 처리
ATR_PERIOD = 20


def compute_base_amount(equity: float, stop_loss_pct: float, risk_per_trade_pct: float) -> float:
    """기본 매수금액 = 자산 × RISK_PER_TRADE_PCT(%) ÷ |손절률(%)|.
    equity/stop_loss_pct/risk_per_trade_pct 는 모두 퍼센트 단위가 아닌 실제 원/퍼센트 숫자
    (예: stop_loss_pct=7.0 은 -7%)."""
    stop_loss_pct = abs(stop_loss_pct)
    if equity <= 0 or stop_loss_pct <= 0:
        return 0.0
    return equity * (risk_per_trade_pct / 100) / (stop_loss_pct / 100)


def compute_atr_pct(rows: list, cur_price: float, period: int = None) -> float:
    """최근 `period`일 ATR(True Range 단순평균)을 현재가 대비 %로 환산.
    데이터 부족(period+1개 미만)이거나 가격이 0 이하면 None (호출부가 보수적 배율로 폴백)."""
    period = period or ATR_PERIOD
    if not cur_price or cur_price <= 0 or len(rows) < period + 1:
        return None
    try:
        highs = [float(r["high"]) for r in rows]
        lows = [float(r["low"]) for r in rows]
        closes = [float(r["close"]) for r in rows]
    except (KeyError, TypeError, ValueError):
        return None
    trs = [
        max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        for i in range(1, len(rows))
    ]
    recent = trs[-period:]
    if len(recent) < period:
        return None
    atr = sum(recent) / period
    return (atr / cur_price) * 100


def volatility_multiplier(atr_pct: float) -> float:
    """ATR%(현재가 대비) 구간별 변동성 배율. atr_pct=None(계산 불가)이면 보수적 기본값."""
    if atr_pct is None:
        return VOLATILITY_MULT_FALLBACK
    for upper, mult in VOLATILITY_BANDS:
        if atr_pct <= upper:
            return mult
    return VOLATILITY_MULT_HIGH
