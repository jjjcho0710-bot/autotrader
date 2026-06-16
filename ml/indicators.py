"""
기술적 지표 계산 모듈
RSI, MACD, 볼린저밴드, 이동평균, ATR, 스토캐스틱
"""
from typing import List, Dict, Optional
import math


def sma(prices: List[float], period: int) -> List[Optional[float]]:
    """단순 이동평균 (Simple Moving Average)"""
    result = [None] * len(prices)
    for i in range(period - 1, len(prices)):
        result[i] = sum(prices[i - period + 1:i + 1]) / period
    return result


def ema(prices: List[float], period: int) -> List[Optional[float]]:
    """지수 이동평균 (Exponential Moving Average)"""
    result = [None] * len(prices)
    if len(prices) < period:
        return result
    k = 2 / (period + 1)
    result[period - 1] = sum(prices[:period]) / period
    for i in range(period, len(prices)):
        result[i] = prices[i] * k + result[i - 1] * (1 - k)
    return result


def rsi(prices: List[float], period: int = 14) -> List[Optional[float]]:
    """RSI (Relative Strength Index) - 과매수/과매도 지표"""
    result = [None] * len(prices)
    if len(prices) < period + 1:
        return result

    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(prices)):
        if i > period:
            avg_gain = (avg_gain * (period - 1) + gains[i - 1]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i - 1]) / period

        if avg_loss == 0:
            result[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[i] = 100 - (100 / (1 + rs))

    return result


def macd(prices: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Dict:
    """
    MACD (Moving Average Convergence Divergence)
    Returns: {macd_line, signal_line, histogram}
    """
    ema_fast = ema(prices, fast)
    ema_slow = ema(prices, slow)

    macd_line = [None] * len(prices)
    for i in range(len(prices)):
        if ema_fast[i] is not None and ema_slow[i] is not None:
            macd_line[i] = ema_fast[i] - ema_slow[i]

    # 시그널 라인 (MACD의 EMA)
    macd_values = [v if v is not None else 0 for v in macd_line]
    signal_line = ema(macd_values, signal)

    histogram = [None] * len(prices)
    for i in range(len(prices)):
        if macd_line[i] is not None and signal_line[i] is not None:
            histogram[i] = macd_line[i] - signal_line[i]

    return {
        "macd": macd_line,
        "signal": signal_line,
        "histogram": histogram
    }


def bollinger_bands(prices: List[float], period: int = 20, std_dev: float = 2.0) -> Dict:
    """
    볼린저 밴드 (Bollinger Bands)
    Returns: {upper, middle, lower, bandwidth, percent_b}
    """
    middle = sma(prices, period)
    upper = [None] * len(prices)
    lower = [None] * len(prices)
    bandwidth = [None] * len(prices)
    percent_b = [None] * len(prices)

    for i in range(period - 1, len(prices)):
        window = prices[i - period + 1:i + 1]
        avg = middle[i]
        std = math.sqrt(sum((x - avg) ** 2 for x in window) / period)
        upper[i] = avg + std_dev * std
        lower[i] = avg - std_dev * std
        if upper[i] != lower[i]:
            bandwidth[i] = (upper[i] - lower[i]) / avg * 100
            percent_b[i] = (prices[i] - lower[i]) / (upper[i] - lower[i])

    return {
        "upper": upper,
        "middle": middle,
        "lower": lower,
        "bandwidth": bandwidth,
        "percent_b": percent_b
    }


def atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> List[Optional[float]]:
    """ATR (Average True Range) - 변동성 지표"""
    result = [None] * len(closes)
    if len(closes) < 2:
        return result

    true_ranges = []
    for i in range(1, len(closes)):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i - 1])
        lc = abs(lows[i] - closes[i - 1])
        true_ranges.append(max(hl, hc, lc))

    if len(true_ranges) < period:
        return result

    result[period] = sum(true_ranges[:period]) / period
    for i in range(period + 1, len(closes)):
        result[i] = (result[i - 1] * (period - 1) + true_ranges[i - 1]) / period

    return result


def stochastic(highs: List[float], lows: List[float], closes: List[float],
               k_period: int = 14, d_period: int = 3) -> Dict:
    """
    스토캐스틱 (Stochastic Oscillator)
    Returns: {k, d}
    """
    k_values = [None] * len(closes)

    for i in range(k_period - 1, len(closes)):
        window_high = max(highs[i - k_period + 1:i + 1])
        window_low = min(lows[i - k_period + 1:i + 1])
        if window_high != window_low:
            k_values[i] = (closes[i] - window_low) / (window_high - window_low) * 100
        else:
            k_values[i] = 50.0

    # D = K의 이동평균
    d_values = sma([v if v is not None else 0 for v in k_values], d_period)

    return {"k": k_values, "d": d_values}


def volume_ratio(volumes: List[float], period: int = 20) -> List[Optional[float]]:
    """거래량 비율 (현재 거래량 / 평균 거래량)"""
    avg_vol = sma(volumes, period)
    result = [None] * len(volumes)
    for i in range(period - 1, len(volumes)):
        if avg_vol[i] and avg_vol[i] > 0:
            result[i] = volumes[i] / avg_vol[i]
    return result


def calculate_all(ohlcv: List[Dict]) -> Dict:
    """
    OHLCV 데이터로 모든 지표 계산
    ohlcv: [{"ts", "open", "high", "low", "close", "volume"}, ...]
    """
    if not ohlcv:
        return {}

    closes = [float(c["close"]) for c in ohlcv]
    highs  = [float(c["high"]) for c in ohlcv]
    lows   = [float(c["low"]) for c in ohlcv]
    vols   = [float(c["volume"]) for c in ohlcv]

    rsi_vals = rsi(closes, 14)
    macd_vals = macd(closes, 12, 26, 9)
    bb_vals = bollinger_bands(closes, 20, 2.0)
    atr_vals = atr(highs, lows, closes, 14)
    stoch_vals = stochastic(highs, lows, closes, 14, 3)
    sma5  = sma(closes, 5)
    sma20 = sma(closes, 20)
    sma60 = sma(closes, 60)
    ema12 = ema(closes, 12)
    vol_ratio = volume_ratio(vols, 20)

    # 마지막 값 반환 (최신 시점)
    idx = len(closes) - 1
    return {
        "close": closes[idx],
        "rsi": rsi_vals[idx],
        "macd": macd_vals["macd"][idx],
        "macd_signal": macd_vals["signal"][idx],
        "macd_hist": macd_vals["histogram"][idx],
        "bb_upper": bb_vals["upper"][idx],
        "bb_middle": bb_vals["middle"][idx],
        "bb_lower": bb_vals["lower"][idx],
        "bb_pct": bb_vals["percent_b"][idx],
        "atr": atr_vals[idx],
        "stoch_k": stoch_vals["k"][idx],
        "stoch_d": stoch_vals["d"][idx],
        "sma5": sma5[idx],
        "sma20": sma20[idx],
        "sma60": sma60[idx],
        "ema12": ema12[idx],
        "vol_ratio": vol_ratio[idx],
        # 시그널
        "golden_cross": (sma5[idx] or 0) > (sma20[idx] or 0),
        "dead_cross": (sma5[idx] or 0) < (sma20[idx] or 0),
        "rsi_oversold": (rsi_vals[idx] or 50) < 30,
        "rsi_overbought": (rsi_vals[idx] or 50) > 70,
        "macd_bullish": (macd_vals["histogram"][idx] or 0) > 0,
        "price_above_bb_upper": closes[idx] > (bb_vals["upper"][idx] or float('inf')),
        "price_below_bb_lower": closes[idx] < (bb_vals["lower"][idx] or 0),
    }
