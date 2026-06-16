"""
피처 엔지니어링 모듈
OHLCV + 기술적 지표 → 학습용 피처 벡터 변환
"""
from typing import List, Dict, Optional
from ml.indicators import (
    sma, ema, rsi as calc_rsi, macd as calc_macd,
    bollinger_bands, atr, stochastic, volume_ratio
)


def build_features(ohlcv: List[Dict], lookback: int = 60) -> List[Dict]:
    """
    OHLCV 데이터를 학습용 피처로 변환
    각 시점마다 피처 벡터 생성

    피처 목록:
    - 가격 변화율 (1일, 3일, 5일, 10일)
    - RSI (14)
    - MACD 히스토그램
    - 볼린저밴드 %B
    - 스토캐스틱 K, D
    - 거래량 비율
    - ATR (변동성)
    - MA 크로스 여부
    - 레이블: 다음 n일 후 수익률 (학습 타겟)
    """
    if len(ohlcv) < lookback:
        return []

    closes  = [float(c["close"]) for c in ohlcv]
    highs   = [float(c["high"]) for c in ohlcv]
    lows    = [float(c["low"]) for c in ohlcv]
    volumes = [float(c["volume"]) for c in ohlcv]

    # 지표 계산
    rsi14      = calc_rsi(closes, 14)
    macd_vals  = calc_macd(closes, 12, 26, 9)
    bb         = bollinger_bands(closes, 20, 2.0)
    atr14      = atr(highs, lows, closes, 14)
    stoch      = stochastic(highs, lows, closes, 14, 3)
    vol_r      = volume_ratio(volumes, 20)
    sma5       = sma(closes, 5)
    sma20      = sma(closes, 20)
    sma60      = sma(closes, 60)
    ema12      = ema(closes, 12)
    ema26      = ema(closes, 26)

    features = []

    for i in range(lookback, len(ohlcv) - 5):  # 5일 후 레이블 필요
        price = closes[i]
        if price <= 0:
            continue

        # 가격 변화율
        def chg(days):
            j = i - days
            return (price - closes[j]) / closes[j] * 100 if j >= 0 and closes[j] > 0 else 0

        # 레이블: 5일 후 수익률
        future_price = closes[i + 5]
        label_5d = (future_price - price) / price * 100
        label_buy = 1 if label_5d > 1.5 else 0  # 1.5% 이상 상승 = 매수 시점

        feat = {
            "ts": ohlcv[i].get("ts", ""),
            "price": price,

            # 가격 변화율
            "chg_1d": chg(1),
            "chg_3d": chg(3),
            "chg_5d": chg(5),
            "chg_10d": chg(10),
            "chg_20d": chg(20),

            # 모멘텀 지표
            "rsi": rsi14[i] or 50,
            "macd_hist": macd_vals["histogram"][i] or 0,
            "stoch_k": stoch["k"][i] or 50,
            "stoch_d": stoch["d"][i] or 50,

            # 추세 지표
            "bb_pct": bb["percent_b"][i] or 0.5,
            "bb_bw": bb["bandwidth"][i] or 0,
            "sma5_dist": (price - sma5[i]) / price * 100 if sma5[i] else 0,
            "sma20_dist": (price - sma20[i]) / price * 100 if sma20[i] else 0,
            "sma60_dist": (price - sma60[i]) / price * 100 if sma60[i] else 0,
            "ema12_dist": (price - ema12[i]) / price * 100 if ema12[i] else 0,

            # 변동성
            "atr_pct": (atr14[i] / price * 100) if atr14[i] else 0,

            # 거래량
            "vol_ratio": vol_r[i] or 1.0,

            # 크로스 시그널
            "golden_cross": 1 if (sma5[i] and sma20[i] and sma5[i] > sma20[i] and
                                   sma5[i-1] and sma20[i-1] and sma5[i-1] <= sma20[i-1]) else 0,
            "dead_cross": 1 if (sma5[i] and sma20[i] and sma5[i] < sma20[i] and
                                 sma5[i-1] and sma20[i-1] and sma5[i-1] >= sma20[i-1]) else 0,
            "macd_cross_up": 1 if (macd_vals["histogram"][i] and macd_vals["histogram"][i] > 0 and
                                    macd_vals["histogram"][i-1] and macd_vals["histogram"][i-1] <= 0) else 0,

            # 레이블
            "label_5d_return": round(label_5d, 2),
            "label_buy": label_buy,
        }
        features.append(feat)

    return features


def get_feature_names() -> List[str]:
    """피처 이름 목록 반환"""
    return [
        "chg_1d", "chg_3d", "chg_5d", "chg_10d", "chg_20d",
        "rsi", "macd_hist", "stoch_k", "stoch_d",
        "bb_pct", "bb_bw", "sma5_dist", "sma20_dist", "sma60_dist", "ema12_dist",
        "atr_pct", "vol_ratio",
        "golden_cross", "dead_cross", "macd_cross_up"
    ]


def feature_summary(features: List[Dict]) -> Dict:
    """피처 통계 요약"""
    if not features:
        return {}

    total = len(features)
    buy_signals = sum(f["label_buy"] for f in features)

    return {
        "total_samples": total,
        "buy_signals": buy_signals,
        "sell_signals": total - buy_signals,
        "buy_ratio": round(buy_signals / total * 100, 1),
        "avg_5d_return": round(sum(f["label_5d_return"] for f in features) / total, 2),
        "positive_samples": sum(1 for f in features if f["label_5d_return"] > 0),
        "feature_count": len(get_feature_names()),
        "date_range": f"{features[0]['ts'][:10]} ~ {features[-1]['ts'][:10]}" if features else "-",
    }
