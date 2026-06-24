"""
RSI 반등 전략
RSI가 과매도(30 이하) 구간에서 반등 시 매수
RSI가 과매수(60 이상) 구간에서 매도
"""
from dataclasses import dataclass
import logging

logger = logging.getLogger("strategy.rsi")


@dataclass
class RSIConfig:
    period:       int   = 14
    entry:        float = 30.0   # 매수 RSI 기준 (이하)
    exit:         float = 60.0   # 매도 RSI 기준 (이상)
    stop_loss:    float = -0.03  # 손절 -3%
    buy_amount:   int   = 500000
    max_positions: int  = 5


class RSIStrategy:
    def __init__(self, cfg: RSIConfig):
        self.cfg = cfg

    def _calc_rsi(self, prices: list) -> float:
        period = self.cfg.period
        if len(prices) < period + 1:
            return 50.0
        gains, losses = [], []
        for i in range(-period, 0):
            diff = prices[i] - prices[i - 1]
            gains.append(max(diff, 0))
            losses.append(max(-diff, 0))
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def generate_signal(self, symbol: str, prices: list) -> str:
        if len(prices) < self.cfg.period + 2:
            return "HOLD"

        rsi_now  = self._calc_rsi(prices)
        rsi_prev = self._calc_rsi(prices[:-1])

        # 과매도 반등: RSI가 entry 이하에서 위로 올라오는 순간
        if rsi_prev <= self.cfg.entry and rsi_now > self.cfg.entry:
            logger.info(f"🟢 RSI 반등 [{symbol}] RSI={rsi_now:.1f} (prev={rsi_prev:.1f})")
            return "BUY"

        # 과매수: RSI가 exit 이상
        if rsi_now >= self.cfg.exit:
            logger.info(f"🔴 RSI 과매수 [{symbol}] RSI={rsi_now:.1f}")
            return "SELL"

        return "HOLD"

    def check_stop_loss(self, avg: float, cur: float) -> bool:
        return (cur - avg) / avg <= self.cfg.stop_loss

    def check_take_profit(self, avg: float, cur: float) -> bool:
        return False  # RSI exit 신호로 매도

    def calc_buy_qty(self, price: float) -> int:
        return max(1, self.cfg.buy_amount // price)
