"""코인 RSI 반등 전략"""
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class RSIConfig:
    period: int = 14
    entry: float = 35.0   # 매수 기준 (이하)
    exit: float = 65.0    # 매도 기준 (이상)
    stop_loss: float = -0.02
    take_profit: float = 0.005  # +0.5% 익절
    buy_amount_krw: float = 10000

class RSIStrategy:
    def __init__(self, cfg: RSIConfig = None):
        self.cfg = cfg or RSIConfig()

    def _calc_rsi(self, prices: list) -> float:
        period = self.cfg.period
        if len(prices) < period + 1:
            return 50.0
        gains = [max(prices[i]-prices[i-1], 0) for i in range(-period, 0)]
        losses = [max(prices[i-1]-prices[i], 0) for i in range(-period, 0)]
        ag = sum(gains) / period
        al = sum(losses) / period
        if al == 0:
            return 100.0
        return 100 - (100 / (1 + ag/al))

    def generate_signal(self, pair: str, prices: list) -> str:
        if len(prices) < self.cfg.period + 2:
            return None
        rsi = self._calc_rsi(prices)
        rsi_prev = self._calc_rsi(prices[:-1])

        # 2분 연속 상승 확인 (가짜 반등 방지)
        rsi_prev2 = self._calc_rsi(prices[:-2]) if len(prices) > self.cfg.period + 3 else rsi_prev
        if rsi_prev2 <= self.cfg.entry and rsi_prev <= self.cfg.entry and rsi > rsi_prev:
            logger.info(f"🟢 코인 RSI 반등 [{pair}] RSI={rsi:.1f} (prev={rsi_prev:.1f})")
            return "BUY"
        if rsi >= self.cfg.exit:
            logger.info(f"🔴 코인 RSI 과매수 [{pair}] RSI={rsi:.1f}")
            return "SELL"
        return None

    def check_stop_loss(self, avg_price: float, cur_price: float) -> bool:
        return (cur_price - avg_price) / avg_price <= self.cfg.stop_loss

    def check_take_profit(self, avg_price: float, cur_price: float) -> bool:
        return (cur_price - avg_price) / avg_price >= self.cfg.take_profit
