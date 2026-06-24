"""
볼린저밴드 전략
가격이 하단밴드 터치 시 매수, 상단밴드 터치 시 매도
"""
from dataclasses import dataclass
import logging

logger = logging.getLogger("strategy.bollinger")


@dataclass
class BollingerConfig:
    period:       int   = 20
    std_dev:      float = 2.0
    stop_loss:    float = -0.03
    buy_amount:   int   = 500000
    max_positions: int  = 5


class BollingerStrategy:
    def __init__(self, cfg: BollingerConfig):
        self.cfg = cfg

    def _calc_bands(self, prices: list):
        period = self.cfg.period
        if len(prices) < period:
            return None, None, None
        window = prices[-period:]
        ma = sum(window) / period
        variance = sum((p - ma) ** 2 for p in window) / period
        std = variance ** 0.5
        upper = ma + self.cfg.std_dev * std
        lower = ma - self.cfg.std_dev * std
        return upper, ma, lower

    def generate_signal(self, symbol: str, prices: list) -> str:
        if len(prices) < self.cfg.period + 1:
            return "HOLD"

        upper, ma, lower = self._calc_bands(prices)
        upper_prev, ma_prev, lower_prev = self._calc_bands(prices[:-1])

        if upper is None or upper_prev is None:
            return "HOLD"

        cur = prices[-1]
        prev = prices[-2]

        # 하단밴드 터치 후 반등: 이전에 하단 이하였다가 지금 위로
        if prev <= lower_prev and cur > lower:
            logger.info(f"🟢 볼린저 하단 반등 [{symbol}] 현재={cur:,} 하단={lower:,.0f}")
            return "BUY"

        # 상단밴드 터치: 매도
        if cur >= upper:
            logger.info(f"🔴 볼린저 상단 터치 [{symbol}] 현재={cur:,} 상단={upper:,.0f}")
            return "SELL"

        return "HOLD"

    def check_stop_loss(self, avg: float, cur: float) -> bool:
        return (cur - avg) / avg <= self.cfg.stop_loss

    def check_take_profit(self, avg: float, cur: float) -> bool:
        return False

    def calc_buy_qty(self, price: float) -> int:
        return max(1, self.cfg.buy_amount // price)
