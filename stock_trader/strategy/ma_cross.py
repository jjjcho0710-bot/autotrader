"""
MA 크로스 전략
단기 이평(MA5) / 장기 이평(MA20) 골든크로스 → 매수
데드크로스 → 매도
"""
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class MACrossConfig:
    short_period: int = 5        # 단기 이평
    long_period: int = 20        # 장기 이평
    stop_loss: float = -0.02     # 손절 -2%
    take_profit: float = 0.05    # 익절 +5%
    buy_amount: int = 500_000    # 1회 매수금액 (원)
    max_positions: int = 5       # 최대 보유 종목수


class MACrossStrategy:
    """이동평균 크로스 전략"""

    def __init__(self, cfg: MACrossConfig = None):
        self.cfg = cfg or MACrossConfig()

    def calculate_ma(self, prices: list, period: int) -> Optional[float]:
        if len(prices) < period:
            return None
        return sum(prices[-period:]) / period

    def generate_signal(self, symbol: str, prices: list) -> Optional[str]:
        """
        prices: 최근 종가 리스트 (오래된 순)
        return: 'BUY' | 'SELL' | None
        """
        if len(prices) < self.cfg.long_period + 1:
            return None

        # 현재 이평
        ma_short_now = self.calculate_ma(prices, self.cfg.short_period)
        ma_long_now  = self.calculate_ma(prices, self.cfg.long_period)

        # 이전 이평 (크로스 확인용)
        ma_short_prev = self.calculate_ma(prices[:-1], self.cfg.short_period)
        ma_long_prev  = self.calculate_ma(prices[:-1], self.cfg.long_period)

        if None in (ma_short_now, ma_long_now, ma_short_prev, ma_long_prev):
            return None

        # 골든크로스: 단기가 장기를 위로 돌파
        golden_cross = (ma_short_prev <= ma_long_prev) and (ma_short_now > ma_long_now)
        # 데드크로스: 단기가 장기를 아래로 돌파
        dead_cross   = (ma_short_prev >= ma_long_prev) and (ma_short_now < ma_long_now)

        if golden_cross:
            logger.info(f"📈 골든크로스 [{symbol}] MA{self.cfg.short_period}={ma_short_now:.0f} / MA{self.cfg.long_period}={ma_long_now:.0f}")
            return "BUY"
        if dead_cross:
            logger.info(f"📉 데드크로스 [{symbol}] MA{self.cfg.short_period}={ma_short_now:.0f} / MA{self.cfg.long_period}={ma_long_now:.0f}")
            return "SELL"

        return None

    def check_stop_loss(self, avg_price: int, cur_price: int) -> bool:
        """손절 여부"""
        rate = (cur_price - avg_price) / avg_price
        if rate <= self.cfg.stop_loss:
            logger.warning(f"🛑 손절 트리거: {rate:.1%} (기준 {self.cfg.stop_loss:.1%})")
            return True
        return False

    def check_take_profit(self, avg_price: int, cur_price: int) -> bool:
        """익절 여부"""
        rate = (cur_price - avg_price) / avg_price
        if rate >= self.cfg.take_profit:
            logger.info(f"🎯 익절 트리거: {rate:.1%} (기준 {self.cfg.take_profit:.1%})")
            return True
        return False

    def calc_buy_qty(self, price: int) -> int:
        """매수 수량 계산"""
        qty = self.cfg.buy_amount // price
        return max(1, qty)
