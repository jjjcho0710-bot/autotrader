"""
MACD 시그널 전략
MACD 라인이 시그널 라인을 골든크로스 → 매수
데드크로스 → 매도
"""
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class MACDConfig:
    fast: int = 12           # 단기 EMA
    slow: int = 26           # 장기 EMA
    signal: int = 9          # 시그널 기간
    stop_loss: float = -0.03   # 손절 -3%
    take_profit: float = 0.07  # 익절 +7%
    buy_amount_krw: float = 500_000   # 1회 매수금액


class MACDStrategy:
    """MACD 크로스 전략"""

    def __init__(self, cfg: MACDConfig = None):
        self.cfg = cfg or MACDConfig()

    def _ema(self, prices: list, period: int) -> list:
        """EMA 계산"""
        if len(prices) < period:
            return []
        k = 2 / (period + 1)
        ema = [sum(prices[:period]) / period]
        for p in prices[period:]:
            ema.append(p * k + ema[-1] * (1 - k))
        return ema

    def calculate_macd(self, prices: list) -> tuple:
        """
        returns: (macd_line, signal_line, histogram) — 최신값 기준
        """
        if len(prices) < self.cfg.slow + self.cfg.signal:
            return None, None, None

        ema_fast = self._ema(prices, self.cfg.fast)
        ema_slow = self._ema(prices, self.cfg.slow)

        # MACD 라인: fast EMA - slow EMA (길이 맞추기)
        min_len = min(len(ema_fast), len(ema_slow))
        macd_line = [ema_fast[-min_len + i] - ema_slow[-min_len + i] for i in range(min_len)]

        if len(macd_line) < self.cfg.signal:
            return None, None, None

        signal_line = self._ema(macd_line, self.cfg.signal)

        macd_now   = macd_line[-1]
        signal_now = signal_line[-1]
        hist       = macd_now - signal_now

        return macd_now, signal_now, hist

    def generate_signal(self, pair: str, prices: list) -> Optional[str]:
        """
        prices: 최근 종가 리스트 (오래된 순)
        return: 'BUY' | 'SELL' | None
        """
        needed = self.cfg.slow + self.cfg.signal + 2
        if len(prices) < needed:
            return None

        # 현재
        m_now, s_now, _ = self.calculate_macd(prices)
        # 이전
        m_prev, s_prev, _ = self.calculate_macd(prices[:-1])

        if None in (m_now, s_now, m_prev, s_prev):
            return None

        # 골든크로스: MACD가 시그널 위로
        if m_prev <= s_prev and m_now > s_now:
            logger.info(f"📈 MACD 골든크로스 [{pair}] MACD={m_now:.2f} / Signal={s_now:.2f}")
            return "BUY"

        # 데드크로스: MACD가 시그널 아래로
        if m_prev >= s_prev and m_now < s_now:
            logger.info(f"📉 MACD 데드크로스 [{pair}] MACD={m_now:.2f} / Signal={s_now:.2f}")
            return "SELL"

        return None

    def check_stop_loss(self, avg_price: float, cur_price: float) -> bool:
        rate = (cur_price - avg_price) / avg_price
        if rate <= self.cfg.stop_loss:
            logger.warning(f"🛑 코인 손절 트리거: {rate:.1%}")
            return True
        return False

    def check_take_profit(self, avg_price: float, cur_price: float) -> bool:
        rate = (cur_price - avg_price) / avg_price
        if rate >= self.cfg.take_profit:
            logger.info(f"🎯 코인 익절 트리거: {rate:.1%}")
            return True
        return False
