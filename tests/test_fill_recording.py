"""
common/fill_recording.py의 resolve_fill_price() 단위 테스트.

배경: trade_history.price가 체결평균가가 아니라 주문가(신호가/지정가) 기준으로 기록되어
매도 pnl·손절 실행가 분석에 오차가 생겼다(009830 9/28 KIS 체결 8주 @32,178원 vs DB
32,200원). resolve_fill_price()가 체결평균가 우선·주문가 대체 판정을 한 곳으로 모은다.
"""
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from common.fill_recording import resolve_fill_price  # noqa: E402


class TestResolveFillPrice(unittest.TestCase):
    def test_uses_avg_fill_price_when_present(self):
        price, used_fallback = resolve_fill_price(32178.0, 32200)
        self.assertEqual(price, 32178.0)
        self.assertFalse(used_fallback)

    def test_falls_back_to_order_price_when_missing(self):
        price, used_fallback = resolve_fill_price(None, 32200)
        self.assertEqual(price, 32200.0)
        self.assertTrue(used_fallback)

    def test_falls_back_to_order_price_when_zero(self):
        """조회 실패 응답(avg_prvs=0 등)도 체결가 없음과 동일하게 주문가로 대체한다."""
        price, used_fallback = resolve_fill_price(0.0, 32200)
        self.assertEqual(price, 32200.0)
        self.assertTrue(used_fallback)

    def test_return_type_is_float(self):
        price, _ = resolve_fill_price(None, 32200)
        self.assertIsInstance(price, float)


if __name__ == "__main__":
    unittest.main()
