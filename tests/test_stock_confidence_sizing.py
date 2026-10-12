"""
dashboard/main.py _apply_stock_confidence_sizing() 단위 테스트 (PM 승인, 2026-09-30).

배경: 리스크 기반 사이징 재설계로 "종목당 최대 손실 고정" 방식을 쓴다. stock_trader가
기본×변동성 금액(signal["base_amount"])과 예수금(signal["cash"])을 신호에 실어 보내면,
dashboard는 AI 판단(decision_engine) 직후 execution_guard.execute() 호출 전에 확신도
배율(EXECUTE=1.0/EXECUTE_SMALL=0.5)을 곱해 최종 수량을 정한다. 기존 "EXECUTE_SMALL이면
qty를 그대로 절반으로 나누는" 로직을 대체하므로, 이중 적용(절반 적용 후 또 배율 적용)이
되지 않는지가 핵심 검증 포인트다.
"""
import sys
import unittest
from unittest.mock import MagicMock

# dashboard.main 로드 전 필요한 외부 모듈 모킹 (test_dashboard_chart_rate_limit.py 와 동일)
for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

import dashboard.main as dm  # noqa: E402


class TestApplyStockConfidenceSizing(unittest.TestCase):
    def test_execute_uses_full_confidence_multiplier(self):
        signal = {"price": 10_000, "qty": 1, "base_amount": 1_000_000, "cash": 50_000_000}
        qty = dm._apply_stock_confidence_sizing(signal, is_small=False)
        self.assertEqual(qty, 100)  # 1,000,000 / 10,000 × 1.0

    def test_execute_small_uses_half_confidence_multiplier(self):
        signal = {"price": 10_000, "qty": 1, "base_amount": 1_000_000, "cash": 50_000_000}
        qty = dm._apply_stock_confidence_sizing(signal, is_small=True)
        self.assertEqual(qty, 50)  # 1,000,000 × 0.5 / 10,000

    def test_no_double_application_small_is_exactly_half_of_execute(self):
        # 이중 적용(절반 적용 후 또 배율 적용) 방지 확인: EXECUTE_SMALL 결과는 EXECUTE의
        # 정확히 절반이어야 하고, 그 절반의 또 절반(1/4)이 되면 안 된다.
        signal = {"price": 10_000, "qty": 1, "base_amount": 2_000_000, "cash": 50_000_000}
        qty_execute = dm._apply_stock_confidence_sizing(dict(signal), is_small=False)
        qty_small = dm._apply_stock_confidence_sizing(dict(signal), is_small=True)
        self.assertEqual(qty_execute, 200)
        self.assertEqual(qty_small, 100)
        self.assertEqual(qty_small, qty_execute // 2)
        self.assertNotEqual(qty_small, qty_execute // 4)

    def test_final_amount_capped_by_cash_when_exceeding(self):
        # 기본×변동성×확신도가 예수금을 초과하면 예수금 기준으로 제한한다.
        signal = {"price": 10_000, "qty": 1, "base_amount": 5_000_000, "cash": 1_200_000}
        qty = dm._apply_stock_confidence_sizing(signal, is_small=False)
        self.assertEqual(qty, 120)  # min(5,000,000, 1,200,000) / 10,000

    def test_cash_cap_applied_after_confidence_multiplier_not_before(self):
        # base_amount(4,000,000) × 0.5(EXECUTE_SMALL) = 2,000,000 은 예수금(3,000,000) 이내라
        # 캡이 걸리지 않아야 한다 — 캡을 배율 적용 전에 걸면 결과가 달라진다.
        signal = {"price": 10_000, "qty": 1, "base_amount": 4_000_000, "cash": 3_000_000}
        qty = dm._apply_stock_confidence_sizing(signal, is_small=True)
        self.assertEqual(qty, 200)  # 4,000,000×0.5=2,000,000 / 10,000 (캡 미적용)

    def test_skip_when_amount_rounds_down_below_one_share(self):
        # [AT] fix/sizing-below-one-share: 과거에는 max(1, ...)로 1주를 강제 매수해
        # RISK_PER_TRADE_PCT 손실 상한이 고가 종목에서 깨졌다. 이 가드를 되돌리면(원복해서
        # max(1, int(final_amount // price))로 바꾸면) 아래 assertEqual(qty, 0)이
        # 실패하며 qty == 1이 나온다 — 이 테스트가 revert 감지 역할을 한다.
        signal = {"price": 10_000, "qty": 1, "base_amount": 100, "cash": 50_000_000}
        qty = dm._apply_stock_confidence_sizing(signal, is_small=False)
        self.assertEqual(qty, 0)
        self.assertIn("1주 가격 미만", signal["sizing_skip_reason"])
        self.assertIn("100원", signal["sizing_skip_reason"])
        self.assertIn("10,000원", signal["sizing_skip_reason"])

    def test_execute_small_high_price_stock_skips_below_one_share(self):
        # EXECUTE_SMALL(0.5배) 적용 후 금액이 고가 종목 1주 가격보다 작아지는 경우.
        signal = {"price": 1_000_000, "qty": 1, "base_amount": 1_500_000, "cash": 50_000_000}
        qty = dm._apply_stock_confidence_sizing(signal, is_small=True)
        self.assertEqual(qty, 0)  # 1,500,000 × 0.5 = 750,000 < 1,000,000
        self.assertIn("sizing_skip_reason", signal)

    def test_cash_cap_causing_skip_below_one_share(self):
        # 예수금 캡 적용 후 금액이 1주 가격보다 작아지는 경우.
        signal = {"price": 1_000_000, "qty": 1, "base_amount": 5_000_000, "cash": 800_000}
        qty = dm._apply_stock_confidence_sizing(signal, is_small=False)
        self.assertEqual(qty, 0)  # min(5,000,000, 800,000) = 800,000 < 1,000,000
        self.assertIn("sizing_skip_reason", signal)

    def test_exactly_one_share_threshold_still_passes(self):
        # final_amount == price(1주 가격)이면 스킵이 아니라 정확히 1주 매수로 통과해야 한다.
        signal = {"price": 10_000, "qty": 1, "base_amount": 10_000, "cash": 50_000_000}
        qty = dm._apply_stock_confidence_sizing(signal, is_small=False)
        self.assertEqual(qty, 1)
        self.assertNotIn("sizing_skip_reason", signal)

    def test_large_order_quantity_unchanged_above_threshold(self):
        # final_amount >= price인 정상 대량 주문 수량 계산은 기존과 동일해야 한다.
        signal = {"price": 50_000, "qty": 1, "base_amount": 10_000_000, "cash": 50_000_000}
        qty = dm._apply_stock_confidence_sizing(signal, is_small=False)
        self.assertEqual(qty, 200)  # 10,000,000 / 50,000

    def test_missing_base_amount_falls_back_to_legacy_half_qty_for_small(self):
        # 구버전 신호(base_amount 없음)는 기존 EXECUTE_SMALL 절반 로직으로만 폴백한다.
        signal = {"price": 10_000, "qty": 11}
        qty = dm._apply_stock_confidence_sizing(signal, is_small=True)
        self.assertEqual(qty, 5)  # 11 // 2

    def test_missing_base_amount_execute_leaves_qty_unchanged(self):
        signal = {"price": 10_000, "qty": 7}
        qty = dm._apply_stock_confidence_sizing(signal, is_small=False)
        self.assertEqual(qty, 7)

    def test_zero_base_amount_treated_as_missing(self):
        signal = {"price": 10_000, "qty": 11, "base_amount": 0}
        qty = dm._apply_stock_confidence_sizing(signal, is_small=True)
        self.assertEqual(qty, 5)  # legacy 절반 폴백


if __name__ == "__main__":
    unittest.main()
