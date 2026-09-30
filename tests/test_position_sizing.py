"""
리스크 기반 포지션 사이징(PM 승인, 2026-09-30) 단위 테스트: stock_trader/main.py 쪽 절반
(기본 매수금액, ATR 변동성 배율, 시작 시 정합성 경고).

배경: ML 정확도(42~64%)가 확신도 기반 사이징의 근거가 되기엔 약해 "종목당 최대 손실
고정" 방식으로 재설계했다. 기본 매수금액 = 자산×RISK_PER_TRADE_PCT÷|손절률|, 여기에
ATR(20일) 변동성 구간별 배율을 곱한다. 확신도 배율(EXECUTE/EXECUTE_SMALL)은 AI 판단
이후 dashboard 쪽(tests/test_stock_confidence_sizing.py)에서 검증한다.
"""
import unittest
from unittest import mock

# tests.test_stock_trader_stop_loss 가 import 시점에 asyncpg/redis/aiohttp 스텁과 sys.path
# (stock_trader/)를 먼저 설정하므로, 뒤이은 `import main`은 이미 로드된 stock_trader/main.py를
# 그대로 재사용한다(중복 import 아님).
from tests.test_stock_trader_stop_loss import StockTrader  # noqa: E402
import main  # noqa: E402 (stock_trader/main.py)
from common.config import config  # noqa: E402


def _trader():
    return StockTrader.__new__(StockTrader)  # __init__(DB/KIS 연결) 없이 순수 메서드만 사용


class TestComputeBaseAmount(unittest.TestCase):
    def test_pm_example_10m_equity_075pct_risk_7pct_stop_loss(self):
        # PM 예시: 1,000만 × 0.75% ÷ 7% ≈ 107만원
        amount = StockTrader._compute_base_amount(10_000_000, 7.0, 0.75)
        self.assertAlmostEqual(amount, 1_071_428.57, places=2)

    def test_negative_stop_loss_pct_is_treated_as_absolute(self):
        # strategy_config.stop_loss 는 -7 처럼 음수 퍼센트로 저장된다.
        amount_negative = StockTrader._compute_base_amount(10_000_000, -7.0, 0.75)
        amount_positive = StockTrader._compute_base_amount(10_000_000, 7.0, 0.75)
        self.assertEqual(amount_negative, amount_positive)

    def test_zero_or_negative_equity_returns_zero(self):
        self.assertEqual(StockTrader._compute_base_amount(0, 7.0, 0.75), 0.0)
        self.assertEqual(StockTrader._compute_base_amount(-100, 7.0, 0.75), 0.0)

    def test_zero_stop_loss_returns_zero_no_division_by_zero(self):
        self.assertEqual(StockTrader._compute_base_amount(10_000_000, 0, 0.75), 0.0)

    def test_higher_risk_pct_yields_proportionally_larger_amount(self):
        low = StockTrader._compute_base_amount(10_000_000, 7.0, 0.75)
        high = StockTrader._compute_base_amount(10_000_000, 7.0, 1.5)
        self.assertAlmostEqual(high, low * 2)


class TestComputeAtrPct(unittest.TestCase):
    def _flat_rows(self, n, high=110.0, low=90.0, close=100.0):
        return [{"high": high, "low": low, "close": close} for _ in range(n)]

    def test_insufficient_rows_returns_none(self):
        rows = self._flat_rows(20)  # period(20) + 1 = 21 개 필요
        self.assertIsNone(StockTrader._compute_atr_pct(rows, 100.0))

    def test_zero_or_negative_price_returns_none(self):
        rows = self._flat_rows(21)
        self.assertIsNone(StockTrader._compute_atr_pct(rows, 0))
        self.assertIsNone(StockTrader._compute_atr_pct(rows, -10))

    def test_known_true_range_produces_expected_atr_pct(self):
        # 종가 100 고정, 매일 high-low 폭 20(고가110/저가90) → TR=20, ATR=20, 현재가 100 → 2.0%
        rows = self._flat_rows(21, high=110.0, low=90.0, close=100.0)
        atr_pct = StockTrader._compute_atr_pct(rows, 100.0)
        self.assertAlmostEqual(atr_pct, 20.0)  # (20/100)*100

    def test_missing_columns_returns_none(self):
        rows = [{"close": 100.0} for _ in range(21)]  # high/low 없음
        self.assertIsNone(StockTrader._compute_atr_pct(rows, 100.0))


class TestVolatilityMultiplier(unittest.TestCase):
    def test_atr_none_falls_back_conservatively(self):
        self.assertEqual(StockTrader._volatility_multiplier(None), 0.75)

    def test_bands_boundaries(self):
        cases = [
            (0.0, 1.00), (2.0, 1.00),      # 2% 이하
            (2.01, 0.75), (4.0, 0.75),     # 2~4%
            (4.01, 0.50), (6.0, 0.50),     # 4~6%
            (6.01, 0.30), (10.0, 0.30),    # 6% 초과
        ]
        for atr_pct, expected in cases:
            self.assertEqual(
                StockTrader._volatility_multiplier(atr_pct), expected,
                f"atr_pct={atr_pct}"
            )


class FakeTrader:
    def __init__(self, balance):
        self._balance = balance

    async def get_balance(self):
        return self._balance


class TestPositionSizingStartupWarning(unittest.IsolatedAsyncioTestCase):
    async def test_warns_when_max_positions_times_base_amount_exceeds_equity(self):
        trader = _trader()
        # projected/equity = max_positions × risk_pct÷stop_loss_pct 는 자산 크기와 무관하다
        # (자산이 분자·분모 양쪽에서 상쇄됨). max_positions=15, 손절 -7%, 위험 0.75% →
        # 15×0.75/7 ≈ 1.61배 → 자산을 초과해야 한다.
        trader.trader = FakeTrader({"total": 5_000_000, "cash": 5_000_000})
        trader.strategies = {
            "MA크로스": {"is_active": True, "params": {"stop_loss": -7, "max_positions": 15}},
        }
        with mock.patch.object(main.logger, "warning") as warn:
            await trader._warn_if_position_sizing_exceeds_equity()
        self.assertTrue(any("포지션 사이징 경고" in call.args[0] for call in warn.call_args_list))

    async def test_no_warning_when_within_equity(self):
        trader = _trader()
        # 현재 운영 설정(max_positions=9, 손절 -7%, 위험 0.75%): 9×0.75/7 ≈ 0.964 → 자산의
        # 96.4%로 100% 미만이니 경고가 없어야 한다.
        trader.trader = FakeTrader({"total": 100_000_000, "cash": 100_000_000})
        trader.strategies = {
            "MA크로스": {"is_active": True, "params": {"stop_loss": -7, "max_positions": 9}},
        }
        with mock.patch.object(main.logger, "warning") as warn:
            await trader._warn_if_position_sizing_exceeds_equity()
        warn.assert_not_called()

    async def test_balance_fetch_failure_falls_back_to_initial_seed_krw_without_crashing(self):
        trader = _trader()
        trader.trader = FakeTrader({"total": 0, "cash": 0, "stale": True})  # 조회 실패 모사
        trader.strategies = {
            "MA크로스": {"is_active": True, "params": {"stop_loss": -7, "max_positions": 9}},
        }
        # INITIAL_SEED_KRW(기본 1,000만원)로 폴백해도 예외 없이 완료되어야 한다.
        await trader._warn_if_position_sizing_exceeds_equity()
        self.assertGreater(config.INITIAL_SEED_KRW, 0)


if __name__ == "__main__":
    unittest.main()
