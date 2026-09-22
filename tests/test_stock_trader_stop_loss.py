"""
stock_trader/main.py build_strategy() 단위 테스트: -7% 손절 강제 집행 검증.

배경: strategy_config.params의 stop_loss는 "퍼센트 숫자 그대로"(-7 = -7%) 저장되는데
(router/handlers/setting_handler.py, migrations/V001 시드값 관례), build_strategy()가
이를 비율(-0.07)로 변환하지 않고 그대로 넘기면 손절선이 -700%가 되어 사실상 절대
트리거되지 않는다. 이게 일동제약 -10.9% 미손절의 실제 원인이었다. 여기서는 DB에
저장된 퍼센트 값이 build_strategy()를 거쳐 전략 객체에 올바른 비율로 반영되고,
check_stop_loss()가 실제 손실률에 대해 정확히 트리거되는지 검증한다.
"""
import sys
import types
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
STOCK_TRADER_DIR = REPO_ROOT / "stock_trader"
for p in (REPO_ROOT, STOCK_TRADER_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _stub_missing_runtime_deps():
    """requirements.txt의 asyncpg/redis/aiohttp가 테스트 환경에 없어도
    build_strategy()(순수 로직)를 검증할 수 있도록 import만 되는 더미로 대체한다.
    실제 패키지가 설치되어 있으면 아무 것도 하지 않는다."""
    try:
        import asyncpg  # noqa: F401
    except ImportError:
        stub = types.ModuleType("asyncpg")
        stub.Pool = object
        sys.modules["asyncpg"] = stub

    try:
        import redis.asyncio  # noqa: F401
    except ImportError:
        redis_mod = types.ModuleType("redis")
        redis_asyncio_mod = types.ModuleType("redis.asyncio")
        redis_asyncio_mod.Redis = object
        redis_mod.asyncio = redis_asyncio_mod
        sys.modules["redis"] = redis_mod
        sys.modules["redis.asyncio"] = redis_asyncio_mod

    try:
        import aiohttp  # noqa: F401
    except ImportError:
        stub = types.ModuleType("aiohttp")
        stub.ClientSession = object
        stub.ClientTimeout = lambda *a, **kw: None
        sys.modules["aiohttp"] = stub


_stub_missing_runtime_deps()

from main import StockTrader  # noqa: E402  (stock_trader/main.py)


class TestBuildStrategyStopLossUnitConversion(unittest.TestCase):
    def test_ma_cross_stop_loss_converts_percent_to_fraction(self):
        strat = StockTrader.build_strategy(None, "MA크로스", {"stop_loss": -7})
        self.assertAlmostEqual(strat.cfg.stop_loss, -0.07)

    def test_rsi_stop_loss_converts_percent_to_fraction(self):
        strat = StockTrader.build_strategy(None, "RSI반등", {"stop_loss": -7})
        self.assertAlmostEqual(strat.cfg.stop_loss, -0.07)

    def test_bollinger_stop_loss_converts_percent_to_fraction(self):
        strat = StockTrader.build_strategy(None, "볼린저밴드", {"stop_loss": -7})
        self.assertAlmostEqual(strat.cfg.stop_loss, -0.07)

    def test_default_stop_loss_without_params_key(self):
        # DB에 stop_loss 키가 없는 예외적인 경우에도 기본값이 퍼센트 관례를 따라
        # 올바른 비율로 변환되어야 한다 (MA크로스 기본 -2%).
        strat = StockTrader.build_strategy(None, "MA크로스", {})
        self.assertAlmostEqual(strat.cfg.stop_loss, -0.02)


class TestStopLossTriggersOnRealisticLoss(unittest.TestCase):
    """일동제약 -10.9% 시나리오 재현: 손절선(-7%)을 넘는 손실은 반드시 트리거되어야 한다."""

    def test_minus_10_9_percent_loss_triggers_stop_loss_at_minus_7_config(self):
        strat = StockTrader.build_strategy(None, "MA크로스", {"stop_loss": -7})
        avg_price = 10000
        cur_price = int(avg_price * (1 - 0.109))  # -10.9% 손실
        self.assertTrue(strat.check_stop_loss(avg_price, cur_price))

    def test_small_loss_within_threshold_does_not_trigger(self):
        strat = StockTrader.build_strategy(None, "MA크로스", {"stop_loss": -7})
        avg_price = 10000
        cur_price = int(avg_price * (1 - 0.03))  # -3% 손실 (손절선 미도달)
        self.assertFalse(strat.check_stop_loss(avg_price, cur_price))

    def test_before_fix_regression_guard(self):
        # 수정 전 버그를 그대로 재현하면(퍼센트를 비율로 잘못 취급) -10.9% 손실로도
        # 트리거되지 않아야 한다 — 회귀 방지를 위해 버그 동작과의 차이를 명시한다.
        from strategy.ma_cross import MACrossConfig, MACrossStrategy
        buggy_strat = MACrossStrategy(MACrossConfig(stop_loss=-7))  # 버그: /100 누락
        avg_price = 10000
        cur_price = int(avg_price * (1 - 0.109))
        self.assertFalse(buggy_strat.check_stop_loss(avg_price, cur_price))


if __name__ == "__main__":
    unittest.main()
