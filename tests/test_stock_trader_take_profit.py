"""
stock_trader/main.py build_strategy() 단위 테스트: MA크로스 take_profit 퍼센트→비율 변환 검증.

배경: strategy_config.params의 take_profit은 stop_loss와 동일하게 "퍼센트 숫자 그대로"
(5 = +5%) 저장 관례를 따른다(router/handlers/setting_handler.py, dashboard/main.py 시드값
'{"take_profit":5,...}'). build_strategy()가 지금까지 이를 /100 변환 없이 그대로 넘겨
MACrossConfig.take_profit이 5.0(=+500%)이 되는 버그가 있었다.

참고: MACrossStrategy.check_take_profit()은 현재 stock_trader/main.py의 매매 사이클에서
호출되지 않는 미사용 코드다(실제 익절은 exit_ai_cool 기반 AI HOLD/HALF/ALL 판단 경로가
담당). 따라서 이 수정은 프로덕션 동작에는 영향이 없으며, 향후 사용 시를 대비한 값
정합성 수정이다.
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


class TestBuildStrategyTakeProfitUnitConversion(unittest.TestCase):
    def test_ma_cross_take_profit_converts_percent_to_fraction(self):
        strat = StockTrader.build_strategy(None, "MA크로스", {"take_profit": 5})
        self.assertAlmostEqual(strat.cfg.take_profit, 0.05)

    def test_default_take_profit_without_params_key(self):
        # DB에 take_profit 키가 없는 예외적인 경우에도 기본값이 퍼센트 관례를 따라
        # 올바른 비율로 변환되어야 한다 (MA크로스 기본 +5%).
        strat = StockTrader.build_strategy(None, "MA크로스", {})
        self.assertAlmostEqual(strat.cfg.take_profit, 0.05)

    def test_custom_take_profit_value_converts_correctly(self):
        strat = StockTrader.build_strategy(None, "MA크로스", {"take_profit": 8})
        self.assertAlmostEqual(strat.cfg.take_profit, 0.08)


class TestTakeProfitTriggersOnRealisticGain(unittest.TestCase):
    def test_plus_5_percent_gain_triggers_take_profit_at_plus_5_config(self):
        strat = StockTrader.build_strategy(None, "MA크로스", {"take_profit": 5})
        avg_price = 10000
        cur_price = int(avg_price * (1 + 0.05))  # +5% 수익
        self.assertTrue(strat.check_take_profit(avg_price, cur_price))

    def test_small_gain_within_threshold_does_not_trigger(self):
        strat = StockTrader.build_strategy(None, "MA크로스", {"take_profit": 5})
        avg_price = 10000
        cur_price = int(avg_price * (1 + 0.03))  # +3% 수익 (익절선 미도달)
        self.assertFalse(strat.check_take_profit(avg_price, cur_price))

    def test_before_fix_regression_guard(self):
        # 수정 전 버그를 그대로 재현하면(퍼센트를 비율로 잘못 취급) +5% 수익으로도
        # 익절선(+500%)에 도달하지 못해 트리거되지 않아야 한다 — 회귀 방지를 위해
        # 버그 동작과의 차이를 명시한다.
        from strategy.ma_cross import MACrossConfig, MACrossStrategy
        buggy_strat = MACrossStrategy(MACrossConfig(take_profit=5))  # 버그: /100 누락
        avg_price = 10000
        cur_price = int(avg_price * (1 + 0.05))
        self.assertFalse(buggy_strat.check_take_profit(avg_price, cur_price))


if __name__ == "__main__":
    unittest.main()
