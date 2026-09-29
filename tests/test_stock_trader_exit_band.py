"""
stock_trader/main.py classify_exit_band() 단위 테스트: 차등 익절 정책(PM 승인) 구간 분류 검증.

배경: 기존에는 익절 로직이 전략마다 제각각이고(MA크로스만 미사용 고정 +5% take_profit,
RSI반등·볼린저밴드는 신호 기반), AI exit_decision은 +3% 이상부터만 작동해 손절 -7% 대비
익절 구간이 좁아 손익비가 불리했다. 이를 구간별로 명시적으로 분리한다.
- <1%: 보유 유지 (판단 근거 부족)
- 1~5%: 진입 신호 유지 여부로 보유/전량매도 판단 (stock_trader/main.py의 SIGNAL_CHECK 분기)
- 5%~: 자비스 AI HOLD/HALF/ALL 판단 (기존 exit_ai_cool 경로, 문턱 3%→5% 상향)
+10% 이상 절반확정·트레일링 스탑은 다음 단계에서 별도 구현 예정이며 이번 커밋 범위 밖이다.
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
    순수 로직(classify_exit_band 등)을 검증할 수 있도록 import만 되는 더미로 대체한다.
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
from strategy.ma_cross import MACrossConfig, MACrossStrategy  # noqa: E402


class TestClassifyExitBand(unittest.TestCase):
    def test_below_1_percent_holds(self):
        self.assertEqual(StockTrader.classify_exit_band(0.0), StockTrader.EXIT_BAND_HOLD)
        self.assertEqual(StockTrader.classify_exit_band(0.5), StockTrader.EXIT_BAND_HOLD)
        self.assertEqual(StockTrader.classify_exit_band(0.99), StockTrader.EXIT_BAND_HOLD)

    def test_negative_pnl_holds(self):
        # 손절(-7%)에 도달하지 않은 손실 구간은 익절 로직과 무관하게 HOLD로 분류되어야 한다
        self.assertEqual(StockTrader.classify_exit_band(-3.0), StockTrader.EXIT_BAND_HOLD)

    def test_1_to_5_percent_is_signal_check(self):
        self.assertEqual(StockTrader.classify_exit_band(1.0), StockTrader.EXIT_BAND_SIGNAL_CHECK)
        self.assertEqual(StockTrader.classify_exit_band(3.0), StockTrader.EXIT_BAND_SIGNAL_CHECK)
        self.assertEqual(StockTrader.classify_exit_band(4.99), StockTrader.EXIT_BAND_SIGNAL_CHECK)

    def test_5_percent_and_above_is_ai_judge(self):
        self.assertEqual(StockTrader.classify_exit_band(5.0), StockTrader.EXIT_BAND_AI_JUDGE)
        self.assertEqual(StockTrader.classify_exit_band(7.5), StockTrader.EXIT_BAND_AI_JUDGE)

    def test_above_10_percent_still_ai_judge_until_next_phase(self):
        # +10% 이상 절반확정·트레일링 스탑은 다음 단계 구현 예정. 그 전까지는
        # 기존 AI 판단 경로가 계속 적용되어야 하며(회귀 방지), 미판단 상태로
        # 방치되면 안 된다.
        self.assertEqual(StockTrader.classify_exit_band(12.0), StockTrader.EXIT_BAND_AI_JUDGE)


class TestSignalCheckBandUsesStrategyGenerateSignal(unittest.TestCase):
    """+1~5% 구간은 기존 전략 객체의 generate_signal()을 재사용해 데드크로스 등
    진입 신호 소멸 여부를 판단한다(main.py SIGNAL_CHECK 분기가 호출하는 바로 그 메서드)."""

    def test_ma_cross_dead_cross_returns_sell(self):
        strat = MACrossStrategy(MACrossConfig(short_period=5, long_period=20))
        # 우상향 후 급락: 단기 이평이 장기 이평을 아래로 돌파하는 데드크로스 구성
        rising = [100 + i for i in range(25)]
        falling_tail = [rising[-1] - i * 3 for i in range(1, 6)]
        prices = rising + falling_tail
        sig = strat.generate_signal("TEST", prices)
        self.assertEqual(sig, "SELL")

    def test_ma_cross_holds_while_golden_cross_intact(self):
        strat = MACrossStrategy(MACrossConfig(short_period=5, long_period=20))
        prices = [100 + i for i in range(30)]  # 꾸준한 우상향 → 데드크로스 없음
        sig = strat.generate_signal("TEST", prices)
        self.assertNotEqual(sig, "SELL")


if __name__ == "__main__":
    unittest.main()
