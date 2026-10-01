"""
stark/execution_guard.buy_gate() 단위 테스트 ([AT] buy-gate-unification).

배경: router/handlers/order_handler.py(채팅 수동 지시·AI 자동 실행이 쓰는 경로)가
stark/execution_guard.py를 전혀 거치지 않아, 신호 경로에만 있던 안전장치(악재공시 차단,
당일 손절 2회 차단, 매수/매도 실패 억제, 물타기 제한, 보유 종목수 9개 한도, 투자경고·VI
차단)가 채팅 매수에는 한 번도 적용되지 않았다(10/1 11:02 보유종목 "추가 매수"가 가드 없이
체결). buy_gate()는 이 안전장치들을 한 곳에서 검사하는 채팅 전용 관문이다.

검증 항목: 투자경고·악재공시·당일 손절 2회·실패 억제·보유한도·물타기 위반 각각에서 차단,
투자경고/VI 조회 실패 시 fail-closed, 그리고 핵심 — get_positions_fn/get_market_warning_fn
의존성이 주입되지 않으면(운영 배선 누락) 조용히 통과시키지 않고 기본적으로 매수를 차단하는지
(allow_missing_deps=True일 때만 테스트 전용으로 건너뜀).
"""
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from stark import execution_guard  # noqa: E402
from tests.test_execution_guard import FakePool, FakeRedis  # noqa: E402


async def _ok_positions():
    return {"success": True, "data": []}


async def _ok_warning(symbol):
    return {"mrkt_warn_cls_code": "00", "vi_cls_code": "N"}


class TestBuyGatePassesWhenEverythingOk(unittest.IsolatedAsyncioTestCase):
    async def test_passes_when_everything_ok(self):
        result = await execution_guard.buy_gate(
            "005930", 70000, pool=FakePool(), redis=FakeRedis(),
            get_positions_fn=_ok_positions, get_market_warning_fn=_ok_warning,
        )
        self.assertIsNone(result)


class TestBuyGatePrecheckBlocks(unittest.IsolatedAsyncioTestCase):
    async def test_blocked_by_bad_disclosure(self):
        pool = FakePool(disclosures=[{"report_name": "관리종목 지정 안내"}])
        result = await execution_guard.buy_gate(
            "005930", 70000, pool=pool, redis=FakeRedis(),
            get_positions_fn=_ok_positions, get_market_warning_fn=_ok_warning,
        )
        self.assertEqual(result["blocked"], "bad_disclosure")
        self.assertIn("공시", result["reason"])

    async def test_blocked_by_daily_stop_loss_limit(self):
        pool = FakePool(stop_loss_count=2)
        result = await execution_guard.buy_gate(
            "005930", 70000, pool=pool, redis=FakeRedis(),
            get_positions_fn=_ok_positions, get_market_warning_fn=_ok_warning,
        )
        self.assertEqual(result["blocked"], "daily_stop_loss_limit")
        self.assertIn("손절", result["reason"])

    async def test_blocked_by_buy_fail_suppress(self):
        redis = FakeRedis({"buy_fail_suppress:005930": "1"})
        result = await execution_guard.buy_gate(
            "005930", 70000, pool=FakePool(), redis=redis,
            get_positions_fn=_ok_positions, get_market_warning_fn=_ok_warning,
        )
        self.assertEqual(result["blocked"], "buy_fail_suppress")

    async def test_precheck_internal_failure_blocks_with_error_text(self):
        pool = FakePool(raise_on_fetchval=True)
        result = await execution_guard.buy_gate(
            "005930", 70000, pool=pool, redis=FakeRedis(),
            get_positions_fn=_ok_positions, get_market_warning_fn=_ok_warning,
        )
        self.assertEqual(result["blocked"], "precheck_error")
        self.assertIn("매수 보류", result["reason"])


class TestBuyGatePositionLimits(unittest.IsolatedAsyncioTestCase):
    async def test_blocked_by_max_positions_limit(self):
        current = [{"symbol": f"0000{i}"} for i in range(1, 6)]

        async def get_positions():
            return {"success": True, "data": current}

        result = await execution_guard.buy_gate(
            "000099", 10000, pool=FakePool(), redis=FakeRedis(),
            get_positions_fn=get_positions, get_market_warning_fn=_ok_warning, max_positions=5,
        )
        self.assertEqual(result["blocked"], "max_positions_limit")
        self.assertIn("한도 초과", result["reason"])

    async def test_blocked_by_averaging_down_price_drop(self):
        pool = FakePool(buy_history={"005930": [{"price": 70000}]})

        async def get_positions():
            return {"success": True, "data": [{"symbol": "005930"}]}

        # 최초 매수가 70,000원 대비 현재가 67,000원(-4.29%, -3% 초과 하락)
        result = await execution_guard.buy_gate(
            "005930", 67000, pool=pool, redis=FakeRedis(),
            get_positions_fn=get_positions, get_market_warning_fn=_ok_warning,
        )
        self.assertEqual(result["blocked"], "averaging_down_price_drop_exceeded")

    async def test_blocked_when_positions_check_fails(self):
        async def get_positions_fail():
            raise RuntimeError("KIS 타임아웃")

        result = await execution_guard.buy_gate(
            "005930", 70000, pool=FakePool(), redis=FakeRedis(),
            get_positions_fn=get_positions_fail, get_market_warning_fn=_ok_warning,
        )
        self.assertEqual(result["blocked"], "positions_check_failed")


class TestBuyGateInvestmentWarningAndVI(unittest.IsolatedAsyncioTestCase):
    async def test_blocked_by_investment_warning(self):
        async def get_warning(symbol):
            return {"mrkt_warn_cls_code": "02", "vi_cls_code": "N"}

        result = await execution_guard.buy_gate(
            "005930", 70000, pool=FakePool(), redis=FakeRedis(),
            get_positions_fn=_ok_positions, get_market_warning_fn=get_warning,
        )
        self.assertEqual(result["blocked"], "investment_warning")
        self.assertIn("투자경고", result["reason"])

    async def test_blocked_by_vi_triggered(self):
        async def get_warning(symbol):
            return {"mrkt_warn_cls_code": "00", "vi_cls_code": "Y"}

        result = await execution_guard.buy_gate(
            "005930", 70000, pool=FakePool(), redis=FakeRedis(),
            get_positions_fn=_ok_positions, get_market_warning_fn=get_warning,
        )
        self.assertEqual(result["blocked"], "vi_triggered")
        self.assertIn("VI", result["reason"])

    async def test_fail_closed_when_warning_query_raises(self):
        async def get_warning_fail(symbol):
            raise RuntimeError("KIS 속도제한")

        result = await execution_guard.buy_gate(
            "005930", 70000, pool=FakePool(), redis=FakeRedis(),
            get_positions_fn=_ok_positions, get_market_warning_fn=get_warning_fail,
        )
        self.assertEqual(result["blocked"], "market_warning_check_failed")

    async def test_fail_closed_when_warning_query_returns_none(self):
        async def get_warning_none(symbol):
            return None

        result = await execution_guard.buy_gate(
            "005930", 70000, pool=FakePool(), redis=FakeRedis(),
            get_positions_fn=_ok_positions, get_market_warning_fn=get_warning_none,
        )
        self.assertEqual(result["blocked"], "market_warning_check_failed")


class TestBuyGateMissingDependenciesFailClosed(unittest.IsolatedAsyncioTestCase):
    """운영 배선이 빠지면(get_positions_fn/get_market_warning_fn 미주입) 조용히 통과시키는
    fail-open이 되면 안 된다 — 지난번 사이징 한도 배선 누락 사고와 같은 패턴을 막기 위함."""

    async def test_missing_positions_fn_blocks_by_default(self):
        result = await execution_guard.buy_gate(
            "005930", 70000, pool=FakePool(), redis=FakeRedis(),
            get_positions_fn=None, get_market_warning_fn=_ok_warning,
        )
        self.assertEqual(result["blocked"], "gate_dependency_missing")

    async def test_missing_warning_fn_blocks_by_default(self):
        result = await execution_guard.buy_gate(
            "005930", 70000, pool=FakePool(), redis=FakeRedis(),
            get_positions_fn=_ok_positions, get_market_warning_fn=None,
        )
        self.assertEqual(result["blocked"], "gate_dependency_missing")

    async def test_both_missing_blocks_by_default(self):
        result = await execution_guard.buy_gate(
            "005930", 70000, pool=FakePool(), redis=FakeRedis(),
            get_positions_fn=None, get_market_warning_fn=None,
        )
        self.assertEqual(result["blocked"], "gate_dependency_missing")

    async def test_allow_missing_deps_flag_bypasses_for_tests_only(self):
        # 명시적 테스트 전용 플래그를 켰을 때만 의존성 없이도 precheck만으로 통과할 수 있다.
        result = await execution_guard.buy_gate(
            "005930", 70000, pool=FakePool(), redis=FakeRedis(),
            get_positions_fn=None, get_market_warning_fn=None, allow_missing_deps=True,
        )
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
