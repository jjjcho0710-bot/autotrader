"""
진입위험 관찰(shadow) 플래그 테스트 ([AT] feat/entry-risk-shadow, PM 승인).

배경: EDITH 41건 매수 분석 — 저가주(<5,000원) 매수 8/8 손실(평균 -2.61%), 직전 5거래일
+20% 이상 급등 후 매수 7/9 손실(평균 -3.30%). 아직 차단 근거로 쓸 만큼 검증되지 않아
차단하지 않고 trade_journal에 플래그만 남긴다(momentum shadow와 동일한 관찰 전용 원칙,
하드룰·AI 프롬프트 변경 없음).

검증 항목:
  1) stark.execution_guard.check_entry_risk_shadow() 단위 — low_price/surge_5d 판정,
     경계값, 종가 데이터 부족/조회 실패 시 fail-open(둘 다 False, 예외 없음).
  2) execute() 통합 — 체결 확정 시 log_journal_fn이 low_price/surge_5d 플래그를
     그대로 받는지(매수는 차단되지 않고 정상 체결되는지도 함께 확인).
"""
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from stark import execution_guard  # noqa: E402
from tests.test_execution_guard import (  # noqa: E402
    FakePool as EgFakePool,
    FakeRedis as EgFakeRedis,
    make_decision,
    make_signal,
)

# 모멘텀 게이트 테스트와 동일하게, 이 모듈의 테스트는 KIS 대체 조회를 기본적으로
# "데이터 없음"으로 고정한다(실제 네트워크 호출/토큰 발급을 타지 않도록).
_fallback_patcher = None
_fallback_mock = None


def setUpModule():
    global _fallback_patcher, _fallback_mock
    _fallback_patcher = patch.object(
        execution_guard, "_fetch_kis_fallback_closes", new_callable=AsyncMock)
    _fallback_mock = _fallback_patcher.start()
    _fallback_mock.return_value = None


def tearDownModule():
    _fallback_patcher.stop()


# ── 1) check_entry_risk_shadow() 단위 테스트 ──────────────────────
class TestCheckEntryRiskShadow(unittest.IsolatedAsyncioTestCase):
    async def test_low_price_true_under_5000(self):
        pool = EgFakePool()
        result = await execution_guard.check_entry_risk_shadow("005930", 4999, pool=pool)
        self.assertTrue(result["low_price"])

    async def test_low_price_false_at_exactly_5000(self):
        """경계값: 5,000원 "미만"만 저가주로 본다(5,000원 자체는 아님)."""
        pool = EgFakePool()
        result = await execution_guard.check_entry_risk_shadow("005930", 5000, pool=pool)
        self.assertFalse(result["low_price"])

    async def test_low_price_false_above_5000(self):
        pool = EgFakePool()
        result = await execution_guard.check_entry_risk_shadow("005930", 70000, pool=pool)
        self.assertFalse(result["low_price"])

    async def test_surge_5d_true_at_20_percent(self):
        pool = EgFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 120]})  # +20.0%
        result = await execution_guard.check_entry_risk_shadow("005930", 70000, pool=pool)
        self.assertAlmostEqual(result["surge_5d_pct"], 20.0)
        self.assertTrue(result["surge_5d"])

    async def test_surge_5d_false_just_below_20_percent(self):
        pool = EgFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 119]})  # +19.0%
        result = await execution_guard.check_entry_risk_shadow("005930", 70000, pool=pool)
        self.assertFalse(result["surge_5d"])

    async def test_surge_5d_false_when_flat(self):
        pool = EgFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 100]})
        result = await execution_guard.check_entry_risk_shadow("005930", 70000, pool=pool)
        self.assertFalse(result["surge_5d"])

    async def test_insufficient_data_is_fail_open(self):
        """종가 데이터가 lookback+1개 미달이면 예외 없이 surge_5d=False(차단 없음)."""
        pool = EgFakePool(daily_closes={"005930": [100, 98]})
        result = await execution_guard.check_entry_risk_shadow("005930", 70000, pool=pool)
        self.assertIsNone(result["surge_5d_pct"])
        self.assertFalse(result["surge_5d"])

    async def test_db_fetch_failure_is_fail_open(self):
        """DB 조회 자체가 예외를 던져도(KIS 대체 조회도 실패) surge_5d=False로 안전하게
        처리하고 호출부로 예외를 전파하지 않는다."""
        pool = EgFakePool(raise_on_daily_closes=True)
        result = await execution_guard.check_entry_risk_shadow("005930", 70000, pool=pool)
        self.assertIsNone(result["surge_5d_pct"])
        self.assertFalse(result["surge_5d"])

    async def test_low_price_and_surge_5d_can_both_be_true(self):
        pool = EgFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 130]})  # +30%
        result = await execution_guard.check_entry_risk_shadow("005930", 3000, pool=pool)
        self.assertTrue(result["low_price"])
        self.assertTrue(result["surge_5d"])

    async def test_price_computation_fail_open(self):
        """price가 숫자로 변환 불가하면 low_price만 False로 안전 처리(예외 없음)."""
        pool = EgFakePool()
        result = await execution_guard.check_entry_risk_shadow("005930", None, pool=pool)
        self.assertFalse(result["low_price"])


# ── 2) execute() 통합 — log_journal_fn이 플래그를 그대로 받는지 ─────
class TestExecuteEntryRiskShadowJournal(unittest.IsolatedAsyncioTestCase):
    async def _run_execute(self, pool, signal_overrides=None):
        redis = EgFakeRedis({"cache:positions:stock": "1", "cache:account:stock": "1"})
        journaled_kwargs = []

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            pass

        async def log_journal(*args, **kwargs):
            journaled_kwargs.append(kwargs)

        async def code_to_name(symbol):
            return "삼성전자"

        result = await execution_guard.execute(
            make_signal(**(signal_overrides or {})), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
        )
        return result, journaled_kwargs

    async def test_low_price_buy_is_not_blocked_and_flag_reaches_journal(self):
        pool = EgFakePool()
        result, journaled = await self._run_execute(pool, {"price": 3000})
        self.assertTrue(result["executed"])  # 관찰만 — 매수는 정상 체결
        self.assertEqual(len(journaled), 1)
        self.assertTrue(journaled[0]["low_price"])
        self.assertFalse(journaled[0]["surge_5d"])

    async def test_surge_5d_buy_is_not_blocked_and_flag_reaches_journal(self):
        pool = EgFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 125]})  # +25%
        result, journaled = await self._run_execute(pool, {"price": 70000})
        self.assertTrue(result["executed"])
        self.assertEqual(len(journaled), 1)
        self.assertFalse(journaled[0]["low_price"])
        self.assertTrue(journaled[0]["surge_5d"])

    async def test_normal_buy_has_both_flags_false(self):
        pool = EgFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 101]})  # +1%
        result, journaled = await self._run_execute(pool, {"price": 70000})
        self.assertTrue(result["executed"])
        self.assertEqual(len(journaled), 1)
        self.assertFalse(journaled[0]["low_price"])
        self.assertFalse(journaled[0]["surge_5d"])

    async def test_sell_order_has_both_flags_false(self):
        """매도는 진입위험 관찰 대상이 아니다(매수 전용) — 플래그는 기본값 False."""
        pool = EgFakePool()
        result, journaled = await self._run_execute(
            pool, {"action": "sell", "price": 3000})
        self.assertTrue(result["executed"])
        self.assertEqual(len(journaled), 1)
        self.assertFalse(journaled[0]["low_price"])
        self.assertFalse(journaled[0]["surge_5d"])

    async def test_entry_risk_shadow_exception_does_not_block_buy(self):
        """check_entry_risk_shadow 자체가 예외를 던져도(이중 방어) 매수는 정상 진행되고
        플래그는 안전하게 False로 기록된다."""
        pool = EgFakePool(daily_closes={"005930": [100, 100, 100, 100, 100, 125]})
        with patch.object(
            execution_guard, "check_entry_risk_shadow",
            AsyncMock(side_effect=RuntimeError("boom")),
        ):
            result, journaled = await self._run_execute(pool, {"price": 70000})
        self.assertTrue(result["executed"])
        self.assertEqual(len(journaled), 1)
        self.assertFalse(journaled[0]["low_price"])
        self.assertFalse(journaled[0]["surge_5d"])


if __name__ == "__main__":
    unittest.main()
