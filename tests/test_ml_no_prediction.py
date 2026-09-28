"""
ML 예측 실패 시 가짜 65% 대신 "예측 없음"을 정확히 전달하는지 검증.

배경: stock_trader/main.py 의 _get_ml_result 는 예측이 실패하거나 어떤 예외가 나도
{"buy_prob": 0.65} 를 반환했다. 그 65% 가 AI 프롬프트에 "ML매수확률:65%" 로 들어가 긍정 근거처럼
인용됐다(stark_decisions 31건 전부 65%). 여기서는
1. 모델 없음/예측 실패/예외 3가지 모두 buy_prob 없이 {"success": False, "reason": ...} 를 돌려주는지,
2. reason 문자열에 성공일 때만 "ML매수확률:NN%", 실패일 때는 "ML예측 없음(사유)" 가 들어가는지,
3. 실패 시 매수 금액이 최소 비율(10%)로 고정되는지,
4. stark/decision_engine.py 의 AI 폴백이 "ML예측 없음" 을 확률 0 으로 읽어 매수하지 않는지
를 고정한다.
"""
import sys
import types
import unittest
from unittest import mock

# stock_trader/main.py 를 import 하려면 asyncpg/redis/aiohttp 스텁과 sys.path 세팅이 필요하다 —
# test_stock_trader_stop_loss 가 import 시점에 이미 처리하므로 그대로 재사용한다.
from tests.test_stock_trader_stop_loss import StockTrader  # noqa: E402
from tests.test_decision_engine import FakePool, make_signal  # noqa: E402
from stark import decision_engine  # noqa: E402

ROWS = [{"ts": "2026-09-28", "open": 1, "high": 2, "low": 1, "close": 2, "volume": 10}]


def _trader():
    return StockTrader.__new__(StockTrader)  # __init__(DB/KIS 연결) 없이 순수 메서드만 사용


def _fake_ml_module(predict):
    """ml.model.MLModelManager 를 predict 동작만 다른 가짜로 대체한다."""
    class FakeManager:
        def __init__(self, db_pool=None):
            pass

        async def predict(self, symbol, ohlcv):
            return await predict(symbol, ohlcv)

    mod = types.ModuleType("ml.model")
    mod.MLModelManager = FakeManager
    return mock.patch.dict(sys.modules, {"ml.model": mod})


class TestGetMlResultFailure(unittest.IsolatedAsyncioTestCase):
    async def _run(self, predict):
        with _fake_ml_module(predict):
            return await _trader()._get_ml_result("005930", ROWS)

    def _assert_failure(self, result, reason_part):
        self.assertFalse(result["success"])
        self.assertNotIn("buy_prob", result)  # 가짜 확률을 만들어 내지 않는다
        self.assertIn(reason_part, result["reason"])

    async def test_no_model(self):
        async def predict(symbol, ohlcv):
            return {"success": False, "error": "학습된 모델 없음"}
        self._assert_failure(await self._run(predict), "모델 없음")

    async def test_prediction_failed(self):
        async def predict(symbol, ohlcv):
            return {"success": False, "error": "피처 생성 실패"}
        self._assert_failure(await self._run(predict), "피처 생성 실패")

    async def test_prediction_failed_without_error_text(self):
        async def predict(symbol, ohlcv):
            return {"success": False}
        self._assert_failure(await self._run(predict), "예측 없음")

    async def test_success_without_buy_prob_is_failure(self):
        async def predict(symbol, ohlcv):
            return {"success": True}
        self._assert_failure(await self._run(predict), "예측 없음")

    async def test_exception_is_logged_and_reported_by_type(self):
        async def predict(symbol, ohlcv):
            raise ValueError("secret-looking detail")
        with self.assertLogs("stock-trader", level="WARNING") as cm:
            result = await self._run(predict)
        self._assert_failure(result, "ValueError")
        self.assertTrue(any("ValueError" in m for m in cm.output))
        self.assertNotIn("secret", result["reason"])  # reason 에는 예외 타입만, 메시지는 넣지 않는다

    async def test_success_returns_predict_result_unchanged(self):
        expected = {"success": True, "buy_prob": 0.83, "signal": "BUY"}

        async def predict(symbol, ohlcv):
            return expected
        self.assertEqual(await self._run(predict), expected)


class TestMlBuyPlan(unittest.TestCase):
    CASH = 5_000_000

    def test_failure_uses_minimum_ratio_and_no_probability_text(self):
        for reason in ("모델 없음", "피처 생성 실패", "예외:ValueError"):
            amount, text = StockTrader._ml_buy_plan({"success": False, "reason": reason}, self.CASH)
            self.assertEqual(amount, 500_000)  # 최소 비율 10%
            self.assertEqual(text, f"ML예측 없음({reason})")
            self.assertNotIn("65%", text)
            self.assertNotIn("ML매수확률", text)

    def test_failure_without_reason_still_marked_no_prediction(self):
        _, text = StockTrader._ml_buy_plan({"success": False}, self.CASH)
        self.assertTrue(text.startswith("ML예측 없음("))

    def test_legacy_default_shape_is_not_treated_as_prediction(self):
        # 옛 실패 기본값 {"buy_prob": 0.65} 처럼 success 가 없는 dict 는 성공으로 취급하지 않는다.
        amount, text = StockTrader._ml_buy_plan({"buy_prob": 0.65}, self.CASH)
        self.assertEqual(amount, 500_000)
        self.assertTrue(text.startswith("ML예측 없음("))

    def test_success_keeps_existing_tiers_and_text(self):
        cases = [(0.95, 1_500_000, "ML매수확률:95%(강함)"),
                 (0.85, 1_000_000, "ML매수확률:85%(보통)"),
                 (0.72, 750_000, "ML매수확률:72%(약함)"),
                 (0.65, 500_000, "ML매수확률:65%(최소)")]  # 모델이 실제로 65% 를 냈다면 그대로 표기
        for prob, want_amount, want_text in cases:
            amount, text = StockTrader._ml_buy_plan({"success": True, "buy_prob": prob}, self.CASH)
            self.assertEqual((amount, text), (want_amount, want_text))

    def test_minimum_order_amount_floor_preserved(self):
        amount, _ = StockTrader._ml_buy_plan({"success": False, "reason": "모델 없음"}, 500_000)
        self.assertEqual(amount, 100_000)


class TestAiFallbackIgnoresNoPrediction(unittest.IsolatedAsyncioTestCase):
    async def _decide(self, ml_result, ai_reply=""):
        _, ml_text = StockTrader._ml_buy_plan(ml_result, 5_000_000)
        reason = f"전략:MA크로스 | {ml_text} | 수급:외국인+1 기관+1 | 뉴스:없음"
        pool = FakePool()

        async def ask_llm(prompt, session_id):
            return ai_reply

        decision = await decision_engine.decide(
            make_signal(action="buy", reason=reason), "prompt", ask_llm_fn=ask_llm, pool=pool)
        return decision, pool

    async def test_no_prediction_reasons_never_trigger_fallback_buy(self):
        for reason in ("모델 없음", "피처 생성 실패", "예측 없음", "예외:ValueError"):
            decision, pool = await self._decide({"success": False, "reason": reason})
            self.assertTrue(decision["ai_failed"], reason)
            self.assertFalse(decision["should_execute"], reason)
            self.assertEqual(decision["confidence"], 0.0, reason)
            self.assertIn("ML확률 0%", decision["reply"], reason)
            self.assertIn("ML예측 없음", str(pool._conn.inserted[0]), reason)  # stark_decisions 에도 그대로 기록

    async def test_real_high_probability_still_triggers_fallback_buy(self):
        decision, _ = await self._decide({"success": True, "buy_prob": 0.85})
        self.assertTrue(decision["should_execute"])
        self.assertEqual(decision["confidence"], 0.85)


if __name__ == "__main__":
    unittest.main()
