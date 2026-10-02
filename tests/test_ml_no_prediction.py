"""
ML 예측 실패 시 가짜 65% 대신 "예측 없음"을 정확히 전달하는지 검증.

배경: stock_trader/main.py 의 _get_ml_result 는 예측이 실패하거나 어떤 예외가 나도
{"buy_prob": 0.65} 를 반환했다. 그 65% 가 AI 프롬프트에 "ML매수확률:65%" 로 들어가 긍정 근거처럼
인용됐다(stark_decisions 31건 전부 65%). 여기서는
1. 모델 없음/예측 실패/예외 3가지 모두 buy_prob 없이 {"success": False, "reason": ...} 를 돌려주는지,
2. stark/decision_engine.py 의 AI 폴백이 ML 확률(reason에 남아있더라도)로 매수를 시도하지
   않는지(PM 승인, 2026-10-02 — ML 확률 기반 폴백 매수 자체를 제거함)
를 고정한다.

ML 확률 구간별 매수 비율·문구를 만들던 _ml_buy_plan 은 reason/AI 프롬프트에 ML 확률을
남기지 않기 위해 제거됐다 — 그 함수가 만들던 "ML매수확률:NN%"/"ML예측 없음(사유)" 문구 자체가
금지 대상이라 관련 테스트(TestMlBuyPlan)도 함께 제거했다.
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


class TestAiFallbackNeverBuysOnMlProbability(unittest.IsolatedAsyncioTestCase):
    """ML 확률 기반 폴백 매수는 완전히 제거됐다 — reason에 어떤 ML 문구가 남아있어도
    (과거 데이터·수동 입력 등으로) AI 응답 실패 시에는 항상 SKIP이어야 한다."""

    async def _decide(self, reason, ai_reply=""):
        pool = FakePool()

        async def ask_llm(prompt, session_id):
            return ai_reply

        decision = await decision_engine.decide(
            make_signal(action="buy", reason=reason), "prompt", ask_llm_fn=ask_llm, pool=pool)
        return decision, pool

    async def test_no_prediction_reasons_never_trigger_fallback_buy(self):
        for reason in ("모델 없음", "피처 생성 실패", "예측 없음", "예외:ValueError"):
            decision, pool = await self._decide(f"전략:MA크로스 | ML예측 없음({reason}) | 수급:외국인+1 기관+1 | 뉴스:없음")
            self.assertTrue(decision["ai_failed"], reason)
            self.assertFalse(decision["should_execute"], reason)
            self.assertEqual(decision["confidence"], 0.30, reason)
            self.assertIn("보수적 SKIP", decision["reply"], reason)

    async def test_high_ml_probability_in_reason_no_longer_triggers_fallback_buy(self):
        """원래는 buy_prob>=0.70이면 AI 폴백이 매수를 시도했다 — 그 경로 자체가 제거됐으므로
        reason에 높은 ML 확률 문구가 있어도 SKIP이어야 한다."""
        decision, pool = await self._decide("전략:MA크로스 | ML매수확률:85%(보통) | 수급:외국인+1 기관+1 | 뉴스:없음")
        self.assertTrue(decision["ai_failed"])
        self.assertFalse(decision["should_execute"])
        self.assertEqual(decision["confidence"], 0.30)
        self.assertIn("SKIP", pool._conn.inserted[0][2])


if __name__ == "__main__":
    unittest.main()
