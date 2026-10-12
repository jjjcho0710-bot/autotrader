"""
2단계 비중 사이징 "1주 미만 금액" 가드 e2e 테스트 ([AT] fix/sizing-below-one-share).

배경: dashboard/main.py _apply_stock_confidence_sizing()이 확신도 배율(EXECUTE_SMALL=0.5)과
예수금 캡 적용 후 final_amount가 1주 가격 미만이어도 max(1, int(final_amount//price))로
1주를 강제 매수해 RISK_PER_TRADE_PCT 손실 상한이 고가 종목에서 깨졌다. 단위 테스트
(tests/test_stock_confidence_sizing.py)는 _apply_stock_confidence_sizing() 자체의 반환값
(0/스킵 사유)만 검증하므로, 이 파일은 실제 /api/jarvis/signal 엔드포인트(jarvis_signal)를
그대로 호출해서 스킵 결과를 호출부(jarvis_signal)가 올바르게 처리하는지 — 즉 KIS 실주문
(execution_guard.execute)이 호출되지 않고 trade_journal에 SKIP이 기록되는지 — 를
운영 경로 그대로 검증한다.

revert 확인(수동): dashboard/main.py _apply_stock_confidence_sizing()의
"if final_amount < price: ... return 0" 블록을 지우고 원래의
"return max(1, int(final_amount // price))"로 되돌리면
test_execute_small_high_price_stock_is_skipped_not_forced_to_one_share가 실패한다
(execute_mock이 qty=1로 호출되어 execute_mock.assert_not_called()가 깨짐).
"""
import importlib
import sys
import types
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()


def _identity_fastapi_stub():
    """실제 FastAPI가 없는 환경에서 @app.get/post 데코레이터가 원본 함수를 그대로
    보존하도록 흉내 내는 최소 스텁(tests/test_jarvis_signal_ml_fallback_e2e.py와 동일)."""
    mod = types.ModuleType("fastapi")

    class _App:
        def __init__(self, *a, **kw):
            pass

        def _decorator(self, *a, **kw):
            def identity(fn):
                return fn
            return identity

        def __getattr__(self, name):
            return self._decorator

        def mount(self, *a, **kw):
            pass

        def add_middleware(self, *a, **kw):
            pass

    class HTTPException(Exception):
        def __init__(self, status_code=500, detail=""):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    mod.FastAPI = _App
    mod.HTTPException = HTTPException
    mod.Request = object
    mod.background = types.SimpleNamespace(BackgroundTasks=object)
    return mod


_fastapi_stub = _identity_fastapi_stub()
_prev_fastapi = sys.modules.get("fastapi")
sys.modules["fastapi"] = _fastapi_stub

import dashboard.main as dm  # noqa: E402
importlib.reload(dm)  # @app.post 등 데코레이터를 identity 스텁으로 다시 적용


def tearDownModule():
    if _prev_fastapi is not None:
        sys.modules["fastapi"] = _prev_fastapi
    else:
        sys.modules.pop("fastapi", None)
    importlib.reload(dm)


class _FakeRequest:
    def __init__(self, body):
        self._body = body

    async def json(self):
        return self._body


class TestSizingBelowOneShareSkip(unittest.IsolatedAsyncioTestCase):
    def _patch_common(self, ai_reply: str):
        execute_mock = AsyncMock(return_value={"success": True, "executed": True})
        log_journal_mock = AsyncMock()
        log_decision_mock = AsyncMock(return_value=1)

        patches = [
            patch.object(dm, "db_pool", None),
            patch.object(dm, "redis_client", None),
            patch.object(dm.execution_guard, "precheck", new=AsyncMock(return_value=None)),
            patch.object(dm.execution_guard, "execute", new=execute_mock),
            patch.object(dm, "get_portfolio_context", new=AsyncMock(return_value="")),
            patch.object(dm, "_get_active_directives", new=AsyncMock(return_value="")),
            patch.object(dm, "_get_jarvis_lessons", new=AsyncMock(return_value="")),
            patch.object(dm, "_get_jarvis_knowledge", new=AsyncMock(return_value="")),
            patch.object(dm, "_get_position_management_principles", new=AsyncMock(return_value="")),
            patch.object(dm, "_analyze_chart", new=AsyncMock(return_value="")),
            patch.object(dm, "_ask_openwebui", new=AsyncMock(return_value=ai_reply)),
            patch.object(dm, "_log_journal", new=log_journal_mock),
            patch.object(dm, "_code_to_name", new=AsyncMock(return_value="테스트종목")),
            patch("stark.decision_logger.log_decision", new=log_decision_mock),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])
        return execute_mock, log_journal_mock, log_decision_mock

    async def test_execute_small_high_price_stock_is_skipped_not_forced_to_one_share(self):
        """EXECUTE_SMALL(0.5배) 적용 후 금액이 고가 종목(100만원) 1주 가격보다 작아지면
        1주 강제 매수 대신 SKIP해야 한다 — execution_guard.execute가 호출되지 않아야 한다."""
        execute_mock, log_journal_mock, log_decision_mock = self._patch_common(
            "EXECUTE_SMALL 일부 조건만 충족, 소액 검증"
        )

        body = {
            "bot": "stock_trader",
            "action": "buy",
            "symbol": "207940",
            "name": "삼성바이오로직스",
            "price": 1_000_000,
            "qty": 1,
            "strategy": "MA크로스",
            "reason": "전략:MA크로스",
            "base_amount": 1_500_000,  # × 0.5(EXECUTE_SMALL) = 750,000 < 1주(1,000,000원)
            "cash": 50_000_000,
        }

        result = await dm.jarvis_signal(_FakeRequest(body))

        execute_mock.assert_not_called()
        self.assertEqual(result["success"], True)
        self.assertEqual(result["executed"], False)
        self.assertEqual(result.get("blocked"), "size_below_one_share")
        self.assertIn("1주 가격 미만", result.get("reason", ""))

        log_journal_mock.assert_awaited_once()
        journal_args = log_journal_mock.await_args.args
        self.assertEqual(journal_args[6], "SKIP")
        self.assertIn("1주 가격 미만", journal_args[7])

        log_decision_mock.assert_awaited_once()
        self.assertEqual(log_decision_mock.await_args.args[2], "SKIP")

    async def test_cash_cap_below_one_share_is_skipped(self):
        """예수금 캡 적용 후 금액이 1주 가격보다 작아지는 경우도 동일하게 SKIP해야 한다."""
        execute_mock, log_journal_mock, _ = self._patch_common(
            "EXECUTE 강한 매수 신호, 추세 확실"
        )

        body = {
            "bot": "stock_trader",
            "action": "buy",
            "symbol": "207940",
            "name": "삼성바이오로직스",
            "price": 1_000_000,
            "qty": 1,
            "strategy": "MA크로스",
            "reason": "전략:MA크로스",
            "base_amount": 5_000_000,
            "cash": 800_000,  # min(5,000,000, 800,000) = 800,000 < 1주(1,000,000원)
        }

        result = await dm.jarvis_signal(_FakeRequest(body))

        execute_mock.assert_not_called()
        self.assertEqual(result.get("blocked"), "size_below_one_share")
        log_journal_mock.assert_awaited_once()
        self.assertEqual(log_journal_mock.await_args.args[6], "SKIP")

    async def test_normal_order_above_threshold_still_executes(self):
        """final_amount >= price인 정상 주문은 기존처럼 execution_guard.execute가 호출되고
        수량이 올바르게 전달돼야 한다(가드가 정상 매수를 막지 않는지 확인)."""
        execute_mock, log_journal_mock, _ = self._patch_common(
            "EXECUTE 강한 매수 신호, 추세 확실"
        )

        body = {
            "bot": "stock_trader",
            "action": "buy",
            "symbol": "005930",
            "name": "삼성전자",
            "price": 70_000,
            "qty": 1,
            "strategy": "MA크로스",
            "reason": "전략:MA크로스",
            "base_amount": 7_000_000,  # × 1.0 / 70,000 = 100주
            "cash": 50_000_000,
        }

        result = await dm.jarvis_signal(_FakeRequest(body))

        execute_mock.assert_awaited_once()
        signal_arg = execute_mock.await_args.args[0]
        self.assertEqual(signal_arg["qty"], 100)
        log_journal_mock.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
