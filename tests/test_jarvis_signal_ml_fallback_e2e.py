"""
실제 운영 경로(stock_trader → /api/jarvis/signal → stark/context_collector →
stark/decision_engine) end-to-end 테스트 ([AT] feat/ml-demote).

배경: buy_prob 확률 구간별 적중률을 붙여 보니 확률이 높을수록 잘 맞는 관계가 없었다
(90~100% 구간 n=362 적중 43.9%/평균수익 -0.51%, 70% 이상 합산 n=505 적중 46.7%/평균수익
0.0%, PM 승인 2026-10-02). stark/decision_engine.py의 AI 폴백이 신호 reason에 적힌
"ML매수확률:NN%"를 읽어 70% 이상이면 매수를 시도하던 경로를 제거했다.

이 테스트는 단위 테스트(tests/test_decision_engine.py, tests/test_ml_no_prediction.py)와
달리 dashboard/main.py의 실제 /api/jarvis/signal 엔드포인트(jarvis_signal)를 그대로
호출해서, AI 응답이 실패하고 신호 reason에 ML 확률이 높게 남아있어도(stock_trader가
과거처럼 ML 문구를 reason에 넣는 경로로 되돌아간 경우를 가정) stark/execution_guard.execute
(실제 KIS 주문 실행)가 전혀 호출되지 않는지를 운영 경로 그대로 검증한다.
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
    """실제 FastAPI 라이브러리가 설치돼 있지 않은 환경에서, @app.get/post/... 데코레이터가
    원래 함수를 그대로 보존하도록(실제 FastAPI처럼) 흉내 내는 최소 스텁.
    다른 테스트 파일이 먼저 `fastapi`를 평범한 MagicMock으로 잡아두면 데코레이터가
    원본 함수를 잃어버려(jarvis_signal이 MagicMock이 되어) 이 테스트가 실제 엔드포인트를
    호출할 수 없게 되므로, 이 테스트 전용으로 더 정확한 스텁을 쓰고 dashboard.main을
    reload해서 데코레이터가 이 스텁으로 다시 적용되게 한다."""
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
    """다른 테스트 파일이 이어서 돌 때를 위해 fastapi 스텁과 dm을 원래 상태로 되돌린다."""
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


class TestJarvisSignalMlFallbackNeverBuys(unittest.IsolatedAsyncioTestCase):
    async def test_ai_failure_with_high_ml_probability_reason_never_executes_buy(self):
        """운영 경로 그대로: AI 응답 실패 + reason에 ML매수확률:95% 가 적혀 있어도
        execution_guard.execute(실주문 실행)가 호출되지 않고 SKIP으로 끝나야 한다."""
        execute_mock = AsyncMock()
        log_journal_mock = AsyncMock()

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
            # AI 호출 실패 재현 (429/빈 응답) — 과거에는 여기서 ML 확률로 매수를 시도했다.
            patch.object(dm, "_ask_openwebui", new=AsyncMock(return_value="")),
            patch.object(dm, "_log_journal", new=log_journal_mock),
            patch.object(dm, "_code_to_name", new=AsyncMock(return_value="삼성전자")),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        body = {
            "bot": "stock_trader",
            "action": "buy",
            "symbol": "005930",
            "name": "삼성전자",
            "price": 70000,
            "qty": 2,
            "strategy": "MA크로스",
            # stock_trader가 과거처럼 ML 확률 문구를 reason에 실어 보내는 경우를 가정
            "reason": "전략:MA크로스 | ML매수확률:95%(강함) | 수급:외국인+1 기관+1 | 뉴스:없음",
        }

        result = await dm.jarvis_signal(_FakeRequest(body))

        execute_mock.assert_not_called()
        self.assertEqual(result, {"success": True, "executed": False,
                                   "jarvis_reply": result["jarvis_reply"]})
        self.assertIn("보수적 SKIP", result["jarvis_reply"])
        log_journal_mock.assert_awaited_once()
        self.assertEqual(log_journal_mock.await_args.args[6], "SKIP")


if __name__ == "__main__":
    unittest.main()
