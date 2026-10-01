"""
dashboard/main.py _jarvis_chat_impl → router.intent_router → order_handler 통합 테스트
([AT] fix/chat-sizing-cap-wiring).

배경: router/intent_router.py 120행이 order_handler.handle_trade_command를 호출할 때
get_balance_fn/get_recent_ohlcv_fn을 넘기지 않아, 채팅 직접 매수의 리스크 기반 사이징
한도가 실제 운영에서 한 번도 적용되지 않았다(10/1 11:02 삼성바이오로직스 2주×143.4만원=
286.8만원 매수가 한도 약 79만원을 초과 체결). dashboard/main.py의 _handle_trade_command
래퍼에는 get_balance_fn을 올바르게 주입했지만, 그 래퍼를 호출하는 곳이 없어 죽은
코드였다(제거함).

기존 tests/test_order_handler_sizing_cap.py는 order_handler.handle_trade_command를
직접 호출해 인자를 넘겼기 때문에 이 배선 문제를 잡지 못했다. 이 테스트는 dashboard/main.py의
실제 RouterContext 생성 코드(_jarvis_chat_impl)를 그대로 실행해 get_balance_fn/
get_recent_ohlcv_fn이 실제로 주입되고 handle_trade_command까지 전달되는지 검증한다.
"""
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

import dashboard.main as dm  # noqa: E402
from tests.test_order_handler import FakePool, FakePriceSession, FakeRedis  # noqa: E402


class FakeSizingConfig:
    RISK_PER_TRADE_PCT = 0.75
    INITIAL_SEED_KRW = 10_000_000
    kis_base_url = "https://example.invalid"
    kis_app_key = "key"
    kis_app_secret = "secret"


def _price_session(price: int, name: str = "삼성바이오로직스"):
    return FakePriceSession({"output": {"stck_prpr": str(price), "hts_kor_isnm": name}})


class TestChatSizingCapWiring(unittest.IsolatedAsyncioTestCase):
    async def test_buy_command_through_real_jarvis_chat_impl_applies_sizing_cap(self):
        universe = MagicMock()
        universe.resolve_symbol = AsyncMock(return_value=("207940", "삼성바이오로직스"))

        balance_calls = []

        async def fake_get_balance():
            balance_calls.append(1)
            return {"total": 10_000_000}

        kis_order_calls = []

        async def fake_kis_order(symbol, price, qty, is_buy):
            kis_order_calls.append((symbol, price, qty, is_buy))
            return {"success": True, "order_no": "0001234567"}

        patches = [
            patch.object(dm, "universe", universe),
            patch.object(dm, "db_pool", FakePool()),
            patch.object(dm, "redis_client", FakeRedis()),
            patch.object(dm, "config", FakeSizingConfig()),
            patch.object(dm, "get_kis_token", new=AsyncMock(return_value="FAKE_TOKEN")),
            patch.object(dm, "_kis_stock_order", new=fake_kis_order),
            patch.object(dm, "get_stock_positions", new=AsyncMock(return_value={"success": True, "data": []})),
            patch.object(dm, "_send_telegram", new=AsyncMock()),
            patch.object(dm, "_log_journal", new=AsyncMock()),
            patch.object(dm, "_get_balance_for_sizing", new=fake_get_balance),
            patch.object(dm, "_get_recent_daily_ohlcv_for_sizing", new=AsyncMock(return_value=[])),
            patch.object(dm.intent_router, "route_early", new=AsyncMock(return_value=None)),
            patch("aiohttp.ClientSession", return_value=_price_session(400_000)),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        # equity 1000만 × 위험 0.75% ÷ 손절 7%(기본값) ≈ 107.1만원 기본금액.
        # ATR 조회 없음(폴백) → 보수적 배율 0.75배 → 한도 ≈80.36만원.
        # 40만원 × 3주 = 120만원이 한도를 넘으므로 floor(80.36만/40만) = 2주로 줄어야 한다.
        res = await dm._jarvis_chat_impl({"message": "삼성바이오로직스 3주 매수", "channel": "web"})

        self.assertTrue(res["success"])
        reply = res["reply"]
        self.assertIn("요청 3주 → 사이징 한도로 2주로 조정", reply)
        self.assertIn("2주 매수 완료", reply)
        self.assertEqual(kis_order_calls, [("207940", 400000, 2, True)])
        self.assertTrue(balance_calls, "실제 RouterContext 생성 경로에서 get_balance_fn이 호출되지 않았다"
                                        " — dashboard.main의 배선이 빠졌을 가능성")


if __name__ == "__main__":
    unittest.main()
