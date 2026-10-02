"""
dashboard/main.py _jarvis_chat_impl → router.intent_router.route_late → order_handler
.handle_trade_command 통합 테스트 ([AT] fix/chat-price-lookup).

배경: 채팅 직접 매매("종목 N주 매수/매도")의 현재가 조회가 재시도 없는 단발 aiohttp
호출이라, KIS 속도제한(EGW00201)에 걸리면 output이 비어 price=0 → "현재가 조회 실패 —
주문 불가"로 끝났다(10/1 SK텔레콤, 10/2 부국철강 실측). 수정 후에는 dashboard의 기존
속도제한 대응 시세 조회(_fetch_kis_inquire_price → _kis_quote_get, 최대 2회 재시도)를
RouterContext.get_quote_fn으로 주입받아 재사용한다.

이 테스트는 dashboard/main.py의 실제 RouterContext 생성 코드(_jarvis_chat_impl)를 그대로
실행해 get_quote_fn이 실제로 주입되고, _fetch_kis_inquire_price의 재시도가 route_late →
handle_trade_command 전체 경로에서 실제로 동작하는지 검증한다(기존
tests/test_order_handler.py는 handle_trade_command를 직접 호출해 get_quote_fn을 넘기므로
이 배선 문제를 잡지 못한다). 픽스처는 실제 KIS 응답 모양(JSON 바디 + HTTP status)을
흉내낸다 — 실제 KIS 속도제한 응답은 HTTP 500으로 온 사례가 있다
(stock_trader/kis_trader.py, fix/kis-rate-limit-http500 참고). dashboard._kis_quote_get
자체는 HTTP status를 보지 않고 msg1의 "초당" 여부만 보고 재시도하므로, 이 테스트는
status=500인 응답도 msg1만으로 재시도되는지 함께 확인한다.

[AT] fix/chat-price-lookup 배선 검증: dashboard/main.py의 RouterContext 생성 코드에서
get_quote_fn=_fetch_kis_inquire_price 줄을 지우고 이 테스트를 돌리면
test_route_late_buy_retries_rate_limit_then_proceeds_with_order가 "연결되지 않았습니다"
문구로 실패한다 — 배선이 빠지면 즉시 드러나도록 설계했다(수정 중 직접 확인함).
"""
import asyncio
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
from tests.test_order_handler import FakePool, FakeRedis  # noqa: E402

RATE_LIMIT_MSG = "초당 거래건수를 초과하였습니다."


class FakeChatConfig:
    kis_base_url = "https://example.invalid"
    kis_app_key = "key"
    kis_app_secret = "secret"
    kis_account_no = "12345678-01"
    KIS_IS_PAPER = False
    # RISK_PER_TRADE_PCT를 일부러 두지 않음 → 사이징 한도 계산을 건너뛰어(현재가 조회
    # 배선 검증에 집중하도록) 요청 수량이 그대로 유지된다.


class FakeResponse:
    """실제 KIS 응답 모양(JSON 바디 + HTTP status)을 흉내낸다."""

    def __init__(self, payload, status=200):
        self._payload = payload
        self.status = status

    async def json(self, *args, **kwargs):
        return self._payload


class FakeSession:
    def __init__(self, handler):
        self._handler = handler

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, **kwargs):
        payload, status = await self._handler(url, kwargs)
        return FakeResponse(payload, status=status)


class TestChatPriceLookupWiring(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.dm = dm
        self.calls = []  # (symbol,) — _fetch_kis_inquire_price가 실제로 호출한 종목코드
        self._patches = []

        def start(p):
            m = p.start()
            self._patches.append(p)
            return m

        # 이벤트 루프가 테스트마다 새로 생성되므로 세마포어도 새로 만든다
        # (tests/test_dashboard_chart_rate_limit.py와 동일한 패턴).
        start(patch.object(dm, "_KIS_QUOTE_SEM", asyncio.Semaphore(dm._KIS_QUOTE_CONCURRENCY)))
        start(patch.object(dm, "get_kis_token", new=AsyncMock(return_value="FAKE_TOKEN")))
        # 매수 안전장치 관문(buy_gate)의 투자경고/VI 조회는 이 파일의 관심사가 아니므로
        # 정상 종목으로 통과시켜 현재가 조회 배선만 분리해서 검증한다.
        start(patch.object(dm, "_get_market_warning_for_gate",
                           new=AsyncMock(return_value={"mrkt_warn_cls_code": "00", "vi_cls_code": "N"})))
        start(patch.object(dm.intent_router, "route_early", new=AsyncMock(return_value=None)))
        start(patch("dashboard.main.asyncio.sleep", new=AsyncMock()))

    def tearDown(self):
        for p in reversed(self._patches):
            p.stop()

    def set_handler(self, handler):
        """handler(url, kwargs) → (payload, status). 동기/코루틴 모두 허용."""
        async def wrapped(url, kw):
            symbol = kw["params"]["FID_INPUT_ISCD"]
            self.calls.append(symbol)
            res = handler(url, kw)
            if asyncio.iscoroutine(res):
                res = await res
            return res

        fake = MagicMock()
        fake.ClientSession = lambda **kw: FakeSession(wrapped)
        fake.TCPConnector = lambda **kw: None
        fake.ClientTimeout = lambda **kw: None
        p = patch.object(self.dm, "_aiohttp", fake)
        p.start()
        self._patches.append(p)

    def _order_patches(self, universe, fake_kis_order):
        return [
            patch.object(self.dm, "universe", universe),
            patch.object(self.dm, "db_pool", FakePool()),
            patch.object(self.dm, "redis_client", FakeRedis()),
            patch.object(self.dm, "config", FakeChatConfig()),
            patch.object(self.dm, "_kis_stock_order", new=fake_kis_order),
            patch.object(self.dm, "get_stock_positions",
                        new=AsyncMock(return_value={"success": True, "data": []})),
            patch.object(self.dm, "_send_telegram", new=AsyncMock()),
            patch.object(self.dm, "_log_journal", new=AsyncMock()),
        ]

    async def test_route_late_buy_retries_rate_limit_then_proceeds_with_order(self):
        """(a) 첫 응답이 KIS 속도제한(EGW00201, 실측상 HTTP 500으로 옴)이어도 0.7초 뒤
        재시도로 가격을 받아오면, route_late → handle_trade_command 전체 경로에서 주문이
        실제로 진행돼야 한다. [AT] fix/chat-price-lookup 배선이 빠지면(get_quote_fn 미주입)
        price=0으로 처리돼 이 테스트가 실패한다."""
        universe = MagicMock()
        universe.resolve_symbol = AsyncMock(return_value=("005930", "SK텔레콤"))

        order_calls = []

        async def fake_kis_order(symbol, price, qty, is_buy):
            order_calls.append((symbol, price, qty, is_buy))
            return {"success": True, "order_no": "0001234567"}

        seq = [
            ({"rt_cd": "1", "msg_cd": "EGW00201", "msg1": RATE_LIMIT_MSG}, 500),
            ({"rt_cd": "0", "msg1": "정상처리",
              "output": {"stck_prpr": "58000", "hts_kor_isnm": "SK텔레콤",
                         "mrkt_warn_cls_code": "00", "vi_cls_code": "N"}}, 200),
        ]
        self.set_handler(lambda url, kw: seq.pop(0))

        patches = self._order_patches(universe, fake_kis_order)
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        res = await self.dm._jarvis_chat_impl({"message": "SK텔레콤 2주 매수", "channel": "web"})

        self.assertTrue(res["success"])
        self.assertIn("2주 매수 완료", res["reply"])
        self.assertEqual(order_calls, [("005930", 58000, 2, True)])
        self.assertEqual(len(self.calls), 2, "속도제한 1회 + 재시도 1회로 총 2번 호출돼야 한다")

    async def test_route_late_buy_blocked_when_quote_keeps_failing(self):
        """(b) 재시도(최대 2회)까지 모두 KIS 속도제한이면 가격을 못 얻어 기존과 동일한
        실패 문구로 주문을 차단해야 한다 — 진짜 실패는 여전히 실패로 끝나야 한다."""
        universe = MagicMock()
        universe.resolve_symbol = AsyncMock(return_value=("005930", "SK텔레콤"))

        order_calls = []

        async def fake_kis_order(symbol, price, qty, is_buy):
            order_calls.append((symbol, price, qty, is_buy))
            return {"success": True}

        self.set_handler(
            lambda url, kw: ({"rt_cd": "1", "msg_cd": "EGW00201", "msg1": RATE_LIMIT_MSG}, 500))

        patches = self._order_patches(universe, fake_kis_order)
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        res = await self.dm._jarvis_chat_impl({"message": "SK텔레콤 2주 매수", "channel": "web"})

        self.assertTrue(res["success"])
        self.assertIn("현재가 조회 실패", res["reply"])
        self.assertEqual(order_calls, [], "현재가를 못 얻었는데 주문이 나가면 안 된다")
        self.assertEqual(len(self.calls), 3, "최초 1회 + 재시도 2회 = 3회 호출 후 실패해야 한다")


if __name__ == "__main__":
    unittest.main()
