"""
router/intent_router.py 단위 테스트: route_early/route_late가 각 handler를 원본과
동일한 우선순위로 호출하는지, 아무 handler도 안 걸리면 None(Tier 2 AI 분류로)을
반환하는지 검증한다.
"""
import asyncio
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from market.universe import Universe  # noqa: E402
from router import intent_router  # noqa: E402


def make_ctx(**overrides):
    defaults = dict(
        pool=None, redis=None, universe=Universe(None), config=None,
        stock_name_map={}, session_id="advice",
        send_telegram=None, get_kis_token=None, kis_order=None,
        analyze_chart=None, get_stock_positions=None, log_journal=None,
        web_research=None, jarvis_chat=None, channel="web",
    )
    defaults.update(overrides)
    return intent_router.RouterContext(**defaults)


class TestRouteEarly(unittest.IsolatedAsyncioTestCase):
    async def test_directive_wins_over_setting_for_directive_message(self):
        # "지시:" 접두는 directive_handler 전담 — setting_handler와 겹칠 여지 없음
        ctx = make_ctx(pool=_FakePool(), redis=_FakeRedis())
        reply = await intent_router.route_early("지시: 손절 -5% 이하로 낮추지 마", ctx)
        self.assertIn("지시", reply)
        self.assertIn("저장 완료", reply)

    async def test_no_pattern_matches_returns_none(self):
        ctx = make_ctx(pool=_FakePool(), redis=_FakeRedis())
        reply = await intent_router.route_early("오늘 날씨 어때?", ctx)
        self.assertIsNone(reply)


class TestRouteLate(unittest.IsolatedAsyncioTestCase):
    async def test_chart_keyword_short_circuits_before_advice_check(self):
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})

        async def analyze_chart(symbol, name):
            return f"[차트] {name}"

        ctx = make_ctx(universe=universe, analyze_chart=analyze_chart, redis=_FakeRedis())
        reply = await intent_router.route_late("삼성전자 차트 어때", ctx)
        self.assertEqual(reply, "[차트] 삼성전자")

    async def test_no_pattern_matches_returns_none(self):
        ctx = make_ctx(redis=_FakeRedis())
        reply = await intent_router.route_late("오늘 날씨 어때?", ctx)
        self.assertIsNone(reply)


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self):
        self.rows = []
        self._next_id = 1

    async def fetch(self, query, *args):
        return list(self.rows)

    async def fetchval(self, query, *args):
        row = {"id": self._next_id, "content": args[0], "is_active": True}
        self.rows.append(row)
        self._next_id += 1
        return row["id"]

    async def execute(self, query, *args):
        return "UPDATE 1"


class _FakePool:
    def __init__(self):
        self._conn = _FakeConn()

    def acquire(self):
        return _AcquireCtx(self._conn)


class _FakeRedis:
    def __init__(self):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, value):
        self.store[key] = value

    async def delete(self, key):
        self.store.pop(key, None)


class TestRouteLateSizingWiring(unittest.IsolatedAsyncioTestCase):
    """route_late(라인 120)가 ctx.get_balance_fn/get_recent_ohlcv_fn을 실제로
    order_handler.handle_trade_command에 전달하는지 검증한다([AT] fix/chat-sizing-cap-wiring).
    기존 사이징 한도 테스트(test_order_handler_sizing_cap.py)는 handle_trade_command를
    직접 호출해 인자를 넘겼기 때문에, RouterContext에 필드가 빠져 있었던 배선 문제를
    잡지 못했다 — 이 테스트는 route_late를 통해서만 호출한다."""

    async def test_buy_command_through_route_late_applies_sizing_cap(self):
        from unittest.mock import patch

        from tests.test_order_handler import FakePriceSession

        universe = Universe(None)
        universe.replace_cache({"삼성바이오로직스": "207940"})

        class FakeSizingConfig:
            RISK_PER_TRADE_PCT = 0.75
            INITIAL_SEED_KRW = 10_000_000
            kis_base_url = "https://example.invalid"
            kis_app_key = "key"
            kis_app_secret = "secret"

        async def get_kis_token():
            return "FAKE_TOKEN"

        async def kis_order(symbol, price_, qty, is_buy):
            return {"success": True}

        async def send_telegram(text, **kw):
            pass

        async def log_journal(*args, **kwargs):
            pass

        balance_calls = []

        async def get_balance():
            balance_calls.append(1)
            return {"total": 10_000_000}

        ctx = make_ctx(
            universe=universe, pool=_FakePool(), redis=_FakeRedis(), config=FakeSizingConfig(),
            get_kis_token=get_kis_token, kis_order=kis_order, send_telegram=send_telegram,
            log_journal=log_journal, get_balance_fn=get_balance, get_recent_ohlcv_fn=None,
        )

        def _price_session(price: int):
            return FakePriceSession({"output": {"stck_prpr": str(price), "hts_kor_isnm": "삼성바이오로직스"}})

        # equity 1000만 × 위험 0.75% ÷ 손절 7%(기본값) ≈ 107.1만원 기본금액.
        # ATR 조회 없음 → 보수적 폴백 배율 0.75배 → 한도 ≈80.36만원.
        # 40만원 × 3주 = 120만원이 한도를 넘으므로 floor(80.36만/40만) = 2주로 줄어야 한다.
        with patch("aiohttp.ClientSession", return_value=_price_session(400_000)):
            reply = await intent_router.route_late("삼성바이오로직스 3주 매수", ctx)

        self.assertIn("요청 3주 → 사이징 한도로 2주로 조정", reply)
        self.assertIn("2주 매수 완료", reply)
        self.assertTrue(balance_calls, "ctx.get_balance_fn이 route_late를 통해 실제로 호출되지 않았다")


if __name__ == "__main__":
    unittest.main()
