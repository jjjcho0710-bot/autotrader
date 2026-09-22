"""
router/handlers/order_handler.py 단위 테스트: 능동제안 승인/거절, 매수제안 승인/거절,
채팅 직접 매매 지시(_handle_trade_command 이관분)를 검증한다.

KIS 실주문·시세 조회는 전부 콜러블로 주입되므로 실제 네트워크 없이 결과만 갈아끼워
분기를 검증한다.
"""
import asyncio
import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from market.universe import Universe  # noqa: E402
from router.handlers import order_handler  # noqa: E402


class FakeRedis:
    def __init__(self, store=None):
        self.store = store or {}
        self.deleted = []
        self.rpushed = []

    async def get(self, key):
        v = self.store.get(key)
        return v

    async def delete(self, key):
        self.deleted.append(key)
        self.store.pop(key, None)

    async def keys(self, pattern):
        prefix = pattern.rstrip("*")
        return [k for k in self.store if k.startswith(prefix)]

    async def rpush(self, key, value):
        self.rpushed.append((key, value))


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakeTradeConnection:
    def __init__(self):
        self.inserted = []

    async def execute(self, query, *args):
        assert "trade_history" in query
        self.inserted.append(args)
        return "INSERT 1"


class FakePool:
    def __init__(self):
        self._conn = FakeTradeConnection()

    def acquire(self):
        return _AcquireCtx(self._conn)


class FakeConfig:
    kis_base_url = "https://example.invalid"
    kis_app_key = "key"
    kis_app_secret = "secret"


class TestHandleAdviceResponse(unittest.IsolatedAsyncioTestCase):
    async def test_no_match_returns_none(self):
        reply = await order_handler.handle_advice_response(
            "오늘 날씨 어때", redis=FakeRedis(), jarvis_chat_fn=None, send_telegram_fn=None, session_id="s")
        self.assertIsNone(reply)

    async def test_expired_advice_reports_gone(self):
        reply = await order_handler.handle_advice_response(
            "승인 3", redis=FakeRedis(), jarvis_chat_fn=None, send_telegram_fn=None, session_id="s")
        self.assertIn("없거나 만료", reply)

    async def test_reject_deletes_and_replies(self):
        redis = FakeRedis({"advice:2": json.dumps({"title": "삼성전자 매수", "command": "삼성전자 1주 매수"})})
        reply = await order_handler.handle_advice_response(
            "거절 2", redis=redis, jarvis_chat_fn=None, send_telegram_fn=None, session_id="s")
        self.assertIn("거절했어요", reply)
        self.assertIn("advice:2", redis.deleted)

    async def test_approve_non_trade_command_executes_via_jarvis_chat(self):
        redis = FakeRedis({"advice:1": json.dumps({"title": "지시 저장", "command": "지시: 테스트 지시"})})
        sent = []

        async def send_telegram(text, **kw):
            sent.append(text)

        async def jarvis_chat(body):
            return {"reply": f"처리됨: {body['message']}"}

        reply = await order_handler.handle_advice_response(
            "승인 1", redis=redis, jarvis_chat_fn=jarvis_chat, send_telegram_fn=send_telegram, session_id="s")
        self.assertIn("제안 1 승인", reply)
        self.assertIn("처리됨: 지시: 테스트 지시", reply)
        self.assertEqual(len(sent), 1)


class TestHandleProposalResponse(unittest.IsolatedAsyncioTestCase):
    async def test_no_trigger_keyword_returns_none(self):
        reply = await order_handler.handle_proposal_response(
            "오늘 날씨 어때", redis=FakeRedis(), kis_order_fn=None, log_journal_fn=None, send_telegram_fn=None)
        self.assertIsNone(reply)

    async def test_no_pending_proposal_returns_none(self):
        redis = FakeRedis({})
        reply = await order_handler.handle_proposal_response(
            "승인", redis=redis, kis_order_fn=None, log_journal_fn=None, send_telegram_fn=None)
        self.assertIsNone(reply)

    async def test_reject_deletes_proposal(self):
        redis = FakeRedis({
            "proposal:latest": "005930",
            "proposal:005930": json.dumps({"symbol": "005930", "name": "삼성전자", "price": 70000, "qty": 1}),
        })
        reply = await order_handler.handle_proposal_response(
            "거절", redis=redis, kis_order_fn=None, log_journal_fn=None, send_telegram_fn=None)
        self.assertIn("거절 처리했어요", reply)
        self.assertIn("proposal:005930", redis.deleted)

    async def test_approve_executes_order_and_logs(self):
        redis = FakeRedis({
            "proposal:latest": "005930",
            "proposal:005930": json.dumps({"symbol": "005930", "name": "삼성전자", "price": 70000, "qty": 1}),
        })
        logged = []
        sent = []

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def log_journal(*args, **kwargs):
            logged.append(args)

        async def send_telegram(text, **kw):
            sent.append(text)

        reply = await order_handler.handle_proposal_response(
            "사자", redis=redis, kis_order_fn=kis_order, log_journal_fn=log_journal, send_telegram_fn=send_telegram)
        self.assertIn("매수 체결", reply)
        self.assertEqual(len(logged), 1)
        self.assertEqual(len(sent), 1)

    async def test_approve_order_failure_reports_error(self):
        redis = FakeRedis({
            "proposal:latest": "005930",
            "proposal:005930": json.dumps({"symbol": "005930", "name": "삼성전자", "price": 70000, "qty": 1}),
        })

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": False, "error": "잔고 부족"}

        async def log_journal(*args, **kwargs):
            pass

        reply = await order_handler.handle_proposal_response(
            "사자", redis=redis, kis_order_fn=kis_order, log_journal_fn=log_journal, send_telegram_fn=None)
        self.assertIn("매수 실패", reply)


class TestHandleTradeCommand(unittest.IsolatedAsyncioTestCase):
    async def test_no_buy_sell_keyword_returns_none(self):
        reply = await order_handler.handle_trade_command(
            "안녕", pool=None, redis=None, universe=Universe(None), get_kis_token_fn=None,
            config=FakeConfig(), kis_order_fn=None, get_stock_positions_fn=None,
            send_telegram_fn=None, log_journal_fn=None)
        self.assertIsNone(reply)

    async def test_missing_quantity_returns_none(self):
        reply = await order_handler.handle_trade_command(
            "삼성전자 매수", pool=None, redis=None, universe=Universe(None), get_kis_token_fn=None,
            config=FakeConfig(), kis_order_fn=None, get_stock_positions_fn=None,
            send_telegram_fn=None, log_journal_fn=None)
        self.assertIsNone(reply)

    async def test_unresolved_symbol_returns_warning(self):
        universe = Universe(None)  # 빈 캐시, DB 없음 → resolve 실패
        reply = await order_handler.handle_trade_command(
            "동원산업 2주 매수", pool=None, redis=None, universe=universe, get_kis_token_fn=None,
            config=FakeConfig(), kis_order_fn=None, get_stock_positions_fn=None,
            send_telegram_fn=None, log_journal_fn=None)
        self.assertIn("종목을 특정할 수 없어요", reply)

    async def test_price_lookup_failure_returns_warning(self):
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})

        async def get_kis_token():
            return ""  # 토큰 없음 → 가격 조회 스킵되어 price=0 유지

        reply = await order_handler.handle_trade_command(
            "삼성전자 2주 매수", pool=None, redis=None, universe=universe, get_kis_token_fn=get_kis_token,
            config=FakeConfig(), kis_order_fn=None, get_stock_positions_fn=None,
            send_telegram_fn=None, log_journal_fn=None)
        self.assertIn("현재가 조회 실패", reply)


if __name__ == "__main__":
    unittest.main()
