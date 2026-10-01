"""
router/handlers/order_handler.py 단위 테스트: 능동제안 승인/거절, 매수제안 승인/거절,
채팅 직접 매매 지시(_handle_trade_command 이관분)를 검증한다.

KIS 실주문·시세 조회는 전부 콜러블로 주입되므로 실제 네트워크 없이 결과만 갈아끼워
분기를 검증한다.
"""
import asyncio
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# order_handler.py는 handle_trade_command 안에서 필요할 때만 aiohttp를 지연 임포트한다.
# 테스트 환경에 aiohttp가 없어도 현재가 조회를 흉내내려면 미리 더미 모듈을 등록해둔다.
try:
    import aiohttp  # noqa: F401
except ImportError:
    _stub = types.ModuleType("aiohttp")
    _stub.ClientSession = object
    _stub.TCPConnector = lambda *a, **kw: None
    _stub.ClientTimeout = lambda *a, **kw: None
    sys.modules["aiohttp"] = _stub

from market.universe import Universe  # noqa: E402
from router.handlers import order_handler  # noqa: E402


class FakePriceResponse:
    """전량매도 분기까지 도달하기 위한 현재가 조회 응답(가격>0) 목: `pr = await sess.get(...)` 형태로 사용됨"""
    def __init__(self, data):
        self._data = data

    def __await__(self):
        async def _coro():
            return self
        return _coro().__await__()

    async def json(self):
        return self._data


class FakePriceSession:
    def __init__(self, data):
        self._data = data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, *args, **kwargs):
        return FakePriceResponse(self._data)


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
    """trade_history INSERT 기록에 더해, stark.execution_guard.precheck/buy_gate가 매수 전에
    조회하는 공시(stock_disclosure)·당일 손절 횟수 쿼리에도 안전한 기본값(공시 없음, 손절 0회)
    으로 응답한다([AT] buy-gate-unification — 채팅 매수가 이 안전장치를 거치게 되면서 필요)."""

    def __init__(self, stop_loss_count=0, disclosures=None, buy_history=None):
        self.inserted = []
        self.stop_loss_count = stop_loss_count
        self.disclosures = disclosures if disclosures is not None else []
        # symbol -> [{"price": ...}, ...] 매수 이력(물타기 정책 검사용, side='BUY' 쿼리)
        self.buy_history = buy_history or {}

    async def fetch(self, query, *args):
        if "stock_disclosure" in query:
            return list(self.disclosures)
        if "trade_history" in query and "side='BUY'" in query:
            symbol = args[1]
            return list(self.buy_history.get(symbol, []))
        return []

    async def fetchval(self, query, *args):
        if "trade_history" in query:
            return self.stop_loss_count
        return None

    async def execute(self, query, *args):
        assert "trade_history" in query
        self.inserted.append(args)
        return "INSERT 1"


class FakePool:
    def __init__(self, stop_loss_count=0, disclosures=None, buy_history=None):
        self._conn = FakeTradeConnection(
            stop_loss_count=stop_loss_count, disclosures=disclosures, buy_history=buy_history)

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

    async def test_all_sell_position_lookup_failure_returns_distinct_message(self):
        """전량매도 중 보유 조회 자체가 예외로 실패한 경우 — "보유 수량이 없어요"와 달리
        구분된 메시지("조회에 실패")로 응답해야 한다. KIS 연결 오류 중인데 실제로는 미청산 상태였던
        9/29 09:37 사고 재현 케이스."""
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})

        async def get_kis_token():
            return "FAKE_TOKEN"

        async def get_stock_positions_fail():
            raise ConnectionError("KIS 조회 오류")

        with patch("aiohttp.ClientSession", return_value=FakePriceSession(
                {"output": {"stck_prpr": "70000", "hts_kor_isnm": "삼성전자"}})):
            reply = await order_handler.handle_trade_command(
                "삼성전자 전량 매도", pool=None, redis=None, universe=universe,
                get_kis_token_fn=get_kis_token, config=FakeConfig(), kis_order_fn=None,
                get_stock_positions_fn=get_stock_positions_fail,
                send_telegram_fn=None, log_journal_fn=None)

        self.assertIn("조회에 실패", reply)
        self.assertNotIn("보유 수량이 없어요", reply)

    async def test_all_sell_zero_quantity_returns_no_holdings_message(self):
        """전량매도 중 조회는 성공했지만 실제 보유 수량이 0인 경우엔 그대로 "보유 수량이 없어요"."""
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})

        async def get_kis_token():
            return "FAKE_TOKEN"

        async def get_stock_positions_empty():
            return {"data": []}

        with patch("aiohttp.ClientSession", return_value=FakePriceSession(
                {"output": {"stck_prpr": "70000", "hts_kor_isnm": "삼성전자"}})):
            reply = await order_handler.handle_trade_command(
                "삼성전자 전량 매도", pool=None, redis=None, universe=universe,
                get_kis_token_fn=get_kis_token, config=FakeConfig(), kis_order_fn=None,
                get_stock_positions_fn=get_stock_positions_empty,
                send_telegram_fn=None, log_journal_fn=None)

        self.assertIn("보유 수량이 없어요", reply)

    async def test_all_sell_success_computes_and_reports_pnl(self):
        """전량매도 체결 성공 시 보유 평단가로 손익을 계산해 메시지·trade_history에 남긴다."""
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})
        pool = FakePool()
        redis = FakeRedis()
        sent = []
        logged = []

        async def get_kis_token():
            return "FAKE_TOKEN"

        async def get_stock_positions():
            return {"data": [{"symbol": "005930", "qty": 22, "avg_price": 65000}]}

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text, **kw):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            logged.append(args)

        with patch("aiohttp.ClientSession", return_value=FakePriceSession(
                {"output": {"stck_prpr": "70000", "hts_kor_isnm": "삼성전자"}})):
            reply = await order_handler.handle_trade_command(
                "삼성전자 전량 매도", pool=pool, redis=redis, universe=universe,
                get_kis_token_fn=get_kis_token, config=FakeConfig(), kis_order_fn=kis_order,
                get_stock_positions_fn=get_stock_positions,
                send_telegram_fn=send_telegram, log_journal_fn=log_journal)

        # pnl = (70000 - 65000) * 22 = 110,000 / pnl_rate = 5000/65000*100 ≈ +7.7%
        self.assertIn("손익 +110,000원 (+7.7%)", reply)
        self.assertIn("손익 +110,000원 (+7.7%)", sent[0])
        inserted_args = pool._conn.inserted[0]
        self.assertEqual(inserted_args[-1], 110000.0)

    async def test_qty_sell_success_looks_up_avg_price_and_reports_pnl(self):
        """수량 지정 매도("N주 매도")도 전량매도와 동일하게 평단가를 조회해 손익을 계산한다."""
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})
        pool = FakePool()
        redis = FakeRedis()

        async def get_kis_token():
            return "FAKE_TOKEN"

        async def get_stock_positions():
            return {"data": [{"symbol": "005930", "qty": 30, "avg_price": 65000}]}

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text, **kw):
            pass

        async def log_journal(*args, **kwargs):
            pass

        with patch("aiohttp.ClientSession", return_value=FakePriceSession(
                {"output": {"stck_prpr": "70000", "hts_kor_isnm": "삼성전자"}})):
            reply = await order_handler.handle_trade_command(
                "삼성전자 22주 매도", pool=pool, redis=redis, universe=universe,
                get_kis_token_fn=get_kis_token, config=FakeConfig(), kis_order_fn=kis_order,
                get_stock_positions_fn=get_stock_positions,
                send_telegram_fn=send_telegram, log_journal_fn=log_journal)

        self.assertIn("손익 +110,000원 (+7.7%)", reply)
        inserted_args = pool._conn.inserted[0]
        self.assertEqual(inserted_args[-1], 110000.0)

    async def test_buy_success_has_no_pnl_display(self):
        """매수는 평단가 조회·손익 계산·표시를 하지 않는다(기존 동작 유지). 매수 안전장치
        관문(stark.execution_guard.buy_gate, [AT] buy-gate-unification)이 보유 종목수 한도
        확인을 위해 get_stock_positions_fn을 호출하지만, 그 결과를 pnl/평단가 계산에는
        쓰지 않는다 — 이전엔 매수 시 보유 조회 자체를 하지 않았지만 이제는 안전장치 때문에
        호출되므로, 호출 자체를 금지하던 예전 단언은 더 이상 유효하지 않다."""
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})
        pool = FakePool()
        redis = FakeRedis()

        async def get_kis_token():
            return "FAKE_TOKEN"

        async def get_stock_positions():
            return {"success": True, "data": []}

        async def get_market_warning(symbol):
            return {"mrkt_warn_cls_code": "00", "vi_cls_code": "N"}

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text, **kw):
            pass

        async def log_journal(*args, **kwargs):
            pass

        with patch("aiohttp.ClientSession", return_value=FakePriceSession(
                {"output": {"stck_prpr": "70000", "hts_kor_isnm": "삼성전자"}})):
            reply = await order_handler.handle_trade_command(
                "삼성전자 2주 매수", pool=pool, redis=redis, universe=universe,
                get_kis_token_fn=get_kis_token, config=FakeConfig(), kis_order_fn=kis_order,
                get_stock_positions_fn=get_stock_positions,
                send_telegram_fn=send_telegram, log_journal_fn=log_journal,
                get_market_warning_fn=get_market_warning)

        self.assertNotIn("손익", reply)
        inserted_args = pool._conn.inserted[0]
        self.assertIsNone(inserted_args[-1])

    async def test_qty_sell_avg_price_lookup_failure_skips_pnl_but_still_sells(self):
        """손익 계산용 평단가 조회가 실패해도 매도 자체는 정상 체결·보고되고, 손익만 생략된다."""
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})
        pool = FakePool()
        redis = FakeRedis()

        async def get_kis_token():
            return "FAKE_TOKEN"

        async def get_stock_positions_fail():
            raise ConnectionError("KIS 조회 오류")

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text, **kw):
            pass

        async def log_journal(*args, **kwargs):
            pass

        with patch("aiohttp.ClientSession", return_value=FakePriceSession(
                {"output": {"stck_prpr": "70000", "hts_kor_isnm": "삼성전자"}})):
            reply = await order_handler.handle_trade_command(
                "삼성전자 22주 매도", pool=pool, redis=redis, universe=universe,
                get_kis_token_fn=get_kis_token, config=FakeConfig(), kis_order_fn=kis_order,
                get_stock_positions_fn=get_stock_positions_fail,
                send_telegram_fn=send_telegram, log_journal_fn=log_journal)

        self.assertIn("[실제 체결]", reply)
        self.assertNotIn("손익", reply)
        inserted_args = pool._conn.inserted[0]
        self.assertIsNone(inserted_args[-1])


if __name__ == "__main__":
    unittest.main()
