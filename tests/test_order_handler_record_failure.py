"""
[AT] fix/order-handler-record-failure 회귀 테스트.

배경: router/handlers/order_handler.py의 trade_history INSERT 4곳(제안승인 매수
직접성공/응답지연재확인, 채팅 직접매매 직접성공/응답지연재확인)이 모두
try/except pass로 DB 기록 실패를 완전히 삼킨 채 "체결 완료" 알림과 응답을 그대로
보내고 있었다. 014100 BUY/SELL 두 건 누락의 유력 후보(확정 아님). 이 테스트는
INSERT가 예외로 실패했을 때:
1) 성공 알림/응답 문구는 기존과 동일하게 유지되고(재주문을 유발하지 않음),
2) logger.error(스택 포함)가 남고,
3) "체결은 됐으나 DB 기록 실패 — 수동 확인 필요" 텔레그램 알림이 추가로 발송되는지,
그리고 정상 동작(INSERT 성공)에는 회귀가 없는지를 검증한다.
"""
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

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


def make_quote_fn(data: dict):
    output = data.get("output", {}) if isinstance(data, dict) else {}

    async def fn(symbol):
        price = int(output.get("stck_prpr", 0) or 0)
        return dict(output) if price > 0 else None
    return fn


class FakeRedis:
    def __init__(self, store=None):
        self.store = store or {}
        self.deleted = []

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, value):
        self.store[key] = value

    async def delete(self, key):
        self.deleted.append(key)
        self.store.pop(key, None)

    async def keys(self, pattern):
        prefix = pattern.rstrip("*")
        return [k for k in self.store if k.startswith(prefix)]


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FailingTradeConnection:
    """trade_history INSERT만 지정된 예외로 실패시키고, 그 외(공시/손절횟수/매수이력
    조회 등 stark.execution_guard.precheck/buy_gate가 매수 전에 쓰는 쿼리)는 안전한
    기본값으로 응답한다."""

    def __init__(self, insert_error: Exception):
        self.inserted = []
        self._insert_error = insert_error

    async def fetch(self, query, *args):
        return []

    async def fetchval(self, query, *args):
        if "trade_history" in query:
            return 0
        return None

    async def execute(self, query, *args):
        assert "trade_history" in query
        raise self._insert_error


class FakeFailingPool:
    def __init__(self, insert_error: Exception):
        self._conn = FailingTradeConnection(insert_error)

    def acquire(self):
        return _AcquireCtx(self._conn)


class TestProposalApprovalDirectFillInsertFailure(unittest.IsolatedAsyncioTestCase):
    async def test_insert_failure_still_reports_success_but_sends_db_failure_alert(self):
        redis = FakeRedis({
            "proposal:latest": "005930",
            "proposal:005930": json.dumps({"symbol": "005930", "name": "삼성전자", "price": 70000, "qty": 10}),
        })
        pool = FakeFailingPool(RuntimeError("connection lost"))
        sent = []

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True, "order_no": "P1", "filled_qty": 10}

        async def log_journal(*args, **kwargs):
            pass

        async def send_telegram(text, **kw):
            sent.append(text)

        async def get_positions():
            return {"success": True, "data": []}

        with self.assertLogs("router.handlers.order", level="ERROR") as logs:
            reply = await order_handler.handle_proposal_response(
                "사자", redis=redis, kis_order_fn=kis_order, log_journal_fn=log_journal,
                send_telegram_fn=send_telegram, get_positions_fn=get_positions, pool=pool)

        # 성공 응답 문구는 그대로 유지(재주문을 유발하지 않음)
        self.assertIn("매수 체결", reply)
        # DB 기록 실패가 로그에 남는다
        self.assertTrue(any("체결 기록 실패" in rec.message for rec in logs.records))
        # 추가 텔레그램 알림이 발송된다
        self.assertEqual(len(sent), 2)  # 기존 성공 알림 1 + DB 기록 실패 알림 1
        self.assertTrue(any("DB 기록 실패" in t for t in sent))
        self.assertTrue(any("수동 확인 필요" in t for t in sent))


class TestProposalApprovalUncertainReconcileInsertFailure(unittest.IsolatedAsyncioTestCase):
    async def test_insert_failure_on_uncertain_reconcile_path_sends_alert(self):
        redis = FakeRedis({
            "proposal:latest": "005930",
            "proposal:005930": json.dumps({"symbol": "005930", "name": "삼성전자", "price": 70000, "qty": 10}),
        })
        pool = FakeFailingPool(RuntimeError("deadlock"))
        sent = []
        state = {"qty": 0.0}

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": False, "uncertain": True, "error": "TimeoutError"}

        async def log_journal(*args, **kwargs):
            pass

        async def send_telegram(text, **kw):
            sent.append(text)

        async def get_positions():
            if state["qty"] > 0:
                return {"success": True, "data": [{"symbol": "005930", "qty": state["qty"]}]}
            return {"success": True, "data": []}

        async def fake_sleep(_sec):
            state["qty"] = 10.0  # 재확인 시점에 체결이 보유 잔고에 반영됨

        with patch("stark.execution_guard.asyncio.sleep", new=fake_sleep), \
             self.assertLogs("router.handlers.order", level="ERROR") as logs:
            reply = await order_handler.handle_proposal_response(
                "사자", redis=redis, kis_order_fn=kis_order, log_journal_fn=log_journal,
                send_telegram_fn=send_telegram, get_positions_fn=get_positions, pool=pool)

        self.assertIn("체결 확인", reply)
        self.assertTrue(any("체결 기록 실패" in rec.message for rec in logs.records))
        self.assertTrue(any("DB 기록 실패" in t for t in sent))


class TestTradeCommandDirectFillInsertFailure(unittest.IsolatedAsyncioTestCase):
    async def test_insert_failure_still_reports_success_but_sends_db_failure_alert(self):
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})
        pool = FakeFailingPool(RuntimeError("connection lost"))
        redis = FakeRedis()
        sent = []

        async def get_kis_token():
            return "FAKE_TOKEN"

        async def get_stock_positions():
            return {"success": True, "data": []}

        async def get_market_warning(symbol):
            return {"mrkt_warn_cls_code": "00", "vi_cls_code": "N"}

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True, "order_no": "T1", "filled_qty": 5}

        async def send_telegram(text, **kw):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            pass

        with self.assertLogs("router.handlers.order", level="ERROR") as logs:
            reply = await order_handler.handle_trade_command(
                "삼성전자 5주 매수", pool=pool, redis=redis, universe=universe,
                get_kis_token_fn=get_kis_token, config=object(), kis_order_fn=kis_order,
                get_stock_positions_fn=get_stock_positions,
                get_market_warning_fn=get_market_warning,
                send_telegram_fn=send_telegram, log_journal_fn=log_journal,
                get_quote_fn=make_quote_fn({"output": {"stck_prpr": "70000", "hts_kor_isnm": "삼성전자"}}))

        self.assertIn("실제 체결", reply)
        self.assertTrue(any("체결 기록 실패" in rec.message for rec in logs.records))
        self.assertEqual(len(sent), 2)
        self.assertTrue(any("DB 기록 실패" in t for t in sent))

    async def test_skip_due_to_zero_filled_qty_logs_warning_not_error(self):
        """체결 수량이 0이면(판정 불가 등) INSERT 자체를 의도적으로 건너뛰는데, 이때도
        왜 건너뛰었는지 WARNING 로그가 남아야 한다(요청사항: 'if pool and qty>0' 조건이
        거짓이라 건너뛴 경우도 WARNING 로그)."""
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})
        pool = FakeFailingPool(RuntimeError("안 쓰임"))  # INSERT가 호출되면 안 됨(필터 전에 skip)
        redis = FakeRedis()

        async def get_kis_token():
            return "FAKE_TOKEN"

        async def get_stock_positions():
            return {"success": True, "data": []}

        async def get_market_warning(symbol):
            return {"mrkt_warn_cls_code": "00", "vi_cls_code": "N"}

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True, "order_no": "T1", "filled_qty": 0}

        async def send_telegram(text, **kw):
            pass

        async def log_journal(*args, **kwargs):
            pass

        with self.assertLogs("router.handlers.order", level="WARNING") as logs:
            await order_handler.handle_trade_command(
                "삼성전자 5주 매수", pool=pool, redis=redis, universe=universe,
                get_kis_token_fn=get_kis_token, config=object(), kis_order_fn=kis_order,
                get_stock_positions_fn=get_stock_positions,
                get_market_warning_fn=get_market_warning,
                send_telegram_fn=send_telegram, log_journal_fn=log_journal,
                get_quote_fn=make_quote_fn({"output": {"stck_prpr": "70000", "hts_kor_isnm": "삼성전자"}}))

        self.assertTrue(any("건너뜀" in rec.message for rec in logs.records))
        self.assertEqual(pool._conn.inserted, [])


class TestTradeCommandUncertainReconcileInsertFailure(unittest.IsolatedAsyncioTestCase):
    async def test_insert_failure_on_uncertain_reconcile_path_sends_alert(self):
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})
        pool = FakeFailingPool(RuntimeError("deadlock"))
        redis = FakeRedis()
        sent = []
        state = {"qty": 0.0}

        async def get_kis_token():
            return "FAKE_TOKEN"

        async def get_stock_positions():
            if state["qty"] > 0:
                return {"success": True, "data": [{"symbol": "005930", "qty": state["qty"]}]}
            return {"success": True, "data": []}

        async def get_market_warning(symbol):
            return {"mrkt_warn_cls_code": "00", "vi_cls_code": "N"}

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True, "order_no": "T2", "fill_unconfirmed": True}

        async def send_telegram(text, **kw):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            pass

        async def fake_sleep(_sec):
            state["qty"] = 5.0

        with patch("stark.execution_guard.asyncio.sleep", new=fake_sleep), \
             self.assertLogs("router.handlers.order", level="ERROR") as logs:
            reply = await order_handler.handle_trade_command(
                "삼성전자 5주 매수", pool=pool, redis=redis, universe=universe,
                get_kis_token_fn=get_kis_token, config=object(), kis_order_fn=kis_order,
                get_stock_positions_fn=get_stock_positions,
                get_market_warning_fn=get_market_warning,
                send_telegram_fn=send_telegram, log_journal_fn=log_journal,
                get_quote_fn=make_quote_fn({"output": {"stck_prpr": "70000", "hts_kor_isnm": "삼성전자"}}))

        self.assertIn("체결 확인", reply)
        self.assertTrue(any("체결 기록 실패" in rec.message for rec in logs.records))
        self.assertTrue(any("DB 기록 실패" in t for t in sent))


class TestNormalInsertSucceedsNoFailureAlert(unittest.IsolatedAsyncioTestCase):
    """회귀 방지: INSERT가 정상 성공하면 기존처럼 DB 기록 실패 알림은 보내지 않는다."""

    async def test_proposal_approval_success_sends_only_fill_alert(self):
        redis = FakeRedis({
            "proposal:latest": "005930",
            "proposal:005930": json.dumps({"symbol": "005930", "name": "삼성전자", "price": 70000, "qty": 10}),
        })

        class OkConn:
            def __init__(self):
                self.inserted = []

            async def execute(self, query, *args):
                assert "trade_history" in query
                self.inserted.append(args)
                return "INSERT 1"

        class OkPool:
            def __init__(self):
                self._conn = OkConn()

            def acquire(self):
                return _AcquireCtx(self._conn)

        pool = OkPool()
        sent = []

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True, "order_no": "P1", "filled_qty": 10}

        async def log_journal(*args, **kwargs):
            pass

        async def send_telegram(text, **kw):
            sent.append(text)

        async def get_positions():
            return {"success": True, "data": []}

        reply = await order_handler.handle_proposal_response(
            "사자", redis=redis, kis_order_fn=kis_order, log_journal_fn=log_journal,
            send_telegram_fn=send_telegram, get_positions_fn=get_positions, pool=pool)

        self.assertIn("매수 체결", reply)
        self.assertEqual(len(pool._conn.inserted), 1)
        self.assertEqual(len(sent), 1)
        self.assertFalse(any("DB 기록 실패" in t for t in sent))


if __name__ == "__main__":
    unittest.main()
