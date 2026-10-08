"""
[AT] fix/order-result-reconcile 회귀 테스트.

배경: 10/1 14:03 AI 자동 실행이 LG디스플레이 5주 매수를 주문했는데 KIS 응답을 못 받아
"실패"로 단정했지만 실제로는 체결됐다(trade_history 기록 누락). dashboard/main.py
_kis_stock_order가 요청 전송 후 생긴 예외(타임아웃 등)를 KIS의 명확한 거절과 구분하지
못하고 둘 다 "실패"로 반환하던 것이 원인이다.

이 파일은 다음을 검증한다:
(c) KIS가 명확히 거절(rt_cd != "0" + msg1)한 경우는 uncertain이 아닌 기존 확정 실패 그대로.
(d) 응답 후 예외의 str(e)가 빈 문자열이어도 error 사유가 비지 않는다(예외 클래스명 대체).
(a) uncertain 응답 + 재조회 시 보유 수량이 늘어났으면(매도는 줄었으면) 체결로 확정·기록.
(b) uncertain 응답 + 보유 수량 변화 없으면 "결과 불명"으로 보고하고 재시도·실패억제 없음.
(e) 불확실(uncertain) 상태로 끝난 후 3분 내 같은 종목·같은 방향 재주문을 차단한다.
(f) 라우터 경로(router/intent_router.route_late → order_handler.handle_trade_command)로
    들어온 채팅 직접매수에서도 (a)의 재확인이 실제로 동작하는 end-to-end 검증.

_kis_stock_order/kis_order_fn 호출부 전수표 (불확실 처리 적용 여부):
  - router/handlers/order_handler.py:handle_proposal_response (매수 제안 승인) — 적용
  - router/handlers/order_handler.py:handle_trade_command (채팅 직접 매매) — 적용
  - stark/execution_guard.py:execute (신호 기반 자동매매, dashboard/main.py 6196행에서
    kis_order_fn=_kis_stock_order로 주입) — 적용
  이 세 곳이 kis_order_fn(...)을 실제로 호출하는 전부다(그 외는 전부 콜러블을 그대로
  전달만 하는 배선 지점).
"""
import asyncio
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "google", "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

from stark import execution_guard  # noqa: E402
from tests.test_execution_guard import FakePool, FakeRedis, make_decision, make_signal  # noqa: E402


# ── (a)(b)(e) stark/execution_guard.execute() 응답불명 재확인 ──────────────────


class TestExecuteUncertainReconcile(unittest.IsolatedAsyncioTestCase):
    async def test_uncertain_with_qty_increase_confirms_fill_and_records(self):
        """(a) 응답불명 + 재조회 시 보유 수량 증가 → 체결 확정, trade_history 기록,
        "체결 확인" 문구 보고, 실패 억제 키는 걸리지 않는다."""
        execution_guard.reset_buy_lock()
        pool = FakePool()
        redis = FakeRedis()
        sent, journaled = [], []
        state = {"qty": 0.0}

        async def get_positions():
            # 실제 KIS 응답처럼 보유 수량 0인 종목은 목록에서 빠진다(qty<=0 필터링) —
            # 그래야 평가 전 단계에서 "이미 보유 중"으로 오인되어 물타기 정책에 걸리지 않는다.
            if state["qty"] > 0:
                return {"success": True, "data": [{"symbol": "005930", "qty": state["qty"], "avg_price": 68000}]}
            return {"success": True, "data": []}

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": False, "uncertain": True, "error": "TimeoutError"}

        async def send_telegram(text):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            journaled.append(args)

        async def code_to_name(symbol):
            return "삼성전자"

        async def fake_sleep(_sec):
            state["qty"] = 5.0  # 재확인 시점에 체결이 보유 잔고에 반영됨

        with patch("stark.execution_guard.asyncio.sleep", new=fake_sleep):
            result = await execution_guard.execute(
                make_signal(qty=5), make_decision(), pool=pool, redis=redis,
                kis_order_fn=kis_order, send_telegram_fn=send_telegram,
                log_journal_fn=log_journal, save_trade_memory_fn=None,
                code_to_name_fn=code_to_name, get_positions_fn=get_positions,
            )

        self.assertTrue(result["success"])
        self.assertTrue(result["executed"])
        self.assertEqual(len(pool._conn.inserted), 1)
        self.assertEqual(pool._conn.inserted[0][5], 5.0)  # quantity 컬럼 = 실제 변동 수량
        self.assertEqual(len(sent), 1)
        self.assertIn("체결 확인", sent[0])
        self.assertEqual(len(journaled), 1)
        self.assertNotIn("buy_fail_suppress:005930", redis.store)
        self.assertNotIn("order_uncertain:005930:buy", redis.store)

    async def test_uncertain_with_no_qty_change_reports_unknown_no_retry_no_suppress(self):
        """(b) 응답불명 + 보유 수량 변화 없음 → "결과 불명" 보고, 재시도 없음(kis_order_fn
        1회만 호출), buy_fail_suppress 같은 실패 억제는 걸지 않는다."""
        execution_guard.reset_buy_lock()
        pool = FakePool()
        redis = FakeRedis()
        sent, journaled, order_calls = [], [], []

        async def get_positions():
            return {"success": True, "data": []}

        async def kis_order(symbol, price, qty, is_buy):
            order_calls.append(1)
            return {"success": False, "uncertain": True, "error": ""}

        async def send_telegram(text):
            sent.append(text)

        async def log_journal(*args, **kwargs):
            journaled.append(args)

        async def code_to_name(symbol):
            return "삼성전자"

        with patch("stark.execution_guard.asyncio.sleep", new=AsyncMock()):
            result = await execution_guard.execute(
                make_signal(qty=5), make_decision(), pool=pool, redis=redis,
                kis_order_fn=kis_order, send_telegram_fn=send_telegram,
                log_journal_fn=log_journal, save_trade_memory_fn=None,
                code_to_name_fn=code_to_name, get_positions_fn=get_positions,
            )

        self.assertFalse(result["success"])
        self.assertFalse(result["executed"])
        self.assertTrue(result.get("uncertain"))
        self.assertEqual(len(order_calls), 1)
        self.assertEqual(len(pool._conn.inserted), 0)
        self.assertIn("결과 불명", sent[0])
        self.assertNotIn("buy_fail_suppress:005930", redis.store)

    async def test_uncertain_unknown_sets_duplicate_block_key(self):
        """(e, 1/2) 결과 불명으로 끝나면 같은 종목·방향 중복주문 차단 키가 3분(180초) TTL로 걸린다."""
        execution_guard.reset_buy_lock()
        pool = FakePool()
        redis = FakeRedis()

        async def get_positions():
            return {"success": True, "data": []}

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": False, "uncertain": True, "error": "connection reset"}

        async def send_telegram(text):
            pass

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        with patch("stark.execution_guard.asyncio.sleep", new=AsyncMock()):
            await execution_guard.execute(
                make_signal(qty=5), make_decision(), pool=pool, redis=redis,
                kis_order_fn=kis_order, send_telegram_fn=send_telegram,
                log_journal_fn=log_journal, save_trade_memory_fn=None,
                code_to_name_fn=code_to_name, get_positions_fn=get_positions,
            )

        self.assertIn("order_uncertain:005930:buy", redis.store)
        self.assertEqual(redis.ttls.get("order_uncertain:005930:buy"), 180)

    async def test_duplicate_buy_blocked_within_uncertain_window(self):
        """(e, 2/2) 차단 키가 이미 걸려 있으면 같은 종목·방향 신규 주문은 kis_order_fn을
        아예 호출하지 않고 "직전 주문 결과 확인 중"으로 보류한다."""
        execution_guard.reset_buy_lock()
        pool = FakePool()
        redis = FakeRedis({"order_uncertain:005930:buy": "1"})
        order_calls = []

        async def kis_order(symbol, price, qty, is_buy):
            order_calls.append(1)
            return {"success": True}

        async def send_telegram(text):
            pass

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        result = await execution_guard.execute(
            make_signal(qty=5), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
        )

        self.assertEqual(len(order_calls), 0)
        self.assertTrue(result.get("skipped"))
        self.assertEqual(result.get("blocked"), "order_uncertain_pending")

    async def test_duplicate_sell_block_is_independent_of_buy_side(self):
        """차단 키는 종목+방향 조합이다 — 매수 쪽 차단 키가 있어도 매도는 막히지 않는다."""
        execution_guard.reset_buy_lock()
        pool = FakePool()
        redis = FakeRedis({"order_uncertain:005930:buy": "1"})
        order_calls = []

        async def kis_order(symbol, price, qty, is_buy):
            order_calls.append(1)
            return {"success": True}

        async def send_telegram(text):
            pass

        async def log_journal(*args, **kwargs):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        result = await execution_guard.execute(
            make_signal(action="sell", qty=5), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None,
            code_to_name_fn=code_to_name,
        )

        self.assertEqual(len(order_calls), 1)
        self.assertTrue(result["executed"])


# ── (c)(d) dashboard/main.py _kis_stock_order 자체 분기 ──────────────────────


class FakeKisConfig:
    kis_base_url = "https://example.invalid"
    kis_app_key = "key"
    kis_app_secret = "secret"
    kis_account_no = "12345678-01"
    KIS_IS_PAPER = True


class FakePostResponse:
    def __init__(self, data):
        self._data = data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._data


class RaisingPostCtx:
    def __init__(self, exc):
        self._exc = exc

    async def __aenter__(self):
        raise self._exc

    async def __aexit__(self, *exc):
        return False


class FakeOrderSession:
    def __init__(self, post_result):
        self._post_result = post_result

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def post(self, *args, **kwargs):
        return self._post_result


class FakeOrderAndFillSession:
    """order-cash(POST) 접수 응답 1회 + inquire-daily-ccld(GET) 체결조회 응답을 순서대로
    돌려주는 가짜 세션. aiohttp.ClientSession()이 몇 번 호출되든(주문 1회 + 체결조회 N회)
    같은 인스턴스가 재사용된다([AT] fix/record-filled-qty 테스트 전용)."""
    def __init__(self, post_result, get_results):
        self._post_result = post_result
        self._get_results = list(get_results)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def post(self, *args, **kwargs):
        return self._post_result

    def get(self, *args, **kwargs):
        return self._get_results.pop(0)


def _load_dashboard_main():
    import dashboard.main as dm
    return dm


class TestKisStockOrderResponseClassification(unittest.IsolatedAsyncioTestCase):
    async def test_post_send_exception_returns_uncertain_not_failure(self):
        """(a/b의 전제) 요청 전송 후 생긴 예외(타임아웃 등)는 uncertain=True로 돌아오고,
        logger.warning으로 예외 종류가 남는다(여기선 결과 dict로 간접 확인)."""
        dm = _load_dashboard_main()
        with patch.object(dm, "config", FakeKisConfig()), \
             patch.object(dm, "get_kis_token", new=AsyncMock(return_value="FAKE_TOKEN")), \
             patch("aiohttp.ClientSession",
                   return_value=FakeOrderSession(RaisingPostCtx(asyncio.TimeoutError()))):
            result = await dm._kis_stock_order("005930", 70000, 5, True)

        self.assertFalse(result["success"])
        self.assertTrue(result.get("uncertain"))
        self.assertTrue(result["error"])

    async def test_empty_exception_message_falls_back_to_exception_class_name(self):
        """(d) str(e)가 빈 문자열이어도 error 사유가 비지 않는다(예외 클래스명으로 대체)."""
        dm = _load_dashboard_main()
        with patch.object(dm, "config", FakeKisConfig()), \
             patch.object(dm, "get_kis_token", new=AsyncMock(return_value="FAKE_TOKEN")), \
             patch("aiohttp.ClientSession",
                   return_value=FakeOrderSession(RaisingPostCtx(ConnectionError()))):
            result = await dm._kis_stock_order("005930", 70000, 5, True)

        self.assertTrue(result.get("uncertain"))
        self.assertEqual(result["error"], "ConnectionError")

    async def test_clear_kis_rejection_stays_confirmed_failure(self):
        """(c) rt_cd != "0" + msg1로 KIS가 명확히 거절한 경우는 uncertain이 아닌 기존
        동작 그대로 확정 실패여야 한다."""
        dm = _load_dashboard_main()
        with patch.object(dm, "config", FakeKisConfig()), \
             patch.object(dm, "get_kis_token", new=AsyncMock(return_value="FAKE_TOKEN")), \
             patch("aiohttp.ClientSession",
                   return_value=FakeOrderSession(
                       FakePostResponse({"rt_cd": "1", "msg1": "주문가능금액을 초과하였습니다"}))):
            result = await dm._kis_stock_order("005930", 70000, 5, True)

        self.assertFalse(result["success"])
        self.assertNotIn("uncertain", result)
        self.assertEqual(result["error"], "주문가능금액을 초과하였습니다")

    async def test_token_missing_before_request_stays_confirmed_failure(self):
        """요청 전 단계 실패(토큰 발급 실패)는 uncertain이 아닌 기존 확정 실패 그대로."""
        dm = _load_dashboard_main()
        with patch.object(dm, "config", FakeKisConfig()), \
             patch.object(dm, "get_kis_token", new=AsyncMock(return_value="")):
            result = await dm._kis_stock_order("005930", 70000, 5, True)

        self.assertFalse(result["success"])
        self.assertNotIn("uncertain", result)
        self.assertEqual(result["error"], "KIS 토큰 없음")


# ── [AT] fix/record-filled-qty: _kis_stock_order 자체가 접수(rt_cd=0) 후 실제 체결
# 수량을 확인하는지 검증한다. 과거엔 접수만 확인하고 바로 success=True를 돌려줘서,
# 부분체결 주문이 전량체결로 기록·보고됐다(10/8 부국철강 100주 주문·34주 체결 사고). ─────


class TestKisStockOrderFillConfirmation(unittest.IsolatedAsyncioTestCase):
    async def test_full_fill_confirmed_returns_filled_qty_without_partial_flag(self):
        """체결조회에서 주문 수량 그대로 체결됐으면 filled_qty만 돌려주고 partial은 없다."""
        dm = _load_dashboard_main()
        get_resp = FakePostResponse({
            "output1": [{"odno": "O1", "tot_ccld_qty": "5", "avg_prvs": "70000"}]})
        with patch.object(dm, "config", FakeKisConfig()), \
             patch.object(dm, "get_kis_token", new=AsyncMock(return_value="FAKE_TOKEN")), \
             patch("dashboard.main.asyncio.sleep", new=AsyncMock()), \
             patch("aiohttp.ClientSession", return_value=FakeOrderAndFillSession(
                 FakePostResponse({"rt_cd": "0", "output": {"ODNO": "O1"}}), [get_resp])):
            result = await dm._kis_stock_order("005930", 70000, 5, True)

        self.assertTrue(result["success"])
        self.assertEqual(result["order_no"], "O1")
        self.assertEqual(result["filled_qty"], 5)
        self.assertNotIn("partial", result)
        self.assertNotIn("fill_unconfirmed", result)

    async def test_partial_fill_sets_partial_flag_and_avg_fill_price(self):
        """부분체결(filled_qty < 주문 qty)이면 partial=True와 매도 평균체결단가를 함께
        돌려준다 — 10/8 부국철강 100주 주문·34주 체결 사고 재현."""
        dm = _load_dashboard_main()
        get_resp = FakePostResponse({
            "output1": [{"odno": "O2", "tot_ccld_qty": "34", "avg_prvs": "5000"}]})
        with patch.object(dm, "config", FakeKisConfig()), \
             patch.object(dm, "get_kis_token", new=AsyncMock(return_value="FAKE_TOKEN")), \
             patch("dashboard.main.asyncio.sleep", new=AsyncMock()), \
             patch("aiohttp.ClientSession", return_value=FakeOrderAndFillSession(
                 FakePostResponse({"rt_cd": "0", "output": {"ODNO": "O2"}}), [get_resp])):
            result = await dm._kis_stock_order("026940", 5000, 100, False)

        self.assertTrue(result["success"])
        self.assertEqual(result["filled_qty"], 34)
        self.assertTrue(result["partial"])
        self.assertEqual(result["avg_fill_price"], 5000.0)

    async def test_zero_fill_after_all_retries_returns_fill_unconfirmed_not_full_qty(self):
        """재조회 2회(+3초,+7초)까지도 체결이 0이면 주문 수량을 그대로 체결로 단정하지
        않고 fill_unconfirmed=True로 돌려줘 호출부가 UNCERTAIN 경로로 재확인하게 한다."""
        dm = _load_dashboard_main()
        empty_resp_1 = FakePostResponse({"output1": []})
        empty_resp_2 = FakePostResponse({"output1": []})
        empty_resp_3 = FakePostResponse({"output1": []})
        with patch.object(dm, "config", FakeKisConfig()), \
             patch.object(dm, "get_kis_token", new=AsyncMock(return_value="FAKE_TOKEN")), \
             patch("dashboard.main.asyncio.sleep", new=AsyncMock()), \
             patch("aiohttp.ClientSession", return_value=FakeOrderAndFillSession(
                 FakePostResponse({"rt_cd": "0", "output": {"ODNO": "O3"}}),
                 [empty_resp_1, empty_resp_2, empty_resp_3])):
            result = await dm._kis_stock_order("005930", 70000, 5, True)

        self.assertTrue(result["success"])
        self.assertTrue(result.get("fill_unconfirmed"))
        self.assertNotIn("filled_qty", result)

    async def test_fill_query_exception_returns_fill_unconfirmed_immediately(self):
        """체결조회 API 자체가 예외로 실패하면(네트워크 오류 등) 재시도 없이 즉시
        fill_unconfirmed=True — 주문 수량을 그대로 체결 완료로 단정하지 않는다."""
        dm = _load_dashboard_main()

        class RaisingGetSession(FakeOrderAndFillSession):
            def get(self, *args, **kwargs):
                raise ConnectionError("체결조회 실패")

        with patch.object(dm, "config", FakeKisConfig()), \
             patch.object(dm, "get_kis_token", new=AsyncMock(return_value="FAKE_TOKEN")), \
             patch("dashboard.main.asyncio.sleep", new=AsyncMock()), \
             patch("aiohttp.ClientSession", return_value=RaisingGetSession(
                 FakePostResponse({"rt_cd": "0", "output": {"ODNO": "O4"}}), [])):
            result = await dm._kis_stock_order("005930", 70000, 5, True)

        self.assertTrue(result["success"])
        self.assertTrue(result.get("fill_unconfirmed"))


# ── (f) 라우터 경로(route_late) end-to-end ────────────────────────────────


class FakeChatConfig:
    kis_base_url = "https://example.invalid"
    kis_app_key = "key"
    kis_app_secret = "secret"
    kis_account_no = "12345678-01"
    KIS_IS_PAPER = True
    # RISK_PER_TRADE_PCT를 일부러 두지 않음 → 사이징 한도 계산을 건너뛰어(fail-open 아님,
    # 테스트가 재확인 로직 자체에 집중하도록) 요청 수량이 그대로 유지된다.


class FakePriceResponse:
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


class TestChatBuyUncertainReconcileEndToEnd(unittest.IsolatedAsyncioTestCase):
    async def test_route_late_buy_uncertain_then_qty_increase_confirms_fill(self):
        """(f) 라우터 경로(router/intent_router.route_late → order_handler.handle_trade_command)로
        들어온 채팅 직접매수에서, KIS 응답불명 후 보유 수량 재확인으로 체결이 확정되는지
        end-to-end로 검증한다. 배선이 끊기면(재확인 호출부가 빠지면) 이 테스트는 실패한다."""
        dm = _load_dashboard_main()
        universe = MagicMock()
        universe.resolve_symbol = AsyncMock(return_value=("005930", "삼성전자"))

        state = {"qty": 0.0}

        async def fake_get_positions():
            if state["qty"] > 0:
                return {"success": True, "data": [{"symbol": "005930", "qty": state["qty"], "avg_price": 68000}]}
            return {"success": True, "data": []}

        kis_order_calls = []

        async def fake_kis_order(symbol, price, qty, is_buy):
            kis_order_calls.append((symbol, price, qty, is_buy))
            return {"success": False, "uncertain": True, "error": "ReadTimeout"}

        sent = []

        async def fake_send_telegram(text, **kw):
            sent.append(text)

        journaled = []

        async def fake_log_journal(*args, **kwargs):
            journaled.append(args)

        async def fake_sleep(_sec):
            state["qty"] = 2.0  # 재확인 시점에 체결이 보유 잔고에 반영됨

        invalidate_calls = []

        def fake_invalidate():
            invalidate_calls.append(1)

        patches = [
            patch.object(dm, "universe", universe),
            patch.object(dm, "db_pool", FakePool()),
            patch.object(dm, "redis_client", FakeRedis()),
            patch.object(dm, "config", FakeChatConfig()),
            patch.object(dm, "get_kis_token", new=AsyncMock(return_value="FAKE_TOKEN")),
            patch.object(dm, "_kis_stock_order", new=fake_kis_order),
            patch.object(dm, "get_stock_positions", new=fake_get_positions),
            patch.object(dm, "invalidate_stock_positions_cache", new=fake_invalidate),
            patch.object(dm, "_send_telegram", new=fake_send_telegram),
            patch.object(dm, "_log_journal", new=fake_log_journal),
            patch.object(dm, "_get_market_warning_for_gate",
                         new=AsyncMock(return_value={"mrkt_warn_cls_code": "00", "vi_cls_code": "N"})),
            patch.object(dm.intent_router, "route_early", new=AsyncMock(return_value=None)),
            patch("aiohttp.ClientSession", return_value=FakePriceSession(
                {"output": {"stck_prpr": "70000", "hts_kor_isnm": "삼성전자"}})),
            patch("stark.execution_guard.asyncio.sleep", new=fake_sleep),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        res = await dm._jarvis_chat_impl({"message": "삼성전자 2주 매수", "channel": "web"})

        self.assertTrue(res["success"])
        self.assertIn("체결 확인", res["reply"])
        self.assertEqual(len(kis_order_calls), 1, "재시도 없이 1회만 주문이 나가야 한다")
        self.assertTrue(any("체결 확인" in s for s in sent))

    async def test_route_late_buy_uncertain_no_qty_change_reports_unknown(self):
        """(f 대조군) 보유 수량 변화가 없으면 "결과 불명"으로 응답하고, 매수 실패 억제
        키는 걸리지 않는다(재지시 시 바로 재시도 가능해야 함)."""
        dm = _load_dashboard_main()
        universe = MagicMock()
        universe.resolve_symbol = AsyncMock(return_value=("005930", "삼성전자"))
        redis = FakeRedis()

        async def fake_get_positions():
            return {"success": True, "data": []}

        async def fake_kis_order(symbol, price, qty, is_buy):
            return {"success": False, "uncertain": True, "error": "ReadTimeout"}

        async def fake_send_telegram(text, **kw):
            pass

        async def fake_log_journal(*args, **kwargs):
            pass

        patches = [
            patch.object(dm, "universe", universe),
            patch.object(dm, "db_pool", FakePool()),
            patch.object(dm, "redis_client", redis),
            patch.object(dm, "config", FakeChatConfig()),
            patch.object(dm, "get_kis_token", new=AsyncMock(return_value="FAKE_TOKEN")),
            patch.object(dm, "_kis_stock_order", new=fake_kis_order),
            patch.object(dm, "get_stock_positions", new=fake_get_positions),
            patch.object(dm, "invalidate_stock_positions_cache", new=MagicMock()),
            patch.object(dm, "_send_telegram", new=fake_send_telegram),
            patch.object(dm, "_log_journal", new=fake_log_journal),
            patch.object(dm, "_get_market_warning_for_gate",
                         new=AsyncMock(return_value={"mrkt_warn_cls_code": "00", "vi_cls_code": "N"})),
            patch.object(dm.intent_router, "route_early", new=AsyncMock(return_value=None)),
            patch("aiohttp.ClientSession", return_value=FakePriceSession(
                {"output": {"stck_prpr": "70000", "hts_kor_isnm": "삼성전자"}})),
            patch("stark.execution_guard.asyncio.sleep", new=AsyncMock()),
        ]
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])

        res = await dm._jarvis_chat_impl({"message": "삼성전자 2주 매수", "channel": "web"})

        self.assertTrue(res["success"])
        self.assertIn("결과 불명", res["reply"])
        self.assertNotIn("buy_fail_suppress:005930", redis.store)


if __name__ == "__main__":
    unittest.main()
