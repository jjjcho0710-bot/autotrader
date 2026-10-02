"""
[AT] feat/telegram-routing 회귀 테스트.

배경: 텔레그램 목적지가 broadcast=True(개인방+채널)/False(개인방만) 두 가지뿐이라 "채널에만"을
표현할 수 없었다. dashboard/main.py._send_telegram에 dest="personal"|"channel"|"both"를
추가하고, 마감 결산을 채널 전용으로 재구성하며, 매수·매도 체결 알림은 개인방(전체)+채널(한 줄
요약) 둘 다 보내도록 바꿨다.

검증 범위:
(1) _send_telegram의 dest별 라우팅(개인방만/채널만/둘 다)
(2) _jarvis_closing_report: 채널 전용, 본문에 금액 없음(마지막 블록 제외), 오늘 수익 계산
    ("오늘 이전" 비교, 집계 시작 전, stale), 보유 목록 전체 표시, 오늘 체결 블록 표시
(3) _jarvis_proactive_advice 결과란 체결 중복 제거
(4) stark/execution_guard.execute(): 체결 시 채널에 금액 없는 한 줄 요약 전송(both)
(5) router/handlers/order_handler: 채팅 직접매매 체결 시 개인방+채널 둘 다 전송
(6) [AT] feat/channel-slim: _jarvis_daily_plan/_score_journal/_jarvis_evening_review/
    _intraday_scan은 CHANNEL_SLIM=True(기본)면 개인방, False면 기존처럼 채널로 간다
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
from tests.test_execution_guard import FakePool as EGFakePool  # noqa: E402
from tests.test_execution_guard import FakeRedis as EGFakeRedis  # noqa: E402
from tests.test_execution_guard import make_decision, make_signal  # noqa: E402
from tests.test_order_handler import FakePool, FakeRedis, make_quote_fn  # noqa: E402
from router.handlers import order_handler  # noqa: E402
from market.universe import Universe  # noqa: E402


def _load_dashboard_main():
    import dashboard.main as dm
    return dm


class FakeTelegramConfig:
    kis_base_url = "https://example.invalid"
    kis_app_key = "key"
    kis_app_secret = "secret"
    kis_account_no = "12345678-01"
    KIS_IS_PAPER = True
    STARK_BOT_TOKEN = "STARKTOKEN"
    TELEGRAM_TOKEN = "LEGACYTOKEN"
    TELEGRAM_CHAT_ID = "PERSONAL_CHAT_ID"
    INITIAL_SEED_KRW = 10_000_000


class _CapturingSession:
    def __init__(self, store):
        self._store = store

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None, timeout=None):
        self._store.append({"url": url, "chat_id": (json or {}).get("chat_id"),
                             "text": (json or {}).get("text")})
        return None


# ── (1) _send_telegram dest 라우팅 ──────────────────────────────────────────


class TestSendTelegramDestRouting(unittest.IsolatedAsyncioTestCase):
    async def _send(self, dm, **kwargs):
        calls = []
        with patch.object(dm, "config", FakeTelegramConfig()), \
             patch.dict("os.environ", {"TELEGRAM_CHANNEL_ID": "CHANNEL_ID"}), \
             patch.object(dm, "_store_notification", new=AsyncMock()), \
             patch("aiohttp.ClientSession", lambda **kw: _CapturingSession(calls)):
            await dm._send_telegram("hello", **kwargs)
        return calls

    async def test_dest_personal_only_hits_personal_chat(self):
        dm = _load_dashboard_main()
        calls = await self._send(dm, dest="personal")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["chat_id"], "PERSONAL_CHAT_ID")

    async def test_dest_channel_only_hits_channel_not_personal(self):
        dm = _load_dashboard_main()
        calls = await self._send(dm, dest="channel")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["chat_id"], "CHANNEL_ID")

    async def test_dest_both_hits_personal_and_channel(self):
        dm = _load_dashboard_main()
        calls = await self._send(dm, dest="both")
        self.assertEqual(len(calls), 2)
        chat_ids = {c["chat_id"] for c in calls}
        self.assertEqual(chat_ids, {"PERSONAL_CHAT_ID", "CHANNEL_ID"})

    async def test_legacy_broadcast_true_maps_to_both(self):
        """dest 미지정 + broadcast=True는 기존 동작과 동일하게 both로 동작해야 한다(하위호환)."""
        dm = _load_dashboard_main()
        calls = await self._send(dm, broadcast=True)
        self.assertEqual(len(calls), 2)

    async def test_legacy_broadcast_false_maps_to_personal(self):
        dm = _load_dashboard_main()
        calls = await self._send(dm, broadcast=False)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["chat_id"], "PERSONAL_CHAT_ID")

    async def test_custom_chat_id_still_reaches_channel_when_dest_both(self):
        """개인방에 커스텀 chat_id/token을 써도 채널 발송은 항상 기본 봇 토큰+채널ID를 쓴다."""
        dm = _load_dashboard_main()
        calls = await self._send(dm, chat_id="OTHER_CHAT", token="OTHER_TOKEN", dest="both")
        self.assertEqual(len(calls), 2)
        self.assertIn("OTHER_CHAT", [c["chat_id"] for c in calls])
        self.assertIn("CHANNEL_ID", [c["chat_id"] for c in calls])


# ── (2) _jarvis_closing_report ──────────────────────────────────────────────


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _ClosingReportConn:
    def __init__(self, trades, wl_count=0, prev_total_krw=None):
        self._trades = trades
        self._wl_count = wl_count
        self._prev_total_krw = prev_total_krw

    async def fetch(self, query, *args):
        return self._trades

    async def fetchval(self, query, *args):
        if "balance_snapshot" in query:
            return self._prev_total_krw
        return self._wl_count


class _ClosingReportPool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


class TestClosingReportChannel(unittest.IsolatedAsyncioTestCase):
    async def _run(self, dm, trades, pos_res, wl_count=0, prev_total_krw=None):
        sent = []

        async def fake_send_telegram(msg, *a, **kw):
            sent.append({"text": msg, "dest": kw.get("dest")})

        with patch.object(dm, "db_pool", _ClosingReportPool(
                _ClosingReportConn(trades, wl_count=wl_count, prev_total_krw=prev_total_krw)), create=True), \
             patch.object(dm, "get_stock_positions", new=AsyncMock(return_value=pos_res)), \
             patch.object(dm, "_code_to_name", new=AsyncMock(side_effect=lambda s: f"종목{s}")), \
             patch.object(dm, "_send_telegram", new=fake_send_telegram), \
             patch.object(dm.config, "INITIAL_SEED_KRW", 10_000_000):
            await dm._jarvis_closing_report()
        return sent

    async def test_sent_to_channel_only(self):
        dm = _load_dashboard_main()
        sent = await self._run(dm, trades=[], pos_res={"success": True, "data": [], "account": {}})
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["dest"], "channel")

    async def test_fill_summary_block_shows_zero_when_no_trades(self):
        """[AT] feat/channel-slim: 채널에 개별 체결이 안 가므로 마감 결산에 "오늘 체결" 요약
        블록을 추가했다 — 체결 0건이면 0건과 "없음"을 보여준다."""
        dm = _load_dashboard_main()
        sent = await self._run(dm, trades=[], pos_res={"success": True, "data": [], "account": {}})
        msg = sent[0]["text"]
        self.assertIn("오늘 체결: 매수 0건 / 매도 0건", msg)
        self.assertIn("없음", msg)

    async def test_fill_summary_block_shows_extra_count_over_ten(self):
        """체결이 10건을 넘으면 앞 10건만 보여주고 "외 N건"을 붙인다
        (6시간 리포트의 체결 목록과 같은 공용 규칙, common.telegram.format_fill_lines)."""
        dm = _load_dashboard_main()
        trades = [
            {"symbol": f"{i:06d}", "side": "BUY", "price": 10000, "quantity": 1,
             "amount": 10000, "pnl": None, "strategy": "t", "ts": None}
            for i in range(11)
        ]
        sent = await self._run(dm, trades=trades, pos_res={"success": True, "data": [], "account": {}})
        msg = sent[0]["text"]
        self.assertIn("오늘 체결: 매수 11건 / 매도 0건", msg)
        self.assertIn("외 1건", msg)

    async def test_no_trade_body_amounts_except_final_block(self):
        """본문(오늘 체결·보유 목록)엔 예수금·총자산·평가금액·실현손익 같은 계좌 규모를
        드러내는 금액이 없다 — 체결 단가(공개 시세)는 "평가금액"이 아니므로 예외.
        마지막 블록에만 원금·오늘 수익·누적 수익·총자산 금액이 허용된다."""
        dm = _load_dashboard_main()
        trades = [
            {"symbol": "005930", "side": "BUY", "price": 70000, "quantity": 2,
             "amount": 140000, "pnl": None, "strategy": "t", "ts": None},
        ]
        pos_res = {
            "success": True,
            "data": [{"name": "삼성전자", "symbol": "005930", "qty": 2, "pnl_rate": 3.0}],
            "account": {"total_eval": 10_200_000, "total_pnl": 200_000, "total_pnl_rate": 2.0},
        }
        sent = await self._run(dm, trades=trades, pos_res=pos_res)
        msg = sent[0]["text"]
        body, _, block = msg.partition("─────────────")
        self.assertNotIn("실현손익", body)
        self.assertNotIn("예수금", body)
        self.assertNotIn("총자산", body)
        self.assertNotIn("누적", body)
        self.assertIn("70,000원", body)  # 체결 단가는 허용(계좌 잔고 규모를 드러내지 않음)
        self.assertIn("원", block)  # 마지막 블록엔 금액 허용

    async def test_holdings_list_shows_all_without_truncation(self):
        """실보유 종목이 8개를 넘어도 전부 표시한다(10/1 결산: 10종목인데 8개만 표시되던 버그)."""
        dm = _load_dashboard_main()
        data = [{"name": f"종목{i}", "symbol": f"{i:06d}", "qty": 1, "pnl_rate": 0.0} for i in range(10)]
        pos_res = {"success": True, "data": data, "account": {}}
        sent = await self._run(dm, trades=[], pos_res=pos_res)
        msg = sent[0]["text"]
        self.assertIn("실보유 10종목", msg)
        for i in range(10):
            self.assertIn(f"종목{i}", msg)

    async def test_today_profit_shows_not_started_when_no_prior_snapshot(self):
        """이전 balance_snapshot 기록이 없으면(예: 10/1 첫 기록) 추정값 없이 집계 시작 전으로 표시한다."""
        dm = _load_dashboard_main()
        pos_res = {"success": True, "data": [], "account": {"total_eval": 10_500_000}}
        sent = await self._run(dm, trades=[], pos_res=pos_res, prev_total_krw=None)
        self.assertIn("오늘 수익: 집계 시작 전", sent[0]["text"])

    async def test_today_profit_computed_from_prior_day_snapshot(self):
        """전일 balance_snapshot(total_krw)과 오늘 총자산을 비교해 오늘 수익을 계산한다."""
        dm = _load_dashboard_main()
        pos_res = {"success": True, "data": [], "account": {"total_eval": 10_500_000}}
        sent = await self._run(dm, trades=[], pos_res=pos_res, prev_total_krw=10_000_000)
        msg = sent[0]["text"]
        self.assertIn("오늘 수익 +500,000원 (+5.00%)", msg)

    async def test_today_profit_not_started_when_today_eval_stale(self):
        """오늘 KIS 총자산 조회가 stale이면 오늘 수익을 추정하지 않는다(이전 기록은 있어도)."""
        dm = _load_dashboard_main()
        pos_res = {"success": True, "stale": True, "data": [], "account": {"total_eval": 10_500_000}}
        sent = await self._run(dm, trades=[], pos_res=pos_res, prev_total_krw=10_000_000)
        self.assertIn("오늘 수익: 집계 시작 전", sent[0]["text"])


# ── (3) _jarvis_proactive_advice 결과란 체결 중복 제거 ──────────────────────


class TestSummarizeExecResult(unittest.TestCase):
    def test_success_marker_compresses_to_filled(self):
        dm = _load_dashboard_main()
        rep = "✅ [실제 체결] 삼성전자(005930) 5주 매도 완료 — 70,000원 × 5주 = 350,000원\n손익 +50,000원 (+7.1%)"
        self.assertEqual(dm._summarize_exec_result(rep), "체결 완료")

    def test_uncertain_filled_compresses_to_filled(self):
        dm = _load_dashboard_main()
        rep = "✅ 체결 확인(응답 지연) — 삼성전자(005930) 5주 매도 — 70,000원 × 5주 = 350,000원"
        self.assertEqual(dm._summarize_exec_result(rep), "체결 완료")

    def test_unknown_result_compresses_to_unknown(self):
        dm = _load_dashboard_main()
        rep = "⚠️ 삼성전자(005930) 주문 결과 불명 — 보유 수량 변화 없음. 미체결일 수 있으니 포트폴리오에서 확인 후 재지시하세요"
        self.assertEqual(dm._summarize_exec_result(rep), "결과 불명")

    def test_failure_compresses_to_failed(self):
        dm = _load_dashboard_main()
        rep = "❌ 삼성전자(005930) 매도 주문 실패: 잔고 부족"
        self.assertEqual(dm._summarize_exec_result(rep), "체결 실패")

    def test_non_fill_response_kept_as_is(self):
        """차단·보류 등 체결 자체가 아닌 응답은 원문을 유지한다(길이만 제한)."""
        dm = _load_dashboard_main()
        rep = "⛔ 투자경고 종목이라 매수할 수 없습니다"
        self.assertEqual(dm._summarize_exec_result(rep), rep)


class TestProactiveAdviceResultCompression(unittest.IsolatedAsyncioTestCase):
    async def test_sell_command_result_is_compressed_in_summary(self):
        """매도 지시 결과는 handle_trade_command가 이미 별도로 체결 알림을 보내므로,
        자동 실행 요약의 결과란에서는 전체 체결 문구를 반복하지 않고 한 줄로 줄인다."""
        dm = _load_dashboard_main()

        class FakeRedisAdvice:
            def __init__(self):
                self.store = {}

            async def get(self, key):
                return self.store.get(key)

            async def rpush(self, key, value):
                pass

            async def setex(self, key, ttl, value):
                self.store[key] = value

        async def fake_ask_openwebui(prompt, session_id=None):
            return ('[{"title":"삼성전자 매도","reason":"급락 위험","command":"삼성전자 전량 매도"}]')

        async def fake_jarvis_chat(body):
            return {"reply": "✅ [실제 체결] 삼성전자(005930) 5주 매도 완료 — 70,000원 × 5주 = 350,000원\n손익 +50,000원 (+7.1%)"}

        sent = []

        async def fake_send_telegram(msg, *a, **kw):
            sent.append(msg)

        with patch.object(dm, "get_stock_positions",
                           new=AsyncMock(return_value={"data": [{"symbol": "005930", "name": "삼성전자",
                                                                   "qty": 5, "avg_price": 65000, "pnl_rate": 7.0}]})), \
             patch.object(dm, "_analyze_chart", new=AsyncMock(return_value="차트: 정배열")), \
             patch.object(dm, "_get_jarvis_knowledge", new=AsyncMock(return_value="")), \
             patch.object(dm, "_get_jarvis_lessons", new=AsyncMock(return_value="")), \
             patch.object(dm, "_get_active_directives", new=AsyncMock(return_value="")), \
             patch.object(dm, "_get_market_index_ctx", new=AsyncMock(return_value="")), \
             patch.object(dm, "redis_client", FakeRedisAdvice()), \
             patch.object(dm, "_ask_openwebui", new=fake_ask_openwebui), \
             patch.object(dm, "jarvis_chat", new=fake_jarvis_chat), \
             patch.object(dm, "_send_telegram", new=fake_send_telegram), \
             patch.object(dm, "KST", __import__("datetime").timezone(__import__("datetime").timedelta(hours=9))):
            # 장중으로 보이도록 주중 낮 시간으로 datetime.now(KST)를 고정
            import datetime as _dt_mod

            class _FixedDatetime(_dt_mod.datetime):
                @classmethod
                def now(cls, tz=None):
                    return cls(2026, 10, 1, 10, 0, tzinfo=tz)  # 2026-10-01은 목요일

            with patch.object(dm, "datetime", _FixedDatetime):
                await dm._jarvis_proactive_advice("manual")

        self.assertEqual(len(sent), 1)
        self.assertIn("결과: 체결 완료", sent[0])
        self.assertNotIn("실제 체결", sent[0])  # 체결 상세 문구가 반복되지 않아야 함


# ── (4) execution_guard: 체결 시 채널 한 줄 요약(금액 없음) ─────────────────


class TestExecutionGuardChannelSummary(unittest.IsolatedAsyncioTestCase):
    async def test_buy_fill_sends_channel_summary_without_amount_when_slim_off(self):
        """CHANNEL_SLIM=False(되돌림)이면 기존처럼 개인방+채널 둘 다 간다."""
        execution_guard.reset_buy_lock()
        pool = EGFakePool()
        redis = EGFakeRedis()
        personal_sent, channel_sent = [], []

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            personal_sent.append(text)

        async def send_channel(text):
            channel_sent.append(text)

        async def log_journal(*a, **kw):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        with patch("common.telegram.CHANNEL_SLIM", False):
            await execution_guard.execute(
                make_signal(qty=5, price=70000), make_decision(), pool=pool, redis=redis,
                kis_order_fn=kis_order, send_telegram_fn=send_telegram, send_channel_fn=send_channel,
                log_journal_fn=log_journal, save_trade_memory_fn=None, code_to_name_fn=code_to_name,
            )

        self.assertEqual(len(personal_sent), 1)
        self.assertEqual(len(channel_sent), 1)
        self.assertIn("원", channel_sent[0])  # 단가는 허용
        self.assertNotIn("금액", channel_sent[0])
        self.assertIn("5주", channel_sent[0])
        self.assertIn("005930", channel_sent[0])

    async def test_sell_fill_channel_summary_includes_pnl_rate_when_slim_off(self):
        """CHANNEL_SLIM=False(되돌림)이면 매도 채널 요약에도 손익률이 그대로 포함된다."""
        execution_guard.reset_buy_lock()
        pool = EGFakePool()
        redis = EGFakeRedis()
        channel_sent = []

        async def get_positions():
            return {"success": True, "data": [{"symbol": "005930", "avg_price": 70000, "qty": 10}]}

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            pass

        async def send_channel(text):
            channel_sent.append(text)

        async def log_journal(*a, **kw):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        with patch("common.telegram.CHANNEL_SLIM", False):
            await execution_guard.execute(
                make_signal(action="sell", qty=10, price=75000), make_decision(), pool=pool, redis=redis,
                kis_order_fn=kis_order, send_telegram_fn=send_telegram, send_channel_fn=send_channel,
                log_journal_fn=log_journal, save_trade_memory_fn=None, code_to_name_fn=code_to_name,
                get_positions_fn=get_positions,
            )

        self.assertEqual(len(channel_sent), 1)

    async def test_channel_slim_default_suppresses_channel_summary_personal_kept(self):
        """[AT] feat/channel-slim: CHANNEL_SLIM=True(기본)면 채널 한 줄 요약은 보내지 않고
        개인방 상세 알림은 그대로 간다."""
        execution_guard.reset_buy_lock()
        pool = EGFakePool()
        redis = EGFakeRedis()
        personal_sent, channel_sent = [], []

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            personal_sent.append(text)

        async def send_channel(text):
            channel_sent.append(text)

        async def log_journal(*a, **kw):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        await execution_guard.execute(
            make_signal(qty=5, price=70000), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram, send_channel_fn=send_channel,
            log_journal_fn=log_journal, save_trade_memory_fn=None, code_to_name_fn=code_to_name,
        )

        self.assertEqual(len(personal_sent), 1)
        self.assertEqual(channel_sent, [])

    async def test_no_send_channel_fn_does_not_break_execute(self):
        """send_channel_fn 미주입(기존 호출부) 시에도 정상 동작해야 한다(하위호환)."""
        execution_guard.reset_buy_lock()
        pool = EGFakePool()
        redis = EGFakeRedis()

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text):
            pass

        async def log_journal(*a, **kw):
            pass

        async def code_to_name(symbol):
            return "삼성전자"

        result = await execution_guard.execute(
            make_signal(), make_decision(), pool=pool, redis=redis,
            kis_order_fn=kis_order, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, save_trade_memory_fn=None, code_to_name_fn=code_to_name,
        )
        self.assertTrue(result["success"])


# ── (5) order_handler: 채팅 직접매매 체결 — 개인방+채널 둘 다 ───────────────


class FakeOrderConfig:
    kis_base_url = "https://example.invalid"
    kis_app_key = "key"
    kis_app_secret = "secret"


class TestOrderHandlerChannelSummary(unittest.IsolatedAsyncioTestCase):
    async def test_buy_success_sends_personal_and_channel_when_slim_off(self):
        """CHANNEL_SLIM=False(되돌림)이면 기존처럼 개인방+채널 둘 다 간다."""
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})
        pool = FakePool()
        redis = FakeRedis()
        sent = []

        async def get_kis_token():
            return "FAKE_TOKEN"

        async def get_stock_positions():
            return {"success": True, "data": []}

        async def get_market_warning(symbol):
            return {"mrkt_warn_cls_code": "00", "vi_cls_code": "N"}

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text, **kw):
            sent.append({"text": text, "dest": kw.get("dest")})

        async def log_journal(*a, **kw):
            pass

        with patch("common.telegram.CHANNEL_SLIM", False):
            await order_handler.handle_trade_command(
                "삼성전자 2주 매수", pool=pool, redis=redis, universe=universe,
                get_kis_token_fn=get_kis_token, config=FakeOrderConfig(), kis_order_fn=kis_order,
                get_stock_positions_fn=get_stock_positions, send_telegram_fn=send_telegram,
                log_journal_fn=log_journal, get_market_warning_fn=get_market_warning,
                get_quote_fn=make_quote_fn({"output": {"stck_prpr": "70000", "hts_kor_isnm": "삼성전자"}}))

        self.assertEqual(len(sent), 2)
        dests = {s["dest"] for s in sent}
        self.assertEqual(dests, {"personal", "channel"})
        channel_msg = next(s["text"] for s in sent if s["dest"] == "channel")
        self.assertNotIn("금액", channel_msg)
        self.assertIn("2주", channel_msg)

    async def test_buy_success_channel_slim_default_sends_personal_only(self):
        """[AT] feat/channel-slim: CHANNEL_SLIM=True(기본)면 채널 한 줄 요약 없이 개인방만 간다."""
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})
        pool = FakePool()
        redis = FakeRedis()
        sent = []

        async def get_kis_token():
            return "FAKE_TOKEN"

        async def get_stock_positions():
            return {"success": True, "data": []}

        async def get_market_warning(symbol):
            return {"mrkt_warn_cls_code": "00", "vi_cls_code": "N"}

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def send_telegram(text, **kw):
            sent.append({"text": text, "dest": kw.get("dest")})

        async def log_journal(*a, **kw):
            pass

        await order_handler.handle_trade_command(
            "삼성전자 2주 매수", pool=pool, redis=redis, universe=universe,
            get_kis_token_fn=get_kis_token, config=FakeOrderConfig(), kis_order_fn=kis_order,
            get_stock_positions_fn=get_stock_positions, send_telegram_fn=send_telegram,
            log_journal_fn=log_journal, get_market_warning_fn=get_market_warning,
            get_quote_fn=make_quote_fn({"output": {"stck_prpr": "70000", "hts_kor_isnm": "삼성전자"}}))

        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["dest"], "personal")

    async def test_proposal_approve_sends_personal_and_channel_when_slim_off(self):
        """CHANNEL_SLIM=False(되돌림)이면 기존처럼 개인방+채널 둘 다 간다."""
        redis = FakeRedis({
            "proposal:latest": "005930",
            "proposal:005930": '{"symbol": "005930", "name": "삼성전자", "price": 70000, "qty": 1}',
        })
        sent = []

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def log_journal(*a, **kw):
            pass

        async def send_telegram(text, **kw):
            sent.append({"text": text, "dest": kw.get("dest")})

        with patch("common.telegram.CHANNEL_SLIM", False):
            reply = await order_handler.handle_proposal_response(
                "사자", redis=redis, kis_order_fn=kis_order, log_journal_fn=log_journal,
                send_telegram_fn=send_telegram)

        self.assertIn("매수 체결", reply)
        self.assertEqual(len(sent), 2)
        dests = {s["dest"] for s in sent}
        self.assertEqual(dests, {"personal", "channel"})

    async def test_proposal_approve_channel_slim_default_sends_personal_only(self):
        """[AT] feat/channel-slim: CHANNEL_SLIM=True(기본)면 채널 요약 없이 개인방만 간다."""
        redis = FakeRedis({
            "proposal:latest": "005930",
            "proposal:005930": '{"symbol": "005930", "name": "삼성전자", "price": 70000, "qty": 1}',
        })
        sent = []

        async def kis_order(symbol, price, qty, is_buy):
            return {"success": True}

        async def log_journal(*a, **kw):
            pass

        async def send_telegram(text, **kw):
            sent.append({"text": text, "dest": kw.get("dest")})

        reply = await order_handler.handle_proposal_response(
            "사자", redis=redis, kis_order_fn=kis_order, log_journal_fn=log_journal,
            send_telegram_fn=send_telegram)

        self.assertIn("매수 체결", reply)
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["dest"], "personal")


# ── (6) [AT] feat/channel-slim: 채널 슬림 대상 4종 함수 목적지 전환 ──────────


class _FakeRedisNoop:
    """redis_client 대체용 — get은 항상 None, setex는 아무것도 안 함."""

    async def get(self, key):
        return None

    async def setex(self, *a, **kw):
        pass


class TestChannelSlimDashboardRouting(unittest.IsolatedAsyncioTestCase):
    """_jarvis_daily_plan(오늘의 작전)/_score_journal(판단 채점)/_jarvis_evening_review(복기)/
    _intraday_scan(장중 보충 스캔)/_jarvis_weekly_preview(다음주 예습 브리핑)은
    CHANNEL_SLIM=True(기본)면 개인방, False면 기존처럼 채널로 간다. 마감 결산·주말 학습보고·
    주간 복습은 이 전환 대상이 아니다(채널 유지, (2)에서 이미 검증)."""

    async def _send(self, sent):
        async def fake_send_telegram(text, **kw):
            sent.append({"text": text, "dest": kw.get("dest")})
        return fake_send_telegram

    async def test_daily_plan_dest_follows_channel_slim(self):
        dm = _load_dashboard_main()

        class _Conn:
            async def fetch(self, query, *args):
                return []

        async def _run():
            sent = []
            with patch.object(dm, "db_pool", _ClosingReportPool(_Conn()), create=True), \
                 patch.object(dm, "redis_client", _FakeRedisNoop(), create=True), \
                 patch.object(dm, "_get_jarvis_lessons", new=AsyncMock(return_value="")), \
                 patch.object(dm, "_get_jarvis_knowledge", new=AsyncMock(return_value="")), \
                 patch.object(dm, "_get_active_directives", new=AsyncMock(return_value="")), \
                 patch.object(dm, "_ask_openwebui", new=AsyncMock(return_value="오늘의 작전 내용")), \
                 patch.object(dm, "_send_telegram", new=await self._send(sent)):
                await dm._jarvis_daily_plan()
            return sent

        sent = await _run()
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["dest"], "personal")  # CHANNEL_SLIM=True(기본)

        with patch("common.telegram.CHANNEL_SLIM", False):
            sent = await _run()
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["dest"], "channel")  # 되돌리기

    async def test_score_journal_zero_records_dest_follows_channel_slim(self):
        dm = _load_dashboard_main()

        class _Conn:
            async def fetch(self, query, *args):
                return []

            async def fetchval(self, query, *args):
                return 0

        async def _run():
            sent = []
            with patch.object(dm, "db_pool", _ClosingReportPool(_Conn()), create=True), \
                 patch.object(dm, "_send_telegram", new=await self._send(sent)):
                await dm._score_journal()
            return sent

        sent = await _run()
        self.assertEqual(len(sent), 1)
        self.assertIn("기록 0건", sent[0]["text"])
        self.assertEqual(sent[0]["dest"], "personal")

        with patch("common.telegram.CHANNEL_SLIM", False):
            sent = await _run()
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["dest"], "channel")

    async def test_evening_review_dest_follows_channel_slim(self):
        dm = _load_dashboard_main()

        class _Conn:
            def __init__(self):
                self.executed = []

            async def fetch(self, query, *args):
                if "trade_journal" in query:
                    return [{"symbol": "005930", "name": "삼성전자",
                              "jarvis_decision": "SKIP", "eval_pnl_rate": 1.2}]
                return []

            async def execute(self, query, *args):
                self.executed.append(args)

        async def _run():
            sent = []
            with patch.object(dm, "db_pool", _ClosingReportPool(_Conn()), create=True), \
                 patch.object(dm, "redis_client", _FakeRedisNoop(), create=True), \
                 patch.object(dm, "_ask_openwebui", new=AsyncMock(return_value="교훈: 다음엔 신중하게")), \
                 patch.object(dm, "_send_telegram", new=await self._send(sent)):
                await dm._jarvis_evening_review()
            return sent

        sent = await _run()
        self.assertEqual(len(sent), 1)
        self.assertIn("복기", sent[0]["text"])
        self.assertEqual(sent[0]["dest"], "personal")

        with patch("common.telegram.CHANNEL_SLIM", False):
            sent = await _run()
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["dest"], "channel")

    async def test_intraday_scan_dest_follows_channel_slim(self):
        dm = _load_dashboard_main()

        class _Conn:
            def __init__(self):
                self.executed = []

            async def fetch(self, query, *args):
                return []

            async def execute(self, query, *args):
                self.executed.append(args)

        candidates = [{"symbol": "005930", "name": "삼성전자", "close": 70000, "change": 2.0,
                       "vol_ratio": 3.0, "score": 5, "rsi": 55.0}]

        async def _run():
            sent = []
            with patch.object(dm, "_kis_scan_candidates", new=AsyncMock(return_value=candidates)), \
                 patch.object(dm, "get_stock_positions",
                               new=AsyncMock(return_value={"success": True, "data": []})), \
                 patch.object(dm, "db_pool", _ClosingReportPool(_Conn()), create=True), \
                 patch.object(dm, "_last_kis_scan_surge_excluded", 0), \
                 patch.object(dm, "_send_telegram", new=await self._send(sent)):
                await dm._intraday_scan()
            return sent

        sent = await _run()
        self.assertEqual(len(sent), 1)
        self.assertIn("장중 보충 스캔", sent[0]["text"])
        self.assertEqual(sent[0]["dest"], "personal")

        with patch("common.telegram.CHANNEL_SLIM", False):
            sent = await _run()
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["dest"], "channel")

    async def test_weekly_preview_dest_follows_channel_slim(self):
        dm = _load_dashboard_main()

        async def _run():
            sent = []
            with patch.object(dm, "get_stock_positions",
                               new=AsyncMock(return_value={"success": True, "data": []})), \
                 patch.object(dm, "_analyze_chart", new=AsyncMock(return_value="차트: 정배열")), \
                 patch.object(dm, "_get_jarvis_lessons", new=AsyncMock(return_value="")), \
                 patch.object(dm, "_get_active_directives", new=AsyncMock(return_value="")), \
                 patch.object(dm, "_ask_openwebui", new=AsyncMock(return_value="다음주 예습 내용")), \
                 patch.object(dm, "redis_client", _FakeRedisNoop(), create=True), \
                 patch.object(dm, "_send_telegram", new=await self._send(sent)):
                await dm._jarvis_weekly_preview()
            return sent

        sent = await _run()
        self.assertEqual(len(sent), 1)
        self.assertIn("다음주 예습 브리핑", sent[0]["text"])
        self.assertEqual(sent[0]["dest"], "personal")  # CHANNEL_SLIM=True(기본)

        with patch("common.telegram.CHANNEL_SLIM", False):
            sent = await _run()
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["dest"], "channel")  # 되돌리기


if __name__ == "__main__":
    unittest.main()
