"""
dashboard/main.py 채팅 종목 스냅샷 통합 단위 테스트

- _build_stock_snapshot: KIS 조회 재사용, 성공만 30초 캐시, 실패 시 숫자 없이 실패 정보만
- _jarvis_chat_impl: 감시/보유 대상이 아닌 종목도 스냅샷이 프롬프트에 들어감,
  감시 추가 제안 문구(비대상 O / 감시·보유 X), 웹 조사 보조 규칙, 기준선 필터 인자 전달
- 기준선 이전 매매 기억 필터 4경로: [최근 매매], jarvis_memory 검색, 대화 요약, LLM 히스토리/폴백
"""
import asyncio
import sys
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

for mod_name in [
    "fastapi", "fastapi.staticfiles", "fastapi.responses", "fastapi.middleware.cors",
    "asyncpg", "redis", "redis.asyncio", "aiohttp", "google", "google.generativeai",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

KST = timezone(timedelta(hours=9))
BASELINE = datetime(2026, 9, 28, 11, 0, tzinfo=KST)


def make_rows(n=25, last_date="20260928", vol_last=3000):
    end = datetime.strptime(last_date, "%Y%m%d")
    return [{"date": (end - timedelta(days=n - 1 - i)).strftime("%Y%m%d"),
             "open": 100 + 2 * i - 1, "high": 100 + 2 * i + 5, "low": 100 + 2 * i - 5,
             "close": 100 + 2 * i, "vol": vol_last if i == n - 1 else 1000} for i in range(n)]


class FakeRedis:
    def __init__(self):
        self.store = {}
        self.setex_calls = []

    async def get(self, k):
        return self.store.get(k)

    async def setex(self, k, ttl, v):
        self.setex_calls.append((k, ttl))
        self.store[k] = v


def fake_pool(fetchval=None, fetch=None):
    conn = MagicMock()
    conn.fetchval = fetchval or AsyncMock(return_value=None)
    conn.fetch = fetch or AsyncMock(return_value=[])
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    pool.conn = conn
    return pool


class TestBuildStockSnapshot(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import dashboard.main as dm
        self.dm = dm
        self.redis = FakeRedis()
        self._p = [patch.object(dm, "redis_client", self.redis)]
        for p in self._p:
            p.start()
        self.closed_now = datetime(2026, 9, 28, 16, 0, tzinfo=KST)

    def tearDown(self):
        for p in self._p:
            p.stop()

    async def test_success_builds_and_caches_30s(self):
        with patch.object(self.dm, "_fetch_daily_ohlcv_ex", new=AsyncMock(return_value=(make_rows(), ""))) as daily, \
             patch.object(self.dm, "_fetch_kis_inquire_price", new=AsyncMock(return_value=None)) as quote:
            s = await self.dm._build_stock_snapshot("024060", "흥구석유", now=self.closed_now)
            self.assertTrue(s["ok"])
            self.assertEqual(s["price"], 148)
            daily.assert_awaited_once_with("024060", 250)
            quote.assert_not_called()  # 마감 후에는 현재가 호출 없음
            self.assertEqual(self.redis.setex_calls, [("chat:stock_snapshot:024060", 30)])
            # 두 번째 호출은 캐시 히트 — KIS 재조회 없음
            s2 = await self.dm._build_stock_snapshot("024060", "흥구석유", now=self.closed_now)
            self.assertEqual(s2["price"], 148)
            daily.assert_awaited_once()

    async def test_intraday_uses_quote_overlay(self):
        now = datetime(2026, 9, 28, 10, 30, tzinfo=KST)
        quote = {"stck_prpr": "160", "stck_oprc": "150", "stck_hgpr": "165", "stck_lwpr": "149", "acml_vol": "5000"}
        with patch.object(self.dm, "_fetch_daily_ohlcv_ex", new=AsyncMock(return_value=(make_rows(), ""))), \
             patch.object(self.dm, "_fetch_kis_inquire_price", new=AsyncMock(return_value=quote)):
            s = await self.dm._build_stock_snapshot("024060", "흥구석유", now=now)
        self.assertTrue(s["live"])
        self.assertEqual(s["price"], 160)
        self.assertIn("현재가(10:30 조회)", self.dm._snap.format_snapshot(s))

    async def test_failure_returns_no_numbers_and_is_not_cached(self):
        with patch.object(self.dm, "_fetch_daily_ohlcv_ex",
                          new=AsyncMock(return_value=([], "KIS 토큰 없음"))), \
             patch.object(self.dm, "_last_known_daily_date", new=AsyncMock(return_value="20260925")):
            s = await self.dm._build_stock_snapshot("024060", "흥구석유", now=self.closed_now)
        self.assertFalse(s["ok"])
        self.assertEqual(s["last_daily_date"], "20260925")
        self.assertNotIn("price", s)
        self.assertEqual(self.redis.setex_calls, [])  # 실패는 캐시하지 않음
        text = self.dm._snap.format_snapshot(s)
        self.assertIn("조회에 실패", text)
        self.assertIn("09/25", text)

    async def test_daily_exception_is_failure(self):
        with patch.object(self.dm, "_fetch_daily_ohlcv_ex", new=AsyncMock(side_effect=RuntimeError("boom"))), \
             patch.object(self.dm, "_last_known_daily_date", new=AsyncMock(return_value="")):
            s = await self.dm._build_stock_snapshot("024060", "흥구석유", now=self.closed_now)
        self.assertFalse(s["ok"])
        self.assertEqual(self.redis.setex_calls, [])


class ChatHarness(unittest.IsolatedAsyncioTestCase):
    """_jarvis_chat_impl을 외부 의존 없이 돌리는 공통 하니스. self.prompt에 LLM에 보낸 전체 프롬프트가 남는다."""
    llm_reply = "흥구석유는 오늘 소폭 올랐습니다."
    resolved = ("024060", "흥구석유")
    watched = None      # watchlist 조회 결과 (None=없음, 1=있음)
    held = []           # get_stock_positions data
    snapshot = None

    def setUp(self):
        import dashboard.main as dm
        self.dm = dm
        self.prompt = None
        self.ask_kwargs = None
        self.portfolio_kwargs = None
        self.past_kwargs = None
        self.summary_kwargs = None
        snap = self.snapshot or dm._snap.build_snapshot(
            "024060", "흥구석유", make_rows(), now=datetime(2026, 9, 28, 16, 0, tzinfo=KST))
        universe = MagicMock()
        universe.resolve_symbol = AsyncMock(return_value=self.resolved)

        async def fake_ask(msg, session_id="x", model=None, trade_memory_since=None):
            self.prompt = msg
            self.ask_kwargs = {"trade_memory_since": trade_memory_since}
            return self.llm_reply

        async def fake_portfolio(trades_since=None):
            self.portfolio_kwargs = {"trades_since": trades_since}
            return "[주식 계좌]\n(테스트)"

        async def fake_past(query, limit=5, since=None):
            self.past_kwargs = {"since": since}
            return ""

        async def fake_summ(limit=3, since=None):
            self.summary_kwargs = {"since": since}
            return ""

        self.web = AsyncMock(return_value="ℹ️ 웹 정보(검증 안 됨)\n- 2026-09-26 (연합뉴스) 내용")
        pool = fake_pool(fetchval=AsyncMock(return_value=self.watched))
        self.patches = [
            patch.object(dm, "universe", universe),
            patch.object(dm, "db_pool", pool),
            patch.object(dm, "redis_client", FakeRedis()),
            patch.object(dm.intent_router, "route_early", new=AsyncMock(return_value=None)),
            patch.object(dm.intent_router, "route_late", new=AsyncMock(return_value=None)),
            patch.object(dm, "_mem_get_reliable_baseline", new=AsyncMock(return_value=BASELINE)),
            patch.object(dm, "get_portfolio_context", new=fake_portfolio),
            patch.object(dm, "_search_past_chats", new=fake_past),
            patch.object(dm, "_get_chat_summaries", new=fake_summ),
            patch.object(dm, "_get_active_directives", new=AsyncMock(return_value="")),
            patch.object(dm, "get_stock_positions", new=AsyncMock(return_value={"success": True, "data": self.held})),
            patch.object(dm, "_build_stock_snapshot", new=AsyncMock(return_value=snap)),
            patch.object(dm, "_save_chat_history", new=AsyncMock()),
            patch.object(dm, "_ask_openwebui", new=fake_ask),
            patch.object(dm, "_web_research_stock", new=self.web),
            patch.object(dm, "_send_telegram", new=AsyncMock()),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()

    async def chat(self, msg):
        return await self.dm._jarvis_chat_impl({"message": msg, "channel": "web"})


class TestChatSnapshotInPrompt(ChatHarness):
    async def test_unwatched_stock_gets_snapshot_and_watch_suggestion(self):
        res = await self.chat("흥구석유 오늘 어땠어?")
        self.assertTrue(res["success"])
        self.assertIn("[종목 스냅샷 — 사실 데이터]", self.prompt)
        self.assertIn("종가(09/28 기준): 148원", self.prompt)
        self.assertIn("전일 대비 +2원 (+1.37%)", self.prompt)
        self.assertIn("직전 20거래일 평균 대비 3.00배", self.prompt)
        self.assertIn("[종목 답변 규칙", self.prompt)
        self.assertIn("최대 12줄", self.prompt)
        self.assertNotIn("최대 4문장", self.prompt)
        self.assertTrue(res["reply"].endswith('감시 종목에 추가할까요? → "감시 추가 흥구석유"'))
        self.assertEqual(res["reply"].count("감시 추가"), 1)

    async def test_prompt_no_longer_uses_old_close_as_current_price(self):
        await self.chat("흥구석유 어때?")
        self.assertNotIn("현재가 148,", self.prompt)  # 옛 '현재가 {일봉 종가}' 표기 제거

    async def test_baseline_filters_applied_to_all_paths(self):
        await self.chat("흥구석유 어때?")
        self.assertEqual(self.portfolio_kwargs, {"trades_since": BASELINE})
        self.assertEqual(self.past_kwargs, {"since": BASELINE})
        self.assertEqual(self.summary_kwargs, {"since": BASELINE})
        self.assertEqual(self.ask_kwargs, {"trade_memory_since": BASELINE})
        self.assertIn("09/28 11:00 KST", self.prompt)
        self.assertIn("언급하지 마라", self.prompt)

    async def test_no_filter_when_user_asks_about_past_but_tag_required(self):
        await self.chat("예전 매매 기록 알려줘 흥구석유")
        self.assertEqual(self.portfolio_kwargs, {"trades_since": None})
        self.assertEqual(self.past_kwargs, {"since": None})
        self.assertEqual(self.summary_kwargs, {"since": None})
        self.assertEqual(self.ask_kwargs, {"trade_memory_since": None})
        self.assertIn("(옛 계좌 기록, 참고용)", self.prompt)
        self.assertIn("사용자가 과거 기록을 직접 물었으므로", self.prompt)

    async def test_ml_and_no_assertion_rules_present(self):
        await self.chat("흥구석유 어때?")
        self.assertIn("ML 매수확률", self.prompt)
        self.assertIn("단정 표현", self.prompt)
        self.assertIn("스냅샷에 없는 가격대는 만들지 마라", self.prompt)


class TestChatWatchSuggestionHeld(ChatHarness):
    held = [{"symbol": "024060", "qty": 3, "avg_price": 1000, "pnl_rate": 1.5}]

    async def test_held_stock_no_suggestion(self):
        res = await self.chat("흥구석유 어때?")
        self.assertNotIn("감시 추가", res["reply"])
        self.assertIn("보유: 3주", self.prompt)


class TestChatWatchSuggestionWatched(ChatHarness):
    watched = 1

    async def test_watched_stock_no_suggestion(self):
        res = await self.chat("흥구석유 어때?")
        self.assertNotIn("감시 추가", res["reply"])
        self.assertIn("[종목 스냅샷 — 사실 데이터]", self.prompt)  # 감시 종목도 같은 스냅샷 사용


class TestChatUnresolvedStock(ChatHarness):
    resolved = (None, None)

    async def test_no_symbol_no_snapshot_no_suggestion(self):
        res = await self.chat("오늘 시장 어때?")
        self.assertNotIn("[종목 스냅샷", self.prompt)
        self.assertNotIn("[종목 답변 규칙", self.prompt)
        self.assertIn("최대 4문장", self.prompt)
        self.assertNotIn("감시 추가", res["reply"])
        self.assertIn("ML 매수확률", self.prompt)  # 공통 규칙은 항상 포함


class TestChatSnapshotFailure(ChatHarness):
    @property
    def snapshot(self):
        from chat import stock_snapshot as s
        return s.build_failure("024060", "흥구석유", "KIS 토큰 없음", "20260925")

    def setUp(self):
        super().setUp()

    async def test_failure_text_in_prompt_without_numbers(self):
        res = await self.chat("흥구석유 어때?")
        self.assertIn("[종목 스냅샷 — 조회 실패]", self.prompt)
        self.assertIn("마지막 일봉 날짜: 09/25", self.prompt)
        self.assertIn("어떤 숫자도 지어내지 마라", self.prompt)
        self.assertNotIn("[종목 스냅샷 — 사실 데이터]", self.prompt)
        # 감시 대상이 아니면 제안은 붙는다
        self.assertIn('"감시 추가 흥구석유"', res["reply"])


class TestChatWebResearchAuxiliary(ChatHarness):
    llm_reply = "흥구석유 분석입니다.\n[[NEED_SEARCH: 흥구석유]]"

    async def test_web_skipped_when_snapshot_and_no_news_question(self):
        res = await self.chat("흥구석유 오늘 어땠어?")
        self.web.assert_not_called()
        self.assertNotIn("NEED_SEARCH", res["reply"])
        self.assertNotIn("웹 정보", res["reply"])

    async def test_web_used_as_auxiliary_when_news_asked(self):
        res = await self.chat("흥구석유 무슨 뉴스 있어?")
        self.web.assert_awaited_once()
        self.assertTrue(self.web.await_args.kwargs.get("aux"))
        self.assertIn("ℹ️ 웹 정보(검증 안 됨)", res["reply"])
        self.assertNotIn("🔎 웹 조사", res["reply"])
        self.assertIn("뉴스·이슈·공시를 물었으므로", self.prompt)


class TestChatWatchAddedByActionNoDuplicate(ChatHarness):
    llm_reply = ('흥구석유 지켜볼게요.\n[[ACTION]]{"watch_add": "흥구석유"}')

    async def test_no_suggestion_when_watch_just_added(self):
        with patch.object(self.dm.watchlist_handler, "add_symbol", new=AsyncMock()) as add:
            res = await self.chat("흥구석유 지켜봐줘")
        add.assert_awaited_once()
        self.assertIn("감시종목 추가", res["reply"])
        self.assertNotIn("감시 종목에 추가할까요", res["reply"])


class TestWebResearchAuxMode(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import dashboard.main as dm
        self.dm = dm

    def _session(self, text):
        resp = MagicMock()
        resp.json = AsyncMock(return_value={"candidates": [{"content": {"parts": [{"text": text}]}}]})
        sess = MagicMock()
        sess.post = AsyncMock(return_value=resp)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=sess)
        cm.__aexit__ = AsyncMock(return_value=False)
        return cm, sess

    async def test_aux_keeps_only_dated_sourced_items_under_header(self):
        text = ("소개 문장입니다.\n2026-09-26 | 연합뉴스 | 신규 계약 공시\n출처 없는 소문\n")
        cm, sess = self._session(text)
        with patch.object(self.dm._aiohttp, "ClientSession", return_value=cm), \
             patch.dict("os.environ", {"GEMINI_API_KEY": "k"}):
            out = await self.dm._web_research_stock("흥구석유", name="흥구석유", aux=True)
        self.assertTrue(out.startswith("ℹ️ 웹 정보(검증 안 됨)\n"))
        self.assertIn("2026-09-26 (연합뉴스) 신규 계약 공시", out)
        self.assertNotIn("소문", out)
        self.assertNotIn("소개 문장", out)
        prompt = sess.post.await_args.kwargs["json"]["contents"][0]["parts"][0]["text"]
        self.assertIn("YYYY-MM-DD | 출처명 | 내용", prompt)

    async def test_aux_returns_empty_when_nothing_qualifies_or_fails(self):
        cm, _ = self._session("확인되는 정보가 없습니다.")
        with patch.object(self.dm._aiohttp, "ClientSession", return_value=cm), \
             patch.dict("os.environ", {"GEMINI_API_KEY": "k"}):
            self.assertEqual(await self.dm._web_research_stock("x", name="x", aux=True), "")
        with patch.object(self.dm._aiohttp, "ClientSession", side_effect=RuntimeError("net")), \
             patch.dict("os.environ", {"GEMINI_API_KEY": "k"}):
            self.assertEqual(await self.dm._web_research_stock("x", name="x", aux=True), "")

    async def test_default_mode_unchanged_for_other_callers(self):
        cm, _ = self._session("일반 설명입니다.")
        with patch.object(self.dm._aiohttp, "ClientSession", return_value=cm), \
             patch.dict("os.environ", {"GEMINI_API_KEY": "k"}):
            out = await self.dm._web_research_stock("흥구석유", name="흥구석유")
        self.assertIn("🔎 웹 조사: 흥구석유", out)
        self.assertIn("일반 설명입니다.", out)


class TestBaselineFilterPaths(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import dashboard.main as dm
        self.dm = dm

    async def test_portfolio_context_recent_trades_filtered_by_baseline(self):
        dm = self.dm
        trades = {"success": True, "data": [
            {"ts": "2026-09-28T14:20:00+09:00", "bot": "stock_trader", "side": "sell", "symbol": "005930",
             "name": "삼성전자", "price": 70000.0, "quantity": 1.0, "pnl": 500.0},
            {"ts": "2026-09-10T13:05:00+09:00", "bot": "stock_trader", "side": "sell", "symbol": "000660",
             "name": "SK하이닉스", "price": 200000.0, "quantity": 1.0, "pnl": 25020.0},
        ]}
        dm.get_crypto_positions = AsyncMock(return_value={"success": True, "data": []})
        with patch.object(dm, "get_stock_positions", new=AsyncMock(return_value={"success": False, "error": "x"})), \
             patch.object(dm, "_get_market_index_ctx", new=AsyncMock(return_value="")), \
             patch.object(dm, "_get_jarvis_knowledge", new=AsyncMock(return_value="")), \
             patch.object(dm, "get_trades", new=AsyncMock(return_value=trades)) as gt:
            filtered = await dm.get_portfolio_context(trades_since=BASELINE)
            unfiltered = await dm.get_portfolio_context()
        self.assertIn("삼성전자", filtered)
        self.assertNotIn("SK하이닉스", filtered)
        self.assertNotIn("+25,020", filtered)
        self.assertIn("[최근 매매 1건]", filtered)
        # 필터 없는 호출(STARK 판단 등)은 기존 동작 — limit=5, 옛 거래도 그대로
        self.assertIn("SK하이닉스", unfiltered)
        self.assertEqual(gt.await_args_list[0].kwargs["limit"], 50)
        self.assertEqual(gt.await_args_list[1].kwargs["limit"], 5)

    async def test_portfolio_context_all_old_trades_says_none_after_baseline(self):
        dm = self.dm
        trades = {"success": True, "data": [
            {"ts": "2026-09-10T13:05:00+09:00", "bot": "stock_trader", "side": "sell", "symbol": "000660",
             "name": "SK하이닉스", "price": 200000.0, "quantity": 1.0, "pnl": 25020.0}]}
        dm.get_crypto_positions = AsyncMock(return_value={"success": True, "data": []})
        with patch.object(dm, "get_stock_positions", new=AsyncMock(return_value={"success": False, "error": "x"})), \
             patch.object(dm, "_get_market_index_ctx", new=AsyncMock(return_value="")), \
             patch.object(dm, "_get_jarvis_knowledge", new=AsyncMock(return_value="")), \
             patch.object(dm, "get_trades", new=AsyncMock(return_value=trades)):
            ctx = await dm.get_portfolio_context(trades_since=BASELINE)
        self.assertNotIn("SK하이닉스", ctx)
        self.assertNotIn("25,020", ctx)
        self.assertIn("이후 매매 없음", ctx)

    async def test_search_past_chats_excludes_pre_baseline_trade_memory(self):
        dm = self.dm
        pool = fake_pool(fetch=AsyncMock(return_value=[]))
        with patch.object(dm, "db_pool", pool):
            await dm._search_past_chats("삼성전자 흥구석유", since=BASELINE)
            await dm._search_past_chats("삼성전자 흥구석유")
        sql_f, *params_f = pool.conn.fetch.await_args_list[0].args
        self.assertIn("NOT (content LIKE '[매매기록 %' AND created_at <", sql_f)
        self.assertIn(BASELINE, params_f)
        sql_n = pool.conn.fetch.await_args_list[1].args[0]
        self.assertNotIn("[매매기록", sql_n)  # 필터 미지정이면 기존 쿼리 그대로

    async def test_chat_summaries_exclude_pre_baseline_days(self):
        dm = self.dm
        rows = [{"content": "[2026-10-06] 10월 요약"}, {"content": "[2026-09-28] 기준선 당일"},
                {"content": "[2026-09-10] 옛 계좌 +25,020원"}]
        pool = fake_pool(fetch=AsyncMock(return_value=rows))
        with patch.object(dm, "db_pool", pool):
            filtered = await dm._get_chat_summaries(since=BASELINE)
            plain = await dm._get_chat_summaries()
        self.assertEqual(filtered, "[2026-10-06] 10월 요약")
        self.assertIn("25,020", plain)  # 필터 없으면 기존 동작

    async def test_openwebui_wrapper_passes_filter_to_history_and_gemini_fallback(self):
        dm = self.dm
        captured = {}

        async def fake_llm(message, **kw):
            captured.update(kw)
            return "ok"

        with patch.object(dm, "_llm_ask_openwebui", new=fake_llm), \
             patch.object(dm, "_ask_gemini_direct", new=AsyncMock(return_value="g")) as gem:
            await dm._ask_openwebui("m", session_id="s", trade_memory_since=BASELINE)
            self.assertEqual(captured["trade_memory_since"], BASELINE)
            await captured["fallback_fn"]("msg")
            gem.assert_awaited_once_with("msg", trades_since=BASELINE)
            # 필터 없는 호출자(STARK 판단·요약 등)는 기존 폴백 그대로
            captured.clear()
            await dm._ask_openwebui("m", session_id="s")
            self.assertIsNone(captured["trade_memory_since"])
            self.assertIs(captured["fallback_fn"], dm._ask_gemini_direct)


if __name__ == "__main__":
    unittest.main()
