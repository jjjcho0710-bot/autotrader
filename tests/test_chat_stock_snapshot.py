"""
chat/stock_snapshot.py · chat/memory.py(기준선 필터) 단위 테스트

- 스냅샷 계산: 등락률, 거래량 배수, MA/RSI, 최근 5일 종가, 20일/52주 고저
- 장중/마감 후/주말 라벨, 장중 현재가 덮어쓰기
- 조회 실패 시 숫자를 만들지 않고 실패 문구만 나오는지
- 웹 정보 필터(날짜|출처|내용), 감시 추가 제안 조건
- 기준선(data_baseline) 이전 매매 기억 필터
"""
import re
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

from chat import stock_snapshot as snap
from chat.memory import (
    RELIABLE_BASELINE_FALLBACK,
    filter_trade_memory,
    get_chat_history,
    get_reliable_baseline,
    is_pre_baseline_trade_memory,
    is_summary_before_baseline,
)

KST = timezone(timedelta(hours=9))
BASELINE = datetime(2026, 9, 28, 11, 0, tzinfo=KST)


def make_rows(n=25, last_date="20260928", vol_last=3000):
    """close=100+2*i, high=close+5, low=close-5, 거래량 1000(마지막만 vol_last). 날짜는 마지막이 last_date."""
    end = datetime.strptime(last_date, "%Y%m%d")
    rows = []
    for i in range(n):
        c = 100 + 2 * i
        d = (end - timedelta(days=n - 1 - i)).strftime("%Y%m%d")
        rows.append({"date": d, "open": c - 1, "high": c + 5, "low": c - 5, "close": c,
                     "vol": vol_last if i == n - 1 else 1000})
    return rows


class TestBuildSnapshot(unittest.TestCase):
    def setUp(self):
        self.closed_now = datetime(2026, 9, 28, 16, 0, tzinfo=KST)  # 월 마감 후

    def test_known_input_values(self):
        s = snap.build_snapshot("024060", "흥구석유", make_rows(25), now=self.closed_now)
        self.assertTrue(s["ok"])
        self.assertEqual(s["price"], 148)                 # 100 + 2*24
        self.assertEqual(s["prev_close"], 146)
        self.assertEqual(s["change"], 2)
        self.assertEqual(s["change_pct"], 1.37)           # 2/146*100
        self.assertEqual(s["volume"], 3000)
        self.assertEqual(s["vol_ratio"], 3.0)             # 3000 / 직전 20거래일 평균 1000
        self.assertEqual(s["ma5"], 144)                   # mean(140,142,144,146,148)
        self.assertEqual(s["ma20"], 129)                  # mean(close[5:25])
        self.assertEqual(s["rsi14"], 100.0)               # 계속 상승
        self.assertEqual(s["high20"], 153)                # 마지막 20봉 고가 최대
        self.assertEqual(s["low20"], 105)                 # 마지막 20봉 저가 최소 (close[5]=110 → 105)
        self.assertEqual([c["close"] for c in s["closes5"]], [140, 142, 144, 146, 148])
        self.assertEqual(s["data_date"], "20260928")

    def test_rsi_exactly_50_for_alternating_series(self):
        rows = [{"date": f"202609{i + 1:02d}", "open": 100, "high": 102, "low": 99,
                 "close": 100 + (i % 2), "vol": 10} for i in range(15)]
        s = snap.build_snapshot("000000", "테스트", rows, now=self.closed_now)
        self.assertEqual(s["rsi14"], 50.0)

    def test_52week_only_when_250_rows(self):
        s249 = snap.build_snapshot("1", "a", make_rows(249), now=self.closed_now)
        self.assertNotIn("high52", s249)
        self.assertNotIn("52주", snap.format_snapshot(s249))
        s250 = snap.build_snapshot("1", "a", make_rows(250), now=self.closed_now)
        self.assertEqual(s250["high52"], 100 + 2 * 249 + 5)
        self.assertEqual(s250["low52"], 100 - 5)
        self.assertIn("52주 고가", snap.format_snapshot(s250))

    def test_short_history_marks_indicators_unavailable(self):
        s = snap.build_snapshot("1", "a", make_rows(3), now=self.closed_now)
        self.assertIsNone(s["ma20"])
        self.assertIsNone(s["rsi14"])
        self.assertIsNone(s["vol_ratio"])
        self.assertIsNone(s["high20"])
        text = snap.format_snapshot(s)
        self.assertIn("산출 불가", text)
        self.assertNotIn("None", text)

    def test_single_row_has_no_change(self):
        s = snap.build_snapshot("1", "a", make_rows(1), now=self.closed_now)
        self.assertIsNone(s["change"])
        self.assertIn("전일 대비: 산출 불가", snap.format_snapshot(s))

    def test_empty_rows_is_failure(self):
        s = snap.build_snapshot("1", "a", [], now=self.closed_now)
        self.assertFalse(s["ok"])


class TestLabels(unittest.TestCase):
    def test_market_hours_boundaries(self):
        self.assertTrue(snap.is_market_hours(datetime(2026, 9, 28, 9, 0, tzinfo=KST)))
        self.assertTrue(snap.is_market_hours(datetime(2026, 9, 28, 15, 30, tzinfo=KST)))
        self.assertFalse(snap.is_market_hours(datetime(2026, 9, 28, 8, 59, tzinfo=KST)))
        self.assertFalse(snap.is_market_hours(datetime(2026, 9, 28, 15, 31, tzinfo=KST)))
        self.assertFalse(snap.is_market_hours(datetime(2026, 9, 26, 11, 0, tzinfo=KST)))  # 토
        # UTC 시각도 KST로 환산해 판단 (UTC 01:30 = KST 10:30)
        self.assertTrue(snap.is_market_hours(datetime(2026, 9, 28, 1, 30, tzinfo=timezone.utc)))

    def test_intraday_label(self):
        now = datetime(2026, 9, 28, 10, 30, tzinfo=KST)
        s = snap.build_snapshot("1", "흥구석유", make_rows(25, "20260928"), now=now)
        self.assertTrue(s["live"])
        text = snap.format_snapshot(s)
        self.assertIn("현재가(10:30 조회)", text)
        self.assertNotIn("종가(", text)
        self.assertIn("장중 누적 기준", text)

    def test_after_close_label(self):
        now = datetime(2026, 9, 28, 16, 0, tzinfo=KST)
        s = snap.build_snapshot("1", "흥구석유", make_rows(25, "20260928"), now=now)
        self.assertFalse(s["live"])
        text = snap.format_snapshot(s)
        self.assertIn("종가(09/28 기준)", text)
        self.assertNotIn("현재가(", text)

    def test_weekend_label_uses_last_bar_date(self):
        now = datetime(2026, 9, 26, 11, 0, tzinfo=KST)  # 토
        s = snap.build_snapshot("1", "a", make_rows(25, "20260925"), now=now)
        self.assertIn("종가(09/25 기준)", snap.format_snapshot(s))

    def test_weekday_holiday_in_session_hours_is_close_label(self):
        # 평일 장중 시간대여도 마지막 일봉이 오늘이 아니면(휴장일/지연) 종가로 표시
        now = datetime(2026, 9, 28, 11, 0, tzinfo=KST)
        s = snap.build_snapshot("1", "a", make_rows(25, "20260925"), now=now)
        self.assertFalse(s["live"])
        self.assertIn("종가(09/25 기준)", snap.format_snapshot(s))

    def test_live_quote_overrides_last_bar(self):
        now = datetime(2026, 9, 28, 10, 30, tzinfo=KST)
        quote = {"stck_prpr": "160", "stck_oprc": "150", "stck_hgpr": "165", "stck_lwpr": "149", "acml_vol": "5000"}
        s = snap.build_snapshot("1", "a", make_rows(25, "20260928"), quote=quote, now=now)
        self.assertEqual((s["price"], s["open"], s["high"], s["low"], s["volume"]), (160, 150, 165, 149, 5000))
        self.assertEqual(s["change"], 14)                 # 160 - 146
        self.assertEqual(s["vol_ratio"], 5.0)
        self.assertTrue(s["quote_used"])
        self.assertNotIn("현재가 조회 실패", snap.format_snapshot(s))

    def test_live_without_quote_says_daily_basis(self):
        now = datetime(2026, 9, 28, 10, 30, tzinfo=KST)
        s = snap.build_snapshot("1", "a", make_rows(25, "20260928"), quote=None, now=now)
        self.assertIn("현재가 조회 실패 — 일봉 기준", snap.format_snapshot(s))

    def test_quote_ignored_after_close(self):
        now = datetime(2026, 9, 28, 16, 0, tzinfo=KST)
        s = snap.build_snapshot("1", "a", make_rows(25, "20260928"), quote={"stck_prpr": "999"}, now=now)
        self.assertEqual(s["price"], 148)


class TestFailureText(unittest.TestCase):
    def test_failure_has_no_numbers_but_last_daily_date(self):
        f = snap.build_failure("024060", "흥구석유", "KIS 일봉 조회 실패", "20260925")
        text = snap.format_snapshot(f)
        self.assertIn(snap.SNAPSHOT_FAIL_HEADER, text)
        self.assertIn("조회에 실패", text)
        self.assertIn("지어내지 마라", text)
        self.assertIn("마지막 일봉 날짜: 09/25", text)
        self.assertNotIn(snap.SNAPSHOT_HEADER, text)
        self.assertIsNone(re.search(r"\d{1,3}(,\d{3})+\s*원", text))
        for label in ("현재가(", "종가(", "MA5 ", "RSI14:", "시가 ", "거래량 "):
            self.assertNotIn(label, text)

    def test_failure_without_daily_date(self):
        text = snap.format_snapshot(snap.build_failure("1", "a", "", ""))
        self.assertIn("마지막 일봉 날짜: 없음", text)


class TestAnswerRules(unittest.TestCase):
    def test_base_rules_always_present(self):
        r = snap.build_answer_rules(has_snapshot=False, baseline=BASELINE, past_asked=False, news_asked=False)
        self.assertIn("단정 표현", r)
        self.assertIn("ML 매수확률", r)
        self.assertIn("09/28 11:00", r)
        self.assertIn("(옛 계좌 기록, 참고용)", r)
        self.assertIn("직접 묻지 않는 한 언급하지 마라", r)
        self.assertNotIn("시나리오", r)

    def test_snapshot_rules(self):
        r = snap.build_answer_rules(has_snapshot=True, baseline=BASELINE, past_asked=False, news_asked=False)
        self.assertIn("수치 먼저", r)
        self.assertIn("시나리오", r)
        self.assertIn("스냅샷에 없는 가격대는 만들지 마라", r)
        self.assertIn("[[NEED_SEARCH]]를 쓰지 마라", r)
        self.assertNotIn("뉴스·이슈·공시를 물었으므로", r)

    def test_news_and_past_variants(self):
        r = snap.build_answer_rules(has_snapshot=True, baseline=BASELINE, past_asked=True, news_asked=True)
        self.assertIn("뉴스·이슈·공시를 물었으므로", r)
        self.assertIn("사용자가 과거 기록을 직접 물었으므로", r)
        self.assertNotIn("직접 묻지 않는 한 언급하지 마라", r)

    def test_keyword_detectors(self):
        self.assertTrue(snap.wants_past_records("예전 매매 기록 알려줘"))
        self.assertTrue(snap.wants_past_records("옛 계좌 손익"))
        self.assertFalse(snap.wants_past_records("흥구석유 오늘 어땠어?"))
        self.assertTrue(snap.asks_news("흥구석유 무슨 뉴스 있어?"))
        self.assertFalse(snap.asks_news("흥구석유 오늘 어땠어?"))


class TestWatchSuggestion(unittest.TestCase):
    def test_text(self):
        self.assertEqual(snap.watch_suggestion("흥구석유"), '감시 종목에 추가할까요? → "감시 추가 흥구석유"')

    def test_conditions(self):
        f = snap.should_suggest_watch
        self.assertTrue(f(resolved=True, tracked=False, reply="분석입니다", already_added=False))
        self.assertFalse(f(resolved=True, tracked=True, reply="분석입니다", already_added=False))   # 감시/보유
        self.assertFalse(f(resolved=True, tracked=None, reply="분석입니다", already_added=False))   # 판별 실패
        self.assertFalse(f(resolved=False, tracked=False, reply="분석입니다", already_added=False))  # 종목 미인식
        self.assertFalse(f(resolved=True, tracked=False, reply="분석입니다", already_added=True))    # 방금 추가됨
        self.assertFalse(f(resolved=True, tracked=False, reply="❌ 오류", already_added=False))
        self.assertFalse(f(resolved=True, tracked=False, reply='... "감시 추가 X"', already_added=False))  # 중복 방지


class TestWebInfo(unittest.TestCase):
    def test_keeps_only_dated_sourced_lines(self):
        raw = ("요약입니다.\n"
               "2026-09-26 | 연합뉴스 | 흥구석유가 신규 유전 개발 계약을 공시했다.\n"
               "- 2026-09-25 | 한국경제 | 실적 발표 예정일이 확정됐다.\n"
               "출처 없는 문장입니다.\n"
               "9월 10일 상승했다는 소문\n"
               "2026-09-24 | | 출처가 비어 있음\n")
        out = snap.filter_web_items(raw)
        self.assertEqual(out.count("\n"), 1)
        self.assertIn("2026-09-26 (연합뉴스)", out)
        self.assertIn("2026-09-25 (한국경제)", out)
        self.assertNotIn("소문", out)
        self.assertNotIn("출처 없는", out)

    def test_empty_when_nothing_qualifies(self):
        self.assertEqual(snap.filter_web_items("확인되는 정보가 없습니다."), "")
        self.assertEqual(snap.format_web_info(""), "")

    def test_header(self):
        text = snap.format_web_info("- 2026-09-26 (연합뉴스) 내용입니다")
        self.assertTrue(text.startswith("ℹ️ 웹 정보(검증 안 됨)\n"))


class TestBaselineFilters(unittest.IsolatedAsyncioTestCase):
    def test_trade_memory_before_and_after_baseline(self):
        old = "[매매기록 2026-09-10 13:05] BUY 005930 100,000원 @ 70,000원 → 체결 PnL: +25,020원"
        edge = "[매매기록 2026-09-28 11:00] BUY 005930 1원"
        new = "[매매기록 2026-09-28 14:20] SELL 005930 1원"
        self.assertTrue(is_pre_baseline_trade_memory(old, BASELINE))
        self.assertFalse(is_pre_baseline_trade_memory(edge, BASELINE))   # 경계 시각은 포함(V009와 동일)
        self.assertFalse(is_pre_baseline_trade_memory(new, BASELINE))
        self.assertFalse(is_pre_baseline_trade_memory("삼성전자 어때?", BASELINE))  # 일반 대화는 건드리지 않음
        self.assertTrue(is_pre_baseline_trade_memory("[매매기록 형식이상] x", BASELINE) is False)  # 형식 다르면 대상 아님
        self.assertTrue(is_pre_baseline_trade_memory("[매매기록 2026-13-45 99:99] x", BASELINE))  # 시각 파싱 실패는 보수적으로 제외

    def test_filter_trade_memory(self):
        items = [
            {"role": "system", "content": "[매매기록 2026-09-10 13:05] BUY A"},
            {"role": "user", "content": "삼성전자 어때?"},
            {"role": "system", "content": "[매매기록 2026-09-28 13:00] BUY B"},
        ]
        kept = filter_trade_memory(items, BASELINE)
        self.assertEqual([i["content"] for i in kept], ["삼성전자 어때?", "[매매기록 2026-09-28 13:00] BUY B"])
        self.assertEqual(filter_trade_memory(items, None), items)

    def test_summary_before_baseline(self):
        self.assertTrue(is_summary_before_baseline("[2026-09-10] 매매 +25,020원", BASELINE))
        self.assertTrue(is_summary_before_baseline("[2026-09-28] 기준선 당일(오전 옛 데이터 섞임)", BASELINE))
        self.assertFalse(is_summary_before_baseline("[2026-09-29] 다음날", BASELINE))
        self.assertTrue(is_summary_before_baseline("날짜 없는 요약", BASELINE))

    async def test_get_chat_history_filters_redis_cache(self):
        import json
        cached = [
            {"role": "system", "content": "[매매기록 2026-09-10 13:05] BUY A", "channel": "web", "is_pure_user": True},
            {"role": "user", "content": "안녕", "channel": "web", "is_pure_user": True},
            {"role": "assistant", "content": "네", "channel": "web", "is_pure_user": True},
            {"role": "system", "content": "[매매기록 2026-09-28 15:00] SELL B", "channel": "web", "is_pure_user": True},
        ]
        redis = MagicMock()
        redis.get = AsyncMock(return_value=json.dumps(cached, ensure_ascii=False))
        h = await get_chat_history("s", max_turns=8, redis=redis, trade_memory_since=BASELINE)
        self.assertEqual([m["content"] for m in h],
                         ["안녕", "네", "[매매기록 2026-09-28 15:00] SELL B"])
        # 필터 미지정이면 기존 동작 그대로
        h_all = await get_chat_history("s", max_turns=8, redis=redis)
        self.assertEqual(len(h_all), 4)

    async def test_get_reliable_baseline_reads_table(self):
        conn = MagicMock()
        conn.fetchval = AsyncMock(return_value=datetime(2026, 9, 28, 2, 0, tzinfo=timezone.utc))
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        b = await get_reliable_baseline(pool)
        self.assertEqual(b, BASELINE)
        args = conn.fetchval.call_args[0]
        self.assertIn("data_baseline", args[0])
        self.assertEqual(args[1], "reliable_trading_data_from")

    async def test_get_reliable_baseline_falls_back(self):
        self.assertEqual(await get_reliable_baseline(None), RELIABLE_BASELINE_FALLBACK)
        self.assertEqual(RELIABLE_BASELINE_FALLBACK, BASELINE)  # V009 시드 값(2026-09-28 11:00 KST)과 동일
        conn = MagicMock()
        conn.fetchval = AsyncMock(side_effect=RuntimeError("relation does not exist"))
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        self.assertEqual(await get_reliable_baseline(pool), RELIABLE_BASELINE_FALLBACK)
        conn.fetchval = AsyncMock(return_value=None)  # 행 없음
        self.assertEqual(await get_reliable_baseline(pool), RELIABLE_BASELINE_FALLBACK)


if __name__ == "__main__":
    unittest.main()
