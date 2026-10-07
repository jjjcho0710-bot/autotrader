"""
tests/test_scoring_improvement.py - [AT] feat/scoring-improvement

PM 지시(2건) 검증:
1) 같은 종목·같은 날 SKIP이 여러 번 기록돼도 _score_journal 채점에는 1건만 반영되고,
   같은 종목에 실제 매수 체결(EXECUTE 등)이 있으면 그 판단이 SKIP보다 우선한다.
2) 능동 제안을 "승인 N"으로 승인해 재진입한 체결은 ADVICE_APPROVED/제안승인 태그로,
   완전 수동 직접지시("종목 N주 매수")는 MANUAL/수동지시 태그로 구분되어 기록된다.
3) 사람이 개입한 매수(MANUAL/ADVICE_APPROVED/PROPOSE_APPROVED 계열)는 STARK 자체
   판단(EXECUTE/EXECUTE_SMALL)과 같은 기준(±0.5%)으로 채점되지만, 요약에서는
   [STARK 자체 판단]과 [사람 개입 매수]로 분리 표시된다.

각 테스트는 이 수정을 되돌리면(원래 코드로 revert하면) 실패하도록 작성했다 — 실제로
git stash로 소스 3개 파일만 되돌리고 재실행해 실패를 확인했다(보고서 참고).
"""
import json
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
from market.universe import Universe  # noqa: E402
from router.handlers import order_handler  # noqa: E402
from tests.test_order_handler import FakeConfig, FakePool, FakeRedis, make_quote_fn  # noqa: E402


# ── (1) _score_journal: 종목·날짜별 1건 집계 + 사람 개입 매수 분리 ───────────


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _JournalConn:
    """_score_journal 전용 — SELECT(오늘 판단)은 생성 시 주입한 rows를 그대로 돌려주고,
    UPDATE(eval_*)/principle_stats INSERT는 기록만 남긴다."""

    def __init__(self, rows):
        self._rows = rows
        self.executed = []

    async def fetch(self, query, *args):
        return list(self._rows)

    async def fetchval(self, query, *args):
        return len(self._rows)

    async def execute(self, query, *args):
        self.executed.append((query, args))


class _JournalPool:
    def __init__(self, rows):
        self._conn = _JournalConn(rows)

    def acquire(self):
        return _AcquireCtx(self._conn)


class _FakeKisResponse:
    def __init__(self, price):
        self._price = price

    async def json(self):
        return {"output": {"stck_prpr": str(self._price)}}


class _FakeKisRateLimitResponse:
    """KIS 속도제한(EGW00201, msg1에 '초당 거래건수 초과') 응답 — _kis_quote_get이
    msg1의 '초당'을 감지해 재시도하는지 검증하는 데 쓴다([AT] fix/score-close-lookup)."""

    async def json(self):
        return {"msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다."}


class _FakeKisSession:
    """_score_journal의 종가 조회(_aiohttp.ClientSession)를 대체 — 종목별 종가를
    price_map에서 찾아 돌려준다."""

    def __init__(self, price_map):
        self._price_map = price_map

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, headers=None, params=None, timeout=None):
        symbol = (params or {}).get("FID_INPUT_ISCD")
        return _FakeKisResponse(self._price_map.get(symbol, 0))


class _FakeKisSessionWithFailures:
    """_score_journal의 종가 조회 — 종목별 호출 결과를 순서대로 재생한다. responses는
    {symbol: [outcome, ...]} 형태이고 outcome은 가격(int), "RATE_LIMIT"(속도제한 응답),
    "RAISE"(네트워크 예외) 중 하나. 속도제한/영구실패 재현용([AT] fix/score-close-lookup)."""

    def __init__(self, responses):
        self._responses = {k: list(v) for k, v in responses.items()}
        self.calls = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, headers=None, params=None, timeout=None):
        symbol = (params or {}).get("FID_INPUT_ISCD")
        self.calls[symbol] = self.calls.get(symbol, 0) + 1
        seq = self._responses.get(symbol)
        outcome = seq.pop(0) if seq else 0
        if outcome == "RAISE":
            raise RuntimeError("network down")
        if outcome == "RATE_LIMIT":
            return _FakeKisRateLimitResponse()
        return _FakeKisResponse(outcome)


class _FakeRedisNoop:
    async def setex(self, *a, **kw):
        pass

    async def get(self, key):
        return None


class _FakeScoreConfig:
    kis_base_url = "https://example.invalid"
    kis_app_key = "key"
    kis_app_secret = "secret"
    kis_account_no = "00000000-00"


def _journal_row(id_, symbol, name, action, jarvis_decision, price):
    """trade_journal 행 더블 — _score_journal은 r["col"]/r.get(...)만 쓰므로 dict로 충분.
    호출부가 리스트에 넣는 순서가 그대로 ORDER BY ts ASC를 흉내낸다(가장 나중 항목이
    최신 판단)."""
    return {
        "id": id_, "symbol": symbol, "name": name, "action": action,
        "jarvis_decision": jarvis_decision, "price": price, "principles": "",
    }


class TestScoreJournalDedupAndHumanBucket(unittest.IsolatedAsyncioTestCase):
    async def _run_score_journal(self, rows, price_map):
        async def fake_send_telegram(text, **kw):
            pass

        with patch.object(dm, "db_pool", _JournalPool(rows), create=True), \
             patch.object(dm, "redis_client", _FakeRedisNoop(), create=True), \
             patch.object(dm, "get_kis_token", new=AsyncMock(return_value="TOKEN")), \
             patch.object(dm, "config", _FakeScoreConfig()), \
             patch.object(dm, "_send_telegram", new=fake_send_telegram), \
             patch("aiohttp.ClientSession", lambda **kw: _FakeKisSession(price_map)), \
             patch("asyncio.sleep", new=AsyncMock()):
            result = await dm._score_journal()
        return result

    async def test_duplicate_skip_same_symbol_counts_once(self):
        """같은 종목(000660)에 SKIP이 하루 3번 기록돼도 채점엔 1건만 반영된다
        (장중 반복 스캔으로 생기는 중복 집계 방지, [AT] feat/scoring-improvement)."""
        rows = [
            _journal_row(1, "000660", "SK하이닉스", "buy", "SKIP", 50000),
            _journal_row(2, "000660", "SK하이닉스", "buy", "SKIP", 50000),
            _journal_row(3, "000660", "SK하이닉스", "buy", "SKIP", 50000),
        ]
        # 종가 49500 → rate -1.0% < 0.5 → "잘거름"(skip_good)
        result = await self._run_score_journal(rows, {"000660": 49500})
        self.assertIn("SKIP 잘거름 1 / 기회놓침 0", result)
        self.assertEqual(result.count("SK하이닉스"), 1,
                          "같은 종목이 요약 라인에 중복으로 나오면 안 됨(중복 집계)")

    async def test_executed_buy_takes_priority_over_skip_same_symbol(self):
        """같은 종목(005930)에 SKIP 2번 뒤 실제 매수 체결(EXECUTE)이 있으면, 그 종목의
        대표 판단은 SKIP이 아니라 EXECUTE여야 한다([AT] feat/scoring-improvement)."""
        rows = [
            _journal_row(1, "005930", "삼성전자", "buy", "SKIP", 70000),
            _journal_row(2, "005930", "삼성전자", "buy", "SKIP", 70000),
            _journal_row(3, "005930", "삼성전자", "buy", "EXECUTE", 70000),
        ]
        # 종가 75000 → rate +7.1% >= 0.5 → "적중"(exec_hit)
        result = await self._run_score_journal(rows, {"005930": 75000})
        self.assertIn("매수 적중 1 / 빗나감 0", result)
        self.assertIn("SKIP 잘거름 0 / 기회놓침 0", result)
        self.assertEqual(result.count("삼성전자"), 1)

    async def test_human_intervention_buy_scored_and_shown_separately(self):
        """MANUAL(채팅 직접 매수)은 STARK 자체 판단과 같은 기준(+0.5%)으로 채점되지만,
        요약에선 [사람 개입 매수]로 분리 표시된다([AT] feat/scoring-improvement)."""
        rows = [
            _journal_row(1, "005930", "삼성전자", "buy", "EXECUTE", 70000),
            _journal_row(2, "003550", "LG", "buy", "MANUAL", 100000),
        ]
        # 005930: 75000 → +7.1%(적중) / 003550: 102000 → +2.0%(적중)
        result = await self._run_score_journal(
            rows, {"005930": 75000, "003550": 102000})
        self.assertIn("[STARK 자체 판단]", result)
        self.assertIn("매수 적중 1 / 빗나감 0", result)
        self.assertIn("[사람 개입 매수]", result)
        self.assertIn("적중 1 / 빗나감 0", result)
        self.assertIn("매수(개입) LG", result)


# ── (1-b) _score_journal: 종가 조회 실패(속도제한/예외) 처리 ───────────────
# 2026-10-07 PM 지시 — 323410/005490 등 종가 조회가 실패한 종목이 조용히 채점에서
# 빠지고 영구히 eval_at NULL로 남던 문제([AT] fix/score-close-lookup).


class TestScoreJournalCloseLookupFailure(unittest.IsolatedAsyncioTestCase):
    async def _run_score_journal(self, rows, session):
        conn = _JournalConn(rows)
        pool = _JournalPool.__new__(_JournalPool)
        pool._conn = conn
        with patch.object(dm, "db_pool", pool, create=True), \
             patch.object(dm, "redis_client", _FakeRedisNoop(), create=True), \
             patch.object(dm, "get_kis_token", new=AsyncMock(return_value="TOKEN")), \
             patch.object(dm, "config", _FakeScoreConfig()), \
             patch.object(dm, "_send_telegram", new=AsyncMock()), \
             patch("aiohttp.ClientSession", lambda **kw: session), \
             patch("asyncio.sleep", new=AsyncMock()):
            result = await dm._score_journal()
        return result, conn

    async def test_rate_limited_symbol_retries_and_still_gets_scored(self):
        """KIS 속도제한(EGW00201, msg1 '초당 거래건수 초과') 응답을 한 번 받아도
        _kis_quote_get의 자동 재시도로 두 번째 호출에서 성공하면 정상 채점된다.
        수정 전 코드는 재시도가 없는 단발 sess.get()이라 이 종목은 close=0으로 빠져
        영구히 채점되지 않았다 — 이 테스트는 수정을 되돌리면 실패한다."""
        rows = [_journal_row(1, "323410", "카카오뱅크", "buy", "EXECUTE", 25000)]
        session = _FakeKisSessionWithFailures({"323410": ["RATE_LIMIT", 26000]})
        result, _ = await self._run_score_journal(rows, session)
        self.assertEqual(session.calls.get("323410"), 2,
                          "속도제한 응답 후 재시도로 2회 호출돼야 함")
        self.assertIn("매수 적중 1 / 빗나감 0", result)
        self.assertNotIn("채점 불가", result)

    async def test_permanently_failed_symbol_is_warned_reported_and_left_unscored(self):
        """종가 조회가 끝까지 실패(네트워크 예외)하는 종목은 1) WARNING 로그를 남기고,
        2) 채점 요약에 '채점 불가 N건(종목명)'으로 표시되며, 3) eval_at이 갱신되지 않아
        다음 채점 실행에서 다시 조회 대상이 된다. 수정 전 코드는 조용히 continue로
        건너뛰고 아무 흔적도 남기지 않았다 — 이 테스트는 수정을 되돌리면 실패한다."""
        rows = [
            _journal_row(1, "005930", "삼성전자", "buy", "EXECUTE", 70000),
            _journal_row(2, "005490", "POSCO홀딩스", "buy", "SKIP", 400000),
        ]
        session = _FakeKisSessionWithFailures({"005930": [75000], "005490": ["RAISE"]})
        with self.assertLogs(dm.logger, level="WARNING") as log_ctx:
            result, conn = await self._run_score_journal(rows, session)

        self.assertIn("채점 불가 1건(POSCO홀딩스)", result)
        self.assertTrue(any("종가 조회" in msg for msg in log_ctx.output),
                         "종가 조회 실패가 WARNING 로그로 남아야 함")

        updated_ids = {args[2] for q, args in conn.executed if "UPDATE trade_journal" in q}
        self.assertIn(1, updated_ids, "조회 성공한 종목(삼성전자)의 eval_at은 갱신돼야 함")
        self.assertNotIn(2, updated_ids,
                          "조회 실패한 종목(POSCO홀딩스)의 eval_at은 갱신되면 안 됨 — "
                          "다음 채점에서 재시도되도록 NULL로 남아야 함")

    async def test_only_failures_still_sends_explicit_report_instead_of_silence(self):
        """채점 가능한 판단이 하나도 없고(total==0) 전부 종가 조회 실패인 경우에도,
        수정 전처럼 빈 문자열로 조용히 끝나지 않고 '채점 불가'를 명시 보고한다."""
        rows = [_journal_row(1, "005490", "POSCO홀딩스", "buy", "SKIP", 400000)]
        session = _FakeKisSessionWithFailures({"005490": ["RAISE"]})
        result, _ = await self._run_score_journal(rows, session)
        self.assertNotEqual(result, "")
        self.assertIn("채점 불가 1건(POSCO홀딩스)", result)


# ── (2) 능동 제안 승인(ADVICE_APPROVED) vs 완전 수동 직접지시(MANUAL) 태그 구분 ──


class TestAdviceApprovedVsDirectManualTagging(unittest.IsolatedAsyncioTestCase):
    async def test_handle_advice_response_tags_reentrant_jarvis_chat_call(self):
        """'승인 N'으로 능동 제안을 승인해 jarvis_chat_fn에 재진입할 때
        _order_source_tag=advice_approved가 실려야 한다([AT] feat/scoring-improvement) —
        이게 없으면 handle_trade_command가 완전 수동 직접지시와 구분할 수 없다."""
        redis = FakeRedis({
            "advice:1": json.dumps({"title": "삼성전자 매수", "command": "삼성전자 1주 매수"})
        })
        captured = {}

        async def jarvis_chat(body):
            captured.update(body)
            return {"reply": "처리됨"}

        async def send_telegram(text, **kw):
            pass

        await order_handler.handle_advice_response(
            "승인 1", redis=redis, jarvis_chat_fn=jarvis_chat,
            send_telegram_fn=send_telegram, session_id="s")

        self.assertEqual(captured.get("_order_source_tag"), "advice_approved")

    async def test_advice_approved_and_direct_manual_orders_get_distinct_tags(self):
        """승인 경로로 들어온 체결은 ADVICE_APPROVED/제안승인, 완전 수동 직접지시는
        MANUAL/수동지시로 매매일지·매매기록에 기록돼야 한다([AT] feat/scoring-improvement)."""
        universe = Universe(None)
        universe.replace_cache({"삼성전자": "005930"})

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

        quote_fn = make_quote_fn({"output": {"stck_prpr": "70000", "hts_kor_isnm": "삼성전자"}})

        async def _run(order_source_tag):
            logged = []

            async def log_journal(*args, **kwargs):
                logged.append(args)

            pool = FakePool()
            redis = FakeRedis()
            await order_handler.handle_trade_command(
                "삼성전자 1주 매수", pool=pool, redis=redis, universe=universe,
                get_kis_token_fn=get_kis_token, config=FakeConfig(), kis_order_fn=kis_order,
                get_stock_positions_fn=get_stock_positions,
                send_telegram_fn=send_telegram, log_journal_fn=log_journal,
                get_market_warning_fn=get_market_warning, get_quote_fn=quote_fn,
                order_source_tag=order_source_tag)
            self.assertEqual(len(logged), 1, "매수 체결이면 매매일지가 정확히 1건 기록돼야 함")
            self.assertEqual(len(pool._conn.inserted), 1, "매수 체결이면 trade_history가 정확히 1건 기록돼야 함")
            return logged[0], pool._conn.inserted[0]

        # log_journal_fn(bot, symbol, name, action, strategy, signal_reason, jarvis_decision, ...)
        manual_journal_args, manual_history_args = await _run("chat")
        self.assertEqual(manual_journal_args[4], "수동지시")
        self.assertEqual(manual_journal_args[6], "MANUAL")
        # trade_history INSERT 파라미터: (symbol, side, price, qty, amount, strategy, pnl)
        self.assertEqual(manual_history_args[-2], "수동지시")

        advice_journal_args, advice_history_args = await _run("advice_approved")
        self.assertEqual(advice_journal_args[4], "제안승인")
        self.assertEqual(advice_journal_args[6], "ADVICE_APPROVED")
        self.assertEqual(advice_history_args[-2], "제안승인")

        # 직접지시와 승인체결의 태그가 서로 달라야 한다(핵심 요구사항)
        self.assertNotEqual(manual_journal_args[4], advice_journal_args[4])
        self.assertNotEqual(manual_journal_args[6], advice_journal_args[6])


if __name__ == "__main__":
    unittest.main()
