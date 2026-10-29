"""
tests/test_health_report.py - [AT] feat/health-report 단위 테스트 (PM 승인)

dashboard/health_report.py는 DB/Redis/KIS에 직접 접근하지 않고 전부 호출부가 주입한
pool/redis/콜러블만 쓰므로(dashboard/kis_history_reconcile.py와 동일한 방식), 이 테스트는
실제 DB/Redis/KIS 없이 로직만 검증한다.

검증 범위:
- 섹션별 계산(서킷브레이커 >=2, 손절 체결률 -7% 슬리피지, pnl_rate 역산, snapshot 나이)이
  올바른지.
- 섹션 하나가 예외를 던져도(_run_section) 나머지 섹션 계산과 전체 리포트 생성이
  멈추지 않고, 실패한 섹션은 "확인불가"로만 표시되는지.
- 맨 위 요약 줄이 "이상 없음"/"점검 필요" 중 올바른 쪽으로 분류되는지(정산 불일치,
  포지션 stale, pnl NULL, 슬리피지, 서킷브레이커, 토큰 재발급 3회 이상 각각).
- _previous_trading_day가 주말/휴장일을 건너뛰는지.
"""
import json
import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dashboard import health_report as hr  # noqa: E402

KST = timezone(timedelta(hours=9))


# ── 테스트용 가짜 pool/conn/redis ────────────────────────────────────────
class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class FakeConn:
    """쿼리 문자열의 특징적인 부분문자열로 분기해 미리 지정한 값을 돌려준다."""

    def __init__(self, *, stop_loss_sell=0, buy_count=0, gate_counts=None,
                 stop_loss_rows=None, pnl_null_count=0,
                 raise_on_today_counts=False, raise_on_gate=False,
                 raise_on_stop_loss_pnl=False, raise_on_pnl_null=False,
                 entry_risk_rows=None, market_index_rows=None):
        self.stop_loss_sell = stop_loss_sell
        self.buy_count = buy_count
        # {gate_name: (journal_n, decisions_n[, paired_n])} — paired_n개의 판단 행은 일지 행과
        # 같은 시각(같은 차단의 이중 기록)으로 만든다. 나머지는 서로 겹치지 않는 시각.
        self.gate_counts = gate_counts or {}
        self.stop_loss_rows = stop_loss_rows or []
        self.pnl_null_count = pnl_null_count
        self.raise_on_today_counts = raise_on_today_counts
        self.raise_on_gate = raise_on_gate
        self.raise_on_stop_loss_pnl = raise_on_stop_loss_pnl
        self.raise_on_pnl_null = raise_on_pnl_null
        # [AT] feat/entry-risk-shadow: i) 저가주/급등 매수 관찰 건수, j) 시장지수 행
        self.entry_risk_rows = entry_risk_rows if entry_risk_rows is not None else []
        self.market_index_rows = market_index_rows if market_index_rows is not None else []

    async def fetchrow(self, query, *args):
        if "stop_loss_sell" in query:
            if self.raise_on_today_counts:
                raise RuntimeError("DB down")
            return {"stop_loss_sell": self.stop_loss_sell, "buy_count": self.buy_count}
        return None

    async def fetchval(self, query, *args):
        if "pnl IS NULL" in query:
            if self.raise_on_pnl_null:
                raise RuntimeError("DB down")
            return self.pnl_null_count
        return None

    async def fetch(self, query, *args):
        if "ILIKE" in query and ("trade_journal" in query or "stark_decisions" in query):
            if self.raise_on_gate:
                raise RuntimeError("DB down")
            gate = args[1].strip("%")
            counts = self.gate_counts.get(gate, (0, 0, 0))
            j_n, d_n = counts[0], counts[1]
            paired = counts[2] if len(counts) > 2 else 0
            base = datetime(2026, 10, 13, 10, 0, tzinfo=KST)
            j_ts = [base + timedelta(hours=i) for i in range(j_n)]
            if "trade_journal" in query:
                return [{"at": t} for t in j_ts]
            d_ts = [j_ts[i] + timedelta(seconds=1) if i < paired
                    else base + timedelta(hours=i, minutes=30) for i in range(d_n)]
            return [{"at": t} for t in d_ts]
        if "strategy LIKE" in query and "ORDER BY ts" in query:
            if self.raise_on_stop_loss_pnl:
                raise RuntimeError("DB down")
            return list(self.stop_loss_rows)
        if "low_price" in query and "surge_5d" in query:
            return list(self.entry_risk_rows)
        if "market_index_daily" in query:
            return list(self.market_index_rows)
        return []


class FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


class FakeRedis:
    def __init__(self, store=None):
        self.store = store or {}

    async def get(self, key):
        v = self.store.get(key)
        return v


def _row(symbol="005930", price=70000.0, quantity=10.0, pnl=-70000.0):
    return {"symbol": symbol, "price": price, "quantity": quantity, "pnl": pnl}


async def _ok_reconcile(_day):
    return {"missing_in_db": [], "extra_in_db": [], "qty_mismatch": [], "manual_review": 0,
            "unavailable_days": []}


async def _ok_positions():
    return {"success": True, "data": [], "stale": False}


# ── 순수 함수 ────────────────────────────────────────────────────────────
class TestPreviousTradingDay(unittest.TestCase):
    def test_skips_weekend(self):
        # 2026-10-13(화)의 전날은 10-12(월)이라 평일이면 바로 반환
        self.assertEqual(hr._previous_trading_day(date(2026, 10, 13)), date(2026, 10, 12))

    def test_skips_weekend_from_monday(self):
        # 월요일(10-5 개천절 대체공휴일 다음주 월요일 예시 대신) 10-12(월)의 전날은
        # 주말(10-11 일, 10-10 토)이라 금요일(10-9, 한글날 휴장)까지 건너뛰어 10-8(목)이어야 함
        self.assertEqual(hr._previous_trading_day(date(2026, 10, 12)), date(2026, 10, 8))


class TestComputePnlRate(unittest.TestCase):
    def test_normal_case(self):
        # 매수금액 = 70000*10 - (-70000) = 770000, pnl_rate = -70000/770000*100 = -9.09%
        rate = hr._compute_pnl_rate(70000.0, 10.0, -70000.0)
        self.assertAlmostEqual(rate, -9.09, places=1)

    def test_missing_field_returns_none(self):
        self.assertIsNone(hr._compute_pnl_rate(None, 10.0, -1000.0))

    def test_zero_buy_amount_returns_none(self):
        self.assertIsNone(hr._compute_pnl_rate(0.0, 10.0, 0.0))


# ── 섹션별 계산 ──────────────────────────────────────────────────────────
class TestSectionTodayCounts(unittest.IsolatedAsyncioTestCase):
    async def test_circuit_breaker_triggers_at_two(self):
        pool = FakePool(FakeConn(stop_loss_sell=2, buy_count=3))
        result = await hr._section_today_counts(pool, date(2026, 10, 13))
        self.assertEqual(result["stop_loss_sell_count"], 2)
        self.assertEqual(result["buy_count"], 3)
        self.assertTrue(result["circuit_breaker_triggered"])

    async def test_one_stop_loss_does_not_trigger_breaker(self):
        pool = FakePool(FakeConn(stop_loss_sell=1))
        result = await hr._section_today_counts(pool, date(2026, 10, 13))
        self.assertFalse(result["circuit_breaker_triggered"])


class TestSectionStopLossPnl(unittest.IsolatedAsyncioTestCase):
    async def test_flags_slippage_at_or_below_threshold(self):
        rows = [
            _row(symbol="A", price=1000.0, quantity=10.0, pnl=-900.0),   # -900/10900 = -8.3% → 슬리피지
            _row(symbol="B", price=1000.0, quantity=10.0, pnl=-300.0),   # -300/10300 = -2.9% → 정상
        ]
        pool = FakePool(FakeConn(stop_loss_rows=rows))
        result = await hr._section_stop_loss_pnl(pool, date(2026, 10, 13))
        self.assertTrue(result["any_slippage"])
        self.assertTrue(result["items"][0]["slippage"])
        self.assertFalse(result["items"][1]["slippage"])

    async def test_no_rows_means_no_slippage(self):
        pool = FakePool(FakeConn(stop_loss_rows=[]))
        result = await hr._section_stop_loss_pnl(pool, date(2026, 10, 13))
        self.assertFalse(result["any_slippage"])
        self.assertEqual(result["items"], [])


class TestSectionGateBlocks(unittest.IsolatedAsyncioTestCase):
    async def test_unpaired_rows_are_both_counted(self):
        pool = FakePool(FakeConn(gate_counts={
            "momentum_gate": (2, 1), "single_stock_cap": (0, 0), "size_below_one_share": (1, 0),
        }))
        result = await hr._section_gate_blocks(pool, date(2026, 10, 13))
        self.assertEqual(result["by_gate"]["momentum_gate"], 3)
        self.assertEqual(result["by_gate"]["single_stock_cap"], 0)
        self.assertEqual(result["by_gate"]["size_below_one_share"], 1)
        self.assertEqual(result["total"], 4)

    async def test_same_block_in_both_tables_counts_once(self):
        """[fix/health-report-accuracy] 10/29 single_stock_cap: 일지 7행 + 판단 7행(동시각) = 7건."""
        pool = FakePool(FakeConn(gate_counts={"single_stock_cap": (7, 7, 7)}))
        result = await hr._section_gate_blocks(pool, date(2026, 10, 13))
        self.assertEqual(result["by_gate"]["single_stock_cap"], 7)
        self.assertEqual(result["total"], 7)

    async def test_partial_pairing(self):
        # 일지 3 + 판단 3 중 2쌍이 겹침 -> 3 + (3-2) = 4
        pool = FakePool(FakeConn(gate_counts={"momentum_gate": (3, 3, 2)}))
        result = await hr._section_gate_blocks(pool, date(2026, 10, 13))
        self.assertEqual(result["by_gate"]["momentum_gate"], 4)

    def test_dedupe_far_apart_rows_are_separate(self):
        t = datetime(2026, 10, 13, 10, 0, tzinfo=KST)
        self.assertEqual(hr._dedupe_gate_events([t], [t + timedelta(minutes=5)]), 2)
        self.assertEqual(hr._dedupe_gate_events([t], [t + timedelta(seconds=2)]), 1)
        self.assertEqual(hr._dedupe_gate_events([], [t]), 1)


class TestSectionPositions(unittest.IsolatedAsyncioTestCase):
    async def test_stale_true_and_snapshot_age(self):
        now = datetime(2026, 10, 13, 10, 0, tzinfo=KST)
        snap_at = (now - timedelta(minutes=42)).isoformat()
        redis = FakeRedis({"positions:last_ok": (
            '{"success": true, "data": [], "account": {}, "snapshot_at": "%s"}' % snap_at
        )})

        async def stale_positions():
            return {"success": True, "data": [], "stale": True}

        result = await hr._section_positions(redis, stale_positions, now)
        self.assertTrue(result["stale"])
        self.assertAlmostEqual(result["snapshot_age_min"], 42.0, delta=0.1)
        self.assertTrue(result["warn"])  # 폴백 스냅샷이 5분 넘게 오래됨

    async def test_stale_fallback_with_fresh_snapshot_does_not_warn(self):
        """[fix/health-report-accuracy] 조회 실패(폴백)여도 스냅샷이 0분 전이면 경고하지 않는다."""
        now = datetime(2026, 10, 13, 10, 0, tzinfo=KST)
        redis = FakeRedis({"positions:last_ok": '{"snapshot_at": "%s"}' % now.isoformat()})

        async def stale_positions():
            return {"success": True, "data": [], "stale": True}

        result = await hr._section_positions(redis, stale_positions, now)
        self.assertTrue(result["stale"])
        self.assertFalse(result["warn"])

    async def test_stale_fallback_over_5_min_warns_and_5_min_boundary_does_not(self):
        now = datetime(2026, 10, 13, 10, 0, tzinfo=KST)

        async def stale_positions():
            return {"success": True, "data": [], "stale": True}

        for minutes, expected in ((5, False), (6, True)):
            snap = (now - timedelta(minutes=minutes)).isoformat()
            redis = FakeRedis({"positions:last_ok": '{"snapshot_at": "%s"}' % snap})
            r = await hr._section_positions(redis, stale_positions, now)
            self.assertEqual(r["warn"], expected, minutes)

    async def test_not_stale_never_warns(self):
        now = datetime(2026, 10, 13, 10, 0, tzinfo=KST)
        snap = (now - timedelta(minutes=120)).isoformat()
        redis = FakeRedis({"positions:last_ok": '{"snapshot_at": "%s"}' % snap})
        r = await hr._section_positions(redis, _ok_positions, now)
        self.assertFalse(r["stale"])
        self.assertFalse(r["warn"])

    async def test_no_snapshot_recorded_yet(self):
        redis = FakeRedis({})
        result = await hr._section_positions(redis, _ok_positions, datetime.now(KST))
        self.assertFalse(result["stale"])
        self.assertIsNone(result["snapshot_age_min"])


class TestSectionEntryRiskShadow(unittest.IsolatedAsyncioTestCase):
    """i) 오늘 체결 매수 중 low_price/surge_5d 관찰 건수([AT] feat/entry-risk-shadow)."""

    async def test_counts_low_price_and_surge_5d_flags(self):
        pool = FakePool(FakeConn(entry_risk_rows=[{"low_price_n": 2, "surge_5d_n": 1}]))
        result = await hr._section_entry_risk_shadow(pool, date(2026, 10, 13))
        self.assertEqual(result["low_price_count"], 2)
        self.assertEqual(result["surge_5d_count"], 1)

    async def test_no_rows_returns_zero_counts(self):
        pool = FakePool(FakeConn())
        result = await hr._section_entry_risk_shadow(pool, date(2026, 10, 13))
        self.assertEqual(result["low_price_count"], 0)
        self.assertEqual(result["surge_5d_count"], 0)


class TestSectionMarketIndex(unittest.IsolatedAsyncioTestCase):
    """j) 당일 코스피/코스닥 등락률([AT] feat/entry-risk-shadow,
    fix/market-index-collect)."""

    async def test_reads_kospi_and_kosdaq_change_rate(self):
        pool = FakePool(FakeConn(market_index_rows=[
            {"index_code": "0001", "change_rate": -1.23},
            {"index_code": "1001", "change_rate": 0.45},
        ]))
        redis = FakeRedis({})
        result = await hr._section_market_index(pool, redis, date(2026, 10, 13))
        self.assertAlmostEqual(result["kospi_change_rate"], -1.23)
        self.assertAlmostEqual(result["kosdaq_change_rate"], 0.45)
        self.assertIsNone(result["kospi_fail_reason"])
        self.assertIsNone(result["kosdaq_fail_reason"])

    async def test_missing_data_without_collect_log_is_not_executed(self):
        """market_index_daily에 행이 없고 health:index_collect 키도 없으면(스케줄러가
        아직 안 돌았음) "미실행"으로 구분해야 한다."""
        pool = FakePool(FakeConn())
        redis = FakeRedis({})
        result = await hr._section_market_index(pool, redis, date(2026, 10, 13))
        self.assertIsNone(result["kospi_change_rate"])
        self.assertIsNone(result["kosdaq_change_rate"])
        self.assertEqual(result["kospi_fail_reason"], "미실행")
        self.assertEqual(result["kosdaq_fail_reason"], "미실행")

    async def test_missing_data_with_collect_log_shows_kis_failure_reason(self):
        """스케줄러는 실행됐지만 KIS 조회가 실패한 경우(health:index_collect 기록 있음) —
        rt_cd/msg_cd/msg1을 짧게 붙여 보여준다."""
        today = date(2026, 10, 13)
        pool = FakePool(FakeConn())
        collect_log = json.dumps({
            "KOSPI": {"ok": False, "error": {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "조회 실패"}},
            "KOSDAQ": {"ok": False, "error": {"reason": "KIS 토큰 발급 실패"}},
        })
        redis = FakeRedis({f"health:index_collect:{today.isoformat()}": collect_log})
        result = await hr._section_market_index(pool, redis, today)
        self.assertIsNone(result["kospi_change_rate"])
        self.assertIn("EGW00123", result["kospi_fail_reason"])
        self.assertEqual(result["kosdaq_fail_reason"], "KIS 토큰 발급 실패")

    async def test_success_entry_does_not_override_stored_value(self):
        """해당 지수는 DB에 값이 있으면(ok=True) fail_reason을 붙이지 않는다."""
        today = date(2026, 10, 13)
        pool = FakePool(FakeConn(market_index_rows=[{"index_code": "0001", "change_rate": -1.23}]))
        collect_log = json.dumps({
            "KOSPI": {"ok": True, "close": 2500.0, "change_rate": -1.23},
            "KOSDAQ": {"ok": False, "error": {"reason": "원인 미상"}},
        })
        redis = FakeRedis({f"health:index_collect:{today.isoformat()}": collect_log})
        result = await hr._section_market_index(pool, redis, today)
        self.assertIsNone(result["kospi_fail_reason"])
        self.assertEqual(result["kosdaq_fail_reason"], "원인 미상")


class TestMarketIndexPending(unittest.IsolatedAsyncioTestCase):
    """[fix/health-report-accuracy] 수집 시간창 안/이전이면 "미실행" 단정 대신 "수집 대기"."""

    async def test_before_collect_window_end_shows_waiting(self):
        for hh, mm in ((9, 40), (15, 45), (16, 30)):
            r = await hr._section_market_index(
                FakePool(FakeConn()), FakeRedis({}), date(2026, 10, 13),
                datetime(2026, 10, 13, hh, mm, tzinfo=KST))
            self.assertEqual(r["kospi_fail_reason"], "수집 대기", (hh, mm))
            self.assertEqual(r["kosdaq_fail_reason"], "수집 대기", (hh, mm))

    async def test_after_collect_window_without_log_is_not_executed(self):
        r = await hr._section_market_index(
            FakePool(FakeConn()), FakeRedis({}), date(2026, 10, 13),
            datetime(2026, 10, 13, 16, 31, tzinfo=KST))
        self.assertEqual(r["kospi_fail_reason"], "미실행")

    async def test_failure_log_still_shown_even_during_window(self):
        today = date(2026, 10, 13)
        log = json.dumps({"KOSPI": {"ok": False, "error": {"reason": "토큰 실패"}},
                          "KOSDAQ": {"ok": False, "error": {"reason": "토큰 실패"}}})
        r = await hr._section_market_index(
            FakePool(FakeConn()), FakeRedis({f"health:index_collect:{today.isoformat()}": log}),
            today, datetime(2026, 10, 13, 15, 50, tzinfo=KST))
        self.assertEqual(r["kospi_fail_reason"], "토큰 실패")

    async def test_stored_value_wins_over_pending(self):
        r = await hr._section_market_index(
            FakePool(FakeConn(market_index_rows=[{"index_code": "0001", "change_rate": -1.0}])),
            FakeRedis({}), date(2026, 10, 13), datetime(2026, 10, 13, 15, 50, tzinfo=KST))
        self.assertAlmostEqual(r["kospi_change_rate"], -1.0)
        self.assertIsNone(r["kospi_fail_reason"])


class TestFmtIndexRate(unittest.TestCase):
    def test_none_renders_no_data_text(self):
        self.assertEqual(hr._fmt_index_rate("KOSPI", None), "KOSPI 데이터 없음")

    def test_none_with_reason_appends_reason(self):
        self.assertEqual(
            hr._fmt_index_rate("KOSPI", None, "미실행"), "KOSPI 데이터 없음 (미실행)")

    def test_value_renders_percent_text(self):
        self.assertEqual(hr._fmt_index_rate("KOSPI", -1.2), "KOSPI -1.20%")


class TestSectionScanTimingAndTokenReissue(unittest.IsolatedAsyncioTestCase):
    async def test_scan_timing_reads_recorded_value(self):
        redis = FakeRedis({"health:scan_timing:2026-10-13": "12.34"})
        result = await hr._section_scan_timing(redis, date(2026, 10, 13))
        self.assertEqual(result["elapsed_sec"], 12.34)

    async def test_scan_timing_missing_is_none(self):
        redis = FakeRedis({})
        result = await hr._section_scan_timing(redis, date(2026, 10, 13))
        self.assertIsNone(result["elapsed_sec"])

    async def test_token_reissue_counts_from_redis(self):
        redis = FakeRedis({"health:token_reissue:2026-10-13": "3"})
        result = await hr._section_token_reissue(redis, date(2026, 10, 13))
        self.assertEqual(result["reissue_count"], 3)

    async def test_token_reissue_missing_is_zero(self):
        redis = FakeRedis({})
        result = await hr._section_token_reissue(redis, date(2026, 10, 13))
        self.assertEqual(result["reissue_count"], 0)


class TestSectionScanDetail(unittest.IsolatedAsyncioTestCase):
    """[AT] fix/scan-parallel — health:scan_detail:YYYY-MM-DD(종목수/실패수/조회초) 읽기."""

    async def test_reads_recorded_detail(self):
        redis = FakeRedis({
            "health:scan_detail:2026-10-13": json.dumps(
                {"total": 2800, "fail": 3, "fetch_elapsed": 410.5, "score_elapsed": 2.1})
        })
        result = await hr._section_scan_detail(redis, date(2026, 10, 13))
        self.assertEqual(result["total"], 2800)
        self.assertEqual(result["fail"], 3)
        self.assertEqual(result["fetch_elapsed"], 410.5)

    async def test_missing_is_none(self):
        redis = FakeRedis({})
        result = await hr._section_scan_detail(redis, date(2026, 10, 13))
        self.assertIsNone(result["total"])
        self.assertIsNone(result["fail"])
        self.assertIsNone(result["fetch_elapsed"])

    async def test_malformed_json_is_none(self):
        redis = FakeRedis({"health:scan_detail:2026-10-13": "not-json"})
        result = await hr._section_scan_detail(redis, date(2026, 10, 13))
        self.assertIsNone(result["total"])


# ── 전체 조립: 섹션 격리 + 요약 줄 판정 ───────────────────────────────────
class TestBuildHealthReportIsolation(unittest.IsolatedAsyncioTestCase):
    async def test_one_section_failure_does_not_block_others(self):
        """gate_blocks 섹션이 예외를 던져도 나머지 섹션은 정상 계산되고 전체 리포트가 만들어진다."""
        pool = FakePool(FakeConn(stop_loss_sell=0, buy_count=1, raise_on_gate=True))
        redis = FakeRedis({})

        report = await hr.build_health_report(
            pool=pool, redis=redis,
            reconcile_dry_run_fn=_ok_reconcile, get_positions_fn=_ok_positions,
            now_kst=datetime(2026, 10, 13, 9, 40, tzinfo=KST),
        )

        self.assertIn("error", report["sections"]["gate_blocks"])
        self.assertIn("확인불가", report["sections"]["gate_blocks"]["error"])
        # 실패한 섹션과 무관한 다른 섹션은 정상값을 유지해야 한다
        self.assertEqual(report["sections"]["today_counts"]["buy_count"], 1)
        self.assertIn("c) 오늘 차단: 확인불가", report["text"])

    async def test_reconcile_exception_isolated(self):
        async def boom_reconcile(_day):
            raise RuntimeError("KIS 토큰 발급 실패")

        pool = FakePool(FakeConn())
        redis = FakeRedis({})
        report = await hr.build_health_report(
            pool=pool, redis=redis,
            reconcile_dry_run_fn=boom_reconcile, get_positions_fn=_ok_positions,
            now_kst=datetime(2026, 10, 13, 9, 40, tzinfo=KST),
        )
        self.assertIn("error", report["sections"]["reconcile"])
        # 다른 섹션들은 영향받지 않음
        self.assertNotIn("error", report["sections"]["today_counts"])


class TestBuildHealthReportSummary(unittest.IsolatedAsyncioTestCase):
    def _base_kwargs(self):
        return dict(
            pool=FakePool(FakeConn()),
            redis=FakeRedis({}),
            reconcile_dry_run_fn=_ok_reconcile,
            get_positions_fn=_ok_positions,
            now_kst=datetime(2026, 10, 13, 9, 40, tzinfo=KST),
        )

    async def test_all_clear_is_ok(self):
        report = await hr.build_health_report(**self._base_kwargs())
        self.assertEqual(report["status"], "ok")
        self.assertTrue(report["text"].startswith("✅ 이상 없음"))
        self.assertEqual(report["issues"], [])
        # [AT] feat/entry-risk-shadow — i)/j) 줄이 실제로 조립되는지(기본값: 0건/데이터 없음).
        # j)는 health:index_collect 키가 없으면(fix/market-index-collect) "미실행"으로 표시된다.
        self.assertIn("i) 오늘 매수 관찰(저가주/급등): low_price 0건, surge_5d 0건", report["text"])
        self.assertIn(
            "j) 당일 지수: KOSPI 데이터 없음 (수집 대기), KOSDAQ 데이터 없음 (수집 대기)", report["text"])

    async def test_circuit_breaker_triggers_warn(self):
        kwargs = self._base_kwargs()
        kwargs["pool"] = FakePool(FakeConn(stop_loss_sell=2))
        report = await hr.build_health_report(**kwargs)
        self.assertEqual(report["status"], "warn")
        self.assertTrue(any("서킷브레이커" in i for i in report["issues"]))

    async def test_pnl_null_triggers_warn(self):
        kwargs = self._base_kwargs()
        kwargs["pool"] = FakePool(FakeConn(pnl_null_count=1))
        report = await hr.build_health_report(**kwargs)
        self.assertEqual(report["status"], "warn")
        self.assertTrue(any("pnl NULL" in i for i in report["issues"]))

    async def test_slippage_triggers_warn(self):
        kwargs = self._base_kwargs()
        kwargs["pool"] = FakePool(FakeConn(
            stop_loss_rows=[_row(price=1000.0, quantity=10.0, pnl=-800.0)]))  # -8.7%
        report = await hr.build_health_report(**kwargs)
        self.assertEqual(report["status"], "warn")
        self.assertTrue(any("슬리피지" in i for i in report["issues"]))

    async def test_stale_position_triggers_warn(self):
        async def stale_positions():
            return {"success": True, "data": [], "stale": True}
        kwargs = self._base_kwargs()
        kwargs["get_positions_fn"] = stale_positions
        report = await hr.build_health_report(**kwargs)
        self.assertEqual(report["status"], "warn")  # 스냅샷 기록 없음 = 시각 불명 -> 경고
        self.assertTrue(any("포지션 조회 실패" in i for i in report["issues"]))
        self.assertIn("d) 포지션 조회: 조회 실패(폴백 사용), 스냅샷 기록 없음", report["text"])

    async def test_stale_with_fresh_snapshot_is_ok_and_label_separated(self):
        """stale + 스냅샷 0분 전: 경고 없이 "조회 실패(폴백 사용), 스냅샷 0분 전"으로 구분 표시."""
        async def stale_positions():
            return {"success": True, "data": [], "stale": True}
        kwargs = self._base_kwargs()
        kwargs["get_positions_fn"] = stale_positions
        kwargs["redis"] = FakeRedis({"positions:last_ok": '{"snapshot_at": "%s"}' % kwargs["now_kst"].isoformat()})
        report = await hr.build_health_report(**kwargs)
        self.assertFalse(any("포지션" in i for i in report["issues"]))
        self.assertIn("d) 포지션 조회: 조회 실패(폴백 사용), 스냅샷 0분 전", report["text"])

    async def test_token_reissue_three_or_more_triggers_warn(self):
        kwargs = self._base_kwargs()
        kwargs["redis"] = FakeRedis({"health:token_reissue:2026-10-13": "3"})
        report = await hr.build_health_report(**kwargs)
        self.assertEqual(report["status"], "warn")
        self.assertTrue(any("토큰 재발급" in i for i in report["issues"]))

    async def test_token_reissue_below_threshold_is_ok(self):
        kwargs = self._base_kwargs()
        kwargs["redis"] = FakeRedis({"health:token_reissue:2026-10-13": "2"})
        report = await hr.build_health_report(**kwargs)
        self.assertEqual(report["status"], "ok")

    async def test_reconcile_mismatch_triggers_warn(self):
        async def mismatch_reconcile(_day):
            return {"missing_in_db": [1], "extra_in_db": [], "qty_mismatch": [],
                     "manual_review": 0, "unavailable_days": []}
        kwargs = self._base_kwargs()
        kwargs["reconcile_dry_run_fn"] = mismatch_reconcile
        report = await hr.build_health_report(**kwargs)
        self.assertEqual(report["status"], "warn")
        self.assertTrue(any("정산 불일치" in i for i in report["issues"]))

    async def test_scan_detail_appends_to_scan_timing_line(self):
        """[AT] fix/scan-parallel — e) 아침스캔 소요 줄에 종목수/실패/조회초가 붙는다."""
        kwargs = self._base_kwargs()
        kwargs["redis"] = FakeRedis({
            "health:scan_timing:2026-10-13": "95.20",
            "health:scan_detail:2026-10-13": json.dumps(
                {"total": 2800, "fail": 12, "fetch_elapsed": 80.3, "score_elapsed": 1.9}),
        })
        report = await hr.build_health_report(**kwargs)
        self.assertIn(
            "e) 아침스캔 소요: 95.2s (종목수 2800 · 실패 12 · 조회 80.3s)",
            report["text"])

    async def test_scan_detail_missing_keeps_line_unchanged(self):
        """scan_detail 기록이 없으면(기존 배포) e) 줄은 소요시간만 표시한다(회귀 아님)."""
        kwargs = self._base_kwargs()
        kwargs["redis"] = FakeRedis({"health:scan_timing:2026-10-13": "95.20"})
        report = await hr.build_health_report(**kwargs)
        self.assertIn("e) 아침스캔 소요: 95.2s", report["text"])
        self.assertNotIn("종목수", report["text"])


if __name__ == "__main__":
    unittest.main()
