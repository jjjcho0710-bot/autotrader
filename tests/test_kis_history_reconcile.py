"""
[AT] feat/kis-history-reconcile / feat/reconcile-apply 회귀 테스트.

배경: trade_history에 기록 누락이 확인됨(10/1 14:03 LG디스플레이(034220) 5주 매수가
KIS에선 체결·보유 중인데 DB에는 없었음). dashboard/kis_history_reconcile.py는 KIS
체결내역(inquire-daily-ccld)과 trade_history를 기간으로 대조하는 순수 로직을 제공한다
(HTTP/DB 접근은 전부 호출부가 주입하므로 이 테스트는 실제 KIS/DB 없이 로직만 검증한다).

이 파일은 다음을 검증한다:
- 체결수량 0(미체결·거부) 주문은 걸러진다.
- 하루치 조회의 CTX_AREA 페이지네이션이 응답 tr_cont가 "M"이 아닐 때까지 계속된다.
- 페이지 상한(MAX_PAGES_PER_DAY)을 넘기면 truncated=True로 안전하게 멈춘다.
- 기간 조회는 하루 단위로 나뉘고, 하루 조회 실패가 다른 날짜에 전파되지 않는다.
- missing_in_db / extra_in_db / qty_mismatch가 각각 올바르게 분류된다.
- 잔고 대조(check_position_consistency)가 구간 내 순증감과 실제 잔고를 올바르게 비교한다.

보정(apply) 관련 추가 검증([AT] feat/reconcile-apply):
- 보정 가능 구간(2026-09-28~어제) 밖은 validate_apply_window가 거절한다.
- build_reconcile_plan은 구간 밖 항목을 계획에서 제외하고, qty_mismatch는 DB 행이
  정확히 1개일 때만 UPDATE 대상으로 삼고 아니면 manual_review로 분류한다.
- compute_plan_hash는 계획이 같으면 같은 해시, 하나라도 다르면 다른 해시를 낸다.
- apply_reconcile_plan은 중복 INSERT를 건너뛰고, 액션마다 감사 행을 남기고,
  중간에 실패하면 트랜잭션 전체가 롤백된다(주입된 페이크 db로 검증).

수동확인 보강 + 지정 보정 관련 추가 검증([AT] feat/reconcile-manual):
- build_reconcile_plan의 manual_review는 qty_mismatch(DB 행 2개 이상)뿐 아니라
  extra_in_db(KIS 체결 없음)도 포함하고, 각 항목에 KIS 체결 상세·DB 행 상세를 그대로
  들고 있다(build_plan_summary_text가 이를 모두 풀어서 표시).
- parse_fix_delete_param/parse_fix_update_param은 "id,id,..."/"id:수량,id:수량,..."을
  파싱하고, 형식이 잘못되면 ValueError를 낸다.
- build_fix_plan은 존재하지 않는(또는 윈도우 밖) id, SELL 삭제, 삭제 5건 초과, 삭제·수정
  id 중복을 전부 거부(부분 적용 없음)하고, 정상 입력에서는 올바른 deletes/updates를
  만든다.
- build_fix_preview는 지정 보정 적용 후 (종목,방향,날짜)별 DB 합계 vs KIS 수량을
  올바르게 계산한다.
- compute_plan_hash는 fix_deletes/fix_updates 유무에 따라 다른 해시를 낸다.
- apply_reconcile_plan은 fix_deletes/fix_updates를 같은 트랜잭션에서 처리해 DELETE/UPDATE
  감사 행(old_price/old_ts 포함)을 남기고, 영향받은 종목의 net_position을 재계산해
  반환한다.
"""
import sys
import unittest
from datetime import date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dashboard import kis_history_reconcile as kr  # noqa: E402


def _row(odno, side_cd, qty, pdno="005930", ord_tmd="093000", avg_prvs="70000"):
    return {
        "odno": odno, "sll_buy_dvsn_cd": side_cd, "tot_ccld_qty": str(qty),
        "pdno": pdno, "ord_tmd": ord_tmd, "avg_prvs": avg_prvs,
    }


class TestParseExecutionRow(unittest.TestCase):
    def test_filled_qty_zero_is_ignored(self):
        """체결 수량이 0인 주문(미체결·거부)은 체결로 취급하지 않는다."""
        self.assertIsNone(kr._parse_execution_row(_row("1", "02", 0), "20261001"))

    def test_buy_and_sell_side_mapping(self):
        buy = kr._parse_execution_row(_row("1", "02", 5), "20261001")
        sell = kr._parse_execution_row(_row("2", "01", 3), "20261001")
        self.assertEqual(buy["side"], "BUY")
        self.assertEqual(buy["qty"], 5)
        self.assertEqual(sell["side"], "SELL")
        self.assertEqual(sell["qty"], 3)

    def test_unknown_side_code_is_ignored(self):
        self.assertIsNone(kr._parse_execution_row(_row("1", "99", 5), "20261001"))

    def test_missing_symbol_is_ignored(self):
        row = _row("1", "02", 5)
        row["pdno"] = ""
        self.assertIsNone(kr._parse_execution_row(row, "20261001"))


class TestFetchDayExecutionsPagination(unittest.IsolatedAsyncioTestCase):
    async def test_follows_pagination_until_final_page(self):
        """CTX_AREA 연속조회 - 응답 tr_cont가 "M"인 동안 계속 다음 페이지를 요청하고,
        그렇지 않은 페이지에서 멈춘다. 각 페이지의 체결행이 전부 합쳐져야 한다."""
        calls = []

        async def fetch_page(date_str, ctx_fk100, ctx_nk100, tr_cont):
            calls.append((ctx_fk100, ctx_nk100, tr_cont))
            page = len(calls)
            if page == 1:
                return {"rt_cd": "0", "output1": [_row("1", "02", 5)],
                        "ctx_area_fk100": "FK1", "ctx_area_nk100": "NK1", "_resp_tr_cont": "M"}
            if page == 2:
                return {"rt_cd": "0", "output1": [_row("2", "02", 3)],
                        "ctx_area_fk100": "FK2", "ctx_area_nk100": "NK2", "_resp_tr_cont": "M"}
            return {"rt_cd": "0", "output1": [_row("3", "01", 1)], "_resp_tr_cont": "F"}

        result = await kr.fetch_day_executions(fetch_page, "20261001")
        self.assertEqual(result["pages"], 3)
        self.assertFalse(result["truncated"])
        self.assertEqual([r["odno"] for r in result["rows"]], ["1", "2", "3"])
        # 2번째 호출은 1번째 응답의 ctx_area를 그대로 넘기고 tr_cont="N"이어야 한다
        self.assertEqual(calls[1], ("FK1", "NK1", "N"))
        self.assertEqual(calls[2], ("FK2", "NK2", "N"))
        # 최초 호출은 공란
        self.assertEqual(calls[0], ("", "", ""))

    async def test_single_page_when_not_more(self):
        async def fetch_page(date_str, ctx_fk100, ctx_nk100, tr_cont):
            return {"rt_cd": "0", "output1": [_row("1", "02", 5)], "_resp_tr_cont": ""}

        result = await kr.fetch_day_executions(fetch_page, "20261001")
        self.assertEqual(result["pages"], 1)
        self.assertFalse(result["truncated"])
        self.assertEqual(len(result["rows"]), 1)

    async def test_truncates_safely_if_always_more(self):
        """비정상 응답으로 tr_cont가 영원히 "M"이면 MAX_PAGES_PER_DAY에서 멈춰야 한다
        (무한루프 금지 안전장치)."""
        async def fetch_page(date_str, ctx_fk100, ctx_nk100, tr_cont):
            return {"rt_cd": "0", "output1": [], "ctx_area_fk100": "x", "ctx_area_nk100": "y",
                    "_resp_tr_cont": "M"}

        result = await kr.fetch_day_executions(fetch_page, "20261001")
        self.assertTrue(result["truncated"])
        self.assertEqual(result["pages"], kr.MAX_PAGES_PER_DAY)

    async def test_raises_on_kis_error_response(self):
        async def fetch_page(date_str, ctx_fk100, ctx_nk100, tr_cont):
            return {"rt_cd": "1", "msg_cd": "EGW00000", "msg1": "조회 실패"}

        with self.assertRaises(RuntimeError):
            await kr.fetch_day_executions(fetch_page, "20261001")


class TestFetchKisExecutions(unittest.IsolatedAsyncioTestCase):
    async def test_queries_day_by_day_and_isolates_failures(self):
        """기간을 하루 단위로 나눠 조회하고(모의투자 기간조회 제한 대응), 특정 날짜 조회
        실패가 다른 날짜의 조회를 막지 않으며 unavailable_days에 기록된다."""
        seen_dates = []

        async def fetch_page(date_str, ctx_fk100, ctx_nk100, tr_cont):
            seen_dates.append(date_str)
            if date_str == "20261002":
                return {"rt_cd": "1", "msg_cd": "EGW00000", "msg1": "기간 조회 제한"}
            return {"rt_cd": "0", "output1": [_row(date_str, "02", 1)], "_resp_tr_cont": ""}

        sleeps = []

        async def fake_sleep():
            sleeps.append(1)

        result = await kr.fetch_kis_executions(
            fetch_page, date(2026, 10, 1), date(2026, 10, 3), sleep_fn=fake_sleep)

        self.assertEqual(seen_dates, ["20261001", "20261002", "20261003"])
        self.assertEqual(len(result["rows"]), 2)  # 10/2는 실패해서 빠짐
        self.assertEqual(len(result["unavailable_days"]), 1)
        self.assertEqual(result["unavailable_days"][0]["date"], "20261002")
        # 날짜 사이마다 간격을 둔다(마지막 날 뒤에는 불필요하므로 횟수는 구간-1)
        self.assertEqual(len(sleeps), 2)


class TestReconcile(unittest.TestCase):
    def test_missing_in_db(self):
        """KIS에는 있고 DB에는 없음 → missing_in_db."""
        kis_rows = [{"symbol": "034220", "side": "BUY", "qty": 5, "date": "20261001",
                     "avg_price": 8456.0, "odno": "1"}]
        result = kr.reconcile(kis_rows, [])
        self.assertEqual(len(result["missing_in_db"]), 1)
        self.assertEqual(result["missing_in_db"][0]["qty"], 5)
        self.assertEqual(result["extra_in_db"], [])
        self.assertEqual(result["qty_mismatch"], [])

    def test_extra_in_db(self):
        """DB에는 있고 KIS에는 없음 → extra_in_db."""
        db_rows = [{"id": 1, "symbol": "002710", "side": "SELL", "qty": 10, "date": "20261001"}]
        result = kr.reconcile([], db_rows)
        self.assertEqual(result["extra_in_db"], [{
            "symbol": "002710", "side": "SELL", "date": "20261001",
            "qty": 10, "db_rows": db_rows,
        }])
        self.assertEqual(result["missing_in_db"], [])
        self.assertEqual(result["qty_mismatch"], [])

    def test_qty_mismatch(self):
        """양쪽에 있으나 수량 합계가 다름 → qty_mismatch."""
        kis_rows = [{"symbol": "005930", "side": "BUY", "qty": 5, "date": "20261001", "odno": "1"}]
        db_rows = [{"id": 1, "symbol": "005930", "side": "BUY", "qty": 3, "date": "20261001"}]
        result = kr.reconcile(kis_rows, db_rows)
        self.assertEqual(len(result["qty_mismatch"]), 1)
        self.assertEqual(result["qty_mismatch"][0]["kis_qty"], 5)
        self.assertEqual(result["qty_mismatch"][0]["db_qty"], 3)
        self.assertEqual(result["missing_in_db"], [])
        self.assertEqual(result["extra_in_db"], [])

    def test_matching_rows_produce_no_findings(self):
        kis_rows = [
            {"symbol": "005930", "side": "BUY", "qty": 2, "date": "20261001", "odno": "1"},
            {"symbol": "005930", "side": "BUY", "qty": 3, "date": "20261001", "odno": "2"},
        ]
        db_rows = [{"id": 1, "symbol": "005930", "side": "BUY", "qty": 5, "date": "20261001"}]
        result = kr.reconcile(kis_rows, db_rows)
        self.assertEqual(result["missing_in_db"], [])
        self.assertEqual(result["extra_in_db"], [])
        self.assertEqual(result["qty_mismatch"], [])

    def test_aggregates_by_symbol_side_and_date_not_just_symbol(self):
        """같은 종목이라도 방향·날짜가 다르면 서로 다른 키로 취급돼야 한다."""
        kis_rows = [{"symbol": "005930", "side": "BUY", "qty": 5, "date": "20261001", "odno": "1"}]
        db_rows = [{"id": 1, "symbol": "005930", "side": "SELL", "qty": 5, "date": "20261001"}]
        result = kr.reconcile(kis_rows, db_rows)
        self.assertEqual(len(result["missing_in_db"]), 1)
        self.assertEqual(len(result["extra_in_db"]), 1)
        self.assertEqual(result["qty_mismatch"], [])


class TestCheckPositionConsistency(unittest.TestCase):
    def test_matching_position(self):
        kis_rows = [
            {"symbol": "034220", "side": "BUY", "qty": 5, "date": "20261001", "odno": "1"},
        ]
        real_positions = {"034220": {"qty": 5, "avg_price": 8456}}
        result = kr.check_position_consistency(kis_rows, real_positions)
        finding = result["findings"][0]
        self.assertEqual(finding["symbol"], "034220")
        self.assertEqual(finding["period_net_change"], 5)
        self.assertEqual(finding["real_qty"], 5)

    def test_mismatched_position_is_still_reported(self):
        kis_rows = [
            {"symbol": "073240", "side": "SELL", "qty": 3, "date": "20261001", "odno": "1"},
        ]
        real_positions = {}
        result = kr.check_position_consistency(kis_rows, real_positions)
        finding = next(f for f in result["findings"] if f["symbol"] == "073240")
        self.assertEqual(finding["period_net_change"], -3)
        self.assertEqual(finding["real_qty"], 0)
        self.assertIn("note", result)


class TestBuildSummaryText(unittest.TestCase):
    def test_includes_counts_and_no_diff_message(self):
        reconcile_result = {"missing_in_db": [], "extra_in_db": [], "qty_mismatch": []}
        text = kr.build_summary_text(date(2026, 9, 1), date(2026, 10, 8), reconcile_result, [])
        self.assertIn("missing_in_db 0건", text)
        self.assertIn("불일치 없음", text)

    def test_includes_missing_item_detail(self):
        reconcile_result = {
            "missing_in_db": [{"symbol": "034220", "side": "BUY", "qty": 5, "date": "20261001",
                                "executions": []}],
            "extra_in_db": [], "qty_mismatch": [],
        }
        text = kr.build_summary_text(date(2026, 9, 1), date(2026, 10, 8), reconcile_result, [])
        self.assertIn("034220", text)
        self.assertIn("missing_in_db 1건", text)


# ── [AT] feat/reconcile-apply ────────────────────────────────────────────────


class TestValidateApplyWindow(unittest.TestCase):
    def test_start_before_window_is_rejected(self):
        """9/27 이하 시작일은 거절 — 그 이전 모의투자 조회는 신뢰되지 않음."""
        err = kr.validate_apply_window(date(2026, 9, 27), date(2026, 10, 1), date(2026, 10, 12))
        self.assertIsNotNone(err)

    def test_end_is_today_is_rejected(self):
        """종료일이 오늘이면 거절 — 오늘은 체결이 진행 중일 수 있음."""
        today = date(2026, 10, 12)
        err = kr.validate_apply_window(date(2026, 9, 28), today, today)
        self.assertIsNotNone(err)

    def test_end_after_today_is_rejected(self):
        today = date(2026, 10, 12)
        err = kr.validate_apply_window(date(2026, 9, 28), date(2026, 10, 13), today)
        self.assertIsNotNone(err)

    def test_start_after_end_is_rejected(self):
        err = kr.validate_apply_window(date(2026, 10, 5), date(2026, 10, 1), date(2026, 10, 12))
        self.assertIsNotNone(err)

    def test_within_window_is_accepted(self):
        """2026-09-28~어제(KST)는 보정 가능 구간 안이라 허용되어야 한다."""
        err = kr.validate_apply_window(date(2026, 9, 28), date(2026, 10, 11), date(2026, 10, 12))
        self.assertIsNone(err)


class TestBuildReconcilePlan(unittest.TestCase):
    def test_missing_in_db_outside_window_is_excluded(self):
        """9/28 이전 누락은 보정 계획에 포함되지 않는다(보정 대상 구간 아님)."""
        reconcile_result = {
            "missing_in_db": [{
                "symbol": "147760", "side": "SELL", "date": "20260927",
                "qty": 300, "executions": [{"qty": 300, "avg_price": 50000, "odno": "1",
                                             "time_kst": "100000"}],
            }],
            "qty_mismatch": [], "extra_in_db": [],
        }
        plan = kr.build_reconcile_plan(reconcile_result, date(2026, 9, 28), date(2026, 10, 11))
        self.assertEqual(plan["inserts"], [])

    def test_missing_in_db_builds_one_insert_per_execution(self):
        """집계행이 아니라 개별 체결(odno) 단위로 INSERT 계획을 만든다."""
        reconcile_result = {
            "missing_in_db": [{
                "symbol": "034220", "side": "BUY", "date": "20261001",
                "qty": 5, "executions": [{"qty": 5, "avg_price": 8456.0, "odno": "123",
                                           "time_kst": "140244"}],
            }],
            "qty_mismatch": [], "extra_in_db": [],
        }
        plan = kr.build_reconcile_plan(reconcile_result, date(2026, 9, 28), date(2026, 10, 11))
        self.assertEqual(len(plan["inserts"]), 1)
        ins = plan["inserts"][0]
        self.assertEqual(ins["symbol"], "034220")
        self.assertEqual(ins["side"], "BUY")
        self.assertEqual(ins["quantity"], 5)
        self.assertEqual(ins["price"], 8456.0)
        self.assertEqual(ins["amount"], 42280.0)
        self.assertEqual(ins["odno"], "123")
        self.assertEqual(ins["strategy"], kr.RECONCILE_STRATEGY)

    def test_qty_mismatch_with_single_db_row_becomes_update(self):
        """DB 행이 정확히 1개면 UPDATE 대상 — 수량/금액은 KIS 값으로, pnl은 비율 조정."""
        reconcile_result = {
            "missing_in_db": [], "extra_in_db": [],
            "qty_mismatch": [{
                "symbol": "090710", "side": "SELL", "date": "20261001",
                "kis_qty": 103, "db_qty": 100,
                "kis_executions": [{"qty": 103, "avg_price": 1000.0, "odno": "9"}],
                "db_rows": [{"id": 55, "qty": 100, "amount": 100000.0, "pnl": 5000.0}],
            }],
        }
        plan = kr.build_reconcile_plan(reconcile_result, date(2026, 9, 28), date(2026, 10, 11))
        self.assertEqual(len(plan["updates"]), 1)
        upd = plan["updates"][0]
        self.assertEqual(upd["trade_history_id"], 55)
        self.assertEqual(upd["old_quantity"], 100)
        self.assertEqual(upd["new_quantity"], 103)
        self.assertEqual(upd["new_amount"], 103000.0)
        self.assertEqual(upd["new_pnl"], round(5000.0 * 103 / 100, 2))
        self.assertEqual(plan["manual_review"], [])

    def test_qty_mismatch_update_recomputes_price_consistent_with_amount(self):
        """[AT] fix/reconcile-price-consistency 회귀: amount는 KIS 체결 가중평균으로
        다시 계산되는데 price(단가)는 그대로 둬 price*quantity != amount가 되는 불일치가
        있었다(002030 BUY 보정 시나리오 — 부분체결가 217,500원으로 기록된 DB 행을 KIS
        실제 체결(1주@217,500 + 2주@205,000)로 보정하면 가중평균 단가는 209,166.67원이어야
        한다). price도 amount/quantity로 재계산돼야 한다."""
        reconcile_result = {
            "missing_in_db": [], "extra_in_db": [],
            "qty_mismatch": [{
                "symbol": "002030", "side": "BUY", "date": "20261016",
                "kis_qty": 3, "db_qty": 1,
                "kis_executions": [
                    {"qty": 1, "avg_price": 217500.0, "odno": "1"},
                    {"qty": 2, "avg_price": 205000.0, "odno": "1"},
                ],
                "db_rows": [{"id": 77, "qty": 1, "price": 217500.0, "amount": 217500.0, "pnl": None}],
            }],
        }
        plan = kr.build_reconcile_plan(reconcile_result, date(2026, 9, 28), date(2026, 10, 18))
        self.assertEqual(len(plan["updates"]), 1)
        upd = plan["updates"][0]
        expected_amount = 1 * 217500.0 + 2 * 205000.0
        self.assertEqual(upd["new_amount"], round(expected_amount, 2))
        self.assertEqual(upd["old_price"], 217500.0)
        self.assertEqual(upd["new_price"], round(expected_amount / 3, 2))
        # 핵심 불변식 — 보정 후에도 price*quantity ≈ amount여야 한다(단가를 2자리에서
        # 반올림하는 데서 오는 오차는 몇 원 이내로 무시 가능한 수준이어야 한다).
        self.assertAlmostEqual(
            upd["new_price"] * upd["new_quantity"], upd["new_amount"], delta=0.1)

    def test_qty_mismatch_with_multiple_db_rows_needs_manual_review(self):
        """DB 행이 2개 이상이면 자동 UPDATE하지 않고 manual_review로만 보고한다."""
        reconcile_result = {
            "missing_in_db": [], "extra_in_db": [],
            "qty_mismatch": [{
                "symbol": "007810", "side": "BUY", "date": "20260928",
                "kis_qty": 20, "db_qty": 15,
                "kis_executions": [{"qty": 20, "avg_price": 1000.0, "odno": "1"}],
                "db_rows": [{"id": 1, "qty": 10, "amount": 10000.0, "pnl": None},
                            {"id": 2, "qty": 5, "amount": 5000.0, "pnl": None}],
            }],
        }
        plan = kr.build_reconcile_plan(reconcile_result, date(2026, 9, 28), date(2026, 10, 11))
        self.assertEqual(plan["updates"], [])
        self.assertEqual(len(plan["manual_review"]), 1)
        self.assertEqual(plan["manual_review"][0]["db_row_ids"], [1, 2])

    def test_qty_mismatch_manual_review_carries_kis_and_db_row_detail(self):
        """수동확인 항목에는 KIS 체결 각 건(시각/수량/체결가/odno)과 DB 후보 행 각 건
        (id/ts/수량/가격/금액/strategy)이 그대로 들어있어야 한다([AT] feat/reconcile-manual —
        build_plan_summary_text가 이를 풀어서 표시)."""
        reconcile_result = {
            "missing_in_db": [], "extra_in_db": [],
            "qty_mismatch": [{
                "symbol": "007810", "side": "BUY", "date": "20260928",
                "kis_qty": 8, "db_qty": 19,
                "kis_executions": [{"qty": 8, "avg_price": 1000.0, "odno": "1", "time_kst": "093000"}],
                "db_rows": [
                    {"id": 317, "qty": 11, "price": 1000.0, "amount": 11000.0, "strategy": None,
                     "ts_kst": None},
                    {"id": 326, "qty": 3, "price": 1000.0, "amount": 3000.0, "strategy": None,
                     "ts_kst": None},
                    {"id": 330, "qty": 5, "price": 1000.0, "amount": 5000.0, "strategy": None,
                     "ts_kst": None},
                ],
            }],
        }
        plan = kr.build_reconcile_plan(reconcile_result, date(2026, 9, 28), date(2026, 10, 11))
        mr = plan["manual_review"][0]
        self.assertEqual(len(mr["kis_executions"]), 1)
        self.assertEqual(mr["kis_executions"][0]["odno"], "1")
        self.assertEqual([r["id"] for r in mr["db_rows"]], [317, 326, 330])

    def test_extra_in_db_within_window_becomes_manual_review(self):
        """KIS 체결이 전혀 없는 DB 전용 항목(extra_in_db)도 자동 보정하지 않고
        manual_review로 표시해야 한다([AT] feat/reconcile-manual — 023790 DB에만 존재
        사례)."""
        reconcile_result = {
            "missing_in_db": [], "qty_mismatch": [],
            "extra_in_db": [{
                "symbol": "023790", "side": "BUY", "date": "20260930", "qty": 273,
                "db_rows": [
                    {"id": 342, "qty": 91, "price": 5000.0, "amount": 455000.0, "strategy": None},
                    {"id": 347, "qty": 182, "price": 5000.0, "amount": 910000.0, "strategy": None},
                ],
            }],
        }
        plan = kr.build_reconcile_plan(reconcile_result, date(2026, 9, 28), date(2026, 10, 11))
        self.assertEqual(len(plan["manual_review"]), 1)
        mr = plan["manual_review"][0]
        self.assertEqual(mr["symbol"], "023790")
        self.assertEqual(mr["kis_executions"], [])
        self.assertEqual(mr["db_row_ids"], [342, 347])

    def test_extra_in_db_outside_window_is_excluded(self):
        reconcile_result = {
            "missing_in_db": [], "qty_mismatch": [],
            "extra_in_db": [{
                "symbol": "023790", "side": "BUY", "date": "20260927", "qty": 10,
                "db_rows": [{"id": 1, "qty": 10}],
            }],
        }
        plan = kr.build_reconcile_plan(reconcile_result, date(2026, 9, 28), date(2026, 10, 11))
        self.assertEqual(plan["manual_review"], [])

    def test_plan_always_has_empty_fix_lists_by_default(self):
        reconcile_result = {"missing_in_db": [], "qty_mismatch": [], "extra_in_db": []}
        plan = kr.build_reconcile_plan(reconcile_result, date(2026, 9, 28), date(2026, 10, 11))
        self.assertEqual(plan["fix_deletes"], [])
        self.assertEqual(plan["fix_updates"], [])

    def test_pnl_none_stays_none_after_update(self):
        reconcile_result = {
            "missing_in_db": [], "extra_in_db": [],
            "qty_mismatch": [{
                "symbol": "009830", "side": "BUY", "date": "20260928",
                "kis_qty": 5, "db_qty": 3,
                "kis_executions": [{"qty": 5, "avg_price": 100.0, "odno": "1"}],
                "db_rows": [{"id": 7, "qty": 3, "amount": 300.0, "pnl": None}],
            }],
        }
        plan = kr.build_reconcile_plan(reconcile_result, date(2026, 9, 28), date(2026, 10, 11))
        self.assertIsNone(plan["updates"][0]["new_pnl"])


class TestComputePlanHash(unittest.TestCase):
    def test_same_plan_produces_same_hash(self):
        plan = {"window": {"start": "2026-09-28", "end": "2026-10-11"},
                "inserts": [{"symbol": "005930", "quantity": 1}], "updates": [], "manual_review": []}
        self.assertEqual(kr.compute_plan_hash(plan), kr.compute_plan_hash(plan))

    def test_different_plan_produces_different_hash(self):
        plan_a = {"window": {}, "inserts": [{"symbol": "005930", "quantity": 1}],
                   "updates": [], "manual_review": []}
        plan_b = {"window": {}, "inserts": [{"symbol": "005930", "quantity": 2}],
                   "updates": [], "manual_review": []}
        self.assertNotEqual(kr.compute_plan_hash(plan_a), kr.compute_plan_hash(plan_b))

    def test_fix_deletes_and_fix_updates_change_the_hash(self):
        """plan_hash 계산에 fix_delete/fix_update 내용이 포함되어야 한다([AT]
        feat/reconcile-manual, PM 지시) — 지정 보정이 있으면 없을 때와 다른 해시가 나와야
        apply=1 confirm이 "지금 요청한 지정 보정"과 정확히 일치하는지 검증할 수 있다."""
        base = {"window": {}, "inserts": [], "updates": [], "manual_review": [],
                "fix_deletes": [], "fix_updates": []}
        with_delete = {**base, "fix_deletes": [{"trade_history_id": 326, "symbol": "007810"}]}
        with_update = {**base, "fix_updates": [{"trade_history_id": 317, "new_quantity": 8}]}
        base_hash = kr.compute_plan_hash(base)
        self.assertNotEqual(base_hash, kr.compute_plan_hash(with_delete))
        self.assertNotEqual(base_hash, kr.compute_plan_hash(with_update))
        self.assertNotEqual(kr.compute_plan_hash(with_delete), kr.compute_plan_hash(with_update))


class TestParseFixParams(unittest.TestCase):
    def test_parse_fix_delete_param_basic(self):
        self.assertEqual(kr.parse_fix_delete_param("326,330"), [326, 330])

    def test_parse_fix_delete_param_empty_or_none(self):
        self.assertEqual(kr.parse_fix_delete_param(None), [])
        self.assertEqual(kr.parse_fix_delete_param(""), [])

    def test_parse_fix_delete_param_malformed_raises(self):
        with self.assertRaises(ValueError):
            kr.parse_fix_delete_param("326,abc")

    def test_parse_fix_update_param_basic(self):
        self.assertEqual(kr.parse_fix_update_param("317:8,1:3.5"), {317: 8.0, 1: 3.5})

    def test_parse_fix_update_param_empty_or_none(self):
        self.assertEqual(kr.parse_fix_update_param(None), {})
        self.assertEqual(kr.parse_fix_update_param(""), {})

    def test_parse_fix_update_param_malformed_raises(self):
        with self.assertRaises(ValueError):
            kr.parse_fix_update_param("317-8")
        with self.assertRaises(ValueError):
            kr.parse_fix_update_param("abc:8")


def _candidate_row(id_, symbol, side, qty, date_str="20260928", price=1000.0,
                    amount=None, pnl=None, ts=None):
    return {"id": id_, "symbol": symbol, "side": side, "qty": qty, "date": date_str,
            "price": price, "amount": amount if amount is not None else price * qty,
            "pnl": pnl, "ts": ts}


class TestBuildFixPlan(unittest.TestCase):
    """007810 BUY 2026-09-28 사례(DB 3행 11+3+5=19주 vs KIS 8주)를 fix_delete=326,330 &
    fix_update=317:8로 보정하는 흐름을 기준으로 안전조건을 검증한다([AT]
    feat/reconcile-manual)."""

    def setUp(self):
        self.window_start, self.window_end = date(2026, 9, 28), date(2026, 10, 11)
        self.candidates = {
            317: _candidate_row(317, "007810", "BUY", 11),
            326: _candidate_row(326, "007810", "BUY", 3),
            330: _candidate_row(330, "007810", "BUY", 5),
        }

    def test_valid_delete_and_update_combination(self):
        result = kr.build_fix_plan([326, 330], {317: 8}, self.candidates,
                                    self.window_start, self.window_end)
        self.assertIsNone(result["error"])
        self.assertEqual({d["trade_history_id"] for d in result["deletes"]}, {326, 330})
        self.assertEqual(len(result["updates"]), 1)
        upd = result["updates"][0]
        self.assertEqual(upd["trade_history_id"], 317)
        self.assertEqual(upd["old_quantity"], 11)
        self.assertEqual(upd["new_quantity"], 8)
        self.assertEqual(upd["new_amount"], 8000.0)

    def test_delete_records_old_values_for_restore(self):
        """DELETE 대상은 복원에 필요한 이전 값(symbol/side/quantity/price/amount/ts)을
        전부 들고 있어야 한다([AT] feat/reconcile-manual, PM 지시)."""
        ts = datetime(2026, 9, 28, 9, 31, 0)
        candidates = dict(self.candidates)
        candidates[326] = _candidate_row(326, "007810", "BUY", 3, amount=3000.0, pnl=None, ts=ts)
        result = kr.build_fix_plan([326], {}, candidates, self.window_start, self.window_end)
        self.assertIsNone(result["error"])
        d = result["deletes"][0]
        self.assertEqual(d["symbol"], "007810")
        self.assertEqual(d["side"], "BUY")
        self.assertEqual(d["old_quantity"], 3)
        self.assertEqual(d["old_price"], 1000.0)
        self.assertEqual(d["old_amount"], 3000.0)
        self.assertEqual(d["old_ts"], ts)

    def test_nonexistent_id_is_rejected_entirely(self):
        result = kr.build_fix_plan([999], {}, self.candidates, self.window_start, self.window_end)
        self.assertIsNotNone(result["error"])
        self.assertEqual(result["deletes"], [])
        self.assertEqual(result["updates"], [])

    def test_id_outside_window_is_rejected(self):
        """윈도우 밖 행은 호출부가 candidate_rows에서 아예 제외하므로, build_fix_plan
        입장에서는 '존재하지 않는 id'와 동일하게 거부된다."""
        candidates_without_317 = {k: v for k, v in self.candidates.items() if k != 317}
        result = kr.build_fix_plan([], {317: 8}, candidates_without_317,
                                    self.window_start, self.window_end)
        self.assertIsNotNone(result["error"])
        self.assertIn("317", result["error"])

    def test_sell_delete_is_rejected(self):
        candidates = dict(self.candidates)
        candidates[400] = _candidate_row(400, "005930", "SELL", 10)
        result = kr.build_fix_plan([400], {}, candidates, self.window_start, self.window_end)
        self.assertIsNotNone(result["error"])
        self.assertEqual(result["deletes"], [])

    def test_more_than_five_deletes_is_rejected(self):
        candidates = {i: _candidate_row(i, "005930", "BUY", 1) for i in range(1, 8)}
        result = kr.build_fix_plan(list(range(1, 8)), {}, candidates,
                                    self.window_start, self.window_end)
        self.assertIsNotNone(result["error"])
        self.assertEqual(result["deletes"], [])

    def test_exactly_five_deletes_is_allowed(self):
        candidates = {i: _candidate_row(i, "005930", "BUY", 1) for i in range(1, 6)}
        result = kr.build_fix_plan(list(range(1, 6)), {}, candidates,
                                    self.window_start, self.window_end)
        self.assertIsNone(result["error"])
        self.assertEqual(len(result["deletes"]), 5)

    def test_same_id_in_both_delete_and_update_is_rejected(self):
        result = kr.build_fix_plan([317], {317: 8}, self.candidates,
                                    self.window_start, self.window_end)
        self.assertIsNotNone(result["error"])

    def test_pnl_scaled_proportionally_on_update(self):
        candidates = dict(self.candidates)
        candidates[317] = _candidate_row(317, "007810", "BUY", 10, amount=10000.0, pnl=500.0)
        result = kr.build_fix_plan([], {317: 5}, candidates, self.window_start, self.window_end)
        self.assertEqual(result["updates"][0]["new_pnl"], 250.0)

    def test_fix_update_keeps_price_unchanged_and_consistent_with_amount(self):
        """fix_update(수동 수량 보정)는 단가를 바꾸지 않고 그 단가로 금액만 재계산한다
        ([AT] fix/reconcile-price-consistency 회귀 — update_trade가 이제 항상 new_price를
        받으므로 build_fix_plan도 일관되게 채워야 한다)."""
        candidates = dict(self.candidates)
        candidates[317] = _candidate_row(317, "007810", "BUY", 10, price=1500.0, amount=15000.0)
        result = kr.build_fix_plan([], {317: 5}, candidates, self.window_start, self.window_end)
        upd = result["updates"][0]
        self.assertEqual(upd["old_price"], 1500.0)
        self.assertEqual(upd["new_price"], 1500.0)
        self.assertEqual(upd["new_amount"], 7500.0)
        self.assertAlmostEqual(upd["new_price"] * upd["new_quantity"], upd["new_amount"], places=2)

class TestBuildFixPreview(unittest.TestCase):
    def test_delete_and_update_combination_matches_kis_after_fix(self):
        """007810 사례: fix_delete=326,330 & fix_update=317:8 적용 후 DB 합계가 KIS
        수량(8)과 일치해야 한다."""
        candidates = {
            317: _candidate_row(317, "007810", "BUY", 11),
            326: _candidate_row(326, "007810", "BUY", 3),
            330: _candidate_row(330, "007810", "BUY", 5),
        }
        kis_qty_by_key = {("007810", "BUY", "20260928"): 8}
        preview = kr.build_fix_preview(candidates, [326, 330], {317: 8}, kis_qty_by_key)
        self.assertEqual(len(preview), 1)
        p = preview[0]
        self.assertEqual(p["old_db_qty"], 19)
        self.assertEqual(p["new_db_qty"], 8)
        self.assertEqual(p["kis_qty"], 8)
        self.assertTrue(p["matches_after_fix"])

    def test_delete_only_db_candidate_with_no_kis_defaults_to_zero(self):
        """023790 사례: KIS 체결이 전혀 없으면 kis_qty는 0으로 취급되고, 두 행을 모두
        삭제하면 DB 합계도 0이 되어 일치해야 한다."""
        candidates = {
            342: _candidate_row(342, "023790", "BUY", 91, date_str="20260930"),
            347: _candidate_row(347, "023790", "BUY", 182, date_str="20260930"),
        }
        preview = kr.build_fix_preview(candidates, [342, 347], {}, {})
        self.assertEqual(len(preview), 1)
        p = preview[0]
        self.assertEqual(p["old_db_qty"], 273)
        self.assertEqual(p["new_db_qty"], 0)
        self.assertEqual(p["kis_qty"], 0)
        self.assertTrue(p["matches_after_fix"])

    def test_mismatch_still_reported_when_fix_does_not_fully_reconcile(self):
        candidates = {317: _candidate_row(317, "007810", "BUY", 11)}
        kis_qty_by_key = {("007810", "BUY", "20260928"): 8}
        preview = kr.build_fix_preview(candidates, [], {317: 9}, kis_qty_by_key)
        self.assertFalse(preview[0]["matches_after_fix"])


class _FakeTxn:
    """FakeReconcileDB의 트랜잭션 — 실패 시 진입 시점 상태로 되돌린다(실제 asyncpg
    conn.transaction()의 롤백 동작을 흉내)."""
    def __init__(self, db):
        self.db = db

    async def __aenter__(self):
        self._snapshot = (
            list(self.db.trades), list(self.db.updates), list(self.db.audits), self.db.next_id,
            dict(self.db.existing_rows), list(self.db.deleted_ids),
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is not None:
            (self.db.trades, self.db.updates, self.db.audits, self.db.next_id,
             self.db.existing_rows, self.db.deleted_ids) = self._snapshot
        return False  # 예외는 그대로 전파(apply_reconcile_plan 호출부가 처리)


class _FakeReconcileDB:
    """apply_reconcile_plan()이 요구하는 인터페이스를 실제 asyncpg 없이 흉내내는 페이크.
    exists_backfill은 self.trades를 직접 검사하므로, 같은 plan을 두 번 적용해도
    실제로 중복 INSERT가 쌓이지 않는지까지 검증할 수 있다. existing_rows는 fix_delete/
    fix_update([AT] feat/reconcile-manual)가 지우거나 고치는, DB에 이미 있던 행을
    흉내낸다(symbol/qty를 들고 있어야 net_position 재계산도 검증할 수 있다)."""
    def __init__(self, fail_update_index=None, existing_rows=None):
        self.trades = []
        self.updates = []
        self.audits = []
        self.next_id = 1000
        self.fail_update_index = fail_update_index
        self.existing_rows = dict(existing_rows or {})
        self.deleted_ids = []

    def transaction(self):
        return _FakeTxn(self)

    async def exists_backfill(self, symbol, side, date_str, quantity):
        return any(
            t["symbol"] == symbol and t["side"] == side and t["date"] == date_str
            and t["quantity"] == quantity and t.get("strategy") == kr.RECONCILE_STRATEGY
            for t in self.trades
        )

    async def insert_trade(self, ins):
        tid = self.next_id
        self.next_id += 1
        self.trades.append({**ins, "id": tid})
        return tid

    async def update_trade(self, upd):
        if self.fail_update_index == len(self.updates):
            raise RuntimeError("induced failure")
        self.updates.append(upd)
        if upd["trade_history_id"] in self.existing_rows:
            self.existing_rows[upd["trade_history_id"]]["qty"] = upd["new_quantity"]

    async def delete_trade(self, trade_history_id):
        self.deleted_ids.append(trade_history_id)
        self.existing_rows.pop(trade_history_id, None)

    async def net_position(self, symbol):
        total = 0.0
        for row in self.existing_rows.values():
            if row["symbol"] != symbol:
                continue
            total += row["qty"] if row["side"] == "BUY" else -row["qty"]
        for t in self.trades:
            if t["symbol"] != symbol:
                continue
            total += t["quantity"] if t["side"] == "BUY" else -t["quantity"]
        return total

    async def insert_audit(self, **fields):
        self.audits.append(fields)


def _sample_plan():
    return {
        "window": {"start": "2026-09-28", "end": "2026-10-11"},
        "inserts": [{
            "symbol": "034220", "side": "BUY", "date": "20261001", "quantity": 5,
            "price": 8456.0, "amount": 42280.0, "time_kst": "140244", "odno": "123",
            "strategy": kr.RECONCILE_STRATEGY,
        }],
        "updates": [{
            "trade_history_id": 55, "symbol": "090710", "side": "SELL", "date": "20261001",
            "old_quantity": 100, "new_quantity": 103, "old_amount": 100000.0,
            "new_amount": 103000.0, "old_pnl": 5000.0, "new_pnl": 5150.0,
        }],
        "manual_review": [],
        "fix_deletes": [], "fix_updates": [],
    }


class TestApplyReconcilePlan(unittest.IsolatedAsyncioTestCase):
    async def test_inserts_and_updates_write_matching_audit_rows(self):
        db = _FakeReconcileDB()
        result = await kr.apply_reconcile_plan(_sample_plan(), db)
        self.assertEqual(result, {"inserted": 1, "updated": 1, "skipped": 0, "deleted": 0,
                                   "manual_review": 0, "net_positions": {}})
        self.assertEqual(len(db.trades), 1)
        self.assertEqual(len(db.updates), 1)
        self.assertEqual(len(db.audits), 2)
        self.assertEqual({a["action"] for a in db.audits}, {"INSERT", "UPDATE"})

    async def test_update_audit_row_carries_old_price(self):
        """[AT] fix/reconcile-price-consistency 회귀: UPDATE 감사 행에 old_price가
        담겨야 복원 시 보정 전 단가를 알 수 있다."""
        plan = _sample_plan()
        plan["updates"][0]["old_price"] = 217500.0
        plan["updates"][0]["new_price"] = 209166.67
        db = _FakeReconcileDB()
        await kr.apply_reconcile_plan(plan, db)
        update_audit = next(a for a in db.audits if a["action"] == "UPDATE")
        self.assertEqual(update_audit["old_price"], 217500.0)

    async def test_applying_same_plan_twice_skips_duplicate_insert(self):
        """같은 보정 계획을 두 번 적용해도 중복 INSERT가 생기지 않아야 한다."""
        db = _FakeReconcileDB()
        first = await kr.apply_reconcile_plan(_sample_plan(), db)
        second = await kr.apply_reconcile_plan(_sample_plan(), db)
        self.assertEqual(first["inserted"], 1)
        self.assertEqual(second["inserted"], 0)
        self.assertEqual(second["skipped"], 1)
        self.assertEqual(len(db.trades), 1, "두 번째 적용에서 trade_history 행이 추가로 생기면 안 됨")

    async def test_manual_review_items_are_never_written(self):
        plan = _sample_plan()
        plan["updates"] = []
        plan["manual_review"] = [{"symbol": "007810", "side": "BUY", "date": "20260928",
                                   "reason": "DB 행 2개"}]
        db = _FakeReconcileDB()
        result = await kr.apply_reconcile_plan(plan, db)
        self.assertEqual(result["manual_review"], 1)
        self.assertEqual(db.updates, [])

    async def test_mid_failure_rolls_back_everything(self):
        """두 번째 update에서 실패하면 그 전에 쓴 insert/audit까지 전부 롤백되어야 한다."""
        plan = _sample_plan()
        plan["updates"] = plan["updates"] * 2  # 두 번째 update에서 induced failure 유발
        db = _FakeReconcileDB(fail_update_index=1)
        with self.assertRaises(RuntimeError):
            await kr.apply_reconcile_plan(plan, db)
        self.assertEqual(db.trades, [], "실패 시 먼저 쓴 INSERT까지 롤백되어야 함")
        self.assertEqual(db.updates, [], "실패 시 먼저 쓴 UPDATE까지 롤백되어야 함")
        self.assertEqual(db.audits, [], "실패 시 감사 행도 남지 않아야 함")


# ── [AT] feat/reconcile-manual — fix_delete/fix_update 적용 ───────────────────


def _fix_plan_with(fix_deletes=None, fix_updates=None):
    plan = _sample_plan()
    plan["inserts"] = []
    plan["updates"] = []
    plan["fix_deletes"] = fix_deletes or []
    plan["fix_updates"] = fix_updates or []
    return plan


class TestApplyReconcilePlanWithFixes(unittest.IsolatedAsyncioTestCase):
    """007810 BUY 2026-09-28 사례: fix_delete=326,330 & fix_update=317:8로 DB 합계를
    19주→8주로 보정하는 적용 흐름을 검증한다([AT] feat/reconcile-manual)."""

    def setUp(self):
        self.existing_rows = {
            317: {"symbol": "007810", "side": "BUY", "qty": 11},
            326: {"symbol": "007810", "side": "BUY", "qty": 3},
            330: {"symbol": "007810", "side": "BUY", "qty": 5},
        }

    async def test_fix_delete_and_update_write_matching_audit_rows_and_net_position(self):
        ts = datetime(2026, 9, 28, 9, 32, 0)
        plan = _fix_plan_with(
            fix_deletes=[
                {"trade_history_id": 326, "symbol": "007810", "side": "BUY", "date": "20260928",
                 "old_quantity": 3, "old_price": 1000.0, "old_amount": 3000.0, "old_pnl": None,
                 "old_ts": ts},
                {"trade_history_id": 330, "symbol": "007810", "side": "BUY", "date": "20260928",
                 "old_quantity": 5, "old_price": 1000.0, "old_amount": 5000.0, "old_pnl": None,
                 "old_ts": ts},
            ],
            fix_updates=[
                {"trade_history_id": 317, "symbol": "007810", "side": "BUY", "date": "20260928",
                 "old_quantity": 11, "new_quantity": 8, "old_amount": 11000.0, "new_amount": 8000.0,
                 "old_pnl": None, "new_pnl": None},
            ],
        )
        db = _FakeReconcileDB(existing_rows=self.existing_rows)
        result = await kr.apply_reconcile_plan(plan, db)

        self.assertEqual(result["deleted"], 2)
        self.assertEqual(result["updated"], 1)
        self.assertEqual(sorted(db.deleted_ids), [326, 330])
        self.assertEqual(db.updates[0]["new_quantity"], 8)

        delete_audits = [a for a in db.audits if a["action"] == "DELETE"]
        self.assertEqual(len(delete_audits), 2)
        for a in delete_audits:
            self.assertEqual(a["symbol"], "007810")
            self.assertEqual(a["side"], "BUY")
            self.assertEqual(a["old_price"], 1000.0)
            self.assertEqual(a["old_ts"], ts)
        update_audits = [a for a in db.audits if a["action"] == "UPDATE"]
        self.assertEqual(len(update_audits), 1)
        self.assertEqual(update_audits[0]["new_quantity"], 8)

        # 보정 후 DB 순포지션: 8(317 수정) + 0(326,330 삭제) = 8주 (KIS 8주와 일치)
        self.assertEqual(result["net_positions"], {"007810": 8})

    async def test_fix_delete_rejects_are_not_this_functions_job(self):
        """build_fix_plan이 이미 안전조건을 검사해 통과시킨 plan만 apply_reconcile_plan에
        들어온다 — 여기서는 주어진 fix_deletes/fix_updates를 그대로 실행만 한다."""
        plan = _fix_plan_with(fix_deletes=[
            {"trade_history_id": 999, "symbol": "999999", "side": "BUY", "date": "20260928",
             "old_quantity": 1, "old_price": 1.0, "old_amount": 1.0, "old_pnl": None, "old_ts": None},
        ])
        db = _FakeReconcileDB()
        result = await kr.apply_reconcile_plan(plan, db)
        self.assertEqual(result["deleted"], 1)
        self.assertEqual(db.deleted_ids, [999])

    async def test_fix_mid_failure_rolls_back_deletes_and_updates(self):
        """fix_updates 중간에 실패하면 그 전에 실행된 fix_deletes까지 전부 롤백되어야
        한다(트랜잭션 전체 롤백)."""
        plan = _fix_plan_with(
            fix_deletes=[
                {"trade_history_id": 326, "symbol": "007810", "side": "BUY", "date": "20260928",
                 "old_quantity": 3, "old_price": 1000.0, "old_amount": 3000.0, "old_pnl": None,
                 "old_ts": None},
            ],
            fix_updates=[
                {"trade_history_id": 317, "symbol": "007810", "side": "BUY", "date": "20260928",
                 "old_quantity": 11, "new_quantity": 8, "old_amount": 11000.0, "new_amount": 8000.0,
                 "old_pnl": None, "new_pnl": None},
            ],
        )
        db = _FakeReconcileDB(fail_update_index=0, existing_rows=self.existing_rows)
        with self.assertRaises(RuntimeError):
            await kr.apply_reconcile_plan(plan, db)
        self.assertEqual(db.deleted_ids, [], "실패 시 먼저 실행된 DELETE까지 롤백되어야 함")
        self.assertEqual(db.audits, [], "실패 시 DELETE 감사 행도 남지 않아야 함")
        self.assertIn(326, db.existing_rows, "롤백되면 삭제된 행이 되돌아와 있어야 함(시뮬레이션)")

    async def test_trade_journal_is_never_touched(self):
        """지정 보정은 trade_journal을 건드리지 않는다([PM 지시]) — 페이크 db 인터페이스에
        trade_journal 관련 메서드가 전혀 없어도 적용이 끝까지 성공하는 것으로 그 사실을
        보장한다(인터페이스 자체가 trade_history/reconcile_audit 범위로만 좁혀져 있음)."""
        plan = _fix_plan_with(fix_deletes=[
            {"trade_history_id": 326, "symbol": "007810", "side": "BUY", "date": "20260928",
             "old_quantity": 3, "old_price": 1000.0, "old_amount": 3000.0, "old_pnl": None,
             "old_ts": None},
        ])
        db = _FakeReconcileDB(existing_rows=self.existing_rows)
        self.assertFalse(hasattr(db, "trade_journal"))
        result = await kr.apply_reconcile_plan(plan, db)
        self.assertEqual(result["deleted"], 1)


if __name__ == "__main__":
    unittest.main()
