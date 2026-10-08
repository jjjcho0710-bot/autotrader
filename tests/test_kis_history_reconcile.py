"""
[AT] feat/kis-history-reconcile 회귀 테스트.

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
"""
import sys
import unittest
from datetime import date
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


if __name__ == "__main__":
    unittest.main()
