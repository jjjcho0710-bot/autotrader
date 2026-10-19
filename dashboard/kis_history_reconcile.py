"""
[AT] feat/kis-history-reconcile / feat/reconcile-apply (PM 승인)

KIS 체결내역(inquire-daily-ccld) ↔ trade_history 기간 대조 + 보정(apply) 계획 수립.

배경: trade_history에 기록 누락이 확인됨(10/1 14:03 LG디스플레이(034220) 5주 매수 등).
이 모듈은 KIS 실제 체결내역과 DB trade_history를 (종목, 방향, KST 날짜) 기준으로
수량 합계를 비교해 missing_in_db / extra_in_db / qty_mismatch를 찾아내고(reconcile),
그중 보정 가능 구간 안의 항목만 추려 실제로 적용할 보정 계획(build_reconcile_plan)과
그 적용(apply_reconcile_plan)까지 다룬다.

HTTP/DB 접근은 이 모듈에 없다 — 전부 호출부(dashboard/main.py)가 주입하는 콜러블/객체를
통해서만 이뤄지므로(PageFetcher, apply_reconcile_plan의 db 어댑터), 테스트가 실제
KIS/DB 없이도 로직만 검증할 수 있다. reconcile()/build_reconcile_plan()/
compute_plan_hash()는 순수 함수로 아무것도 쓰지 않는다 — 실제 INSERT/UPDATE는
apply_reconcile_plan()이 주입된 db를 통해서만 수행한다.
"""
import asyncio
import hashlib
import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# 보정 가능 구간의 시작일 — 9/28 이전은 KIS 모의투자 조회가 신뢰되지 않아 보정 대상이
# 아니다(PM 지시). 이 날짜 이전 불일치는 reconcile()에는 나오지만 보정 계획에는 포함하지
# 않는다.
RECONCILE_WINDOW_START = date(2026, 9, 28)

# 보정으로 새로 INSERT하는 행에 쓰는 strategy 값 — 중복 적용 방지(exists_backfill 판단
# 기준)와 "이 행은 자동매매가 아니라 사후 보정으로 생겼다"는 표시를 겸한다.
RECONCILE_STRATEGY = "RECONCILE_BACKFILL"

# 하루치 체결내역이 끝없이 이어질 리 없으므로, 비정상 응답(ctx_area가 계속 "더 있음"으로
# 돌아오는 경우)에 무한루프로 빠지지 않도록 안전장치를 둔다.
MAX_PAGES_PER_DAY = 20

# KIS 속도제한(EGW00201) 재시도 횟수/간격 — dashboard/main.py의 기존 _kis_quote_get과
# 동일한 재시도 횟수를 쓴다(간격은 호출부에서 주입하는 fetch_page가 처리).
RATE_LIMIT_RETRIES = 2

# fix_delete로 한 번에 지정 삭제할 수 있는 최대 건수([AT] feat/reconcile-manual, PM 지시) —
# 수동확인 보정은 소수 건을 신중히 지정하는 용도이므로, 대량 삭제 요청은 잘못된 범위
# 지정의 신호로 보고 전체 거부한다.
MAX_FIX_DELETE_COUNT = 5

PageFetcher = Callable[[str, str, str, str], Awaitable[Dict[str, Any]]]
# fetch_page(date_str, ctx_area_fk100, ctx_area_nk100, tr_cont_request) -> KIS 응답 dict.
# 응답 dict는 KIS JSON 바디 그대로에, 연속조회 여부 판단을 위한 응답 헤더 tr_cont 값을
# "_resp_tr_cont" 키로 얹어서 돌려줘야 한다(호출부 책임).


def _parse_execution_row(row: dict, date_str: str) -> Optional[dict]:
    """KIS output1 1행을 파싱. 체결수량이 0(미체결·거부)인 주문은 None을 반환해 걸러낸다."""
    filled_qty = int(row.get("tot_ccld_qty", 0) or 0)
    if filled_qty <= 0:
        return None
    side_cd = row.get("sll_buy_dvsn_cd")
    if side_cd == "01":
        side = "SELL"
    elif side_cd == "02":
        side = "BUY"
    else:
        logger.warning("체결내역 대조: 알 수 없는 매도매수구분코드 무시 — %r", side_cd)
        return None
    symbol = row.get("pdno")
    if not symbol:
        return None
    return {
        "symbol": symbol,
        "side": side,
        "qty": filled_qty,
        "avg_price": float(row.get("avg_prvs", 0) or 0),
        "odno": row.get("odno"),
        "date": date_str,
        "time_kst": str(row.get("ord_tmd", "") or "").zfill(6),
    }


async def fetch_day_executions(fetch_page: PageFetcher, date_str: str) -> dict:
    """하루치(date_str, YYYYMMDD) 체결내역을 CTX_AREA 연속조회가 끝날 때까지 페이지네이션한다.

    반환: {"rows": [...], "pages": n, "truncated": bool}
    truncated=True면 MAX_PAGES_PER_DAY에 걸려 중단한 것(정상 상황이 아님 — 호출부가
    unavailable_days에 반영해야 한다)."""
    rows: List[dict] = []
    ctx_fk100, ctx_nk100, tr_cont = "", "", ""
    for page in range(MAX_PAGES_PER_DAY):
        data = await fetch_page(date_str, ctx_fk100, ctx_nk100, tr_cont)
        if data.get("rt_cd") != "0":
            raise RuntimeError(
                f"KIS 체결내역 조회 실패 [{date_str}]: msg_cd={data.get('msg_cd')} msg1={data.get('msg1')}"
            )
        for row in (data.get("output1") or []):
            parsed = _parse_execution_row(row, date_str)
            if parsed:
                rows.append(parsed)
        if data.get("_resp_tr_cont") != "M":
            return {"rows": rows, "pages": page + 1, "truncated": False}
        ctx_fk100 = data.get("ctx_area_fk100", "")
        ctx_nk100 = data.get("ctx_area_nk100", "")
        tr_cont = "N"
    logger.warning("체결내역 대조: [%s] 페이지 상한(%d) 초과 — 중단", date_str, MAX_PAGES_PER_DAY)
    return {"rows": rows, "pages": MAX_PAGES_PER_DAY, "truncated": True}


async def fetch_kis_executions(
    fetch_page: PageFetcher,
    start: date,
    end: date,
    sleep_fn: Optional[Callable[[], Awaitable[None]]] = None,
) -> dict:
    """start~end(포함, KST 날짜) 구간의 체결내역을 하루 단위로 조회한다.

    모의투자 inquire-daily-ccld가 기간 조회에 제한이 있을 수 있어([PM 지시]) 멀티데이
    범위 쿼리 대신 항상 하루씩 끊어 호출한다 — 날짜별로 하나의 호출(및 그 페이지네이션)이
    실패해도 다른 날짜 조회는 계속 진행하고, 실패한 날짜는 unavailable_days에 기록한다.
    sleep_fn은 호출 사이 간격(속도제한 방지)을 주입받기 위한 것 — 생략 시 대기 없음."""
    all_rows: List[dict] = []
    unavailable_days: List[dict] = []
    day = start
    while day <= end:
        date_str = day.strftime("%Y%m%d")
        try:
            result = await fetch_day_executions(fetch_page, date_str)
            all_rows.extend(result["rows"])
            if result["truncated"]:
                unavailable_days.append({"date": date_str, "reason": "페이지 상한 초과(비정상 응답 의심)"})
        except Exception as e:
            unavailable_days.append({"date": date_str, "reason": str(e)})
        day += timedelta(days=1)
        if sleep_fn and day <= end:
            await sleep_fn()
    return {"rows": all_rows, "unavailable_days": unavailable_days}


def _agg_key(symbol: str, side: str, date_str: str) -> tuple:
    return (symbol, side, date_str)


def reconcile(kis_rows: List[dict], db_rows: List[dict]) -> dict:
    """KIS 체결내역과 trade_history 행을 (종목, 방향, KST 날짜) 키로 수량 합계 비교.

    kis_rows: fetch_kis_executions()가 반환하는 rows 형식 — symbol/side/qty/date 필수.
    db_rows: [{"id":, "symbol":, "side": "BUY"|"SELL", "qty": float, "date": "YYYYMMDD"}, ...]

    반환: {"missing_in_db": [...], "extra_in_db": [...], "qty_mismatch": [...]}
    - missing_in_db: KIS에는 있고 DB에는 없음 (종목·방향·수량 합계 + 개별 체결 내역)
    - extra_in_db: DB에는 있고 KIS에는 없음
    - qty_mismatch: 양쪽에 있으나 수량 합계가 다름
    """
    kis_by_key: Dict[tuple, dict] = {}
    for r in kis_rows:
        k = _agg_key(r["symbol"], r["side"], r["date"])
        agg = kis_by_key.setdefault(k, {"qty": 0, "executions": []})
        agg["qty"] += r["qty"]
        agg["executions"].append(r)

    db_by_key: Dict[tuple, dict] = {}
    for r in db_rows:
        k = _agg_key(r["symbol"], r["side"], r["date"])
        agg = db_by_key.setdefault(k, {"qty": 0, "db_rows": []})
        agg["qty"] += r["qty"]
        agg["db_rows"].append(r)

    missing_in_db: List[dict] = []
    extra_in_db: List[dict] = []
    qty_mismatch: List[dict] = []

    for k in sorted(set(kis_by_key) | set(db_by_key)):
        symbol, side, date_str = k
        kis_agg = kis_by_key.get(k)
        db_agg = db_by_key.get(k)
        if kis_agg and not db_agg:
            missing_in_db.append({
                "symbol": symbol, "side": side, "date": date_str,
                "qty": kis_agg["qty"], "executions": kis_agg["executions"],
            })
        elif db_agg and not kis_agg:
            extra_in_db.append({
                "symbol": symbol, "side": side, "date": date_str,
                "qty": db_agg["qty"], "db_rows": db_agg["db_rows"],
            })
        elif kis_agg["qty"] != db_agg["qty"]:
            qty_mismatch.append({
                "symbol": symbol, "side": side, "date": date_str,
                "kis_qty": kis_agg["qty"], "db_qty": db_agg["qty"],
                # build_reconcile_plan()이 UPDATE 대상(db_rows 1개)과 KIS 기준 새 금액을
                # 계산하는 데 쓴다 — reconcile() 자체는 여전히 비교만 하고 아무것도 쓰지 않음.
                "kis_executions": kis_agg["executions"], "db_rows": db_agg["db_rows"],
            })

    return {
        "missing_in_db": missing_in_db,
        "extra_in_db": extra_in_db,
        "qty_mismatch": qty_mismatch,
    }


def check_position_consistency(kis_rows: List[dict], real_positions: Dict[str, dict]) -> dict:
    """조회 구간의 KIS 체결내역을 반영했을 때 나오는 종목별 순증감과, 실제 KIS 잔고
    (real_positions: {symbol: {"qty":, "avg_price":}})를 비교한다.

    주의: 조회 시작일 이전부터 보유 중이던 포지션이 있으면 이 비교는 "조회 구간 내
    순증감"과 "전체 보유수량"을 비교하는 것이라 일치하지 않을 수 있다 — 참고용 신호로만
    쓰고, note로 그 가정을 명시한다."""
    computed_change: Dict[str, int] = {}
    for r in kis_rows:
        sign = 1 if r["side"] == "BUY" else -1
        computed_change[r["symbol"]] = computed_change.get(r["symbol"], 0) + sign * r["qty"]

    findings = []
    for symbol in sorted(set(computed_change) | set(real_positions)):
        change = computed_change.get(symbol, 0)
        real = real_positions.get(symbol, {})
        real_qty = real.get("qty", 0)
        findings.append({
            "symbol": symbol,
            "period_net_change": change,
            "real_qty": real_qty,
            "real_avg_price": real.get("avg_price"),
        })
    return {
        "findings": findings,
        "note": "period_net_change는 조회 구간 내 KIS 체결 순증감이며, 조회 시작일 이전부터 "
                "보유 중이던 포지션이 있으면 real_qty와 다를 수 있음(참고용).",
    }


def build_summary_text(start: date, end: date, reconcile_result: dict, unavailable_days: List[dict]) -> str:
    """텔레그램 개인방 요약 텍스트 생성."""
    missing = reconcile_result["missing_in_db"]
    extra = reconcile_result["extra_in_db"]
    mismatch = reconcile_result["qty_mismatch"]
    lines = [
        f"🔍 KIS 체결내역 ↔ trade_history 대조 ({start.strftime('%Y-%m-%d')}~{end.strftime('%Y-%m-%d')})",
        f"missing_in_db {len(missing)}건 / extra_in_db {len(extra)}건 / qty_mismatch {len(mismatch)}건",
    ]
    for item in missing[:10]:
        lines.append(f"  ❗ DB 누락: {item['symbol']} {item['side']} {item['qty']}주 ({item['date']})")
    for item in mismatch[:10]:
        lines.append(
            f"  ⚠️ 수량 불일치: {item['symbol']} {item['side']} KIS={item['kis_qty']} DB={item['db_qty']} ({item['date']})"
        )
    for item in extra[:10]:
        lines.append(f"  ❓ DB에만 있음: {item['symbol']} {item['side']} {item['qty']}주 ({item['date']})")
    if unavailable_days:
        lines.append(f"⚠️ 조회 실패/제한된 날짜 {len(unavailable_days)}건: " +
                     ", ".join(d["date"] for d in unavailable_days[:15]))
    if not (missing or extra or mismatch):
        lines.append("✅ 불일치 없음")
    return "\n".join(lines)


def _yyyymmdd_to_date(date_str: str) -> date:
    return datetime.strptime(date_str, "%Y%m%d").date()


def validate_apply_window(start: date, end: date, today_kst: date) -> Optional[str]:
    """보정을 실제로 적용(apply)해도 되는 날짜 구간인지 검증한다. 문제가 있으면 에러
    메시지를, 없으면 None을 반환한다.

    보정 가능 구간: RECONCILE_WINDOW_START(2026-09-28) <= start <= end <= 어제(KST).
    오늘은 체결이 진행 중일 수 있어 제외한다(PM 지시). 드라이런(대조 자체)에는 이
    제약이 없다 — build_reconcile_plan()이 보정 가능 구간 밖 항목을 계획에서 그냥
    제외하므로, 이 검증은 apply=1일 때만 호출부(dashboard/main.py)가 사용한다."""
    yesterday = today_kst - timedelta(days=1)
    if start < RECONCILE_WINDOW_START:
        return (f"보정 시작일은 {RECONCILE_WINDOW_START.isoformat()} 이후여야 합니다 "
                "(그 이전 KIS 모의투자 조회는 신뢰되지 않아 보정 대상이 아님)")
    if end > yesterday:
        return "보정 종료일은 어제(KST)까지만 가능합니다(오늘은 체결이 진행 중일 수 있음)"
    if start > end:
        return "start가 end보다 늦을 수 없음"
    return None


def build_reconcile_plan(reconcile_result: dict, window_start: date, window_end: date) -> dict:
    """reconcile() 결과에서 [window_start, window_end](포함, KST) 구간 안의 항목만 추려
    실제로 적용할 보정 계획을 만든다 — 아무것도 쓰지 않는 순수 함수.

    - missing_in_db: 각 개별 KIS 체결(odno 단위)을 그대로 하나의 INSERT로 — 집계행이
      아니라 체결 시각·단가가 서로 다를 수 있는 개별 체결 그대로 기록해야 한다.
    - qty_mismatch: 대조된 DB 행이 정확히 1개일 때만 UPDATE 대상. 0개 또는 2개 이상이면
      자동 보정하지 않고 manual_review로 분류한다(수동 확인 필요).
    - extra_in_db: 자동으로 삭제하지 않는다([AT] feat/reconcile-manual) — KIS 체결이
      전혀 없는 DB 전용 항목이라 자동 보정 대상이 아니지만, 수동확인(manual_review)에는
      포함해 KIS 체결 없음 + DB 행 상세를 그대로 보여준다. 실제 삭제는 호출부가
      fix_delete로 명시 지정해야만 일어난다(build_fix_plan).

    manual_review 각 항목은 build_plan_summary_text가 KIS 체결 각 건(시각/수량/체결가/
    odno)과 DB 후보 행 각 건(id/ts/수량/가격/금액/strategy)을 모두 표시할 수 있도록
    "kis_executions"/"db_rows" 원본을 그대로 들고 있다.
    """
    def _in_window(date_str: str) -> bool:
        return window_start <= _yyyymmdd_to_date(date_str) <= window_end

    inserts: List[dict] = []
    for item in reconcile_result["missing_in_db"]:
        if not _in_window(item["date"]):
            continue
        for ex in item["executions"]:
            qty = ex["qty"]
            price = ex.get("avg_price", 0) or 0
            inserts.append({
                "symbol": item["symbol"], "side": item["side"], "date": item["date"],
                "quantity": qty, "price": price, "amount": round(price * qty, 2),
                "time_kst": ex.get("time_kst", "") or "000000", "odno": ex.get("odno"),
                "strategy": RECONCILE_STRATEGY,
            })

    updates: List[dict] = []
    manual_review: List[dict] = []
    for item in reconcile_result["qty_mismatch"]:
        if not _in_window(item["date"]):
            continue
        db_rows = item.get("db_rows") or []
        if len(db_rows) != 1:
            manual_review.append({
                "symbol": item["symbol"], "side": item["side"], "date": item["date"],
                "kis_qty": item["kis_qty"], "db_qty": item["db_qty"],
                "db_row_ids": [r["id"] for r in db_rows],
                "reason": ("DB 행이 2개 이상이라 자동 보정 대상 아님 — 수동 확인 필요"
                           if len(db_rows) > 1 else "대조된 DB 행을 찾지 못함 — 수동 확인 필요"),
                "kis_executions": item.get("kis_executions") or [],
                "db_rows": db_rows,
            })
            continue
        row = db_rows[0]
        old_qty = row["qty"]
        new_qty = item["kis_qty"]
        old_amount = row.get("amount")
        new_amount = round(
            sum((ex.get("avg_price", 0) or 0) * ex["qty"] for ex in item.get("kis_executions") or []), 2)
        old_pnl = row.get("pnl")
        new_pnl = round(old_pnl * (new_qty / old_qty), 2) if (old_pnl is not None and old_qty) else old_pnl
        old_price = row.get("price")
        # amount는 KIS 체결 가중평균(여러 체결가가 섞여 있을 수 있음)으로 다시 계산하면서
        # price(단가) 컬럼은 갱신하지 않아 price*quantity != amount가 되는 불일치가 있었다
        # ([AT] fix/reconcile-price-consistency) — amount/new_qty로 가중평균 단가를 역산해
        # 같이 갱신한다.
        new_price = round(new_amount / new_qty, 2) if new_qty else old_price
        updates.append({
            "trade_history_id": row["id"], "symbol": item["symbol"], "side": item["side"],
            "date": item["date"], "old_quantity": old_qty, "new_quantity": new_qty,
            "old_amount": old_amount, "new_amount": new_amount,
            "old_price": old_price, "new_price": new_price,
            "old_pnl": old_pnl, "new_pnl": new_pnl,
        })

    for item in reconcile_result.get("extra_in_db", []):
        if not _in_window(item["date"]):
            continue
        db_rows = item.get("db_rows") or []
        manual_review.append({
            "symbol": item["symbol"], "side": item["side"], "date": item["date"],
            "kis_qty": 0, "db_qty": item["qty"],
            "db_row_ids": [r["id"] for r in db_rows],
            "reason": "KIS 체결 없음 — DB에만 존재(수동 확인 필요)",
            "kis_executions": [],
            "db_rows": db_rows,
        })

    return {
        "window": {"start": window_start.isoformat(), "end": window_end.isoformat()},
        "inserts": inserts, "updates": updates, "manual_review": manual_review,
        "fix_deletes": [], "fix_updates": [],
    }


def compute_plan_hash(plan: dict) -> str:
    """보정 계획 내용 전체의 SHA-256 해시 — apply=1 호출의 confirm 파라미터가 이 값과
    정확히 일치해야만 실제로 쓴다(PM 지시: 새로고침해도 두 번 적용되지 않게). 계획이
    조금이라도 바뀌면(KIS/DB 최신 상태가 달라졌으면) 해시도 달라져 거절된다."""
    canonical = json.dumps(plan, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _fmt_time_kst(time_kst: str) -> str:
    t = str(time_kst or "000000").zfill(6)
    return f"{t[0:2]}:{t[2:4]}:{t[4:6]}"


def _fmt_kis_execution_line(ex: dict) -> str:
    """수동확인 항목의 KIS 체결 1건 표시 — 시각(KST)/수량/체결평균가/주문번호([AT]
    feat/reconcile-manual)."""
    price = ex.get("avg_price", 0) or 0
    return (f"KIS 체결 {_fmt_time_kst(ex.get('time_kst'))} {ex.get('qty')}주 "
            f"@{price:.0f}원 (odno={ex.get('odno')})")


def _fmt_db_row_line(row: dict) -> str:
    """수동확인 항목의 DB 후보 행 1건 표시 — id/ts(KST)/quantity/price/amount/
    strategy([AT] feat/reconcile-manual)."""
    ts = row.get("ts_kst")
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S") if ts else "?"
    price = row.get("price")
    price_str = f"{price:.0f}" if price is not None else "?"
    amount = row.get("amount")
    amount_str = f"{amount:.0f}" if amount is not None else "?"
    return (f"DB id={row.get('id')} ts={ts_str} 수량={row.get('qty')} "
            f"가격={price_str} 금액={amount_str} strategy={row.get('strategy')}")


def build_plan_summary_text(plan: dict, plan_hash: str) -> str:
    """보정 계획(드라이런) 텔레그램 개인방 요약 텍스트.

    manual_review는 각 항목의 KIS 체결 각 건과 DB 후보 행 각 건을 모두 풀어서 보여준다
    (수동확인 시 지정 보정(fix_delete/fix_update) 대상을 고르는 데 필요한 정보 — [AT]
    feat/reconcile-manual). fix_deletes/fix_updates는 fix_delete/fix_update 파라미터로
    지정한 보정이 있을 때만 표시된다."""
    inserts, updates, manual = plan["inserts"], plan["updates"], plan["manual_review"]
    fix_deletes = plan.get("fix_deletes") or []
    fix_updates = plan.get("fix_updates") or []
    lines = [
        f"🛠 보정 계획 ({plan['window']['start']}~{plan['window']['end']})",
        f"INSERT {len(inserts)}건 / UPDATE {len(updates)}건 / 수동확인 {len(manual)}건",
        f"plan_hash={plan_hash}",
    ]
    for ins in inserts[:10]:
        lines.append(f"  ➕ {ins['symbol']} {ins['side']} {ins['quantity']}주 @{ins['price']:.0f}원 "
                     f"({ins['date']} {ins['time_kst']})")
    for upd in updates[:10]:
        old_price, new_price = upd.get("old_price"), upd.get("new_price")
        price_part = (
            f" 단가 {old_price:,.0f}→{new_price:,.0f}원"
            if old_price is not None and new_price is not None and old_price != new_price else ""
        )
        lines.append(f"  ✏️ {upd['symbol']} {upd['side']} 수량 {upd['old_quantity']}→{upd['new_quantity']}"
                     f"{price_part} ({upd['date']})")
    for mr in manual[:10]:
        lines.append(f"  🙋 {mr['symbol']} {mr['side']} ({mr['date']}) — {mr['reason']}")
        kis_executions = mr.get("kis_executions") or []
        if kis_executions:
            for ex in kis_executions:
                lines.append(f"     {_fmt_kis_execution_line(ex)}")
        else:
            lines.append("     KIS 체결 없음")
        for row in mr.get("db_rows") or []:
            lines.append(f"     {_fmt_db_row_line(row)}")
    if fix_deletes or fix_updates:
        lines.append(f"지정 보정: fix_delete {len(fix_deletes)}건 / fix_update {len(fix_updates)}건")
        for d in fix_deletes:
            lines.append(f"  🗑 id={d['trade_history_id']} {d['symbol']} {d['side']} "
                         f"{d['old_quantity']}주 삭제 ({d['date']})")
        for u in fix_updates:
            lines.append(f"  ✏️ id={u['trade_history_id']} {u['symbol']} {u['side']} 수량 "
                         f"{u['old_quantity']}→{u['new_quantity']} ({u['date']})")
    if not (inserts or updates or manual):
        lines.append("✅ 보정 대상 없음")
    return "\n".join(lines)


def build_fix_preview_text(preview: List[dict]) -> str:
    """fix_delete/fix_update를 적용했다고 가정했을 때(드라이런) 영향받는 (종목,방향,날짜)별
    보정 후 DB 합계 vs KIS 수량 비교 텍스트([AT] feat/reconcile-manual)."""
    if not preview:
        return ""
    lines = ["🔎 지정 보정 미리보기(적용 전) — 보정 후 DB 합계 vs KIS 수량"]
    for p in preview:
        mark = "✅" if p["matches_after_fix"] else "⚠️"
        lines.append(
            f"  {mark} {p['symbol']} {p['side']} ({p['date']}) DB {p['old_db_qty']}→{p['new_db_qty']} "
            f"vs KIS {p['kis_qty']}"
        )
    return "\n".join(lines)


def build_apply_result_text(result: dict) -> str:
    """보정 적용(apply=1) 결과 텔레그램 개인방 요약 텍스트."""
    text = (f"✅ 보정 적용 완료 — INSERT {result['inserted']}건 / UPDATE {result['updated']}건 / "
            f"DELETE {result.get('deleted', 0)}건 / 건너뜀(중복) {result['skipped']}건 / "
            f"수동확인 {result['manual_review']}건(미적용)")
    net_positions = result.get("net_positions") or {}
    for symbol, qty in net_positions.items():
        text += f"\n  📊 {symbol} 보정 후 DB 순포지션: {qty}주"
    return text


# ── [AT] feat/reconcile-manual — 지정 보정(fix_delete/fix_update) ──────────────


def parse_fix_delete_param(raw: Optional[str]) -> List[int]:
    """쿼리 파라미터 fix_delete=<id,id,...>를 파싱한다. 비었거나 None이면 빈 리스트."""
    if not raw or not raw.strip():
        return []
    try:
        return [int(x) for x in raw.split(",") if x.strip() != ""]
    except ValueError:
        raise ValueError(f"fix_delete 형식 오류: {raw!r} (id,id,... 형식이어야 함)")


def parse_fix_update_param(raw: Optional[str]) -> Dict[int, float]:
    """쿼리 파라미터 fix_update=<id:수량,id:수량,...>를 파싱한다. 비었거나 None이면 빈 dict."""
    if not raw or not raw.strip():
        return {}
    result: Dict[int, float] = {}
    try:
        for part in raw.split(","):
            if not part.strip():
                continue
            id_str, qty_str = part.split(":")
            result[int(id_str)] = float(qty_str)
    except ValueError:
        raise ValueError(f"fix_update 형식 오류: {raw!r} (id:수량,id:수량,... 형식이어야 함)")
    return result


def aggregate_kis_qty_by_key(kis_rows: List[dict]) -> Dict[tuple, float]:
    """KIS 체결행을 reconcile()과 동일한 (종목,방향,날짜) 키로 수량 합산 — build_fix_preview가
    지정 보정 후 DB 합계와 비교할 KIS 기준값을 얻는 데 쓴다."""
    agg: Dict[tuple, float] = {}
    for r in kis_rows:
        k = _agg_key(r["symbol"], r["side"], r["date"])
        agg[k] = agg.get(k, 0) + r["qty"]
    return agg


def build_fix_plan(delete_ids: List[int], update_map: Dict[int, float],
                    candidate_rows: Dict[int, dict],
                    window_start: date, window_end: date) -> dict:
    """fix_delete/fix_update로 지정된 id들에 대한 보정 계획을 만든다 — 아무것도 쓰지 않는
    순수 함수([AT] feat/reconcile-manual).

    candidate_rows: {id: {"symbol","side","date","qty","price","amount","pnl","ts"}} —
    reconcile()/build_reconcile_plan()의 db_rows와 같은 필드명("qty")을 쓴다. 호출부가
    보정 가능 구간(window_start~window_end) 안의 trade_history 행만 추려 넘겨야 한다.
    이 dict에 없는 id는 "존재하지 않거나 윈도우 밖"으로 취급해 전체 거부한다.

    안전 조건(하나라도 위반하면 전체 거부 — 부분 적용 없음):
    - delete_ids/update_map의 모든 id가 candidate_rows(윈도우 내 trade_history)에 있어야 함
    - DELETE 대상은 side == "BUY"인 행만 허용
    - 한 번에 삭제 가능한 건수는 MAX_FIX_DELETE_COUNT(5)건까지
    - 같은 id를 삭제와 수정에 동시에 지정할 수 없음

    반환: 문제 없으면 {"deletes": [...], "updates": [...], "error": None}
          문제 있으면 {"deletes": [], "updates": [], "error": "메시지"} (아무 것도 만들지 않음)
    """
    all_ids = list(delete_ids) + list(update_map.keys())
    missing = sorted({i for i in all_ids if i not in candidate_rows})
    if missing:
        return {"deletes": [], "updates": [], "error":
                f"지정한 id가 보정 가능 구간({window_start.isoformat()}~{window_end.isoformat()}) 내 "
                f"trade_history에 없음(존재하지 않거나 윈도우 밖): {missing}"}

    if len(delete_ids) > MAX_FIX_DELETE_COUNT:
        return {"deletes": [], "updates": [], "error":
                f"한 번에 삭제할 수 있는 건수는 최대 {MAX_FIX_DELETE_COUNT}건입니다 "
                f"(요청: {len(delete_ids)}건)"}

    non_buy = sorted(i for i in delete_ids if candidate_rows[i]["side"] != "BUY")
    if non_buy:
        return {"deletes": [], "updates": [], "error":
                f"BUY가 아닌 행은 삭제할 수 없음: {non_buy}"}

    overlap = sorted(set(delete_ids) & set(update_map.keys()))
    if overlap:
        return {"deletes": [], "updates": [], "error":
                f"같은 id를 삭제와 수정에 동시에 지정할 수 없음: {overlap}"}

    deletes: List[dict] = []
    for i in delete_ids:
        row = candidate_rows[i]
        deletes.append({
            "trade_history_id": i, "symbol": row["symbol"], "side": row["side"],
            "date": row["date"], "old_quantity": row["qty"], "old_price": row.get("price"),
            "old_amount": row.get("amount"), "old_pnl": row.get("pnl"), "old_ts": row.get("ts"),
        })

    updates: List[dict] = []
    for i in sorted(update_map):
        new_qty = update_map[i]
        row = candidate_rows[i]
        old_qty = row["qty"]
        price = row.get("price") or 0
        old_amount = row.get("amount")
        new_amount = round(price * new_qty, 2)
        old_pnl = row.get("pnl")
        new_pnl = round(old_pnl * (new_qty / old_qty), 2) if (old_pnl is not None and old_qty) else old_pnl
        updates.append({
            "trade_history_id": i, "symbol": row["symbol"], "side": row["side"],
            "date": row["date"], "old_quantity": old_qty, "new_quantity": new_qty,
            "old_amount": old_amount, "new_amount": new_amount,
            # fix_update는 수량만 운영자가 직접 지정하는 수동 보정이라 단가(price)는
            # 그대로 둔 채 금액만 그 단가로 재계산한다 — price*quantity=amount 일관성 유지
            # ([AT] fix/reconcile-price-consistency, update_trade()가 항상 new_price를
            # 받도록 통일).
            "old_price": row.get("price"), "new_price": row.get("price"),
            "old_pnl": old_pnl, "new_pnl": new_pnl,
        })

    return {"deletes": deletes, "updates": updates, "error": None}


def build_fix_preview(candidate_rows: Dict[int, dict], delete_ids: List[int],
                       update_map: Dict[int, float], kis_qty_by_key: Dict[tuple, float]) -> List[dict]:
    """fix_delete/fix_update를 적용했다고 가정했을 때 영향받는 (종목,방향,날짜) 키별로
    보정 후 DB 합계와 KIS 수량을 비교한 미리보기를 만든다 — 아무것도 쓰지 않는 순수
    함수([AT] feat/reconcile-manual, 드라이런용).

    candidate_rows는 같은 키에 속한 다른(지정되지 않은) 행도 포함해야 정확한 합계가
    나온다(예: 007810처럼 3개 DB 행 중 1개만 수정·나머지 삭제하는 경우)."""
    delete_set = set(delete_ids)
    affected_ids = delete_set | set(update_map.keys())
    affected_keys = {
        (candidate_rows[i]["symbol"], candidate_rows[i]["side"], candidate_rows[i]["date"])
        for i in affected_ids if i in candidate_rows
    }
    preview: List[dict] = []
    for key in sorted(affected_keys):
        symbol, side, date_str = key
        old_total = 0.0
        new_total = 0.0
        for rid, row in candidate_rows.items():
            if (row["symbol"], row["side"], row["date"]) != key:
                continue
            old_total += row["qty"]
            if rid in delete_set:
                continue
            new_total += update_map.get(rid, row["qty"])
        kis_qty = kis_qty_by_key.get(key, 0)
        preview.append({
            "symbol": symbol, "side": side, "date": date_str,
            "old_db_qty": old_total, "new_db_qty": new_total, "kis_qty": kis_qty,
            "matches_after_fix": new_total == kis_qty,
        })
    return preview


async def apply_reconcile_plan(plan: dict, db) -> dict:
    """build_reconcile_plan() 결과를 실제로 적용한다 — 이 함수는 asyncpg를 직접 쓰지
    않고 모두 db(호출부가 주입하는 어댑터)를 통해서만 수행하므로, 테스트는 인메모리
    페이크 db로 실제 DB 없이 적용 로직(중복 skip, 감사 기록, 트랜잭션 롤백)을 검증할
    수 있다.

    db가 제공해야 하는 인터페이스:
      - db.transaction() -> 비동기 컨텍스트 매니저(실패 시 전체 롤백)
      - await db.exists_backfill(symbol, side, date_str, quantity) -> bool
        (symbol/side/date/quantity가 같고 strategy=RECONCILE_BACKFILL인 행이 이미
        있으면 True — 같은 보정을 두 번 적용해도 중복 INSERT가 생기지 않게 하는 가드)
      - await db.insert_trade(insert_spec) -> 새로 생성된 trade_history.id
      - await db.update_trade(update_spec) -> None
      - await db.delete_trade(trade_history_id) -> None ([AT] feat/reconcile-manual —
        plan["fix_deletes"]가 있을 때만 쓰인다)
      - await db.net_position(symbol) -> float ([AT] feat/reconcile-manual — fix_deletes/
        fix_updates가 건드린 종목의 보정 후 DB 순포지션을 결과에 표시하는 데 쓴다)
      - await db.insert_audit(**fields) -> None (reconcile_audit에 한 행. action=DELETE는
        old_price/old_ts도 함께 넘겨 삭제된 행을 복원 가능하게 기록한다)

    트랜잭션 중간에 예외가 나면 db.transaction()이 전부 롤백해야 한다(이 함수는 그
    예외를 그대로 전파만 한다 — 호출부가 success:false로 보고).

    plan["fix_deletes"]/plan["fix_updates"]는 build_fix_plan()이 만든, fix_delete/
    fix_update로 사용자가 직접 지정한 보정이다([AT] feat/reconcile-manual) — plan["updates"]
    (qty_mismatch 자동 보정)와 구분해 별도로 처리하고 감사 note도 다르게 남긴다.
    연관 trade_journal은 건드리지 않는다."""
    inserted = updated = skipped = deleted = 0
    touched_symbols: List[str] = []
    async with db.transaction():
        for ins in plan["inserts"]:
            if await db.exists_backfill(ins["symbol"], ins["side"], ins["date"], ins["quantity"]):
                skipped += 1
                continue
            trade_id = await db.insert_trade(ins)
            await db.insert_audit(
                action="INSERT", trade_history_id=trade_id, symbol=ins["symbol"], side=ins["side"],
                trade_date=_yyyymmdd_to_date(ins["date"]),
                old_quantity=None, new_quantity=ins["quantity"],
                old_amount=None, new_amount=ins["amount"], old_pnl=None, new_pnl=None,
                note=f"KIS 체결 보정 INSERT (odno={ins.get('odno')})",
            )
            inserted += 1
        for upd in plan["updates"]:
            await db.update_trade(upd)
            await db.insert_audit(
                action="UPDATE", trade_history_id=upd["trade_history_id"], symbol=upd["symbol"],
                side=upd["side"], trade_date=_yyyymmdd_to_date(upd["date"]),
                old_quantity=upd["old_quantity"], new_quantity=upd["new_quantity"],
                old_amount=upd["old_amount"], new_amount=upd["new_amount"],
                old_pnl=upd["old_pnl"], new_pnl=upd["new_pnl"], old_price=upd.get("old_price"),
                note="KIS 체결 보정 UPDATE",
            )
            updated += 1
        for upd in plan.get("fix_updates") or []:
            await db.update_trade(upd)
            await db.insert_audit(
                action="UPDATE", trade_history_id=upd["trade_history_id"], symbol=upd["symbol"],
                side=upd["side"], trade_date=_yyyymmdd_to_date(upd["date"]),
                old_quantity=upd["old_quantity"], new_quantity=upd["new_quantity"],
                old_amount=upd["old_amount"], new_amount=upd["new_amount"],
                old_pnl=upd["old_pnl"], new_pnl=upd["new_pnl"], old_price=upd.get("old_price"),
                note="지정 보정 UPDATE(fix_update)",
            )
            updated += 1
            touched_symbols.append(upd["symbol"])
        for delt in plan.get("fix_deletes") or []:
            await db.delete_trade(delt["trade_history_id"])
            await db.insert_audit(
                action="DELETE", trade_history_id=delt["trade_history_id"], symbol=delt["symbol"],
                side=delt["side"], trade_date=_yyyymmdd_to_date(delt["date"]),
                old_quantity=delt["old_quantity"], new_quantity=None,
                old_amount=delt["old_amount"], new_amount=None,
                old_pnl=delt["old_pnl"], new_pnl=None,
                old_price=delt.get("old_price"), old_ts=delt.get("old_ts"),
                note="지정 보정 DELETE(fix_delete)",
            )
            deleted += 1
            touched_symbols.append(delt["symbol"])

        net_positions = {}
        for symbol in sorted(set(touched_symbols)):
            net_positions[symbol] = await db.net_position(symbol)

    return {
        "inserted": inserted, "updated": updated, "skipped": skipped, "deleted": deleted,
        "manual_review": len(plan["manual_review"]), "net_positions": net_positions,
    }
