"""
[AT] feat/kis-history-reconcile (PM 승인)

KIS 체결내역(inquire-daily-ccld) ↔ trade_history 기간 대조 — 읽기 전용.

배경: trade_history에 기록 누락이 확인됨(10/1 14:03 LG디스플레이(034220) 5주 매수 등).
이 모듈은 KIS 실제 체결내역과 DB trade_history를 (종목, 방향, KST 날짜) 기준으로
수량 합계를 비교해 missing_in_db / extra_in_db / qty_mismatch를 찾아낸다.

HTTP/DB 접근은 이 모듈에 없다 — 전부 호출부(dashboard/main.py)가 주입하는 콜러블을
통해서만 이뤄지므로(PageFetcher), 테스트가 실제 KIS/DB 없이도 대조 로직만 검증할 수 있다.
DB에는 쓰지 않는다(INSERT/UPDATE/DELETE 없음).
"""
import asyncio
import logging
from datetime import date, timedelta
from typing import Any, Awaitable, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# 하루치 체결내역이 끝없이 이어질 리 없으므로, 비정상 응답(ctx_area가 계속 "더 있음"으로
# 돌아오는 경우)에 무한루프로 빠지지 않도록 안전장치를 둔다.
MAX_PAGES_PER_DAY = 20

# KIS 속도제한(EGW00201) 재시도 횟수/간격 — dashboard/main.py의 기존 _kis_quote_get과
# 동일한 재시도 횟수를 쓴다(간격은 호출부에서 주입하는 fetch_page가 처리).
RATE_LIMIT_RETRIES = 2

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
