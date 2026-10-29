"""
dashboard/health_report.py - [AT] feat/health-report (PM 승인)

매 거래일 자동 점검 리포트를 만들어 텔레그램으로 보낸다. 읽기 전용이다 — 매매
로직·주문·설정(strategy_config 등)은 이 모듈에서 절대 쓰지 않는다. 유일한 쓰기는
dashboard/main.py의 기존 코드에 한 줄씩 추가한 Redis 카운터(health:scan_timing:*,
health:token_reissue:*)뿐이고, 둘 다 매매 판단에는 영향을 주지 않는 관측용 값이다.

build_health_report()는 DB/Redis/KIS 접근을 직접 하지 않고 호출부(dashboard/main.py)가
주입하는 pool/redis/콜러블만 받는다(dashboard/kis_history_reconcile.py와 동일한 의존성
주입 방식) — 이 모듈만으로는 실제 DB/KIS 없이도 섹션별 로직을 테스트할 수 있다.

섹션마다 독립적으로 try/except로 감싸 격리한다: 한 섹션이 실패해도 나머지 섹션과
전체 리포트 생성·발송은 계속된다. 실패한 섹션은 "확인불가(사유)"로만 표시된다.
"""
import json
import logging
from datetime import date, datetime, time as dtime, timedelta
from typing import Any, Awaitable, Callable, Dict, List, Optional

from common.market_calendar import is_trading_day

logger = logging.getLogger(__name__)

# 차단(게이트) 사유 — trade_journal.jarvis_reason/signal_reason, stark_decisions.reason/
# rationale에서 이 문자열을 포함하는 오늘자 행 건수를 센다(stark/execution_guard.py가
# 차단 시 reason 문자열에 그대로 남기는 식별자와 동일).
GATE_REASONS = ("momentum_gate", "single_stock_cap", "size_below_one_share")

# 손절 체결 손익률이 이 값 이하면 "슬리피지 큼"으로 표시한다(stock_trader의 기존
# 손절 기준 -7%보다 더 나쁘게 체결됐다는 뜻).
SLIPPAGE_THRESHOLD_PCT = -7.0

# 서킷브레이커(당일 신규 매수 중단) 발동 기준 — stock_trader/main.py
# StockTrader._daily_stop_count()의 "오늘 손절 횟수 2회 이상" 기준과 동일하게 맞춘다.
CIRCUIT_BREAKER_THRESHOLD = 2

# 토큰 재발급 경보 기준(한 줄 요약 ⚠️ 판정용).
TOKEN_REISSUE_WARN_THRESHOLD = 3

# d) 포지션: 조회 실패로 폴백 스냅샷을 쓴 경우, 스냅샷이 이 분 수를 넘게 오래됐을 때만 경고한다
# ([AT] fix/health-report-accuracy — "stale + 0분 전" 자기모순 제거).
POSITION_SNAPSHOT_WARN_MIN = 5.0

# 같은 차단이 trade_journal과 stark_decisions에 각각 한 행씩 기록되므로(거의 같은 시각),
# 두 행을 한 건으로 합치는 시간 허용치(초).
GATE_DEDUP_WINDOW_SEC = 10.0

# j) 지수: dashboard/main.py 스케줄러가 market_index_daily를 저장하는 시간창(15:41~16:30).
# 이 창 안(또는 그 전)에 저장 기록이 없으면 "미실행"이 아니라 "수집 대기"로 표시한다.
MARKET_INDEX_COLLECT_START = dtime(15, 41)
MARKET_INDEX_COLLECT_END = dtime(16, 30)


def _previous_trading_day(d: date, max_lookback: int = 10) -> Optional[date]:
    """d 이전의 가장 가까운 거래일. max_lookback일 안에 못 찾으면 None(확인불가)."""
    cur = d - timedelta(days=1)
    for _ in range(max_lookback):
        if is_trading_day(cur):
            return cur
        cur -= timedelta(days=1)
    return None


async def _section_reconcile(reconcile_dry_run_fn: Callable[[date], Awaitable[dict]],
                              target_day: Optional[date]) -> Dict[str, Any]:
    """a) 전일 정산 dry-run. apply는 reconcile_dry_run_fn 쪽에서 절대 하지 않는다."""
    if target_day is None:
        return {"error": "확인불가(직전 거래일을 달력에서 못 찾음)"}
    result = await reconcile_dry_run_fn(target_day)
    mismatch = (
        len(result.get("missing_in_db") or []) if isinstance(result.get("missing_in_db"), list)
        else int(result.get("missing_in_db") or 0)
    )
    extra = (
        len(result.get("extra_in_db") or []) if isinstance(result.get("extra_in_db"), list)
        else int(result.get("extra_in_db") or 0)
    )
    qty_mismatch = (
        len(result.get("qty_mismatch") or []) if isinstance(result.get("qty_mismatch"), list)
        else int(result.get("qty_mismatch") or 0)
    )
    manual_review = int(result.get("manual_review") or 0)
    return {
        "date": target_day.isoformat(),
        "missing_in_db": mismatch,
        "extra_in_db": extra,
        "qty_mismatch": qty_mismatch,
        "manual_review": manual_review,
        "total_mismatch": mismatch + extra + qty_mismatch + manual_review,
        "unavailable_days": result.get("unavailable_days") or [],
    }


async def _section_today_counts(pool: Any, today: date) -> Dict[str, Any]:
    """b) 오늘 손절 SELL 건수 + 서킷브레이커 발동 여부 + 오늘 BUY 건수.
    stock_trader._daily_stop_count()와 동일한 조건(strategy LIKE '%손절%')으로 센다.
    crypto_trader는 제거되어 bot='stock_trader'로 한정한다."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE side='SELL' AND strategy LIKE '%손절%') AS stop_loss_sell,
                COUNT(*) FILTER (WHERE side='BUY') AS buy_count
            FROM trade_history
            WHERE bot='stock_trader' AND DATE(ts AT TIME ZONE 'Asia/Seoul') = $1
        """, today)
    stop_loss_sell = int(row["stop_loss_sell"] or 0)
    return {
        "stop_loss_sell_count": stop_loss_sell,
        "buy_count": int(row["buy_count"] or 0),
        "circuit_breaker_triggered": stop_loss_sell >= CIRCUIT_BREAKER_THRESHOLD,
    }


def _dedupe_gate_events(journal_ts: List[datetime], decision_ts: List[datetime],
                        window_sec: float = GATE_DEDUP_WINDOW_SEC) -> int:
    """trade_journal 행과 stark_decisions 행 중 같은 차단 한 건을 가리키는 쌍을 합쳐
    실제 차단 건수를 센다. execution_guard는 차단 시 log_decision()과 매매일지를 거의 동시에
    각각 기록하므로, 한 쪽 행마다 window_sec 안에 있는 다른 쪽 행과 1:1로 짝지어 한 건으로 본다."""
    unmatched = sorted(decision_ts)
    total = len(journal_ts)  # 일지 행은 모두 한 건으로 센다
    for j in journal_ts:
        hit = next((d for d in unmatched if abs((d - j).total_seconds()) <= window_sec), None)
        if hit is not None:
            unmatched.remove(hit)
    return total + len(unmatched)  # 짝이 없는 판단 행만 추가


async def _section_gate_blocks(pool: Any, today: date) -> Dict[str, Any]:
    """c) 오늘 차단 기록 건수 — trade_journal(jarvis_reason/signal_reason)과
    stark_decisions(reason/rationale)에서 게이트 이름을 포함하는 행을 모은 뒤, 같은 차단이 두
    테이블에 중복 기록된 것은 한 건으로 합친다([AT] fix/health-report-accuracy — 과거에는 두
    테이블 건수를 단순 합산해 실제의 2배로 표시됐다: 10/29 single_stock_cap 7건 -> 14건)."""
    counts: Dict[str, int] = {}
    async with pool.acquire() as conn:
        for gate in GATE_REASONS:
            pattern = f"%{gate}%"
            journal_rows = await conn.fetch("""
                SELECT ts AS at FROM trade_journal
                WHERE DATE(ts AT TIME ZONE 'Asia/Seoul') = $1
                  AND (jarvis_reason ILIKE $2 OR signal_reason ILIKE $2)
            """, today, pattern)
            decision_rows = await conn.fetch("""
                SELECT decided_at AS at FROM stark_decisions
                WHERE DATE(decided_at AT TIME ZONE 'Asia/Seoul') = $1
                  AND (reason ILIKE $2 OR rationale ILIKE $2)
            """, today, pattern)
            counts[gate] = _dedupe_gate_events(
                [r["at"] for r in journal_rows], [r["at"] for r in decision_rows])
    return {"by_gate": counts, "total": sum(counts.values())}


async def _section_positions(redis: Any, get_positions_fn: Callable[[], Awaitable[dict]],
                              now_kst: datetime) -> Dict[str, Any]:
    """d) 포지션 조회 상태.
    - stale: 이번 조회가 실패해 폴백 스냅샷(positions:last_ok)을 받았다는 뜻("조회 실패").
    - snapshot_age_min: 마지막 성공 스냅샷이 몇 분 전인지.
    - warn: 조회 실패(폴백)이면서 스냅샷이 POSITION_SNAPSHOT_WARN_MIN분을 넘게 오래됐거나 시각을
      알 수 없을 때만 True. 방금(0분 전) 성공한 스냅샷으로 폴백한 일시 실패는 경고하지 않는다."""
    pos_result = await get_positions_fn()
    stale = bool(isinstance(pos_result, dict) and pos_result.get("stale"))

    snapshot_age_min = None
    raw = await redis.get("positions:last_ok")
    if raw:
        d = json.loads(raw if isinstance(raw, str) else raw.decode())
        snap_at = d.get("snapshot_at")
        if snap_at:
            snap_dt = datetime.fromisoformat(snap_at)
            snapshot_age_min = round((now_kst - snap_dt).total_seconds() / 60, 1)
    warn = stale and (snapshot_age_min is None or snapshot_age_min > POSITION_SNAPSHOT_WARN_MIN)
    return {"stale": stale, "snapshot_age_min": snapshot_age_min, "warn": warn}


async def _section_scan_timing(redis: Any, today: date) -> Dict[str, Any]:
    """e) 아침스캔(morning_routine) 소요시간 — dashboard/main.py의 _run_scheduled_job이
    기록한 health:scan_timing:YYYY-MM-DD를 읽기만 한다."""
    raw = await redis.get(f"health:scan_timing:{today.isoformat()}")
    if raw is None:
        return {"elapsed_sec": None}
    try:
        value = raw if isinstance(raw, str) else raw.decode()
        return {"elapsed_sec": float(value)}
    except (TypeError, ValueError):
        return {"elapsed_sec": None}


async def _section_scan_detail(redis: Any, today: date) -> Dict[str, Any]:
    """e) 아침스캔 상세 계측 — dashboard/main.py의 _jarvis_stock_scanner가 기록한
    health:scan_detail:YYYY-MM-DD(종목수/실패수/조회초)를 읽기만 한다
    ([AT] fix/scan-parallel)."""
    raw = await redis.get(f"health:scan_detail:{today.isoformat()}")
    if raw is None:
        return {"total": None, "fail": None, "fetch_elapsed": None}
    try:
        d = json.loads(raw if isinstance(raw, str) else raw.decode())
        return {
            "total": d.get("total"),
            "fail": d.get("fail"),
            "fetch_elapsed": d.get("fetch_elapsed"),
        }
    except (TypeError, ValueError, json.JSONDecodeError):
        return {"total": None, "fail": None, "fetch_elapsed": None}


async def _section_token_reissue(redis: Any, today: date) -> Dict[str, Any]:
    """f) 포지션 조회 토큰 재발급 횟수 — _get_stock_positions_raw가 기록한
    health:token_reissue:YYYY-MM-DD를 읽기만 한다."""
    raw = await redis.get(f"health:token_reissue:{today.isoformat()}")
    count = 0
    if raw is not None:
        try:
            count = int(raw if isinstance(raw, str) else raw.decode())
        except (TypeError, ValueError):
            count = 0
    return {"reissue_count": count}


def _compute_pnl_rate(price: Optional[float], quantity: Optional[float],
                       pnl: Optional[float]) -> Optional[float]:
    """stock_trader/main.py의 손절 기록 방식(pnl=(체결가-평단)*수량, amount=체결가*수량)을
    역산한다: 매수금액 = price*quantity - pnl, pnl_rate = pnl / 매수금액 * 100."""
    if price is None or quantity is None or pnl is None:
        return None
    buy_amount = price * quantity - pnl
    if buy_amount == 0:
        return None
    return round(pnl / buy_amount * 100, 2)


async def _section_stop_loss_pnl(pool: Any, today: date) -> Dict[str, Any]:
    """g) 오늘 손절 SELL별 체결 손익률 — -7% 이하면 슬리피지 큼으로 표시."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol, price, quantity, pnl
            FROM trade_history
            WHERE bot='stock_trader' AND side='SELL' AND strategy LIKE '%손절%'
              AND DATE(ts AT TIME ZONE 'Asia/Seoul') = $1
            ORDER BY ts
        """, today)
    items = []
    worst_slippage = False
    for r in rows:
        price = float(r["price"]) if r["price"] is not None else None
        qty = float(r["quantity"]) if r["quantity"] is not None else None
        pnl = float(r["pnl"]) if r["pnl"] is not None else None
        rate = _compute_pnl_rate(price, qty, pnl)
        slip = rate is not None and rate <= SLIPPAGE_THRESHOLD_PCT
        worst_slippage = worst_slippage or slip
        items.append({"symbol": r["symbol"], "pnl_rate": rate, "slippage": slip})
    return {"items": items, "any_slippage": worst_slippage}


async def _section_pnl_null(pool: Any, today: date) -> Dict[str, Any]:
    """h) 오늘 SELL 중 pnl NULL 건수."""
    async with pool.acquire() as conn:
        n = await conn.fetchval("""
            SELECT COUNT(*) FROM trade_history
            WHERE bot='stock_trader' AND side='SELL' AND pnl IS NULL
              AND DATE(ts AT TIME ZONE 'Asia/Seoul') = $1
        """, today)
    return {"pnl_null_count": int(n or 0)}


async def _section_entry_risk_shadow(pool: Any, today: date) -> Dict[str, Any]:
    """i) 오늘 체결된 매수 중 저가주(low_price)/단기급등(surge_5d) 관찰 플래그 건수
    ([AT] feat/entry-risk-shadow — 관찰 전용, 차단 없음). trade_journal.low_price/
    surge_5d는 stark/execution_guard.py가 체결 확정 시점에 기록한다."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                COUNT(*) FILTER (WHERE low_price IS TRUE) AS low_price_n,
                COUNT(*) FILTER (WHERE surge_5d IS TRUE) AS surge_5d_n
            FROM trade_journal
            WHERE action IN ('buy', 'BUY') AND executed IS TRUE
              AND DATE(ts AT TIME ZONE 'Asia/Seoul') = $1
        """, today)
    if not rows:
        return {"low_price_count": 0, "surge_5d_count": 0}
    row = rows[0]
    return {
        "low_price_count": int(row["low_price_n"] or 0),
        "surge_5d_count": int(row["surge_5d_n"] or 0),
    }


def _fmt_index_rate(label: str, rate: Optional[float], fail_reason: Optional[str] = None) -> str:
    if rate is not None:
        return f"{label} {rate:+.2f}%"
    suffix = f" ({fail_reason})" if fail_reason else ""
    return f"{label} 데이터 없음{suffix}"


def _market_index_pending(now_kst: Optional[datetime]) -> bool:
    """지수 수집이 아직 끝났을 수 없는 시각인가 — 마감 후 저장 시간창(15:41~16:30) 이전/안이면
    True. now_kst를 모르면 False(기존 "미실행" 동작 유지)."""
    if now_kst is None:
        return False
    return now_kst.time() <= MARKET_INDEX_COLLECT_END


def _market_index_fail_reason(collect_log: Optional[dict], label: str,
                              pending: bool = False) -> Optional[str]:
    """health:index_collect:YYYY-MM-DD(dashboard/main.py._collect_market_index_daily가
    저장, [AT] fix/market-index-collect)에서 지수(label="KOSPI"/"KOSDAQ")별 실패 사유를
    짧은 문자열로 뽑아낸다. 키 자체가 없으면("미실행") 스케줄러가 아직 돌지 않았다는
    뜻이고, 있지만 ok=False면 조회/저장이 실패했다는 뜻이다."""
    if collect_log is None:
        # 저장 시간창 안/이전이면 아직 안 돈 게 아니라 수집 대기 중일 수 있어 단정하지 않는다.
        return "수집 대기" if pending else "미실행"
    entry = collect_log.get(label)
    if not entry or entry.get("ok"):
        return None
    err = entry.get("error") or {}
    if not isinstance(err, dict):
        return str(err)
    if err.get("rt_cd") or err.get("msg_cd") or err.get("msg1"):
        return f"rt_cd={err.get('rt_cd')} msg_cd={err.get('msg_cd')} msg1={err.get('msg1')}"
    return err.get("reason", "원인 미상")


async def _section_market_index(pool: Any, redis: Any, today: date,
                                 now_kst: Optional[datetime] = None) -> Dict[str, Any]:
    """j) 당일 코스피/코스닥 등락률 — market_index_daily(장마감 후 1회 저장, [AT]
    feat/entry-risk-shadow)를 읽기만 한다. 아직 저장 전이거나 실패했으면 해당 지수는
    None(표시는 "데이터 없음")이고, health:index_collect:YYYY-MM-DD([AT]
    fix/market-index-collect)에 기록된 실패 사유를 함께 붙인다."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT index_code, change_rate FROM market_index_daily
            WHERE date = $1 AND index_code IN ('0001', '1001')
        """, today)
    by_code = {
        r["index_code"]: float(r["change_rate"]) if r["change_rate"] is not None else None
        for r in rows
    }

    collect_log: Optional[dict] = None
    try:
        raw = await redis.get(f"health:index_collect:{today.isoformat()}")
        if raw:
            collect_log = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
    except Exception:
        collect_log = None

    pending = _market_index_pending(now_kst)
    kospi_rate = by_code.get("0001")
    kosdaq_rate = by_code.get("1001")
    return {
        "kospi_change_rate": kospi_rate,
        "kosdaq_change_rate": kosdaq_rate,
        "kospi_fail_reason": _market_index_fail_reason(collect_log, "KOSPI", pending) if kospi_rate is None else None,
        "kosdaq_fail_reason": _market_index_fail_reason(collect_log, "KOSDAQ", pending) if kosdaq_rate is None else None,
    }


async def _run_section(label: str, coro: Awaitable[dict]) -> Dict[str, Any]:
    try:
        return await coro
    except Exception as e:
        logger.error(f"헬스리포트 섹션 실패({label}): {e}")
        return {"error": f"확인불가({type(e).__name__}: {e})"}


def _fmt_section_line(prefix: str, data: Dict[str, Any], ok_text_fn) -> str:
    if "error" in data:
        return f"{prefix} {data['error']}"
    return f"{prefix} {ok_text_fn(data)}"


def _fmt_positions(d: Dict[str, Any]) -> str:
    """d) 줄 문구 — "조회 실패(폴백 사용)"과 "스냅샷 N분 전"을 분리해 표시한다."""
    age = d.get("snapshot_age_min")
    age_text = f"스냅샷 {age:.0f}분 전" if age is not None else "스냅샷 기록 없음"
    if d.get("stale"):
        return f"조회 실패(폴백 사용), {age_text}" + ("" if d.get("warn") else " — 경고 기준 이내")
    return f"정상, {age_text}"


async def build_health_report(
    *,
    pool: Any,
    redis: Any,
    reconcile_dry_run_fn: Callable[[date], Awaitable[dict]],
    get_positions_fn: Callable[[], Awaitable[dict]],
    now_kst: datetime,
) -> Dict[str, Any]:
    """점검 리포트 전체를 만든다. 반환: {"text": str, "status": "ok"|"warn", "sections": {...}}.
    섹션 하나가 예외를 던져도 다른 섹션·요약 줄 생성은 계속된다."""
    today = now_kst.date()
    prev_day = _previous_trading_day(today)

    sections: Dict[str, Any] = {}
    sections["reconcile"] = await _run_section(
        "reconcile", _section_reconcile(reconcile_dry_run_fn, prev_day))
    sections["today_counts"] = await _run_section(
        "today_counts", _section_today_counts(pool, today))
    sections["gate_blocks"] = await _run_section(
        "gate_blocks", _section_gate_blocks(pool, today))
    sections["positions"] = await _run_section(
        "positions", _section_positions(redis, get_positions_fn, now_kst))
    sections["scan_timing"] = await _run_section(
        "scan_timing", _section_scan_timing(redis, today))
    sections["scan_detail"] = await _run_section(
        "scan_detail", _section_scan_detail(redis, today))
    sections["token_reissue"] = await _run_section(
        "token_reissue", _section_token_reissue(redis, today))
    sections["stop_loss_pnl"] = await _run_section(
        "stop_loss_pnl", _section_stop_loss_pnl(pool, today))
    sections["pnl_null"] = await _run_section(
        "pnl_null", _section_pnl_null(pool, today))
    sections["entry_risk_shadow"] = await _run_section(
        "entry_risk_shadow", _section_entry_risk_shadow(pool, today))
    sections["market_index"] = await _run_section(
        "market_index", _section_market_index(pool, redis, today, now_kst))

    issues: List[str] = []

    rec = sections["reconcile"]
    if "error" not in rec and rec.get("total_mismatch", 0) > 0:
        issues.append(f"정산 불일치 {rec['total_mismatch']}건")

    pos = sections["positions"]
    if "error" not in pos and pos.get("warn"):
        age = pos.get("snapshot_age_min")
        issues.append(
            f"포지션 조회 실패(폴백 스냅샷 {age:.0f}분 전)" if age is not None
            else "포지션 조회 실패(폴백 스냅샷 시각 불명)")

    pnl_null = sections["pnl_null"]
    if "error" not in pnl_null and pnl_null.get("pnl_null_count", 0) > 0:
        issues.append(f"pnl NULL {pnl_null['pnl_null_count']}건")

    slp = sections["stop_loss_pnl"]
    if "error" not in slp and slp.get("any_slippage"):
        issues.append("손절 체결 슬리피지 큼(-7% 이하)")

    tc = sections["today_counts"]
    if "error" not in tc and tc.get("circuit_breaker_triggered"):
        issues.append(f"서킷브레이커 발동(손절 {tc['stop_loss_sell_count']}건)")

    reissue = sections["token_reissue"]
    if "error" not in reissue and reissue.get("reissue_count", 0) >= TOKEN_REISSUE_WARN_THRESHOLD:
        issues.append(f"토큰 재발급 {reissue['reissue_count']}회")

    status = "warn" if issues else "ok"
    headline = "✅ 이상 없음" if status == "ok" else f"⚠️ 점검 필요: {', '.join(issues)}"

    lines = [headline, f"[자동 점검] {today.isoformat()} (KST)", ""]

    lines.append(_fmt_section_line(
        "a) 전일 정산:", rec,
        lambda d: (
            f"{d['date']} 누락{d['missing_in_db']}·초과{d['extra_in_db']}·"
            f"수량불일치{d['qty_mismatch']}·수동확인{d['manual_review']}"
            + (f" (조회 불가일: {len(d['unavailable_days'])})" if d.get("unavailable_days") else "")
        )))
    lines.append(_fmt_section_line(
        "b) 오늘 매매:", tc,
        lambda d: (
            f"손절SELL {d['stop_loss_sell_count']}건 · BUY {d['buy_count']}건 · "
            f"서킷브레이커 {'발동' if d['circuit_breaker_triggered'] else '정상'}"
        )))
    gb = sections["gate_blocks"]
    lines.append(_fmt_section_line(
        "c) 오늘 차단:", gb,
        lambda d: (
            f"총 {d['total']}건 ("
            + ", ".join(f"{k} {v}" for k, v in d["by_gate"].items())
            + ")"
        )))
    lines.append(_fmt_section_line(
        "d) 포지션 조회:", pos,
        lambda d: _fmt_positions(d)))
    st = sections["scan_timing"]
    sd = sections["scan_detail"]

    def _fmt_scan_timing(d: Dict[str, Any]) -> str:
        base = f"{d['elapsed_sec']:.1f}s" if d.get("elapsed_sec") is not None else "기록 없음"
        if "error" not in sd and sd.get("total") is not None:
            fetch_sec = sd.get("fetch_elapsed")
            base += (f" (종목수 {sd['total']} · 실패 {sd.get('fail') or 0} · "
                     + (f"조회 {fetch_sec:.1f}s)" if fetch_sec is not None else "조회 기록 없음)"))
        return base

    lines.append(_fmt_section_line("e) 아침스캔 소요:", st, _fmt_scan_timing))
    lines.append(_fmt_section_line(
        "f) 토큰 재발급:", reissue,
        lambda d: f"{d['reissue_count']}회"
    ))
    lines.append(_fmt_section_line(
        "g) 손절 체결률:", slp,
        lambda d: (
            ", ".join(
                f"{it['symbol']} {it['pnl_rate']:+.1f}%" + (" ⚠️슬리피지" if it["slippage"] else "")
                for it in d["items"]
            ) if d["items"] else "오늘 손절 없음"
        )))
    lines.append(_fmt_section_line(
        "h) pnl NULL:", pnl_null,
        lambda d: f"{d['pnl_null_count']}건"
    ))
    er = sections["entry_risk_shadow"]
    lines.append(_fmt_section_line(
        "i) 오늘 매수 관찰(저가주/급등):", er,
        lambda d: f"low_price {d['low_price_count']}건, surge_5d {d['surge_5d_count']}건"
    ))
    mi = sections["market_index"]
    lines.append(_fmt_section_line(
        "j) 당일 지수:", mi,
        lambda d: (
            f"{_fmt_index_rate('KOSPI', d['kospi_change_rate'], d.get('kospi_fail_reason'))}, "
            f"{_fmt_index_rate('KOSDAQ', d['kosdaq_change_rate'], d.get('kosdaq_fail_reason'))}"
        )
    ))

    return {"text": "\n".join(lines), "status": status, "issues": issues, "sections": sections}
