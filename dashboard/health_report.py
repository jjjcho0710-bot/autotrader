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
from datetime import date, datetime, timedelta
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


async def _section_gate_blocks(pool: Any, today: date) -> Dict[str, Any]:
    """c) 오늘 차단 기록 건수 — trade_journal(jarvis_reason/signal_reason) +
    stark_decisions(reason/rationale)에서 게이트 이름을 포함하는 행 수를 합친다."""
    counts: Dict[str, int] = {}
    async with pool.acquire() as conn:
        for gate in GATE_REASONS:
            pattern = f"%{gate}%"
            journal_n = await conn.fetchval("""
                SELECT COUNT(*) FROM trade_journal
                WHERE DATE(ts AT TIME ZONE 'Asia/Seoul') = $1
                  AND (jarvis_reason ILIKE $2 OR signal_reason ILIKE $2)
            """, today, pattern)
            decisions_n = await conn.fetchval("""
                SELECT COUNT(*) FROM stark_decisions
                WHERE DATE(decided_at AT TIME ZONE 'Asia/Seoul') = $1
                  AND (reason ILIKE $2 OR rationale ILIKE $2)
            """, today, pattern)
            counts[gate] = int(journal_n or 0) + int(decisions_n or 0)
    return {"by_gate": counts, "total": sum(counts.values())}


async def _section_positions(redis: Any, get_positions_fn: Callable[[], Awaitable[dict]],
                              now_kst: datetime) -> Dict[str, Any]:
    """d) 포지션 조회 상태 — 현재 조회가 stale인지, 마지막 성공 스냅샷(positions:last_ok)이
    얼마나 오래됐는지(분)."""
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
    return {"stale": stale, "snapshot_age_min": snapshot_age_min}


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


def _fmt_index_rate(label: str, rate: Optional[float]) -> str:
    return f"{label} 데이터 없음" if rate is None else f"{label} {rate:+.2f}%"


async def _section_market_index(pool: Any, today: date) -> Dict[str, Any]:
    """j) 당일 코스피/코스닥 등락률 — market_index_daily(장마감 후 1회 저장, [AT]
    feat/entry-risk-shadow)를 읽기만 한다. 아직 저장 전이거나 실패했으면 해당 지수는
    None(표시는 "데이터 없음")."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT index_code, change_rate FROM market_index_daily
            WHERE date = $1 AND index_code IN ('0001', '1001')
        """, today)
    by_code = {
        r["index_code"]: float(r["change_rate"]) if r["change_rate"] is not None else None
        for r in rows
    }
    return {
        "kospi_change_rate": by_code.get("0001"),
        "kosdaq_change_rate": by_code.get("1001"),
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
    sections["token_reissue"] = await _run_section(
        "token_reissue", _section_token_reissue(redis, today))
    sections["stop_loss_pnl"] = await _run_section(
        "stop_loss_pnl", _section_stop_loss_pnl(pool, today))
    sections["pnl_null"] = await _run_section(
        "pnl_null", _section_pnl_null(pool, today))
    sections["entry_risk_shadow"] = await _run_section(
        "entry_risk_shadow", _section_entry_risk_shadow(pool, today))
    sections["market_index"] = await _run_section(
        "market_index", _section_market_index(pool, today))

    issues: List[str] = []

    rec = sections["reconcile"]
    if "error" not in rec and rec.get("total_mismatch", 0) > 0:
        issues.append(f"정산 불일치 {rec['total_mismatch']}건")

    pos = sections["positions"]
    if "error" not in pos and pos.get("stale"):
        issues.append("포지션 조회 stale")

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
        lambda d: (
            f"{'stale' if d['stale'] else '정상'}"
            + (f" (최근 스냅샷 {d['snapshot_age_min']:.0f}분 전)"
               if d.get("snapshot_age_min") is not None else " (스냅샷 기록 없음)")
        )))
    st = sections["scan_timing"]
    lines.append(_fmt_section_line(
        "e) 아침스캔 소요:", st,
        lambda d: (f"{d['elapsed_sec']:.1f}s" if d.get("elapsed_sec") is not None else "기록 없음")
    ))
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
            f"{_fmt_index_rate('KOSPI', d['kospi_change_rate'])}, "
            f"{_fmt_index_rate('KOSDAQ', d['kosdaq_change_rate'])}"
        )
    ))

    return {"text": "\n".join(lines), "status": status, "issues": issues, "sections": sections}
