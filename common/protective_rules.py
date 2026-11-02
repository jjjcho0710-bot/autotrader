"""AI 자동매수 보호 규칙 + 섀도 기록 ([AT] feat/protective-rules, PM 승인).

- daily_loss_limit: 당일 실현 pnl 합계 <= 총자산 x DAILY_LOSS_LIMIT_PCT(%) -> 신규 매수 차단.
  총자산/손익 조회 실패 시 fail-open(차단 안 함).
- market_gate: KOSPI·KOSDAQ 등락률이 둘 다 <= MARKET_GATE_PCT(%) -> 신규 매수 차단.
  지수 조회 실패/한쪽 누락 시 fail-open.
- surge_block: 5거래일 수익률 또는 당일 등락이 임계 이상이면 신규 매수 차단(순수 계산부만 여기, 조회는
  stark/execution_guard.check_surge_block). 데이터 없으면 fail-open.
- 마감 전 손실 정리 판정 보조(eod_*): 매수 후 1거래일 이내 + 평가손익 <= EOD_CUT_PCT.
- build_shadow_features: 모든 BUY 결정에 stark_decisions.features_json 으로 남길 임시 관찰 값
  (KST 시각대, KOSPI·KOSDAQ 등락, 당일 실현 pnl). 조회 실패 항목은 None. 매매 판단에 쓰지 않는다.
모든 조회는 예외를 삼키고 None/통과로 처리한다 — 보호 규칙의 오류가 매매를 막아서는 안 된다."""
import logging
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from common.config import config
from common.market_calendar import is_trading_day

logger = logging.getLogger("common.protective_rules")
KST = timezone(timedelta(hours=9))


async def realized_pnl_today(pool: Any) -> Optional[float]:
    """오늘(KST) stock_trader 실현 pnl 합계. 조회 실패 시 None."""
    try:
        async with pool.acquire() as conn:
            v = await conn.fetchval("""
                SELECT COALESCE(SUM(pnl), 0) FROM trade_history
                WHERE bot='stock_trader' AND side='SELL' AND pnl IS NOT NULL
                  AND DATE(ts AT TIME ZONE 'Asia/Seoul') = (NOW() AT TIME ZONE 'Asia/Seoul')::date
            """)
        return float(v or 0)
    except Exception as e:
        logger.warning(f"당일 실현 pnl 조회 실패(fail-open): {e}")
        return None


def parse_index_rates(res: Any) -> Tuple[Optional[float], Optional[float]]:
    """get_market_index 응답 -> (KOSPI 등락률, KOSDAQ 등락률). 실패/누락은 None."""
    try:
        if not isinstance(res, dict) or not res.get("success", False):
            return None, None
        data = res.get("data") or {}
        def rate(k):
            e = data.get(k)
            if not isinstance(e, dict) or e.get("change_rate") is None:
                return None
            return float(e["change_rate"])
        return rate("KOSPI"), rate("KOSDAQ")
    except Exception:
        return None, None


async def _equity(get_positions_fn: Any) -> Optional[float]:
    try:
        res = await get_positions_fn()
        if not isinstance(res, dict) or not res.get("success", False) or res.get("stale", False):
            return None
        eq = float((res.get("account") or {}).get("total_eval", 0) or 0)
        return eq if eq > 0 else None
    except Exception as e:
        logger.warning(f"총자산 조회 실패(fail-open): {e}")
        return None


async def check_daily_loss_limit(pool: Any, get_positions_fn: Any,
                                 limit_pct: Optional[float] = None) -> Optional[Dict[str, Any]]:
    """차단 시 {"blocked": "daily_loss_limit", "reason": ..., "pnl": ..., "equity": ..., "limit_amount": ...}."""
    if get_positions_fn is None or pool is None:
        return None
    limit = -abs(limit_pct if limit_pct is not None else config.DAILY_LOSS_LIMIT_PCT)
    equity = await _equity(get_positions_fn)
    if equity is None:
        return None
    pnl = await realized_pnl_today(pool)
    if pnl is None:
        return None
    limit_amount = equity * limit / 100.0
    if pnl <= limit_amount:
        return {
            "blocked": "daily_loss_limit",
            "reason": (f"daily_loss_limit: 당일 실현손익 {pnl:+,.0f}원 <= 한도 {limit_amount:+,.0f}원 "
                       f"(총자산 {equity:,.0f}원 x {limit:.1f}%) — 신규 매수 차단"),
            "pnl": pnl, "equity": equity, "limit_amount": limit_amount,
        }
    return None


async def check_market_gate(get_market_index_fn: Any,
                            gate_pct: Optional[float] = None) -> Optional[Dict[str, Any]]:
    if get_market_index_fn is None:
        return None
    gate = -abs(gate_pct if gate_pct is not None else config.MARKET_GATE_PCT)
    try:
        kospi, kosdaq = parse_index_rates(await get_market_index_fn())
    except Exception as e:
        logger.warning(f"지수 조회 실패(fail-open): {e}")
        return None
    if kospi is None or kosdaq is None:
        return None
    if kospi <= gate and kosdaq <= gate:
        return {
            "blocked": "market_gate",
            "reason": (f"market_gate: KOSPI {kospi:+.2f}%, KOSDAQ {kosdaq:+.2f}% 모두 <= {gate:.1f}% "
                       f"— 신규 매수 차단"),
            "kospi": kospi, "kosdaq": kosdaq,
        }
    return None


async def build_shadow_features(pool: Any, get_market_index_fn: Any,
                                now: Optional[datetime] = None) -> Dict[str, Any]:
    """BUY 결정 섀도 기록. 항목별로 실패해도 나머지는 채운다."""
    now = (now or datetime.now(KST)).astimezone(KST)
    feats: Dict[str, Any] = {
        "kst_time": now.strftime("%H:%M"),
        "kst_hour_band": f"{now.hour:02d}:00-{now.hour:02d}:59",
        "kospi_rate": None, "kosdaq_rate": None, "realized_pnl_today": None,
    }
    if get_market_index_fn is not None:
        try:
            feats["kospi_rate"], feats["kosdaq_rate"] = parse_index_rates(await get_market_index_fn())
        except Exception:
            pass
    if pool is not None:
        feats["realized_pnl_today"] = await realized_pnl_today(pool)
    return feats


# ── 급등주 진입 차단 (순수 계산) ─────────────────────────────────────
def prior_closes(closes: List[float], latest_date: Optional[date], today: date) -> List[float]:
    """일봉 종가(날짜 오름차순)에서 '오늘 이전에 완성된 봉'만 남긴다. 마지막 봉 날짜가 오늘(장 마감 후
    수집분)이면 그 봉은 제외한다. latest_date를 모르면(None) 전부 완성된 봉으로 본다."""
    if latest_date is not None and latest_date >= today and closes:
        return list(closes[:-1])
    return list(closes)


def surge_pcts(base: List[float], price: float) -> Tuple[Optional[float], Optional[float]]:
    """(5거래일 수익률%, 당일 등락%). base = 오늘 이전 완성 종가(오름차순), price = 현재가.
    당일 등락 = 현재가 vs 직전 거래일 종가(base[-1]), 5일 수익률 = 현재가 vs 5거래일 전 종가(base[-5]).
    계산 불가 항목은 None."""
    try:
        price = float(price)
    except Exception:
        return None, None
    if price <= 0:
        return None, None
    pct1 = pct5 = None
    if len(base) >= 1 and base[-1] > 0:
        pct1 = (price - base[-1]) / base[-1] * 100
    if len(base) >= 5 and base[-5] > 0:
        pct5 = (price - base[-5]) / base[-5] * 100
    return pct5, pct1


def evaluate_surge(pct5: Optional[float], pct1: Optional[float],
                   th5: Optional[float] = None, th1: Optional[float] = None) -> Optional[Dict[str, Any]]:
    """임계 이상이면 차단 정보 dict, 아니면 None. 값이 None인 항목은 판단에서 제외(fail-open)."""
    th5 = config.SURGE_5D_BLOCK_PCT if th5 is None else th5
    th1 = config.SURGE_1D_BLOCK_PCT if th1 is None else th1
    parts = []
    if pct5 is not None and pct5 >= th5:
        parts.append(f"5일 수익률 {pct5:+.1f}% (>= {th5:+.1f}%)")
    if pct1 is not None and pct1 >= th1:
        parts.append(f"당일 등락 {pct1:+.1f}% (>= {th1:+.1f}%)")
    if not parts:
        return None
    return {"blocked": "surge_block", "pct5": pct5, "pct1": pct1,
            "reason": "surge_block: " + ", ".join(parts) + " — 신규 매수 차단"}


# ── 마감 전 손실 정리 (순수 판정) ────────────────────────────────────
EOD_CUT_LABEL = "마감전손실정리"


def eod_cut_time() -> time:
    """config.EOD_CUT_TIME_HHMM(KST). 형식이 잘못되면 15:20."""
    try:
        hh, mm = str(config.EOD_CUT_TIME_HHMM).split(":")
        return time(int(hh), int(mm))
    except Exception:
        return time(15, 20)


def prev_trading_day(d: date) -> date:
    x = d - timedelta(days=1)
    for _ in range(15):
        if is_trading_day(x):
            return x
        x -= timedelta(days=1)
    return x


def bought_within_one_trading_day(last_buy_ts: datetime, today: date) -> bool:
    """마지막 매수일(KST)이 오늘 또는 직전 거래일이면 True."""
    if last_buy_ts is None:
        return False
    if last_buy_ts.tzinfo is None:
        last_buy_ts = last_buy_ts.replace(tzinfo=timezone.utc)
    buy_date = last_buy_ts.astimezone(KST).date()
    return prev_trading_day(today) <= buy_date <= today
