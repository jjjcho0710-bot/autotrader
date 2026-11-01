"""AI 자동매수 보호 규칙 + 섀도 기록 ([AT] feat/protective-rules, PM 승인).

- daily_loss_limit: 당일 실현 pnl 합계 <= 총자산 x DAILY_LOSS_LIMIT_PCT(%) -> 신규 매수 차단.
  총자산/손익 조회 실패 시 fail-open(차단 안 함).
- market_gate: KOSPI·KOSDAQ 등락률이 둘 다 <= MARKET_GATE_PCT(%) -> 신규 매수 차단.
  지수 조회 실패/한쪽 누락 시 fail-open.
- build_shadow_features: 모든 BUY 결정에 stark_decisions.features_json 으로 남길 임시 관찰 값
  (KST 시각대, KOSPI·KOSDAQ 등락, 당일 실현 pnl). 조회 실패 항목은 None. 매매 판단에 쓰지 않는다.
모든 조회는 예외를 삼키고 None/통과로 처리한다 — 보호 규칙의 오류가 매매를 막아서는 안 된다."""
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from common.config import config

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
