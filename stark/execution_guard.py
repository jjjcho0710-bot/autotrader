"""
stark/execution_guard.py - STARK v2 실행 레이어 (룰 기반 안전장치 + KIS 실주문 집행)

dashboard/main.py의 jarvis_signal(구 6081행)에서 두 부분을 이관했다:
1. precheck(): AI 판단 호출 "전에" 먼저 거르는 안전장치 — 당일 잔고없음 매도 억제
   (구 6106~6112행), 당일 2회 이상 손절 시 신규 매수 차단(구 6114~6129행). AI를 부르기 전에
   차단해서 불필요한 LLM 호출 비용을 막는 최적화를 그대로 유지한다.
2. execute(): stark/decision_engine.decide()가 EXECUTE/EXECUTE_SMALL로 판단한 신호를
   실제 KIS 주문으로 집행 + 매매기록/텔레그램 보고(구 6263~6407행 중 주식 경로만).
   코인(crypto_trader) 경로는 STARK_PLAN상 폐기 예정이라 이 모듈로 옮기지 않고
   dashboard/main.py의 jarvis_signal에 그대로 남겨둔다.

STARK_PLAN 4번 원칙("판단(AI)과 실행(룰)을 코드 레벨로 분리")의 실행 절반 — 이 모듈은
LLM을 호출하지 않는다. execute()는 decision_engine이 이미 내린 판단(decision dict)을
그대로 신뢰하고 집행만 담당한다.
"""
import asyncio
import json
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger("stark.execution_guard")

# 악재성 공시 키워드 — 매칭되면 투자경고 종목 차단과 동일한 수준으로 신규·추가매수를
# 기계적으로 차단한다(AI 판단에 맡기지 않음). 조정이 필요하면 이 목록만 고치면 된다.
BAD_DISCLOSURE_KEYWORDS = [
    "관리종목 지정",
    "상장폐지",
    "감사의견 거절",
    "감사의견 한정",
    "횡령",
    "배임",
    "불성실공시",
    "거래정지",
]


class AsyncRLock:
    """비동기 재진입 가능 Lock. 동일 태스크 내 중첩 진입을 허용하고 다른 태스크는 대기시킨다."""
    def __init__(self):
        self._lock = asyncio.Lock()
        self._owner: Optional[asyncio.Task] = None
        self._count = 0

    async def acquire(self) -> bool:
        me = asyncio.current_task()
        if self._owner == me:
            self._count += 1
            return True
        await self._lock.acquire()
        self._owner = me
        self._count = 1
        return True

    async def release(self) -> None:
        me = asyncio.current_task()
        if self._owner != me:
            raise RuntimeError("Cannot release un-acquired lock")
        self._count -= 1
        if self._count == 0:
            self._owner = None
            self._lock.release()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()


_buy_lock: Optional[AsyncRLock] = None


def get_buy_lock() -> AsyncRLock:
    global _buy_lock
    if _buy_lock is None:
        _buy_lock = AsyncRLock()
    return _buy_lock


def reset_buy_lock() -> None:
    """테스트 또는 이벤트루프 격리용 lock 리셋"""
    global _buy_lock
    _buy_lock = None


async def _get_max_positions(pool: Any, default: int = 5) -> int:
    """strategy_config에서 stock_trader의 max_positions 조회 (실패 시 기본값 반환)."""
    if pool is None:
        return default
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT params FROM strategy_config
                WHERE bot='stock_trader' AND is_active=TRUE
                LIMIT 1
            """)
            if row and row.get("params"):
                params = row["params"]
                if isinstance(params, str):
                    params = json.loads(params)
                if isinstance(params, dict) and "max_positions" in params:
                    return int(params["max_positions"])
    except Exception as e:
        logger.warning(f"max_positions 조회 실패 (기본값 {default} 사용): {e}")
    return default


async def _check_averaging_down_guard(pool: Any, bot: str, symbol: str, price: float) -> Optional[str]:
    """이미 보유 중인 종목에 대한 추가매수(물타기) 정책 검사.

    정책: 원칙적으로 금지. 예외 — 최초 매수가 대비 현재가가 -3% 이내인 경우에 한해
    종목당 평생 1회만 허용(trade_history 전체 이력 기준, 보유 기간과 무관).
    반환: 허용 시 None, 차단 시 사유 코드 문자열."""
    try:
        async with pool.acquire() as conn:
            buys = await conn.fetch("""
                SELECT price FROM trade_history
                WHERE bot=$1 AND symbol=$2 AND side='BUY'
                ORDER BY ts ASC
            """, bot, symbol)
    except Exception as e:
        logger.error(f"물타기 정책 확인 실패(안전을 위해 매수 차단): {e}")
        return "averaging_down_check_failed"

    if not buys:
        # KIS 잔고상 보유 중인데 trade_history에 매수 이력이 없음(시스템 밖 매수 등) —
        # 최초 매수가를 확인할 수 없어 조건 (b)를 검증할 수 없으므로 보수적으로 차단
        return "averaging_down_no_entry_history"

    if len(buys) >= 2:
        return "averaging_down_already_used"

    first_price = float(buys[0]["price"] or 0)
    if first_price <= 0:
        return "averaging_down_check_failed"

    drift_pct = (float(price) - first_price) / first_price * 100
    if drift_pct < -3.0:
        return "averaging_down_price_drop_exceeded"

    return None


# 물타기(추가매수) 정책 위반 사유 코드 → 사람이 읽을 문구. execute()의 신호 경로와 채팅
# 직접매수 경로(buy_gate)가 동일 문구를 쓰도록 공용으로 둔다.
_AVERAGING_DOWN_REASON_TEXT = {
    "averaging_down_already_used": "물타기 정책: 종목당 평생 1회 한도 이미 사용",
    "averaging_down_price_drop_exceeded": "물타기 정책: 최초 매수가 대비 -3% 초과 하락",
    "averaging_down_no_entry_history": "물타기 정책: 최초 매수 이력 확인 불가",
    "averaging_down_check_failed": "물타기 정책 확인 실패로 매수 보류",
}


async def _resolve_held_and_inflight_symbols(redis: Any, get_positions_fn) -> tuple:
    """KIS 실제 보유 종목 + 접수됐지만 잔고에 아직 안 나타난 in-flight 매수 기록의 합집합을
    구한다. 반환: (합집합 set 또는 None, 조회실패 여부). 조회 자체가 실패/예외/stale이면
    (None, True)로 fail-closed를 알린다(호출부는 이 경우 매수를 차단해야 한다)."""
    try:
        pos_res = await get_positions_fn()
        if not isinstance(pos_res, dict) or not pos_res.get("success", False) or pos_res.get("stale", False):
            return None, True
        positions = pos_res.get("data") or []
    except Exception as e:
        logger.error(f"보유 종목수 조회 예외(fail-closed 적용): {e}")
        return None, True

    kis_held_symbols = {
        p.get("symbol") for p in positions
        if isinstance(p, dict) and p.get("symbol")
    }

    inflight_symbols = set()
    if redis is not None:
        try:
            keys = await redis.keys("stark:inflight_buy:*")
            for k in (keys or []):
                k_str = k.decode() if isinstance(k, bytes) else str(k)
                inflight_symbols.add(k_str.split(":")[-1])
        except Exception as e:
            logger.warning(f"inflight buy keys 조회 실패: {e}")

    # KIS 잔고에 나타나면 in-flight 기록 삭제
    if redis is not None and inflight_symbols and kis_held_symbols:
        resolved = inflight_symbols & kis_held_symbols
        for s in resolved:
            try:
                await redis.delete(f"stark:inflight_buy:{s}")
            except Exception:
                pass
            inflight_symbols.discard(s)

    return kis_held_symbols | inflight_symbols, False


async def check_buy_position_limits(
    symbol: str, price: float, *, pool: Any, redis: Any, bot: str,
    get_positions_fn, max_positions: Optional[int] = None,
) -> Optional[Dict[str, str]]:
    """보유 종목수 한도(fail-closed) + 이미 보유 중인 종목의 추가매수(물타기) 정책을 한
    곳에서 검사한다. 신호 경로(execute())와 채팅 직접매수 경로(buy_gate())가 공용으로 쓴다
    (PM 지시: 코드를 복사하지 말고 기존 함수를 재사용·추출). 반환: 통과 시 None. 차단 시
    {"blocked": 사유코드, "reason": 사람이 읽을 문구}(max_positions_limit은 "limit" 키도 포함)."""
    total_held_symbols, check_failed = await _resolve_held_and_inflight_symbols(redis, get_positions_fn)
    if check_failed:
        return {"blocked": "positions_check_failed", "reason": "보유 종목수 확인 불가"}

    is_already_held = symbol in total_held_symbols

    # 이미 보유 중인 종목에 대한 추가매수(물타기) 정책 강제 — (a) 종목당 평생 1회 한도,
    # (b) 최초 매수가 대비 -3% 이내 조건 중 하나라도 위반하면 차단한다.
    if is_already_held:
        block_reason = await _check_averaging_down_guard(pool, bot, symbol, price)
        if block_reason:
            reason_text = _AVERAGING_DOWN_REASON_TEXT.get(block_reason, "물타기 정책 위반")
            return {"blocked": block_reason, "reason": reason_text}

    limit = max_positions
    if limit is None:
        limit = await _get_max_positions(pool, default=5)

    # 신규 종목 매수인데 합집합이 max_positions 이상이면 차단
    if not is_already_held and len(total_held_symbols) >= limit:
        reason_text = f"최대 보유 종목수 한도 초과 ({len(total_held_symbols)}/{limit})"
        return {"blocked": "max_positions_limit", "reason": reason_text, "limit": limit}

    return None


# 주문 응답이 불명(uncertain)일 때 재확인 대기로 같은 종목·방향 중복 주문을 막는 TTL(초)
ORDER_UNCERTAIN_BLOCK_TTL = 180


async def check_order_uncertain_block(symbol: str, is_buy: bool, redis: Any) -> bool:
    """직전 같은 종목·같은 방향 주문이 결과 불명(uncertain)으로 끝나 재확인 대기 중인지 확인.
    True면 호출부가 신규 주문을 보류해야 한다(중복 주문 방지)."""
    if redis is None:
        return False
    side = "buy" if is_buy else "sell"
    try:
        return bool(await redis.get(f"order_uncertain:{symbol}:{side}"))
    except Exception:
        return False


async def get_held_qty(symbol: str, get_positions_fn) -> Optional[float]:
    """get_positions_fn() 결과에서 특정 종목의 보유 수량을 조회. 조회 실패/stale이면
    None(호출부는 판단 불가 상태로 처리해야 한다 — fail-closed)."""
    if get_positions_fn is None:
        return None
    try:
        pos_res = await get_positions_fn()
    except Exception as e:
        logger.warning(f"보유 수량 조회 실패(fail-closed) [{symbol}]: {e}")
        return None
    if not isinstance(pos_res, dict) or not pos_res.get("success", False) or pos_res.get("stale", False):
        return None
    pos_row = next(
        (p for p in (pos_res.get("data") or [])
         if isinstance(p, dict) and p.get("symbol") == symbol),
        None,
    )
    return float(pos_row.get("qty", 0) or 0) if pos_row else 0.0


async def reconcile_uncertain_order(
    symbol: str, is_buy: bool, *, pre_qty: Optional[float],
    get_positions_fn=None, invalidate_cache_fn=None, redis: Any = None,
    wait_sec: float = 4.0,
) -> Dict[str, Any]:
    """_kis_stock_order가 uncertain(응답 불명) 결과를 반환했을 때, 주문 전 보유 수량(pre_qty)과
    wait_sec 후 재조회한 보유 수량을 비교해 실제 체결 여부를 판정한다. 캐시(메모리+redis)를
    무효화한 뒤 조회하므로 반드시 최신 KIS 잔고를 기준으로 판정한다.

    반환: {"status": "filled", "qty_diff": 변동 수량} 또는 {"status": "unknown"}
    (조회 자체가 실패해도 "unknown"으로 묶는다 — 호출부는 재시도하지 말고 사람에게 확인을 요청해야 한다).
    status != "filled"이면 같은 종목·방향 중복 주문 차단 키를 ORDER_UNCERTAIN_BLOCK_TTL초 동안 건다."""
    post_qty = None
    if get_positions_fn is not None and pre_qty is not None:
        await asyncio.sleep(wait_sec)
        if invalidate_cache_fn:
            try:
                if asyncio.iscoroutinefunction(invalidate_cache_fn):
                    await invalidate_cache_fn()
                else:
                    invalidate_cache_fn()
            except Exception:
                pass
        if redis is not None:
            try:
                await redis.delete("cache:positions:stock")
            except Exception:
                pass
        post_qty = await get_held_qty(symbol, get_positions_fn)

    if pre_qty is None or post_qty is None:
        status = "unknown"
    else:
        diff = post_qty - pre_qty
        status = "filled" if (diff > 0 if is_buy else diff < 0) else "unknown"

    side = "buy" if is_buy else "sell"
    if status != "filled" and redis is not None:
        try:
            await redis.setex(f"order_uncertain:{symbol}:{side}", ORDER_UNCERTAIN_BLOCK_TTL, "1")
        except Exception:
            pass

    result: Dict[str, Any] = {"status": status}
    if status == "filled":
        result["qty_diff"] = abs(post_qty - pre_qty)
    return result


# 매수 실패 사유 중 몇 초~몇 분이면 풀리는 일시적 오류로 보고 짧게 재시도할 키워드
# (stock_trader/main.py._STOP_LOSS_TRANSIENT_MARKERS와 동일 원칙을 매수 쪽에도 적용)
BUY_FAIL_TRANSIENT_MARKERS = ("초당", "거래건수", "체결 0주", "체결수량 0", "미체결", "rate", "Rate")


def _format_suppress_sec(sec: int) -> str:
    if sec < 60:
        return f"{sec}초간"
    return f"{sec // 60}분간"


async def _compute_buy_fail_suppress_sec(symbol: str, err_msg: str, redis: Any) -> tuple:
    """매수 실패 사유별 재시도 억제 시간 계산.
    stock_trader/main.py._compute_stop_loss_suppress_sec()과 동일한 원칙:
    - "초당 거래건수" 등 속도제한/일시 오류로 보이는 사유: 60~90초 뒤 재시도
    - 같은 사유로 3회 이상 연속 실패: 5분 → 15분 → 30분으로 점진 확대(그 이상은 30분 고정)
    - 그 외 사유(잔고 부족 등): 기존과 동일하게 30분 억제
    반환: (suppress_sec, 연속 실패 횟수)"""
    from common.alert_throttle import normalize_cause
    norm_reason = normalize_cause(err_msg)
    streak_key = f"buy_fail_streak:{symbol}"
    streak = None
    try:
        raw = await redis.get(streak_key)
        if raw:
            streak = json.loads(raw)
    except Exception:
        pass

    fail_count = 1
    if streak and streak.get("reason") == norm_reason:
        fail_count = int(streak.get("count", 0)) + 1

    try:
        await redis.setex(
            streak_key, 3600,
            json.dumps({"reason": norm_reason, "count": fail_count}),
        )
    except Exception:
        pass

    if fail_count >= 3:
        tier = min(fail_count - 2, 3)
        suppress_sec = {1: 300, 2: 900, 3: 1800}[tier]
    elif any(marker in err_msg for marker in BUY_FAIL_TRANSIENT_MARKERS):
        suppress_sec = 90
    else:
        suppress_sec = 1800

    return suppress_sec, fail_count


async def precheck(symbol: str, action: str, bot: str, *, pool: Any, redis: Any) -> Optional[Dict[str, str]]:
    """AI 판단 호출 전 룰 기반 사전 차단.

    반환: 통과 시 None. 차단 시 {"blocked": 사유} (정상 차단) 또는 {"error": 메시지}
    (안전장치 확인 자체가 실패해서 보수적으로 막은 경우 — 호출부는 success=False로 응답해야 함)."""
    if action in ("sell", "SELL"):
        try:
            if await redis.get(f"sell_fail_suppress:{symbol}"):
                logger.info(f"⏸️ 매도 신호 무시 (매도 실패 억제 중): {symbol}")
                return {"blocked": "sell_fail_suppress"}
        except Exception:
            pass

    if action in ("buy", "BUY"):
        try:
            if await redis.get(f"buy_fail_suppress:{symbol}"):
                logger.info(f"⏸️ 매수 신호 무시 (매수 실패 억제 중): {symbol}")
                return {"blocked": "buy_fail_suppress"}
        except Exception:
            pass

    # 악재성 공시(관리종목 지정·상장폐지·감사의견 거절/한정·횡령·배임·불성실공시·거래정지) →
    # 투자경고 종목 차단(stock_trader/main.py)과 동일한 수준으로 신규·추가매수 강제 차단
    if bot == "stock_trader" and action in ("buy", "BUY"):
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT report_name FROM stock_disclosure
                    WHERE symbol=$1
                    ORDER BY rcept_dt DESC LIMIT 20
                """, symbol)
            for row in rows:
                title = row["report_name"] if isinstance(row, dict) else row[0]
                if any(kw in (title or "") for kw in BAD_DISCLOSURE_KEYWORDS):
                    logger.info(f"🛑 악재성 공시({title}) — 신규 매수 강제 차단: {symbol}")
                    return {"blocked": "bad_disclosure"}
        except Exception as e:
            logger.error(f"공시 확인 실패(안전을 위해 매수 보류): {e}")
            return {"error": "공시 확인 실패로 매수 보류"}

    # 당일 2회 이상 손절 → 신규 매수 강제 차단 (예전엔 프롬프트 텍스트로만 존재해 AI 판단에만
    # 의존했던 안전장치를 코드 레벨 강제 집행으로 전환한 부분 — 그대로 유지)
    if bot == "stock_trader" and action in ("buy", "BUY"):
        try:
            async with pool.acquire() as conn:
                sc = await conn.fetchval("""
                    SELECT COUNT(*) FROM trade_history
                    WHERE bot='stock_trader' AND side='SELL' AND strategy LIKE '%손절%'
                      AND DATE(ts AT TIME ZONE 'Asia/Seoul') = (NOW() AT TIME ZONE 'Asia/Seoul')::date
                """)
            if int(sc or 0) >= 2:
                logger.info(f"🛑 당일 손절 {sc}회 — 신규 매수 강제 차단: {symbol}")
                return {"blocked": "daily_stop_loss_limit"}
        except Exception as e:
            logger.error(f"당일 손절횟수 확인 실패(안전을 위해 매수 차단): {e}")
            return {"error": "안전장치 확인 실패로 매수 보류"}

    return None


async def _finalize_filled_order(
    *, symbol: str, name: str, bot: str, action: str, action_kr: str, price: float, qty: float,
    strategy: str, reason: str, is_buy: bool, is_small: bool, reply: str,
    pre_avg_price: Optional[float], pool: Any, redis: Any, send_telegram_fn, log_journal_fn,
    save_trade_memory_fn, invalidate_cache_fn, label: str = "완료",
) -> Dict[str, Any]:
    """체결이 확인된 주문(정상 성공 또는 응답불명→보유수량 재확인으로 체결 확정)의 공통
    후처리: in-flight 기록, pnl 계산, trade_history/매매일지 기록, 텔레그램 보고, 캐시 무효화."""
    if is_buy and bot == "stock_trader" and redis is not None:
        try:
            await redis.setex(f"stark:inflight_buy:{symbol}", 120, "1")
        except Exception as e:
            logger.warning(f"inflight_buy 등록 실패: {e}")

    pnl = None
    pnl_rate = None
    pnl_text = ""
    if not is_buy and bot == "stock_trader" and pre_avg_price:
        pnl = (price - pre_avg_price) * qty
        pnl_rate = (price - pre_avg_price) / pre_avg_price * 100
        pnl_text = f"\n손익 {pnl:+,.0f}원 ({pnl_rate:+.1f}%)"

    if pool:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO trade_history (bot,asset_type,symbol,side,price,quantity,amount,strategy,pnl)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """, bot, "stock", symbol, action.upper(), float(price), float(qty), float(price * qty), strategy, pnl)

    emoji = "📈" if action == "buy" else "📉"
    msg = (
        f"{emoji} <b>{name} {action_kr} {label}</b>\n"
        f"가격: {price:,}원 × {qty:.0f}주\n"
        f"금액: {price*qty:,.0f}원{pnl_text}\n"
        f"전략: {strategy}\n"
        f"한강뷰매니저 판단: {reply[:80]}"
    )
    await send_telegram_fn(msg)
    logger.info(f"✅ Jarvis 자동 {action_kr}({label}): {symbol} {price:,}원 × {qty}주")
    try:
        for k in ("cache:positions:stock", "cache:account:stock"):
            await redis.delete(k)
        if is_buy:
            await redis.delete(f"buy_fail_streak:{symbol}")
    except Exception:
        pass

    if invalidate_cache_fn:
        try:
            if asyncio.iscoroutinefunction(invalidate_cache_fn):
                await invalidate_cache_fn()
            else:
                invalidate_cache_fn()
        except Exception:
            pass

    if save_trade_memory_fn:
        await save_trade_memory_fn(symbol=symbol, action=action_kr, price=float(price),
                                    amount=float(price * qty), result="성공", reason=reason)
    await log_journal_fn(bot, symbol, name, action, strategy, reason,
                          "EXECUTE_SMALL" if is_small else "EXECUTE",
                          reply, True, True, price, qty)
    result = {"success": True, "executed": True, "jarvis_reply": reply}
    if pnl is not None:
        result["pnl"] = pnl
        result["pnl_rate"] = pnl_rate
    return result


async def execute(
    signal: Dict[str, Any],
    decision: Dict[str, Any],
    *,
    pool: Any,
    redis: Any,
    kis_order_fn,
    send_telegram_fn,
    log_journal_fn,
    save_trade_memory_fn,
    code_to_name_fn,
    get_positions_fn: Optional[Any] = None,
    max_positions: Optional[int] = None,
    invalidate_cache_fn: Optional[Any] = None,
) -> Dict[str, Any]:
    """decision_engine이 EXECUTE/EXECUTE_SMALL로 승인한 신호를 실제 KIS 주문으로 집행.
    signal["qty"]는 호출부가 이미 EXECUTE_SMALL 절반 수량 보정을 마친 값이어야 한다
    (코인 경로와 수량 보정 로직을 공유해야 해서 dashboard/main.py 쪽에서 한 번만 수행)."""
    symbol = signal["symbol"]
    name = signal.get("name") or symbol
    bot = signal.get("bot", "stock_trader")
    action = signal.get("action", "buy")
    action_kr = "매수" if action == "buy" else "매도"
    price = signal.get("price", 0)
    qty = signal.get("qty", 0)
    strategy = signal.get("strategy", "")
    reason = signal.get("reason", "")
    is_small = decision.get("is_small", False)
    reply = decision.get("reply", "")
    is_buy = action in ("buy", "BUY")

    # (2) 매수 판단과 주문을 하나의 Lock으로 직렬화
    async with get_buy_lock():
        # (0) 직전 같은 종목·같은 방향 주문이 응답불명(uncertain) 상태로 끝나 재확인 대기
        # 중이면 중복 주문을 막는다 ([AT] order-result-reconcile)
        if redis is not None and await check_order_uncertain_block(symbol, is_buy, redis):
            reason_text = "직전 주문 결과 확인 중"
            logger.info(f"⏸️ {reason_text} — 주문 보류: {symbol}")
            if log_journal_fn:
                await log_journal_fn(bot, symbol, name, action, strategy, reason,
                                     "SKIP", reason_text, False, False, price, qty)
            return {
                "success": True,
                "executed": False,
                "skipped": True,
                "blocked": "order_uncertain_pending",
                "reason": reason_text,
                "jarvis_reply": reply,
            }

        # (1) 매수 경로에서 주문 직전에 보유 종목수 한도(fail-closed) + 물타기 정책을
        # 한 곳에서 검사 (check_buy_position_limits — buy_gate()와 공용)
        if is_buy and bot == "stock_trader" and get_positions_fn is not None:
            limit = max_positions
            if limit is None:
                limit = signal.get("max_positions")
            blocked = await check_buy_position_limits(
                symbol, price, pool=pool, redis=redis, bot=bot,
                get_positions_fn=get_positions_fn, max_positions=limit,
            )
            if blocked:
                block_code = blocked["blocked"]
                reason_text = blocked["reason"]
                logger.info(f"⏭️ {reason_text} — 매수 SKIP: {symbol} ({name})")

                if block_code == "positions_check_failed":
                    rationale = "실행 레이어 안전장치: KIS 보유 종목수 확인 실패/예외/stale 상태로 매수 차단(fail-closed)"
                elif block_code == "max_positions_limit":
                    rationale = f"실행 레이어 안전장치: 최대 보유 종목수({blocked.get('limit')}) 도달로 인한 매수 차단"
                else:
                    rationale = f"실행 레이어 안전장치: 보유중 종목 추가매수(물타기) 정책 위반({block_code})"

                from stark.decision_logger import log_decision
                await log_decision(
                    pool, symbol, "SKIP",
                    name=name,
                    confidence=decision.get("confidence", 0.0),
                    reason=reason_text,
                    rationale=rationale,
                    strategy=strategy,
                    source="execution_guard",
                    executed=False,
                    order_success=None,
                    price=float(price),
                    quantity=float(qty),
                )

                if log_journal_fn:
                    await log_journal_fn(bot, symbol, name, action, strategy, reason,
                                         "SKIP", reason_text, False, False, price, qty)

                return {
                    "success": True,
                    "executed": False,
                    "skipped": True,
                    "blocked": block_code,
                    "reason": reason_text,
                    "jarvis_reply": reply,
                }

        # 주문 전 보유 현황 사전 조회 — (a) 매도 손익(pnl) 계산용 평단가: 주문 성공 후에
        # 조회하면 전량 매도 시 이미 보유 목록에서 빠져 avg_price를 못 구해 pnl=None이 되므로
        # 주문 전에 미리 조회해 둔다. (b) 보유 수량(pre_qty): 주문 응답이 불명(uncertain)일 때
        # 체결 여부를 판정할 기준값([AT] order-result-reconcile). 조회 실패/미보유 시 None으로
        # 두고 주문 자체는 그대로 진행(안전 우선 원칙).
        pre_avg_price = None
        pre_qty = None
        if bot == "stock_trader" and get_positions_fn is not None:
            try:
                pos_res = await get_positions_fn()
                if isinstance(pos_res, dict) and pos_res.get("success", False):
                    pos_row = next(
                        (p for p in (pos_res.get("data") or [])
                         if isinstance(p, dict) and p.get("symbol") == symbol),
                        None,
                    )
                    pre_qty = float(pos_row.get("qty", 0) or 0) if pos_row else 0.0
                    if not is_buy:
                        pre_avg_price = float(pos_row.get("avg_price", 0) or 0) if pos_row else 0.0
            except Exception as e:
                logger.warning(f"주문 전 보유 현황 조회 실패 [{symbol}]: {e}")

        order = await kis_order_fn(symbol, int(price), int(qty), is_buy)

        if order.get("success"):
            return await _finalize_filled_order(
                symbol=symbol, name=name, bot=bot, action=action, action_kr=action_kr,
                price=price, qty=qty, strategy=strategy, reason=reason, is_buy=is_buy,
                is_small=is_small, reply=reply, pre_avg_price=pre_avg_price,
                pool=pool, redis=redis, send_telegram_fn=send_telegram_fn,
                log_journal_fn=log_journal_fn, save_trade_memory_fn=save_trade_memory_fn,
                invalidate_cache_fn=invalidate_cache_fn,
            )

        if order.get("uncertain"):
            logger.warning(f"⚠️ 주문 응답 불명 [{symbol}] — 보유 수량 재확인 시작: {order.get('error')}")
            recon = await reconcile_uncertain_order(
                symbol, is_buy, pre_qty=pre_qty, get_positions_fn=get_positions_fn,
                invalidate_cache_fn=invalidate_cache_fn, redis=redis,
            )
            if recon["status"] == "filled":
                diff_qty = recon.get("qty_diff") or qty
                return await _finalize_filled_order(
                    symbol=symbol, name=name, bot=bot, action=action, action_kr=action_kr,
                    price=price, qty=diff_qty, strategy=strategy, reason=reason, is_buy=is_buy,
                    is_small=is_small, reply=reply, pre_avg_price=pre_avg_price,
                    pool=pool, redis=redis, send_telegram_fn=send_telegram_fn,
                    log_journal_fn=log_journal_fn, save_trade_memory_fn=save_trade_memory_fn,
                    invalidate_cache_fn=invalidate_cache_fn, label="체결 확인(응답 지연)",
                )
            reason_text = "주문 결과 불명 — 보유 수량 변화 없음"
            logger.warning(f"⚠️ {reason_text}: {symbol}")
            await send_telegram_fn(
                f"⚠️ {name} {action_kr} 결과 불명 — 보유 수량 변화 없음. "
                f"미체결일 수 있으니 포트폴리오에서 확인 후 재지시하세요")
            await log_journal_fn(bot, symbol, name, action, strategy, reason,
                                  "EXECUTE_SMALL" if is_small else "EXECUTE",
                                  reply, True, False, price, qty)
            # 실패 억제(buy_fail_suppress 등)는 걸지 않는다 — 결과가 불명확할 뿐 실패가
            # 확정된 게 아니므로, 재지시가 들어오면 바로 다시 시도할 수 있어야 한다
            # (중복 주문 자체는 reconcile_uncertain_order가 건 order_uncertain 키가 막는다).
            return {"success": False, "executed": False, "uncertain": True, "reason": reason_text}

    err = str(order.get("error") or "")
    disp = await code_to_name_fn(symbol)

    from common.alert_throttle import check_cause_fail_throttle
    should_send, summary_disp = await check_cause_fail_throttle(
        action_kr, symbol, disp, err, min_interval_sec=3600, redis_client=redis
    )

    if action in ("sell", "SELL"):
        # '잔고 없음' 류 실패는 당일(6시간) 재시도·재알림 차단
        is_no_balance = any(k in err for k in ("잔고", "보유", "수량이 부족", "매도가능"))
        suppress_key = f"sell_fail_suppress:{symbol}"
        ttl = 6 * 3600 if is_no_balance else 1800  # 잔고 부족은 6시간, 일반 실패(손절 실패 등)는 30분
        try:
            already = await redis.get(suppress_key)
        except Exception:
            already = None
        if not already:
            try:
                await redis.setex(suppress_key, ttl, "1")
            except Exception:
                pass

        if should_send:
            if is_no_balance:
                await send_telegram_fn(
                    f"❌ {summary_disp} {action_kr} 실패\n{err}\n"
                    f"⚠️ 시스템 보유목록과 KIS 실계좌가 불일치할 수 있어요. "
                    f"보유목록 새로고침 후 계속 보이면 알려주세요. (당일 재시도 중단)")
            else:
                await send_telegram_fn(
                    f"❌ {summary_disp} {action_kr} 실패 (30분간 재시도 억제)\n{err}")
    else:
        # 매수 실패: 사유별 억제 시간 차등 적용(속도제한은 짧게, 연속 실패는 점진 확대) +
        # 원인 기준 스로틀(1시간 1건 묶음, N종목 요약)
        suppress_sec, fail_count = await _compute_buy_fail_suppress_sec(symbol, err, redis)
        suppress_key = f"buy_fail_suppress:{symbol}"
        try:
            already = await redis.get(suppress_key)
        except Exception:
            already = None
        if not already:
            try:
                await redis.setex(suppress_key, suppress_sec, "1")
            except Exception:
                pass

        if should_send:
            await send_telegram_fn(
                f"❌ {summary_disp} {action_kr} 실패 "
                f"({_format_suppress_sec(suppress_sec)} 재시도 억제, 연속 {fail_count}회)\n{err}")

    await log_journal_fn(bot, symbol, name, action, strategy, reason,
                          "EXECUTE_SMALL" if is_small else "EXECUTE",
                          reply, True, False, price, qty)
    return {"success": False, "executed": False, "error": order.get("error")}


# 투자경고/VI 미발동(정상) 기준값. mrkt_warn_cls_code는 stock_trader/main.py(1316행)와 동일하게
# "00"만 정상으로 본다. vi_cls_code는 코드베이스 내 실측 응답(tests/test_kis_market_warning.py)에서
# 정상 종목이 "N"으로 관측된 값만 확인되어 있어("확인 필요": 그 외 "발동" 코드 전체 목록은 KIS
# 공식 문서로 교차검증 필요) 그 외 값(빈 문자열 포함)은 보수적으로 차단한다.
_MRKT_WARN_OK_CODE = "00"
_VI_OK_CODE = "N"

# buy_gate()가 반환하는 차단 사유 코드 → 채팅 응답에 노출할 문구.
_PRECHECK_REASON_TEXT = {
    "buy_fail_suppress": "최근 매수 실패로 재시도가 잠시 제한돼 있어요",
    "bad_disclosure": "악재성 공시 종목이라 매수할 수 없습니다",
    "daily_stop_loss_limit": "당일 손절 2회 이상으로 오늘 신규 매수가 차단됐습니다",
}


async def buy_gate(
    symbol: str, price: float, *, pool: Any, redis: Any, bot: str = "stock_trader",
    get_positions_fn: Optional[Any] = None, get_market_warning_fn: Optional[Any] = None,
    max_positions: Optional[int] = None, allow_missing_deps: bool = False,
) -> Optional[Dict[str, str]]:
    """채팅 직접 매수(router/handlers/order_handler.handle_trade_command) 전용 매수 관문.
    신호 경로(stark/execution_guard.precheck + execute() 내 보유종목수/물타기 체크)와 동일한
    안전장치를 한 곳에서 검사한다(PM 지시, [AT] buy-gate-unification — router/handlers/
    order_handler.py가 execution_guard를 전혀 거치지 않아 신규·추가매수 안전장치가 채팅
    경로에서는 한 번도 적용되지 않았던 문제를 고친다).

    검사 순서: (1) precheck — 매수 실패 억제/악재성 공시/당일 손절 2회, (2) 보유 종목수
    한도(fail-closed)·물타기 정책(check_buy_position_limits), (3) 투자경고/VI 상태
    (fail-closed — 조회 실패 시 차단).

    get_positions_fn/get_market_warning_fn은 운영 ctx에서 반드시 주입해야 한다. 둘 중
    하나라도 None이면 조용히 건너뛰는 fail-open이 되지 않도록 기본적으로 매수를 차단한다
    (allow_missing_deps=True는 두 의존성과 무관한 분기만 검증하는 테스트 전용 플래그).

    반환: 통과 시 None. 차단 시 {"blocked": 사유코드, "reason": 사람이 읽을 문구}."""
    blocked = await precheck(symbol, "buy", bot, pool=pool, redis=redis)
    if blocked:
        if "error" in blocked:
            return {"blocked": "precheck_error", "reason": blocked["error"]}
        code = blocked.get("blocked", "precheck_blocked")
        return {"blocked": code, "reason": _PRECHECK_REASON_TEXT.get(code, "안전장치에 의해 매수가 차단됐습니다")}

    if get_positions_fn is None or get_market_warning_fn is None:
        if not allow_missing_deps:
            logger.error(
                f"buy_gate 의존성 누락(fail-closed) [{symbol}]: "
                f"get_positions_fn={'O' if get_positions_fn else 'X'}, "
                f"get_market_warning_fn={'O' if get_market_warning_fn else 'X'}"
            )
            return {"blocked": "gate_dependency_missing", "reason": "종목 상태 확인 실패 — 잠시 후 다시 시도"}

    if get_positions_fn is not None:
        position_blocked = await check_buy_position_limits(
            symbol, price, pool=pool, redis=redis, bot=bot,
            get_positions_fn=get_positions_fn, max_positions=max_positions,
        )
        if position_blocked:
            return position_blocked

    if get_market_warning_fn is not None:
        try:
            warning = await get_market_warning_fn(symbol)
        except Exception as e:
            logger.warning(f"투자경고/VI 조회 예외(안전을 위해 매수 차단) [{symbol}]: {e}")
            warning = None
        if not warning:
            return {"blocked": "market_warning_check_failed", "reason": "종목 상태 확인 실패 — 잠시 후 다시 시도"}
        warn_code = warning.get("mrkt_warn_cls_code", "")
        if warn_code != _MRKT_WARN_OK_CODE:
            return {"blocked": "investment_warning", "reason": "투자경고 종목이라 매수할 수 없습니다"}
        vi_code = warning.get("vi_cls_code", "")
        if vi_code != _VI_OK_CODE:
            return {"blocked": "vi_triggered", "reason": "VI(변동성완화장치) 발동 종목이라 매수할 수 없습니다"}

    return None
