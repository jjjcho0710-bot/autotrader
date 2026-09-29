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
        # (1) 매수 경로에서 주문 직전에 KIS 실제 보유 종목 수를 다시 조회 (실패 시 fail-closed)
        if is_buy and bot == "stock_trader" and get_positions_fn is not None:
            is_check_failed = False
            positions = []
            try:
                pos_res = await get_positions_fn()
                if not isinstance(pos_res, dict) or not pos_res.get("success", False) or pos_res.get("stale", False):
                    is_check_failed = True
                else:
                    positions = pos_res.get("data") or []
            except Exception as e:
                logger.error(f"보유 종목수 조회 예외(fail-closed 적용): {e}")
                is_check_failed = True

            # (1) fail-closed: 확인 불가 시 주문하지 않고 SKIP 처리
            if is_check_failed:
                skip_reason = "보유 종목수 확인 불가"
                logger.info(f"⏭️ {skip_reason} — 매수 SKIP: {symbol} ({name})")

                from stark.decision_logger import log_decision
                await log_decision(
                    pool, symbol, "SKIP",
                    name=name,
                    confidence=decision.get("confidence", 0.0),
                    reason=skip_reason,
                    rationale="실행 레이어 안전장치: KIS 보유 종목수 확인 실패/예외/stale 상태로 매수 차단(fail-closed)",
                    strategy=strategy,
                    source="execution_guard",
                    executed=False,
                    order_success=None,
                    price=float(price),
                    quantity=float(qty),
                )

                if log_journal_fn:
                    await log_journal_fn(bot, symbol, name, action, strategy, reason,
                                         "SKIP", skip_reason, False, False, price, qty)

                return {
                    "success": True,
                    "executed": False,
                    "skipped": True,
                    "blocked": "positions_check_failed",
                    "reason": skip_reason,
                    "jarvis_reply": reply,
                }

            # (2) 접수됐지만 잔고에 아직 안 나타난 in-flight 매수 추적 및 합집합 계산
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

            # 한도 계산: KIS 보유 종목과 in-flight 기록의 합집합
            total_held_symbols = kis_held_symbols | inflight_symbols
            is_already_held = symbol in total_held_symbols

            # 이미 보유 중인 종목에 대한 추가매수(물타기) 정책 강제 — AI가 EXECUTE/EXECUTE_SMALL로
            # 판단했더라도 (a) 종목당 평생 1회 한도, (b) 최초 매수가 대비 -3% 이내 조건 중
            # 하나라도 위반하면 코드 레벨에서 SKIP 처리한다.
            if is_already_held:
                block_reason = await _check_averaging_down_guard(pool, bot, symbol, price)
                if block_reason:
                    reason_text = {
                        "averaging_down_already_used": "물타기 정책: 종목당 평생 1회 한도 이미 사용",
                        "averaging_down_price_drop_exceeded": "물타기 정책: 최초 매수가 대비 -3% 초과 하락",
                        "averaging_down_no_entry_history": "물타기 정책: 최초 매수 이력 확인 불가",
                        "averaging_down_check_failed": "물타기 정책 확인 실패로 매수 보류",
                    }.get(block_reason, "물타기 정책 위반")
                    logger.info(f"⏭️ {reason_text} — 매수 SKIP: {symbol} ({name})")

                    from stark.decision_logger import log_decision
                    await log_decision(
                        pool, symbol, "SKIP",
                        name=name,
                        confidence=decision.get("confidence", 0.0),
                        reason=reason_text,
                        rationale=f"실행 레이어 안전장치: 보유중 종목 추가매수(물타기) 정책 위반({block_reason})",
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
                        "blocked": block_reason,
                        "reason": reason_text,
                        "jarvis_reply": reply,
                    }

            limit = max_positions
            if limit is None:
                limit = signal.get("max_positions")
            if limit is None:
                limit = await _get_max_positions(pool, default=5)

            # 신규 종목 매수인데 합집합이 max_positions 이상이면 SKIP
            if not is_already_held and len(total_held_symbols) >= limit:
                skip_reason = f"최대 보유 종목수 한도 초과 ({len(total_held_symbols)}/{limit})"
                logger.info(f"⏭️ {skip_reason} — 매수 SKIP: {symbol} ({name})")

                # (3) stark_decisions에 SKIP과 사유로 기록 (텔레그램 알림은 보내지 않음)
                from stark.decision_logger import log_decision
                await log_decision(
                    pool, symbol, "SKIP",
                    name=name,
                    confidence=decision.get("confidence", 0.0),
                    reason=skip_reason,
                    rationale=f"실행 레이어 안전장치: 최대 보유 종목수({limit}) 도달로 인한 매수 차단",
                    strategy=strategy,
                    source="execution_guard",
                    executed=False,
                    order_success=None,
                    price=float(price),
                    quantity=float(qty),
                )

                # 매매일지 기록 (텔레그램 알림은 호출하지 않음)
                if log_journal_fn:
                    await log_journal_fn(bot, symbol, name, action, strategy, reason,
                                         "SKIP", skip_reason, False, False, price, qty)

                return {
                    "success": True,
                    "executed": False,
                    "skipped": True,
                    "blocked": "max_positions_limit",
                    "reason": skip_reason,
                    "jarvis_reply": reply,
                }

        order = await kis_order_fn(symbol, int(price), int(qty), is_buy)

        if order.get("success"):
            # (2) 주문 접수 성공 즉시 해당 종목을 Redis(TTL 120초)에 in-flight buy로 기록
            if is_buy and bot == "stock_trader" and redis is not None:
                try:
                    await redis.setex(f"stark:inflight_buy:{symbol}", 120, "1")
                except Exception as e:
                    logger.warning(f"inflight_buy 등록 실패: {e}")
            if pool:
                async with pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO trade_history (bot,asset_type,symbol,side,price,quantity,amount,strategy)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    """, bot, "stock", symbol, action.upper(), float(price), float(qty), float(price * qty), strategy)

            emoji = "📈" if action == "buy" else "📉"
            msg = (
                f"{emoji} <b>{name} {action_kr} 완료</b>\n"
                f"가격: {price:,}원 × {qty}주\n"
                f"금액: {price*qty:,}원\n"
                f"전략: {strategy}\n"
                f"Jarvis 판단: {reply[:80]}"
            )
            await send_telegram_fn(msg)
            logger.info(f"✅ Jarvis 자동 {action_kr}: {symbol} {price:,}원 × {qty}주")
            try:
                for k in ("cache:positions:stock", "cache:account:stock"):
                    await redis.delete(k)
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
            return {"success": True, "executed": True, "jarvis_reply": reply}

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
        # 매수 실패: 30분 억제 키 설정 및 원인 기준 스로틀(1시간 1건 묶음, N종목 요약)
        suppress_key = f"buy_fail_suppress:{symbol}"
        try:
            already = await redis.get(suppress_key)
        except Exception:
            already = None
        if not already:
            try:
                await redis.setex(suppress_key, 1800, "1")
            except Exception:
                pass

        if should_send:
            await send_telegram_fn(
                f"❌ {summary_disp} {action_kr} 실패 (30분간 재시도 억제)\n{err}")

    await log_journal_fn(bot, symbol, name, action, strategy, reason,
                          "EXECUTE_SMALL" if is_small else "EXECUTE",
                          reply, True, False, price, qty)
    return {"success": False, "executed": False, "error": order.get("error")}
