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
        # (1) 매수 경로에서 주문 직전에 KIS 실제 보유 종목 수를 다시 조회
        if is_buy and bot == "stock_trader" and get_positions_fn is not None:
            try:
                pos_res = await get_positions_fn()
                if isinstance(pos_res, dict):
                    positions = pos_res.get("data") or []
                elif isinstance(pos_res, list):
                    positions = pos_res
                else:
                    positions = []

                limit = max_positions
                if limit is None:
                    limit = signal.get("max_positions")
                if limit is None:
                    limit = await _get_max_positions(pool, default=5)

                held_symbols = {
                    p.get("symbol") for p in positions
                    if isinstance(p, dict) and p.get("symbol")
                }
                is_already_held = symbol in held_symbols

                # 신규 종목 매수인데 이미 max_positions 이상이면 SKIP
                if not is_already_held and len(positions) >= limit:
                    skip_reason = f"최대 보유 종목수 한도 초과 ({len(positions)}/{limit})"
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
            except Exception as e:
                logger.error(f"보유 종목수 한도 확인 실패: {e}")

        order = await kis_order_fn(symbol, int(price), int(qty), is_buy)

        if order.get("success"):
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
