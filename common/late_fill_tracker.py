"""부분체결로 trade_history에 일부만 기록된 주문이 이후 추가로 체결되는 수량을 추적해
증가분만 trade_history에 더 기록한다.

배경: 10/8 부국철강 100주 매도 주문이 접수 후 34주, 44주로 늘다가 결국 100주 전량 체결됐다.
dashboard의 _kis_stock_order는 주문 후 최대 약 11초 안에 확인된 체결 수량만 기록하므로
그 이후에 체결되는 수량은 trade_history에 남지 않았다. 이 모듈은 주문 당시 부분체결로
기록된 주문을 Redis에 등록해두고, 백그라운드에서 주기적으로 체결 수량을 다시 조회해
늘어난 만큼만 추가로 INSERT한다(기존 행은 수정하지 않음) — stock_trader/main.py의
_reconcile_pending_orders_once(완전 미체결 추적)와 같은 원리를 부분체결 후속 추적에 적용한
것. KIS API 호출(토큰 발급, inquire-daily-ccld 조회)은 dashboard/main.py가 콜러블로
주입한다 — 이 모듈 자체는 KIS API를 직접 호출하지 않는다."""
import asyncio
import json
import logging
import time as _time
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger("common.late_fill_tracker")

KST = timezone(timedelta(hours=9))
LATE_FILL_KEY_PREFIX = "late_fill_pending:"
LATE_FILL_POLL_INTERVAL_SEC = 150  # 2.5분
LATE_FILL_KEY_TTL_SEC = 6 * 3600  # 프로세스 재시작 등으로 추적이 끊겨도 Redis에 영원히 남지 않게


async def register_partial_fill(
    redis: Any, *, order_no: str, symbol: str, side: str, bot: str,
    requested_qty: float, recorded_qty: float, strategy: str, price: float,
    cost_basis_avg_price: Optional[float] = None,
) -> None:
    """부분체결로 trade_history에 recorded_qty만 기록한 주문을 후속 추적 대상으로 등록한다.
    이미 주문 수량만큼 다 기록됐으면(recorded_qty >= requested_qty) 등록하지 않는다.

    cost_basis_avg_price: 매도 주문일 때 pnl 계산에 쓸 포지션 평균단가(주문 전 보유 평단가).
    매수거나 평단가를 모르면 None — 이후 증가분 pnl은 계산하지 않는다."""
    if redis is None or not order_no or recorded_qty >= requested_qty:
        return
    ctx = {
        "order_no": order_no, "symbol": symbol, "side": side, "bot": bot,
        "requested_qty": requested_qty, "recorded_qty": recorded_qty,
        "strategy": strategy, "price": price,
        "cost_basis_avg_price": cost_basis_avg_price,
        "registered_at": _time.time(), "post_close_check_done": False,
    }
    try:
        await redis.setex(f"{LATE_FILL_KEY_PREFIX}{order_no}", LATE_FILL_KEY_TTL_SEC, json.dumps(ctx))
    except Exception as e:
        logger.warning(f"지연 추가체결 추적 등록 실패 [{symbol}] 주문 {order_no}: {e}")


def _is_market_closed_kst(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(KST)
    return now.time() >= dtime(15, 30)


async def _poll_one(
    redis: Any, pool: Any, key: str,
    get_filled_qty_fn: Callable[[str, str, bool], Awaitable[tuple]],
    send_telegram_fn: Callable[[str], Awaitable[None]],
) -> None:
    raw = await redis.get(key)
    if not raw:
        return
    ctx = json.loads(raw if isinstance(raw, str) else raw.decode())
    order_no = ctx["order_no"]
    symbol = ctx["symbol"]
    side = ctx["side"]
    bot = ctx["bot"]
    requested_qty = ctx["requested_qty"]
    recorded_qty = ctx["recorded_qty"]
    strategy = ctx.get("strategy") or ""
    fallback_price = ctx.get("price")
    cost_basis_avg_price = ctx.get("cost_basis_avg_price")
    is_buy = side == "BUY"

    filled_qty, avg_price = await get_filled_qty_fn(order_no, symbol, is_buy)
    if filled_qty is None:
        logger.warning(f"지연 추가체결 재조회 실패 — 다음 주기 재시도 [{symbol}] 주문 {order_no}")
        return

    increment = filled_qty - recorded_qty
    if increment > 0:
        price = avg_price or fallback_price or 0
        pnl = None
        if side == "SELL" and cost_basis_avg_price is not None:
            pnl = (price - cost_basis_avg_price) * increment
        if pool:
            try:
                async with pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO trade_history (bot,asset_type,symbol,side,price,quantity,amount,strategy,pnl)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    """, bot, "stock", symbol, side, float(price), float(increment),
                        float(price * increment), strategy, pnl)
            except Exception as e:
                logger.warning(f"지연 추가체결 trade_history 기록 실패 [{symbol}] 주문 {order_no}: {e}")
                return  # 기록 실패 시 recorded_qty를 올리지 않아야 다음 주기에 재시도된다
        recorded_qty = filled_qty
        ctx["recorded_qty"] = recorded_qty
        try:
            await send_telegram_fn(
                f"➕ <b>추가 체결 [{symbol}]</b> +{increment:.0f}주 "
                f"(누적 {recorded_qty:.0f}/{requested_qty:.0f}주)"
            )
        except Exception as e:
            logger.warning(f"지연 추가체결 알림 전송 실패 [{symbol}]: {e}")

    if recorded_qty >= requested_qty:
        await redis.delete(key)
        return

    if _is_market_closed_kst():
        if ctx.get("post_close_check_done"):
            await redis.delete(key)
            return
        ctx["post_close_check_done"] = True

    try:
        await redis.setex(key, LATE_FILL_KEY_TTL_SEC, json.dumps(ctx))
    except Exception:
        pass


async def poll_late_fills_once(
    redis: Any, pool: Any,
    get_filled_qty_fn: Callable[[str, str, bool], Awaitable[tuple]],
    send_telegram_fn: Callable[[str], Awaitable[None]],
) -> None:
    if redis is None:
        return
    try:
        keys = [k async for k in redis.scan_iter(match=f"{LATE_FILL_KEY_PREFIX}*")]
    except Exception as e:
        logger.warning(f"지연 추가체결 키 조회 실패: {e}")
        return
    for key in keys:
        try:
            await _poll_one(redis, pool, key, get_filled_qty_fn, send_telegram_fn)
        except Exception as e:
            logger.warning(f"지연 추가체결 처리 오류 [{key}]: {e}")


async def late_fill_tracker_loop(
    redis: Any, pool: Any,
    get_filled_qty_fn: Callable[[str, str, bool], Awaitable[tuple]],
    send_telegram_fn: Callable[[str], Awaitable[None]],
) -> None:
    while True:
        await asyncio.sleep(LATE_FILL_POLL_INTERVAL_SEC)
        try:
            await poll_late_fills_once(redis, pool, get_filled_qty_fn, send_telegram_fn)
        except Exception as e:
            logger.warning(f"지연 추가체결 추적 루프 오류: {e}")
