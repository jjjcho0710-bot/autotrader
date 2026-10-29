"""uncertain(응답 불명) 주문의 후속 백그라운드 재확인.

배경: 10/14 005880 BUY 497주(@2135, 09:32:56)가 "결과 불명" 알림 후 실제로는
체결됐는데 DB에 기록이 누락됐다. stark/execution_guard.py reconcile_uncertain_order가
4초 1회 확인 후 status=unknown으로 끝나고 이후 자동 재확인이 없던 것이 원인이다.

이 모듈은 unknown으로 끝난 주문을 Redis에 등록해두고 +30초/+90초/+180초 체크포인트마다
보유수량을 주문 전 수량(pre_qty)과 다시 비교한다(common/late_fill_tracker.py와 같은
원리 — 프로세스 재시작에도 Redis에 상태가 남아 있어 재확인이 끊기지 않는다). 체결이
확인되면 trade_history/매매일지 기록 + 텔레그램 "체결 확인(지연)" 알림 + order_uncertain
차단 키 해제를 한다. 끝까지 확인 안 되면 "미확인 — 수동 확인 필요" 알림을 1회 보낸다.

KIS API 호출(토큰 발급 등)은 전혀 하지 않는다 — stark/execution_guard.reconcile_uncertain_order와
동일하게 dashboard가 주입하는 get_positions_fn/invalidate_cache_fn(보유수량 비교)과
get_filled_qty_fn(order_no 기반 체결수량 조회, inquire-daily-ccld)만 쓴다.

[AT] fix/uncertain-order-extended-tracking (PM 승인): 180초 체크포인트에서 추적을 끝내고
order_uncertain 차단 키를 지우던 구멍(180초 이후 체결을 놓치고 같은 종목 재주문 가능)을 없앤다.
  - 초기 체크포인트(30/90/180초) 이후에도 150초 간격으로 장 마감(15:30)까지 계속 추적한다.
  - 마감 후(15:30 + 60초) 1회 최종 확인을 하고, 그때 결과(체결 확정 또는 미확인 알림)로 추적을 종료한다.
  - order_uncertain 차단 키는 추적이 끝날 때까지 유지한다(폴링마다 TTL 갱신, 종료 시 삭제).
  - order_no가 있으면 inquire-daily-ccld 체결수량을 우선 쓰고, 없거나 0이면 보유수량 변화로 판정한다."""
import asyncio
import json
import logging
import time as _time
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger("common.uncertain_order_tracker")

UNCERTAIN_ORDER_KEY_PREFIX = "uncertain_order_pending:"
# stark/execution_guard.py의 ORDER_UNCERTAIN_BLOCK_TTL(180초) 안에서 세 번 재확인한다.
# 초기 빠른 체크포인트. 마지막(180초) 이후에는 종료하지 않고 연장 추적 단계로 넘어간다.
UNCERTAIN_ORDER_CHECKPOINTS_SEC = (30, 90, 180)
UNCERTAIN_ORDER_POLL_INTERVAL_SEC = 15
UNCERTAIN_ORDER_EXTENDED_INTERVAL_SEC = 150  # 연장 추적 간격
UNCERTAIN_ORDER_KEY_TTL_SEC = 12 * 3600  # 장 마감까지 + 재시작 대비(체크포인트마다 갱신)
UNCERTAIN_ORDER_MARKET_CLOSE = dtime(15, 30)
UNCERTAIN_ORDER_FINAL_DELAY_SEC = 60  # 마감 후 최종 확인은 15:30 + 60초 이후
UNCERTAIN_ORDER_FINAL_MAX_ATTEMPTS = 5  # 최종 확인 조회가 계속 실패하면 수동 확인 알림 후 종료
# order_uncertain 차단 키 TTL — 폴링(15초)마다 갱신하므로 짧게 둔다. 추적기가 죽으면 이 시간 뒤 풀린다.
UNCERTAIN_BLOCK_REFRESH_TTL_SEC = 300

KST = timezone(timedelta(hours=9))


def _now_kst() -> datetime:
    return datetime.now(KST)


def _is_after_close(registered_at: float) -> bool:
    """등록일(KST) 기준 마감(15:30) + 최종확인 지연(60초)이 지났는지."""
    reg_date = datetime.fromtimestamp(registered_at, KST).date()
    deadline = datetime.combine(reg_date, UNCERTAIN_ORDER_MARKET_CLOSE, tzinfo=KST) + timedelta(
        seconds=UNCERTAIN_ORDER_FINAL_DELAY_SEC)
    return _now_kst() >= deadline


async def _refresh_block_key(redis: Any, symbol: str, side: str) -> None:
    """같은 종목·방향 중복 주문 차단 키를 추적이 끝날 때까지 유지한다."""
    if redis is None:
        return
    try:
        await redis.setex(f"order_uncertain:{symbol}:{side}", UNCERTAIN_BLOCK_REFRESH_TTL_SEC, "1")
    except Exception as e:
        logger.warning(f"order_uncertain 차단 키 갱신 실패 [{symbol}:{side}]: {e}")


async def register_uncertain_order(
    redis: Any, *, symbol: str, name: str, is_buy: bool, bot: str,
    pre_qty: Optional[float], qty: float, price: float, strategy: str,
    reason: str = "", reply: str = "", order_no: Optional[str] = None,
    pre_avg_price: Optional[float] = None,
) -> None:
    """reconcile_uncertain_order의 즉시 확인(4초)으로도 체결 여부를 판정 못 했을 때 등록한다.
    pre_qty가 없으면(주문 전 보유 현황 조회 자체가 실패) 비교 기준이 없어 등록하지 않는다
    (기존처럼 "결과 불명" 1회 알림만 하고 끝난다)."""
    if redis is None or pre_qty is None:
        return
    side = "buy" if is_buy else "sell"
    registered_at = _time.time()
    ctx = {
        "symbol": symbol, "name": name, "side": side, "bot": bot,
        "pre_qty": pre_qty, "qty": qty, "price": price, "strategy": strategy,
        "reason": reason, "reply": reply, "order_no": order_no,
        "pre_avg_price": pre_avg_price,
        "registered_at": registered_at, "checkpoint_idx": 0,
    }
    key = f"{UNCERTAIN_ORDER_KEY_PREFIX}{order_no or int(registered_at * 1000)}"
    try:
        await redis.setex(key, UNCERTAIN_ORDER_KEY_TTL_SEC, json.dumps(ctx))
    except Exception as e:
        logger.warning(f"불명 주문 후속 재확인 등록 실패 [{symbol}]: {e}")
        return
    # 차단 키는 reconcile_uncertain_order가 180초 TTL로 이미 걸어 두었고, 추적 폴링(15초)이
    # 매번 갱신해 추적이 끝날 때까지 유지한다(_poll_one). 여기서 TTL을 덮어쓰지 않는다.
    logger.info(
        f"불명 주문 연장 추적 등록 [{symbol}] {side} {qty}주 order_no={order_no} "
        f"(초기 {UNCERTAIN_ORDER_CHECKPOINTS_SEC}초 → {UNCERTAIN_ORDER_EXTENDED_INTERVAL_SEC}초 간격, "
        f"{UNCERTAIN_ORDER_MARKET_CLOSE.strftime('%H:%M')}까지 + 마감 후 최종 확인)")


async def _get_held_qty(symbol: str, get_positions_fn, invalidate_cache_fn, redis: Any) -> Optional[float]:
    """최신 KIS 잔고를 기준으로 판정하기 위해 캐시를 먼저 무효화한 뒤 조회한다
    (stark.execution_guard.reconcile_uncertain_order의 즉시 확인과 동일한 원칙).
    조회 실패/stale이면 None(판정 불가 — 호출부는 재시도해야 한다, fail-safe)."""
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
    try:
        pos_res = await get_positions_fn()
    except Exception as e:
        logger.warning(f"불명 주문 재확인 중 보유 수량 조회 실패(fail-safe) [{symbol}]: {e}")
        return None
    if not isinstance(pos_res, dict) or not pos_res.get("success", False) or pos_res.get("stale", False):
        return None
    pos_row = next(
        (p for p in (pos_res.get("data") or [])
         if isinstance(p, dict) and p.get("symbol") == symbol),
        None,
    )
    return float(pos_row.get("qty", 0) or 0) if pos_row else 0.0


async def _already_recorded(pool: Any, *, bot: str, symbol: str, side: str, qty: float) -> bool:
    """같은 종목·방향·날짜·수량의 trade_history 행이 이미 있으면 True — 정산 보정
    (dashboard/kis_history_reconcile.py)이나 다른 경로가 먼저 기록했을 수 있어 이중
    기록을 막는다."""
    if pool is None:
        return False
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchval("""
                SELECT 1 FROM trade_history
                WHERE bot=$1 AND symbol=$2 AND side=$3 AND quantity=$4
                  AND DATE(ts AT TIME ZONE 'Asia/Seoul') = (NOW() AT TIME ZONE 'Asia/Seoul')::date
                LIMIT 1
            """, bot, symbol, side.upper(), float(qty))
        return row is not None
    except Exception as e:
        logger.warning(f"중복 기록 확인 실패(안전하게 기록 진행) [{symbol}]: {e}")
        return False


async def _finalize_confirmed(
    redis: Any, pool: Any, key: str, ctx: dict, qty_diff: float, send_telegram_fn, log_journal_fn,
    fill_price: Optional[float] = None,
) -> None:
    symbol = ctx["symbol"]
    name = ctx.get("name") or symbol
    side = ctx["side"]
    bot = ctx["bot"]
    # order_no 기반 체결평균가가 있으면 신호가 대신 쓴다.
    price = fill_price if (fill_price and fill_price > 0) else ctx["price"]
    strategy = ctx.get("strategy") or ""
    reason = ctx.get("reason") or ""
    reply = ctx.get("reply") or ""
    pre_avg_price = ctx.get("pre_avg_price")
    ordered_qty = ctx.get("qty") or 0
    db_side = "BUY" if side == "buy" else "SELL"

    if await _already_recorded(pool, bot=bot, symbol=symbol, side=db_side, qty=qty_diff):
        logger.info(f"불명 주문 재확인 — 이미 기록됨(중복 방지로 skip) [{symbol}]")
    else:
        pnl = None
        if side == "sell" and pre_avg_price:
            pnl = (price - pre_avg_price) * qty_diff
        if pool:
            try:
                async with pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO trade_history (bot,asset_type,symbol,side,price,quantity,amount,strategy,pnl)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    """, bot, "stock", symbol, db_side, float(price), float(qty_diff),
                        float(price) * qty_diff, strategy, pnl)
            except Exception as e:
                logger.warning(f"불명 주문 지연 체결 기록 실패 — 다음 주기 재시도 [{symbol}]: {e}")
                return  # 기록 실패 시 키를 지우지 않아야 다음 주기에 재시도된다
        if log_journal_fn:
            try:
                await log_journal_fn(bot, symbol, name, side, strategy, reason,
                                      "EXECUTE_UNCERTAIN_FILLED_LATE", reply, True, True,
                                      price, qty_diff)
            except Exception as e:
                logger.warning(f"불명 주문 지연 체결 매매일지 기록 실패(무시) [{symbol}]: {e}")
        shortfall = ordered_qty - qty_diff
        if shortfall > 0:
            logger.warning(
                f"불명 주문 마지막 체크포인트까지도 주문 수량 미달 — 확인된 증가분만 기록 "
                f"[{symbol}] 주문 {ordered_qty:.0f}주 중 {qty_diff:.0f}주만 확인(나머지 {shortfall:.0f}주 수동 확인 필요)"
            )
        try:
            await send_telegram_fn(
                f"✅ <b>체결 확인(지연) [{name}]</b>\n"
                f"{'매수' if side == 'buy' else '매도'} {qty_diff:.0f}주 @ {price:,.0f}원 "
                f"(응답 불명 후 백그라운드 재확인으로 확정)"
                + (
                    f"\n⚠️ 주문 {ordered_qty:.0f}주 중 {qty_diff:.0f}주만 확인됨 — "
                    f"나머지 {shortfall:.0f}주는 수동 확인 필요"
                    if shortfall > 0 else ""
                )
            )
        except Exception as e:
            logger.warning(f"불명 주문 지연 체결 알림 실패(무시) [{symbol}]: {e}")

    if redis is not None:
        try:
            await redis.delete(f"order_uncertain:{symbol}:{side}")
        except Exception:
            pass
        try:
            await redis.delete(key)
        except Exception:
            pass


async def _observe_fill(
    ctx: dict, get_positions_fn, invalidate_cache_fn, redis: Any,
    get_filled_qty_fn: Optional[Callable[[str, str, bool], Awaitable[tuple]]],
) -> tuple:
    """이 주문으로 체결된 수량을 관찰한다. 반환: (체결수량 또는 None, 체결평균가 또는 None).
    체결수량 None = 판정 불가(호출부는 같은 체크포인트로 재시도), 0 = 체결 없음.
    1순위 order_no 기반 체결조회(다른 거래·매도에 오염되지 않음). 없거나 0이면 보유수량 변화
    (기존 방식)로 보완한다."""
    symbol, side = ctx["symbol"], ctx["side"]
    is_buy = side == "buy"
    order_no = ctx.get("order_no")

    ccld_qty = None
    if get_filled_qty_fn is not None and order_no:
        try:
            fq, avg = await get_filled_qty_fn(order_no, symbol, is_buy)
            if fq is not None:
                ccld_qty = float(fq)
                if ccld_qty > 0:
                    return ccld_qty, (float(avg) if avg else None)
        except Exception as e:
            logger.warning(f"불명 주문 order_no 체결조회 실패(보유수량으로 보완) [{symbol}] {order_no}: {e}")

    post_qty = await _get_held_qty(symbol, get_positions_fn, invalidate_cache_fn, redis)
    if post_qty is None:
        return (0.0, None) if ccld_qty == 0 else (None, None)
    diff = post_qty - ctx["pre_qty"]
    matched = (diff > 0) if is_buy else (diff < 0)
    return (abs(diff) if matched else 0.0), None


async def _end_tracking(redis: Any, key: str, symbol: str, side: str) -> None:
    """추적 종료 — 이 시점에만 차단 키와 추적 키를 지운다."""
    for k in (f"order_uncertain:{symbol}:{side}", key):
        try:
            await redis.delete(k)
        except Exception:
            pass


async def _poll_one(
    redis: Any, pool: Any, key: str, get_positions_fn, invalidate_cache_fn, send_telegram_fn, log_journal_fn,
    get_filled_qty_fn=None,
) -> None:
    raw = await redis.get(key)
    if not raw:
        return
    ctx = json.loads(raw if isinstance(raw, str) else raw.decode())
    symbol = ctx["symbol"]
    side = ctx["side"]
    qty = ctx["qty"]
    name = ctx.get("name") or symbol
    registered_at = ctx["registered_at"]
    checkpoint_idx = ctx.get("checkpoint_idx", 0)
    n_early = len(UNCERTAIN_ORDER_CHECKPOINTS_SEC)
    is_buy = side == "buy"

    # 추적 중에는 차단 키를 계속 유지한다(15초 폴링마다 갱신).
    await _refresh_block_key(redis, symbol, side)

    now_ts = _time.time()
    closed = _is_after_close(registered_at)
    final = False
    if checkpoint_idx < n_early:
        if now_ts - registered_at < UNCERTAIN_ORDER_CHECKPOINTS_SEC[checkpoint_idx]:
            return  # 아직 다음 체크포인트 시각이 안 됐다
    elif closed:
        final = True  # 마감 후 1회 최종 확인
    else:
        last = ctx.get("last_check_at") or registered_at
        if now_ts - last < UNCERTAIN_ORDER_EXTENDED_INTERVAL_SEC:
            return

    filled, avg_price = await _observe_fill(
        ctx, get_positions_fn, invalidate_cache_fn, redis, get_filled_qty_fn)

    if filled is None:
        if not final:
            return  # 조회 실패 — 체크포인트를 넘기지 않고 다음 주기에 재시도(fail-safe)
        attempts = int(ctx.get("final_attempts", 0)) + 1
        if attempts < UNCERTAIN_ORDER_FINAL_MAX_ATTEMPTS:
            ctx["final_attempts"] = attempts
            try:
                await redis.setex(key, UNCERTAIN_ORDER_KEY_TTL_SEC, json.dumps(ctx))
            except Exception:
                pass
            logger.warning(f"불명 주문 최종 확인 조회 실패 — 재시도 {attempts}/{UNCERTAIN_ORDER_FINAL_MAX_ATTEMPTS} [{symbol}]")
            return
        try:
            await send_telegram_fn(
                f"⚠️ <b>{name} 주문 결과 미확인 — 수동 확인 필요</b>\n"
                f"{'매수' if is_buy else '매도'} {qty:.0f}주 주문이 응답 불명 후 마감 확인에서 "
                f"잔고/체결 조회가 계속 실패했습니다. 자동 재주문은 하지 않으니 직접 확인해 주세요."
            )
        except Exception as e:
            logger.warning(f"불명 주문 수동확인 알림 실패(무시) [{symbol}]: {e}")
        await _end_tracking(redis, key, symbol, side)
        return

    matched = filled > 0

    # 주문 수량을 채웠으면 확정. 부분 반영이면 주문 수량을 채우거나 마감 후 최종 확인에
    # 도달할 때까지 계속 추적한다(누적 값을 매번 다시 재므로 추가 체결분이 자동 반영된다,
    # [AT] fix/uncertain-order-partial-finalize).
    if matched and (filled >= qty or final):
        logger.info(f"불명 주문 체결 확정 [{symbol}] {side} {filled:.0f}/{qty:.0f}주 (final={final})")
        await _finalize_confirmed(redis, pool, key, ctx, filled, send_telegram_fn, log_journal_fn,
                                   fill_price=avg_price)
        return

    if final:
        try:
            await send_telegram_fn(
                f"⚠️ <b>{name} 주문 결과 미확인 — 수동 확인 필요</b>\n"
                f"{'매수' if is_buy else '매도'} {qty:.0f}주 주문이 응답 불명 후 장 마감"
                f"({UNCERTAIN_ORDER_MARKET_CLOSE.strftime('%H:%M')}) 뒤 최종 확인에서도 체결이 확인되지 않았습니다. "
                f"자동 재주문은 하지 않으니 직접 확인해 주세요."
            )
        except Exception as e:
            logger.warning(f"불명 주문 수동확인 알림 실패(무시) [{symbol}]: {e}")
        await _end_tracking(redis, key, symbol, side)
        return

    # 주문 수량 미달 & 마감 전 — 계속 추적한다(차단 키 유지, 종료 금지).
    if checkpoint_idx < n_early:
        checkpoint_idx += 1
        ctx["checkpoint_idx"] = checkpoint_idx
    ctx["last_check_at"] = now_ts
    ctx["observed_qty"] = filled
    if checkpoint_idx == n_early and not ctx.get("interim_notified") and not closed:
        ctx["interim_notified"] = True
        try:
            await send_telegram_fn(
                f"⏳ <b>{name} 주문 체결 미확인 — 계속 추적 중</b>\n"
                f"{'매수' if is_buy else '매도'} {qty:.0f}주 주문이 {UNCERTAIN_ORDER_CHECKPOINTS_SEC[-1]}초 안에 "
                f"확인되지 않아 {UNCERTAIN_ORDER_EXTENDED_INTERVAL_SEC}초 간격으로 "
                f"{UNCERTAIN_ORDER_MARKET_CLOSE.strftime('%H:%M')}까지 계속 확인합니다. "
                f"같은 종목 {'매수' if is_buy else '매도'}는 추적이 끝날 때까지 보류됩니다."
            )
        except Exception as e:
            logger.warning(f"불명 주문 연장추적 안내 알림 실패(무시) [{symbol}]: {e}")
    logger.info(f"불명 주문 연장 추적 계속 [{symbol}] {side} 확인 {filled:.0f}/{qty:.0f}주 idx={checkpoint_idx}")
    try:
        await redis.setex(key, UNCERTAIN_ORDER_KEY_TTL_SEC, json.dumps(ctx))
    except Exception:
        pass


async def poll_uncertain_orders_once(
    redis: Any, pool: Any, get_positions_fn, invalidate_cache_fn, send_telegram_fn, log_journal_fn,
    get_filled_qty_fn=None,
) -> None:
    if redis is None:
        return
    try:
        keys = [k async for k in redis.scan_iter(match=f"{UNCERTAIN_ORDER_KEY_PREFIX}*")]
    except Exception as e:
        logger.warning(f"불명 주문 추적 키 조회 실패: {e}")
        return
    for key in keys:
        try:
            await _poll_one(redis, pool, key, get_positions_fn, invalidate_cache_fn,
                            send_telegram_fn, log_journal_fn, get_filled_qty_fn)
        except Exception as e:
            logger.warning(f"불명 주문 추적 처리 오류 [{key}]: {e}")


async def uncertain_order_tracker_loop(
    redis: Any, pool: Any, get_positions_fn, invalidate_cache_fn, send_telegram_fn, log_journal_fn,
    get_filled_qty_fn=None,
) -> None:
    while True:
        await asyncio.sleep(UNCERTAIN_ORDER_POLL_INTERVAL_SEC)
        try:
            await poll_uncertain_orders_once(
                redis, pool, get_positions_fn, invalidate_cache_fn, send_telegram_fn, log_journal_fn,
                get_filled_qty_fn)
        except Exception as e:
            logger.warning(f"불명 주문 추적 루프 오류: {e}")
