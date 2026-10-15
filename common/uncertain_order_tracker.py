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
동일하게 dashboard가 주입하는 get_positions_fn/invalidate_cache_fn으로 보유수량만 비교한다."""
import asyncio
import json
import logging
import time as _time
from typing import Any, Optional

logger = logging.getLogger("common.uncertain_order_tracker")

UNCERTAIN_ORDER_KEY_PREFIX = "uncertain_order_pending:"
# stark/execution_guard.py의 ORDER_UNCERTAIN_BLOCK_TTL(180초) 안에서 세 번 재확인한다.
UNCERTAIN_ORDER_CHECKPOINTS_SEC = (30, 90, 180)
UNCERTAIN_ORDER_POLL_INTERVAL_SEC = 15
UNCERTAIN_ORDER_KEY_TTL_SEC = 1800  # 마지막 체크포인트(180초)보다 충분히 길게(재시작 대비)


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
) -> None:
    symbol = ctx["symbol"]
    name = ctx.get("name") or symbol
    side = ctx["side"]
    bot = ctx["bot"]
    price = ctx["price"]
    strategy = ctx.get("strategy") or ""
    reason = ctx.get("reason") or ""
    reply = ctx.get("reply") or ""
    pre_avg_price = ctx.get("pre_avg_price")
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
        try:
            await send_telegram_fn(
                f"✅ <b>체결 확인(지연) [{name}]</b>\n"
                f"{'매수' if side == 'buy' else '매도'} {qty_diff:.0f}주 @ {price:,.0f}원 "
                f"(응답 불명 후 백그라운드 재확인으로 확정)"
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


async def _poll_one(
    redis: Any, pool: Any, key: str, get_positions_fn, invalidate_cache_fn, send_telegram_fn, log_journal_fn,
) -> None:
    raw = await redis.get(key)
    if not raw:
        return
    ctx = json.loads(raw if isinstance(raw, str) else raw.decode())
    symbol = ctx["symbol"]
    side = ctx["side"]
    pre_qty = ctx["pre_qty"]
    qty = ctx["qty"]
    registered_at = ctx["registered_at"]
    checkpoint_idx = ctx.get("checkpoint_idx", 0)
    if checkpoint_idx >= len(UNCERTAIN_ORDER_CHECKPOINTS_SEC):
        return  # 안전망 — 정상 경로로는 마지막 체크포인트에서 키를 지우므로 도달하지 않는다

    elapsed = _time.time() - registered_at
    if elapsed < UNCERTAIN_ORDER_CHECKPOINTS_SEC[checkpoint_idx]:
        return  # 아직 다음 체크포인트 시각이 안 됐다

    is_buy = side == "buy"
    post_qty = await _get_held_qty(symbol, get_positions_fn, invalidate_cache_fn, redis)
    if post_qty is None:
        return  # 조회 실패 — 체크포인트를 넘기지 않고 다음 주기에 같은 체크포인트로 재시도

    diff = post_qty - pre_qty
    if (diff > 0) if is_buy else (diff < 0):
        await _finalize_confirmed(redis, pool, key, ctx, abs(diff), send_telegram_fn, log_journal_fn)
        return

    checkpoint_idx += 1
    if checkpoint_idx >= len(UNCERTAIN_ORDER_CHECKPOINTS_SEC):
        name = ctx.get("name") or symbol
        try:
            await send_telegram_fn(
                f"⚠️ <b>{name} 주문 결과 미확인 — 수동 확인 필요</b>\n"
                f"{'매수' if is_buy else '매도'} {qty:.0f}주 주문이 응답 불명 후 "
                f"{UNCERTAIN_ORDER_CHECKPOINTS_SEC[-1]}초가 지나도 체결 확인이 되지 않았습니다. "
                f"자동 재주문은 하지 않으니 직접 확인해 주세요."
            )
        except Exception as e:
            logger.warning(f"불명 주문 수동확인 알림 실패(무시) [{symbol}]: {e}")
        try:
            await redis.delete(key)
        except Exception:
            pass
        return

    ctx["checkpoint_idx"] = checkpoint_idx
    try:
        await redis.setex(key, UNCERTAIN_ORDER_KEY_TTL_SEC, json.dumps(ctx))
    except Exception:
        pass


async def poll_uncertain_orders_once(
    redis: Any, pool: Any, get_positions_fn, invalidate_cache_fn, send_telegram_fn, log_journal_fn,
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
            await _poll_one(redis, pool, key, get_positions_fn, invalidate_cache_fn, send_telegram_fn, log_journal_fn)
        except Exception as e:
            logger.warning(f"불명 주문 추적 처리 오류 [{key}]: {e}")


async def uncertain_order_tracker_loop(
    redis: Any, pool: Any, get_positions_fn, invalidate_cache_fn, send_telegram_fn, log_journal_fn,
) -> None:
    while True:
        await asyncio.sleep(UNCERTAIN_ORDER_POLL_INTERVAL_SEC)
        try:
            await poll_uncertain_orders_once(
                redis, pool, get_positions_fn, invalidate_cache_fn, send_telegram_fn, log_journal_fn)
        except Exception as e:
            logger.warning(f"불명 주문 추적 루프 오류: {e}")
