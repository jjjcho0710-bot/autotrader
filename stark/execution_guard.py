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
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger("stark.execution_guard")


async def precheck(symbol: str, action: str, bot: str, *, pool: Any, redis: Any) -> Optional[Dict[str, str]]:
    """AI 판단 호출 전 룰 기반 사전 차단.

    반환: 통과 시 None. 차단 시 {"blocked": 사유} (정상 차단) 또는 {"error": 메시지}
    (안전장치 확인 자체가 실패해서 보수적으로 막은 경우 — 호출부는 success=False로 응답해야 함)."""
    if action in ("sell", "SELL"):
        try:
            if await redis.get(f"sell_fail_suppress:{symbol}"):
                logger.info(f"⏸️ 매도 신호 무시 (당일 잔고없음 차단): {symbol}")
                return {"blocked": "sell_fail_suppress"}
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

    order = await kis_order_fn(symbol, int(price), int(qty), action in ("buy", "BUY"))

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

        await save_trade_memory_fn(symbol=symbol, action=action_kr, price=float(price),
                                    amount=float(price * qty), result="성공", reason=reason)
        await log_journal_fn(bot, symbol, name, action, strategy, reason,
                              "EXECUTE_SMALL" if is_small else "EXECUTE",
                              reply, True, True, price, qty)
        return {"success": True, "executed": True, "jarvis_reply": reply}

    err = str(order.get("error") or "")
    disp = await code_to_name_fn(symbol)
    # '잔고 없음' 류 실패는 재시도해도 소용없음 → 당일 재시도·재알림 차단 (반복 스팸 방지)
    is_no_balance = any(k in err for k in ("잔고", "보유", "수량이 부족", "매도가능"))
    suppress_key = f"sell_fail_suppress:{symbol}"
    if is_no_balance:
        try:
            already = await redis.get(suppress_key)
        except Exception:
            already = None
        if not already:
            try:
                await redis.setex(suppress_key, 6 * 3600, "1")
            except Exception:
                pass
            await send_telegram_fn(
                f"❌ {disp} {action_kr} 실패\n{err}\n"
                f"⚠️ 시스템 보유목록과 KIS 실계좌가 불일치할 수 있어요. "
                f"보유목록 새로고침 후 계속 보이면 알려주세요. (당일 재시도 중단)")
    else:
        await send_telegram_fn(f"❌ {disp} {action_kr} 실패\n{err}")

    await log_journal_fn(bot, symbol, name, action, strategy, reason,
                          "EXECUTE_SMALL" if is_small else "EXECUTE",
                          reply, True, False, price, qty)
    return {"success": False, "executed": False, "error": order.get("error")}
