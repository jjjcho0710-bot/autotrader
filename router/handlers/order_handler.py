"""
router/handlers/order_handler.py - 매매 승인/거절 및 직접 매매 명령 처리기

dashboard/main.py의 _jarvis_chat_impl 내 3개 블록을 이관:
- 능동 제안(advice) 승인/거절 (구 4438행)
- 매수 제안(proposal) 승인/거절 (구 4464행)
- 채팅 직접 매매 지시 "종목 N주 매수/매도" → _handle_trade_command (구 4017행)

KIS 실주문 실행(_kis_stock_order), 시세·잔고 조회, 텔레그램 발송, 매매일지 기록 등은
전부 dashboard/main.py가 들고 있는 함수를 콜러블로 주입받는다 — 이 모듈 자체는 KIS API를
직접 호출하지 않는다(실행 인프라와 라우팅 로직을 분리).
"""
import json
import logging
import re
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Any, Optional

from common.position_sizing import (
    DEFAULT_STOP_LOSS_PCT,
    compute_atr_pct,
    compute_base_amount,
    volatility_multiplier,
)
from stark.execution_guard import (
    buy_gate,
    check_order_uncertain_block,
    get_held_qty,
    reconcile_uncertain_order,
)

logger = logging.getLogger("router.handlers.order")

KST = timezone(timedelta(hours=9))

_RE_PROP = re.compile(r"(승인|오케이|오케|ok|ㅇㅋ|사자|매수 ?해|매수 ?하자|고고|거절|취소해|사지 ?마)", re.I)
_RE_REJECT = re.compile(r"(거절|취소해|사지 ?마|안 ?사)")


def _fill_channel_summary(name: str, symbol: str, action_kr: str, qty, price: float,
                           pnl_rate: Optional[float] = None) -> str:
    """매수·매도 체결 알림의 채널용 한 줄 요약 — 금액(평가손익 등) 없이 가격·수량·손익률(%)만
    ([AT] feat/telegram-routing). 가격은 종목 단가(공개 시세)라 계좌 잔고 규모를 드러내지 않는다."""
    emoji = "📈" if action_kr == "매수" else "📉"
    line = f"{emoji} {name}({symbol}) {action_kr} {qty}주 @ {price:,.0f}원"
    if pnl_rate is not None:
        line += f" ({pnl_rate:+.1f}%)"
    return line


async def handle_advice_response(
    user_msg: str, *, redis: Any, jarvis_chat_fn, send_telegram_fn, session_id: str,
) -> Optional[str]:
    """능동 제안 승인/거절: "승인 2" / "거절 1" """
    um = user_msg.strip()
    m = re.match(r"^(승인|거절|오케이|ok)\s*(\d)\s*$", um, re.I)
    if not m:
        return None
    n = m.group(2)
    raw = await redis.get(f"advice:{n}")
    if not raw:
        return f"제안 {n}은(는) 없거나 만료됐어요."

    it = json.loads(raw if isinstance(raw, str) else raw.decode())
    await redis.delete(f"advice:{n}")
    if m.group(1) == "거절":
        return f"❌ 제안 {n} '{it.get('title')}' 거절했어요."

    cmd = it.get("command", "")
    is_trade = ("매수" in cmd or "매도" in cmd) and not cmd.startswith("지시:")
    now = datetime.now(KST)
    is_open = (now.weekday() < 5 and dtime(9, 0) <= now.time().replace(tzinfo=None) <= dtime(15, 20))
    if is_trade and not is_open:
        await redis.rpush("advice:queue", json.dumps({"command": cmd, "title": it.get("title", "")}, ensure_ascii=False))
        out = f"⏰ 제안 {n} 승인 — 장외라 다음 개장(09:01)에 자동 실행 예약: {cmd}"
        await send_telegram_fn(out, dest="personal")
        return out

    sub = await jarvis_chat_fn({"message": cmd, "session_id": session_id, "_no_mirror": True})
    rep = sub.get("reply") or sub.get("error") or "실행 결과 없음"
    out = f"✅ 제안 {n} 승인 → 실행: {cmd}\n{rep}"
    await send_telegram_fn(out, dest="personal")
    return out


async def handle_proposal_response(
    user_msg: str, *, redis: Any, kis_order_fn, log_journal_fn, send_telegram_fn,
    get_positions_fn: Optional[Any] = None, pool: Any = None,
    invalidate_cache_fn: Optional[Any] = None,
) -> Optional[str]:
    """매수 제안(proposal:{symbol}) 승인/거절 처리"""
    from common.telegram import CHANNEL_SLIM  # 지연 임포트(테스트 환경에 aiohttp 없어도 동작)
    um = user_msg.strip()
    if not _RE_PROP.search(um):
        return None
    try:
        target = None
        latest = await redis.get("proposal:latest")
        latest = latest if isinstance(latest, str) else (latest or b"").decode()
        keys = [k if isinstance(k, str) else k.decode() for k in await redis.keys("proposal:*")]
        cands = [k.split(":", 1)[1] for k in keys if not k.startswith("proposal:cool") and k != "proposal:latest"]
        for sym_ in cands:
            raw = await redis.get(f"proposal:{sym_}")
            pj = json.loads(raw if isinstance(raw, str) else raw.decode())
            if pj.get("name") and pj["name"] in um:
                target = pj
                break
        if not target and latest:
            raw = await redis.get(f"proposal:{latest}")
            if raw:
                target = json.loads(raw if isinstance(raw, str) else raw.decode())
        if not target:
            return None  # 대기 제안 없음 → 일반 대화로 진행

        symbol = target["symbol"]

        if _RE_REJECT.search(um):
            await redis.delete(f"proposal:{symbol}")
            return f"❌ {target['name']} 매수 제안 거절 처리했어요."

        # 직전 같은 종목 매수 주문이 응답불명으로 끝나 재확인 대기 중이면 중복 주문 차단
        if await check_order_uncertain_block(symbol, True, redis):
            return f"⏳ {target['name']} 직전 주문 결과 확인 중 — 잠시 후 다시 시도해주세요"

        # 승인 → 실제 매수 (주문 전 보유 수량을 응답불명 시 체결 재확인용으로 미리 조회)
        pre_qty = await get_held_qty(symbol, get_positions_fn) if get_positions_fn else None
        order = await kis_order_fn(symbol, int(target["price"]), int(target["qty"]), True)
        await redis.delete(f"proposal:{symbol}")
        if order.get("success"):
            await log_journal_fn("stock_trader", symbol, target["name"], "buy",
                                  target.get("strategy", "제안"), "주인 승인", "PROPOSE_APPROVED",
                                  target.get("reason", ""), True, True,
                                  int(target["price"]), int(target["qty"]))
            msg = (f"✅ <b>{target['name']} 매수 체결 (주인 승인)</b>\n"
                   f"{target['qty']}주 @ {int(target['price']):,}원")
            await send_telegram_fn(msg, dest="personal")
            if not CHANNEL_SLIM:
                await send_telegram_fn(
                    _fill_channel_summary(target["name"], symbol, "매수", target["qty"], float(target["price"])),
                    dest="channel")
            return msg.replace("<b>", "").replace("</b>", "")

        if order.get("uncertain") and get_positions_fn is not None:
            recon = await reconcile_uncertain_order(
                symbol, True, pre_qty=pre_qty, get_positions_fn=get_positions_fn, redis=redis,
                invalidate_cache_fn=invalidate_cache_fn)
            if recon["status"] == "filled":
                diff_qty = int(round(recon.get("qty_diff") or target["qty"]))
                if pool:
                    try:
                        async with pool.acquire() as conn:
                            await conn.execute("""
                                INSERT INTO trade_history (bot,asset_type,symbol,side,price,quantity,amount,strategy,pnl)
                                VALUES ('stock_trader','stock',$1,'BUY',$2,$3,$4,$5,NULL)
                            """, symbol, float(target["price"]), float(diff_qty),
                                float(target["price"]) * diff_qty, target.get("strategy", "제안"))
                    except Exception:
                        pass
                await log_journal_fn("stock_trader", symbol, target["name"], "buy",
                                      target.get("strategy", "제안"), "주인 승인(응답지연)",
                                      "PROPOSE_APPROVED_UNCERTAIN_FILLED",
                                      target.get("reason", ""), True, True,
                                      int(target["price"]), diff_qty)
                msg = (f"✅ <b>{target['name']} 매수 체결 확인 (응답 지연, 주인 승인)</b>\n"
                       f"{diff_qty}주 @ {int(target['price']):,}원")
                await send_telegram_fn(msg, dest="personal")
                if not CHANNEL_SLIM:
                    await send_telegram_fn(
                        _fill_channel_summary(target["name"], symbol, "매수", diff_qty, float(target["price"])),
                        dest="channel")
                return msg.replace("<b>", "").replace("</b>", "")
            return (f"⚠️ {target['name']} 주문 결과 불명 — 보유 수량 변화 없음. "
                    f"미체결일 수 있으니 포트폴리오에서 확인 후 재지시하세요")

        return f"❌ 매수 실패: {order.get('error') or '사유 미확인'}"
    except Exception as pe:
        logger.warning(f"제안 승인 처리 오류: {pe}")
        return None


async def _compute_buy_sizing_cap(
    *, config: Any, get_balance_fn, get_recent_ohlcv_fn, symbol: str, cur_price: float,
) -> Optional[float]:
    """채팅 직접 매수 사이징 상한(원) = 기본금액(자산×RISK_PER_TRADE_PCT÷|손절률|) × ATR 변동성배율
    (common/position_sizing.py 공용 로직, PM 승인 2026-09-30).
    get_balance_fn 미주입(테스트 등 사이징 의존성 없는 호출) 시 None을 반환해 호출부가
    사이징을 건너뛰게 한다. 채팅 직접 매매는 특정 전략에 묶이지 않으므로 손절률은
    strategy_config 조회 없이 참고값(DEFAULT_STOP_LOSS_PCT)을 쓴다."""
    if get_balance_fn is None:
        return None
    risk_pct = getattr(config, "RISK_PER_TRADE_PCT", None)
    if risk_pct is None:
        return None
    balance = await get_balance_fn() or {}
    equity = float(balance.get("total") or 0) or float(getattr(config, "INITIAL_SEED_KRW", 0) or 0)
    if equity <= 0:
        return None
    base_amount = compute_base_amount(equity, DEFAULT_STOP_LOSS_PCT, risk_pct)
    atr_pct = None
    if get_recent_ohlcv_fn is not None:
        try:
            rows = await get_recent_ohlcv_fn(symbol)
            atr_pct = compute_atr_pct(rows, cur_price)
        except Exception as e:
            logger.warning(f"수동주문 ATR 조회 실패 [{symbol}]: {e}")
    return base_amount * volatility_multiplier(atr_pct)


async def handle_trade_command(
    user_msg: str, *, pool: Any, redis: Any, universe: Any, get_kis_token_fn, config: Any,
    kis_order_fn, get_stock_positions_fn, send_telegram_fn, log_journal_fn,
    get_balance_fn=None, get_recent_ohlcv_fn=None, get_market_warning_fn=None,
    get_quote_fn=None, invalidate_cache_fn=None,
) -> Optional[str]:
    """채팅에서 '종목 N주 매수/매도' 명령 → 실제 KIS 주문 실행. 해당 없으면 None"""
    from common.telegram import CHANNEL_SLIM  # 지연 임포트(테스트 환경에 aiohttp 없어도 동작)
    msg = user_msg.strip()
    is_buy = bool(re.search(r"(매수|사자|사줘|사라)", msg))
    is_sell = bool(re.search(r"(매도|팔아|팔자|팔아줘)", msg))
    if not (is_buy or is_sell):
        return None
    qty_m = re.search(r"(\d+)\s*주", msg)
    all_sell = "전량" in msg or "다 팔" in msg
    if not qty_m and not all_sell:
        return None  # 수량 없는 문장은 일반 대화로

    symbol, name = await universe.resolve_symbol(msg)
    if not symbol:
        return "⚠️ 종목을 특정할 수 없어요. 종목코드 6자리 또는 감시종목 이름으로 다시 지시해주세요. (예: 000660 2주 매수)"

    action = "buy" if is_buy else "sell"
    action_kr = "매수" if is_buy else "매도"

    # 현재가 — dashboard의 KIS 속도제한 대응 시세 조회(_fetch_kis_inquire_price →
    # _kis_quote_get, 최대 2회 재시도)를 RouterContext로 주입받아 재사용한다
    # ([AT] fix/chat-price-lookup). 예전엔 재시도 없는 단발 조회라 KIS 속도제한(EGW00201)에
    # 걸리면 output이 비어 price=0 → "현재가 조회 실패"로 끝났다(10/1 SK텔레콤, 10/2 부국철강
    # 실측). get_market_warning_fn과 동일하게 안전 관련 의존성이라, 운영 ctx에서 미주입이면
    # 조용히 건너뛰지 않고 fail-closed로 주문을 차단한다.
    if get_quote_fn is None:
        logger.error(f"수동주문 현재가 조회 함수 누락(fail-closed) [{symbol}]")
        return f"⚠️ {name}({symbol}) 현재가 조회 기능이 연결되지 않았습니다 — 주문 불가"
    price = 0
    try:
        quote = await get_quote_fn(symbol)
    except Exception as e:
        logger.warning(f"수동주문 현재가 조회 실패 [{symbol}]: {e}")
        quote = None
    if quote:
        price = int(quote.get("stck_prpr", 0) or 0)
        if quote.get("hts_kor_isnm"):
            name = quote.get("hts_kor_isnm")
    if price <= 0:
        return f"⚠️ {name}({symbol}) 현재가 조회 실패 — 주문 불가"

    # 매수 안전장치 관문 — 신호 경로(stark/execution_guard.precheck + execute())와 동일한
    # 악재공시·당일 손절 2회·실패 억제·물타기·보유종목수 한도·투자경고/VI 차단을 채팅 직접
    # 매수에도 적용한다(PM 지시, [AT] buy-gate-unification). "한도무시"는 사이징 금액
    # 한도만 무시할 뿐 이 안전 차단은 넘지 못한다 — 사이징보다 먼저 검사한다.
    if is_buy:
        gate_blocked = await buy_gate(
            symbol, price, pool=pool, redis=redis, bot="stock_trader",
            get_positions_fn=get_stock_positions_fn, get_market_warning_fn=get_market_warning_fn,
        )
        if gate_blocked:
            logger.info(f"⛔ 채팅 매수 차단 [{symbol}] {gate_blocked['blocked']}: {gate_blocked['reason']}")
            return f"⛔ {gate_blocked['reason']}"

    # 수량 (+ 매도 시 손익 계산용 평단가 조회)
    avg_price = 0.0
    if all_sell and not qty_m:
        try:
            pos = await get_stock_positions_fn()
        except Exception as e:
            logger.warning(f"수동주문 보유 수량 조회 실패 [{symbol}]: {e}")
            return f"⚠️ {name}({symbol}) 종목 조회에 실패했습니다. 잠시 후 다시 시도해주세요"
        pos_row = next((p for p in pos.get("data", []) if p["symbol"] == symbol), None)
        qty = int(pos_row["qty"]) if pos_row else 0
        if qty <= 0:
            return f"⚠️ {name}({symbol}) 보유 수량이 없어요"
        if is_sell and pos_row:
            avg_price = float(pos_row.get("avg_price", 0) or 0)
    else:
        qty = int(qty_m.group(1))
        if is_sell:
            try:
                pos = await get_stock_positions_fn()
                pos_row = next((p for p in pos.get("data", []) if p["symbol"] == symbol), None)
                avg_price = float(pos_row.get("avg_price", 0) or 0) if pos_row else 0.0
            except Exception as e:
                logger.warning(f"매도 손익 계산용 평단가 조회 실패 [{symbol}]: {e}")
                avg_price = 0.0

    # 매수 사이징 한도 적용 (PM 승인, 2026-09-30 리스크 기반 사이징을 채팅 직접 매매에도 적용).
    # "한도무시" 키워드가 있으면 사용자가 의도적으로 한도를 넘기는 것이므로 조정하지 않는다.
    sizing_note = ""
    if is_buy:
        if "한도무시" in msg:
            sizing_note = "\n⚠️ 한도무시 적용"
        else:
            requested_qty = qty
            try:
                max_amount = await _compute_buy_sizing_cap(
                    config=config, get_balance_fn=get_balance_fn,
                    get_recent_ohlcv_fn=get_recent_ohlcv_fn, symbol=symbol, cur_price=price,
                )
            except Exception as e:
                # get_balance_fn 미주입(테스트 전용 분기)과 달리, 운영 경로에서 한도 계산이
                # 예외로 실패하면 한도 없이 조용히 주문이 나가면 안 되므로 알려야 한다.
                logger.warning(f"수동주문 사이징 한도 계산 실패 [{symbol}]: {e}")
                await send_telegram_fn(f"⚠️ [{name}] 사이징 한도 계산 실패 — 한도 미적용 ({e})", dest="personal")
                max_amount = None
                sizing_note = "\n⚠️ 사이징 한도 계산 실패 — 한도 미적용"
            if max_amount is not None and price * qty > max_amount:
                qty = int(max_amount // price)
                if qty <= 0:
                    return (f"⚠️ 사이징 한도(약 {max_amount / 10000:,.0f}만원) 초과로 "
                            f"1주도 매수 불가 — 고가 종목")
                sizing_note = f"\n요청 {requested_qty}주 → 사이징 한도로 {qty}주로 조정"

    # 직전 같은 종목·같은 방향 주문이 응답불명으로 끝나 재확인 대기 중이면 중복 주문 차단
    if redis is not None and await check_order_uncertain_block(symbol, is_buy, redis):
        return f"⏳ {name}({symbol}) 직전 주문 결과 확인 중 — 잠시 후 다시 시도해주세요"

    # 응답불명 시 체결 재확인용 — 주문 전 보유 수량을 미리 조회해 둔다
    pre_qty = await get_held_qty(symbol, get_stock_positions_fn) if get_stock_positions_fn else None

    result = await kis_order_fn(symbol, price, qty, is_buy)

    if result.get("success"):
        pnl = None
        pnl_rate = None
        pnl_text = ""
        if is_sell and avg_price > 0:
            pnl = (price - avg_price) * qty
            pnl_rate = (price - avg_price) / avg_price * 100
            pnl_text = f"\n손익 {pnl:+,.0f}원 ({pnl_rate:+.1f}%)"

        try:
            async with pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO trade_history (bot,asset_type,symbol,side,price,quantity,amount,strategy,pnl)
                    VALUES ('stock_trader','stock',$1,$2,$3,$4,$5,'수동지시',$6)
                """, symbol, action.upper(), float(price), float(qty), float(price * qty), pnl)
        except Exception:
            pass
        # 체결 즉시 보유/계좌 캐시 무효화 — 화면에 옛 데이터 남는 것 방지
        try:
            for k in ("cache:positions:stock", "cache:account:stock"):
                await redis.delete(k)
        except Exception:
            pass
        await send_telegram_fn(
            f"{'📈' if is_buy else '📉'} <b>{name} {action_kr} 체결 (수동지시)</b>\n"
            f"가격: {price:,}원 × {qty}주 = {price*qty:,}원{pnl_text}{sizing_note}",
            dest="personal")
        if not CHANNEL_SLIM:
            await send_telegram_fn(
                _fill_channel_summary(name, symbol, action_kr, qty, price, pnl_rate), dest="channel")
        await log_journal_fn("stock_trader", symbol, name, action, "수동지시",
                              user_msg[:200], "MANUAL", "사용자 직접 지시",
                              True, True, price, qty, source="chat")
        return (f"✅ [실제 체결] {name}({symbol}) {qty}주 {action_kr} 완료 — "
                f"{price:,}원 × {qty}주 = {price*qty:,}원{pnl_text}{sizing_note}")

    if result.get("uncertain") and get_stock_positions_fn is not None:
        recon = await reconcile_uncertain_order(
            symbol, is_buy, pre_qty=pre_qty, get_positions_fn=get_stock_positions_fn, redis=redis,
            invalidate_cache_fn=invalidate_cache_fn)
        if recon["status"] == "filled":
            diff_qty = int(round(recon.get("qty_diff") or qty))
            pnl = None
            pnl_rate = None
            pnl_text = ""
            if is_sell and avg_price > 0:
                pnl = (price - avg_price) * diff_qty
                pnl_rate = (price - avg_price) / avg_price * 100
                pnl_text = f"\n손익 {pnl:+,.0f}원 ({pnl_rate:+.1f}%)"
            try:
                async with pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO trade_history (bot,asset_type,symbol,side,price,quantity,amount,strategy,pnl)
                        VALUES ('stock_trader','stock',$1,$2,$3,$4,$5,'수동지시(응답지연)',$6)
                    """, symbol, action.upper(), float(price), float(diff_qty),
                        float(price) * diff_qty, pnl)
            except Exception:
                pass
            try:
                for k in ("cache:positions:stock", "cache:account:stock"):
                    await redis.delete(k)
            except Exception:
                pass
            await send_telegram_fn(
                f"{'📈' if is_buy else '📉'} <b>{name} {action_kr} 체결 확인 (응답 지연)</b>\n"
                f"가격: {price:,}원 × {diff_qty}주 = {price*diff_qty:,}원{pnl_text}{sizing_note}",
                dest="personal")
            if not CHANNEL_SLIM:
                await send_telegram_fn(
                    _fill_channel_summary(name, symbol, action_kr, diff_qty, price, pnl_rate), dest="channel")
            await log_journal_fn("stock_trader", symbol, name, action, "수동지시",
                                  user_msg[:200], "MANUAL_UNCERTAIN_FILLED", "사용자 직접 지시(응답지연 체결확인)",
                                  True, True, price, diff_qty, source="chat")
            return (f"✅ 체결 확인(응답 지연) — {name}({symbol}) {diff_qty}주 {action_kr} "
                    f"— {price:,}원 × {diff_qty}주 = {price*diff_qty:,}원{pnl_text}{sizing_note}")
        return (f"⚠️ {name}({symbol}) 주문 결과 불명 — 보유 수량 변화 없음. "
                f"미체결일 수 있으니 포트폴리오에서 확인 후 재지시하세요")

    return f"❌ {name}({symbol}) {action_kr} 주문 실패: {result.get('error') or '사유 미확인'}"
