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

logger = logging.getLogger("router.handlers.order")

KST = timezone(timedelta(hours=9))

_RE_PROP = re.compile(r"(승인|오케이|오케|ok|ㅇㅋ|사자|매수 ?해|매수 ?하자|고고|거절|취소해|사지 ?마)", re.I)
_RE_REJECT = re.compile(r"(거절|취소해|사지 ?마|안 ?사)")


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
        await send_telegram_fn(out)
        return out

    sub = await jarvis_chat_fn({"message": cmd, "session_id": session_id, "_no_mirror": True})
    rep = sub.get("reply") or sub.get("error") or "실행 결과 없음"
    out = f"✅ 제안 {n} 승인 → 실행: {cmd}\n{rep}"
    await send_telegram_fn(out)
    return out


async def handle_proposal_response(
    user_msg: str, *, redis: Any, kis_order_fn, log_journal_fn, send_telegram_fn,
) -> Optional[str]:
    """매수 제안(proposal:{symbol}) 승인/거절 처리"""
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

        if _RE_REJECT.search(um):
            await redis.delete(f"proposal:{target['symbol']}")
            return f"❌ {target['name']} 매수 제안 거절 처리했어요."

        # 승인 → 실제 매수
        order = await kis_order_fn(target["symbol"], int(target["price"]), int(target["qty"]), True)
        await redis.delete(f"proposal:{target['symbol']}")
        if order.get("success"):
            await log_journal_fn("stock_trader", target["symbol"], target["name"], "buy",
                                  target.get("strategy", "제안"), "주인 승인", "PROPOSE_APPROVED",
                                  target.get("reason", ""), True, True,
                                  int(target["price"]), int(target["qty"]))
            msg = (f"✅ <b>{target['name']} 매수 체결 (주인 승인)</b>\n"
                   f"{target['qty']}주 @ {int(target['price']):,}원")
            await send_telegram_fn(msg, broadcast=True)
            return msg.replace("<b>", "").replace("</b>", "")
        return f"❌ 매수 실패: {order.get('error')}"
    except Exception as pe:
        logger.warning(f"제안 승인 처리 오류: {pe}")
        return None


async def handle_trade_command(
    user_msg: str, *, pool: Any, redis: Any, universe: Any, get_kis_token_fn, config: Any,
    kis_order_fn, get_stock_positions_fn, send_telegram_fn, log_journal_fn,
) -> Optional[str]:
    """채팅에서 '종목 N주 매수/매도' 명령 → 실제 KIS 주문 실행. 해당 없으면 None"""
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

    # 현재가 (검증된 KIS 직접 조회 경로)
    price = 0
    try:
        token = await get_kis_token_fn()
        if token:
            import aiohttp as _aiohttp
            import ssl as _ssl
            _c = _ssl.create_default_context()
            _c.check_hostname = False
            _c.verify_mode = _ssl.CERT_NONE
            async with _aiohttp.ClientSession(connector=_aiohttp.TCPConnector(ssl=_c)) as sess:
                pr = await sess.get(
                    f"{config.kis_base_url}/uapi/domestic-stock/v1/quotations/inquire-price",
                    headers={"authorization": f"Bearer {token}", "appkey": config.kis_app_key,
                             "appsecret": config.kis_app_secret,
                             "tr_id": "FHKST01010100", "custtype": "P"},
                    params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol},
                    timeout=_aiohttp.ClientTimeout(total=8))
                o = (await pr.json()).get("output", {})
                price = int(o.get("stck_prpr", 0) or 0)
                if o.get("hts_kor_isnm"):
                    name = o.get("hts_kor_isnm")
    except Exception as e:
        logger.warning(f"수동주문 현재가 조회 실패 [{symbol}]: {e}")
    if price <= 0:
        return f"⚠️ {name}({symbol}) 현재가 조회 실패 — 주문 불가"

    # 수량
    if all_sell and not qty_m:
        try:
            pos = await get_stock_positions_fn()
            qty = next((int(p["qty"]) for p in pos.get("data", []) if p["symbol"] == symbol), 0)
        except Exception:
            qty = 0
        if qty <= 0:
            return f"⚠️ {name}({symbol}) 보유 수량이 없어요"
    else:
        qty = int(qty_m.group(1))

    result = await kis_order_fn(symbol, price, qty, is_buy)

    if result.get("success"):
        try:
            async with pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO trade_history (bot,asset_type,symbol,side,price,quantity,amount,strategy)
                    VALUES ('stock_trader','stock',$1,$2,$3,$4,$5,'수동지시')
                """, symbol, action.upper(), float(price), float(qty), float(price * qty))
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
            f"가격: {price:,}원 × {qty}주 = {price*qty:,}원",
            broadcast=True)
        await log_journal_fn("stock_trader", symbol, name, action, "수동지시",
                              user_msg[:200], "MANUAL", "사용자 직접 지시",
                              True, True, price, qty, source="chat")
        return (f"✅ [실제 체결] {name}({symbol}) {qty}주 {action_kr} 완료 — "
                f"{price:,}원 × {qty}주 = {price*qty:,}원")
    else:
        return f"❌ {name}({symbol}) {action_kr} 주문 실패: {result.get('error', '알 수 없음')}"
