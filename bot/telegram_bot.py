"""
bot/telegram_bot.py - STARK 텔레그램 봇(한강뷰매니저) 전용 웹훅 처리 모듈

STARK_PLAN 1번 원칙("입구는 하나로 통일: STARK 텔레그램 봇 1개")에 따라 신설된 통합 봇 모듈.
dashboard/main.py의 /api/telegram/webhook 핸들러(구 5018~5153행)와 webhook 등록
(구 5156~5181행, 서버 기동 시 자동등록 구 228~247행)을 이관했다 — 명령어(/start,/analyze,
/status,/positions,/history)·인라인 버튼 콜백(callback_query)·자유대화를 모두 이 모듈이
처리하고, dashboard/main.py는 FastAPI 라우트에서 요청 바디를 그대로 넘겨주기만 한다.

봇 토큰은 Railway에 새로 등록된 STARK_BOT_TOKEN을 최우선으로 쓴다(PM 승인 사항 — 신규 STARK
봇으로 기존 트레이더 알리미/자비스 애널리스트 봇을 통합). STARK_BOT_TOKEN이 없는 환경(로컬
개발 등 과도기)에서는 기존 JARVIS_ANALYST_TOKEN → TELEGRAM_TOKEN 순으로 폴백한다.
"""
import logging
import os
from dataclasses import dataclass
from typing import Any, Callable

logger = logging.getLogger("bot.telegram_bot")


def resolve_token(config: Any) -> str:
    """STARK 봇 전용 토큰 — STARK_BOT_TOKEN 우선, 없으면 과도기 폴백."""
    return os.getenv("STARK_BOT_TOKEN") or config.JARVIS_ANALYST_TOKEN or config.TELEGRAM_TOKEN


@dataclass
class BotContext:
    """dashboard/main.py가 들고 있는 인프라 함수를 주입받는다 — 이 모듈은 텔레그램 프로토콜
    처리만 전담하고, DB/Redis/KIS/LLM 호출은 전부 콜러블로 받는다."""

    config: Any
    send_telegram: Callable[..., Any]           # (text, chat_id=None, token=None) -> None
    typing_action: Callable[..., Any]            # (chat_id, token=None) -> None
    ask_openwebui: Callable[..., Any]            # (message, session_id) -> str
    jarvis_chat: Callable[..., Any]              # (dict) -> dict — 라우터+AI 채팅 공용 파이프라인
    get_trades: Callable[..., Any]               # (limit) -> dict
    trigger_manual_collect: Callable[..., Any]   # () -> None (백그라운드 작업 시작, fire-and-forget)
    session_id: str                              # 웹 대시보드와 공유하는 Jarvis 세션 id


async def handle_update(body: dict, ctx: BotContext) -> dict:
    """텔레그램 webhook 페이로드 1건 처리. 항상 {"ok": True}류를 반환한다 — 내부 예외를 여기서
    삼켜야 텔레그램이 실패로 보고 재전송을 반복하지 않는다(원본과 동일한 방침)."""
    try:
        # 채널 게시물(channel_post)은 봇이 굳이 처리할 내용 없음 — 조용히 무시
        if body.get("channel_post"):
            return {"ok": True}

        cq = body.get("callback_query")
        if cq:
            return await _handle_callback_query(cq, ctx)

        message = body.get("message", {})
        chat_id = str(message.get("chat", {}).get("id", ""))
        text = (message.get("text") or "").strip()

        if not text or not chat_id:
            return {"ok": True}

        logger.info(f"텔레그램 수신: {text} (chat_id: {chat_id})")
        token = resolve_token(ctx.config)

        if text == "/start":
            await ctx.send_telegram(
                "🤖 <b>STARK입니다!</b>\n\n"
                "AI 트레이딩 어시스턴트예요. 실시간 데이터로 분석해드려요!\n\n"
                "<b>명령어:</b>\n"
                "/analyze — 포트폴리오 종합 분석\n"
                "/positions — 보유 포지션\n"
                "/status — 봇 상태\n"
                "/history — 매매 이력\n\n"
                "또는 자유롭게 질문하세요! 💬", chat_id, token)
            return {"ok": True}

        if text == "/analyze":
            await ctx.typing_action(chat_id, token)
            reply = await ctx.ask_openwebui(
                "현재 포트폴리오를 종합 분석하고 리스크와 액션 포인트를 알려줘", session_id=chat_id)
            await ctx.send_telegram(f"📊 <b>STARK 분석</b>\n\n{reply}", chat_id, token)
            return {"ok": True}

        if text == "/status":
            reply = await ctx.ask_openwebui("현재 봇 상태 알려줘", session_id=chat_id)
            await ctx.send_telegram(f"🤖 <b>STARK</b>\n\n{reply}", chat_id, token)
            return {"ok": True}

        if text == "/positions":
            await ctx.typing_action(chat_id, token)
            reply = await ctx.ask_openwebui("현재 보유 포지션 현황 알려줘", session_id=chat_id)
            await ctx.send_telegram(f"🤖 <b>STARK</b>\n\n{reply}", chat_id, token)
            return {"ok": True}

        if text == "/history":
            try:
                trades_res = await ctx.get_trades(limit=10)
                trades = trades_res.get("data", [])
                if not trades:
                    await ctx.send_telegram("📋 오늘 매매 이력 없음", chat_id, token)
                else:
                    lines = [f"📋 <b>최근 매매 {len(trades)}건</b>\n"]
                    for t in trades:
                        ts = t["ts"][11:16] if t["ts"] else "-"
                        side = "매수" if t["side"] == "BUY" else "매도"
                        pnl = f" {t['pnl']:+,}원" if t.get("pnl") else ""
                        lines.append(f"  {ts} {side} {t.get('name') or t['symbol']}{pnl}")
                    await ctx.send_telegram("\n".join(lines), chat_id, token)
            except Exception as e:
                await ctx.send_telegram(f"❌ 이력 조회 실패: {e}", chat_id, token)
            return {"ok": True}

        # 수동 수집 명령
        if any(k in text for k in ["뉴스 수집", "수동 수집", "감성 수집", "데이터 수집"]):
            await ctx.send_telegram("📰 데이터 수집 시작했어요! 잠시 기다려주세요...", chat_id, token)
            ctx.trigger_manual_collect()
            return {"ok": True}

        # 자유 대화 → 라우터 + AI 채팅 공용 파이프라인 (웹 대시보드와 세션 공유,
        # channel="telegram"으로 채널 출처를 구분해 대화 기록에 남긴다)
        await ctx.typing_action(chat_id, token)
        try:
            res = await ctx.jarvis_chat({
                "message": text, "session_id": ctx.session_id,
                "_no_mirror": True, "channel": "telegram",
            })
            reply = res.get("reply") or res.get("error") or "응답 없음"
        except Exception:
            reply = await ctx.ask_openwebui(text, session_id=ctx.session_id)
        if len(reply) > 3800:
            reply = reply[:3800] + "...\n(내용이 길어 일부 생략됨)"
        await ctx.send_telegram(f"🤖 <b>STARK</b>\n\n{reply}", chat_id, token)
        return {"ok": True}
    except Exception as e:
        logger.error(f"텔레그램 webhook 오류: {e}")
        return {"ok": True}


async def _handle_callback_query(cq: dict, ctx: BotContext) -> dict:
    """인라인 버튼(승인/거절 등) 클릭 처리 — 명령 문자열로 변환해 jarvis_chat 파이프라인에 재투입."""
    data = cq.get("data", "")
    cq_chat = str(cq.get("message", {}).get("chat", {}).get("id", ""))
    cq_msg_id = cq.get("message", {}).get("message_id")
    token = resolve_token(ctx.config)
    cmd = None
    if data.startswith("adv:"):
        _, act, n = data.split(":")
        cmd = f"{'승인' if act == 'ok' else '거절'} {n}"
    elif data.startswith("prop:"):
        _, act, _sym = data.split(":")
        cmd = "승인" if act == "ok" else "거절"

    reply = ""
    if cmd:
        try:
            res = await ctx.jarvis_chat({
                "message": cmd, "session_id": ctx.session_id,
                "_no_mirror": True, "channel": "telegram",
            })
            reply = res.get("reply") or res.get("error") or "처리됨"
        except Exception as e:
            reply = f"❌ 처리 실패: {e}"

    try:
        import aiohttp as http
        async with http.ClientSession() as sess:
            await sess.post(f"https://api.telegram.org/bot{token}/answerCallbackQuery",
                             json={"callback_query_id": cq.get("id"), "text": "처리 중..."},
                             timeout=http.ClientTimeout(total=5))
            # 버튼 제거 (중복 클릭 방지)
            await sess.post(f"https://api.telegram.org/bot{token}/editMessageReplyMarkup",
                             json={"chat_id": cq_chat, "message_id": cq_msg_id,
                                   "reply_markup": {"inline_keyboard": []}},
                             timeout=http.ClientTimeout(total=5))
    except Exception:
        pass

    if reply:
        await ctx.send_telegram(reply[:3500], cq_chat, token)
    return {"ok": True}


async def register_webhook(base_url: str, config: Any) -> dict:
    """STARK 봇 webhook URL 등록. base_url은 스킴 포함 전체 URL(끝 슬래시 무관)."""
    import aiohttp as http
    token = resolve_token(config)
    if not token:
        return {"success": False, "error": "STARK_BOT_TOKEN 없음"}
    webhook_url = f"{base_url.rstrip('/')}/api/telegram/webhook"
    try:
        async with http.ClientSession() as session:
            res = await session.post(
                f"https://api.telegram.org/bot{token}/setWebhook",
                json={"url": webhook_url, "drop_pending_updates": True})
            data = await res.json()
        logger.info(f"텔레그램 webhook 등록: {webhook_url} → {data}")
        return {"success": data.get("ok"), "webhook_url": webhook_url, "result": data}
    except Exception as e:
        return {"success": False, "error": str(e)}
