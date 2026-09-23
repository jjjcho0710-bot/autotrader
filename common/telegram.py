"""
텔레그램 알림 모듈
- TradeJarvis: AI 분석/명령
- 주식봇: stock-trader 매매 알림
- 코인봇: crypto-trader 매매 알림
"""
import logging
import aiohttp
from common.config import config

logger = logging.getLogger(__name__)

TG_API = "https://api.telegram.org/bot"


async def _send(token: str, chat_id: str, text: str):
    """기본 전송 함수"""
    if not token or not chat_id:
        logger.warning("텔레그램 토큰/채팅ID 없음 — 스킵")
        return
    if len(text) > 4096:
        text = text[:4000] + "\n...(생략)"
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(
                f"{TG_API}{token}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                timeout=aiohttp.ClientTimeout(total=10),
            )
    except Exception as e:
        logger.error(f"텔레그램 전송 실패: {e}")


# ── 기본 (STARK_BOT_TOKEN 최우선, 없으면 TELEGRAM_TOKEN) ──
async def send_message(text: str):
    token = config.STARK_BOT_TOKEN or config.TELEGRAM_TOKEN
    await _send(token, config.TELEGRAM_CHAT_ID, text)


# ── 주식봇 전송 (STARK_BOT_TOKEN 최우선) ───────────────
async def send_stock(text: str):
    """주식봇 전송 (한강뷰매니저 STARK_BOT_TOKEN 최우선 통합)"""
    token   = config.STARK_BOT_TOKEN or config.STOCK_BOT_TOKEN or config.TELEGRAM_TOKEN
    chat_id = config.STOCK_CHAT_ID or config.TELEGRAM_CHAT_ID
    await _send(token, chat_id, text)


# ── Jarvis/한강뷰매니저 전송 (STARK_BOT_TOKEN 최우선) ──
async def send_jarvis(text: str):
    """STARK 한강뷰매니저 봇으로 전송"""
    token   = config.STARK_BOT_TOKEN or config.JARVIS_ANALYST_TOKEN or config.TELEGRAM_TOKEN
    chat_id = config.JARVIS_ANALYST_CHAT_ID or config.TELEGRAM_CHAT_ID
    await _send(token, chat_id, text)


# ── 편의 함수들 ───────────────────────────────────────
async def notify_buy(bot: str, symbol: str, price: float, qty: float, strategy: str):
    emoji = "📈"
    msg = (
        f"{emoji} <b>매수 체결</b>\n"
        f"봇: {bot}\n"
        f"종목: {symbol}\n"
        f"가격: {price:,.0f}원\n"
        f"수량: {qty}\n"
        f"전략: {strategy}"
    )
    if bot == "stock_trader":
        await send_stock(msg)
    else:
        await send_message(msg)


async def notify_sell(bot: str, symbol: str, price: float, qty: float, pnl: float, strategy: str):
    emoji = "🎯" if pnl >= 0 else "🛑"
    msg = (
        f"{emoji} <b>매도 체결</b>\n"
        f"봇: {bot}\n"
        f"종목: {symbol}\n"
        f"가격: {price:,.0f}원\n"
        f"수량: {qty}\n"
        f"손익: {pnl:+,.0f}원\n"
        f"전략: {strategy}"
    )
    if bot == "stock_trader":
        await send_stock(msg)
    else:
        await send_message(msg)


async def notify_error(bot: str, error: str):
    msg = f"⚠️ <b>에러 발생</b>\n봇: {bot}\n내용: {error}"
    await send_jarvis(msg)
    await send_message(msg)


async def notify_daily_report(today_pnl: float, total_pnl: float, trade_count: int):
    emoji = "📈" if today_pnl >= 0 else "📉"
    msg = (
        f"{emoji} <b>일별 리포트</b>\n"
        f"오늘 수익: {today_pnl:+,.0f}원\n"
        f"오늘 체결: {trade_count}건\n"
        f"누적 수익: {total_pnl:+,.0f}원"
    )
    await send_jarvis(msg)
