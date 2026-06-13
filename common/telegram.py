"""
텔레그램 알림 모듈
매수/매도/에러/일별 리포트 알림
"""
import logging
import aiohttp
from common.config import config

logger = logging.getLogger(__name__)

TELEGRAM_API = f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}"


async def send_message(text: str):
    """텔레그램 메시지 전송"""
    if not config.TELEGRAM_TOKEN or not config.TELEGRAM_CHAT_ID:
        logger.warning("텔레그램 설정 없음 — 알림 스킵")
        return

    try:
        async with aiohttp.ClientSession() as session:
            await session.post(
                f"{TELEGRAM_API}/sendMessage",
                json={
                    "chat_id": config.TELEGRAM_CHAT_ID,
                    "text": text,
                    "parse_mode": "HTML",
                }
            )
    except Exception as e:
        logger.error(f"텔레그램 전송 실패: {e}")


async def notify_buy(bot: str, symbol: str, price: float, qty: float, strategy: str):
    msg = (
        f"📈 <b>매수 체결</b>\n"
        f"봇: {bot}\n"
        f"종목: {symbol}\n"
        f"가격: {price:,.0f}원\n"
        f"수량: {qty}\n"
        f"전략: {strategy}"
    )
    await send_message(msg)


async def notify_sell(bot: str, symbol: str, price: float, qty: float, pnl: float, strategy: str):
    emoji = "🎯" if pnl >= 0 else "🛑"
    pnl_str = f"{pnl:+,.0f}원"
    msg = (
        f"{emoji} <b>매도 체결</b>\n"
        f"봇: {bot}\n"
        f"종목: {symbol}\n"
        f"가격: {price:,.0f}원\n"
        f"수량: {qty}\n"
        f"손익: {pnl_str}\n"
        f"전략: {strategy}"
    )
    await send_message(msg)


async def notify_error(bot: str, error: str):
    msg = (
        f"⚠️ <b>에러 발생</b>\n"
        f"봇: {bot}\n"
        f"내용: {error}"
    )
    await send_message(msg)


async def notify_daily_report(today_pnl: float, total_pnl: float, trade_count: int):
    emoji = "📈" if today_pnl >= 0 else "📉"
    msg = (
        f"{emoji} <b>일별 리포트</b>\n"
        f"오늘 수익: {today_pnl:+,.0f}원\n"
        f"오늘 체결: {trade_count}건\n"
        f"누적 수익: {total_pnl:+,.0f}원"
    )
    await send_message(msg)
