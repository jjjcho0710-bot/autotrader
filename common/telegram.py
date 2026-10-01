"""
텔레그램 알림 모듈
- TradeJarvis: AI 분석/명령
- 주식봇: stock-trader 매매 알림
- 코인봇: crypto-trader 매매 알림
"""
import logging
import os
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


# ── 보고서 전송 (채널 우선, 없으면 주식 알림방 폴백) ──
REPORT_CHUNK_LIMIT = 4000


def split_text(text: str, limit: int = REPORT_CHUNK_LIMIT) -> list:
    """텔레그램 글자 수 한도에 맞춰 줄 단위로 분할 (한 줄이 limit 를 넘으면 강제 분할)"""
    if len(text) <= limit:
        return [text]
    chunks, cur = [], ""
    for line in text.split("\n"):
        while len(line) > limit:
            if cur:
                chunks.append(cur)
                cur = ""
            chunks.append(line[:limit])
            line = line[limit:]
        candidate = f"{cur}\n{line}" if cur else line
        if len(candidate) > limit:
            chunks.append(cur)
            cur = line
        else:
            cur = candidate
    if cur:
        chunks.append(cur)
    return chunks


async def _post_channel(token: str, channel_id: str, text: str) -> bool:
    """채널 전송. 실패해도 예외를 던지지 않고 warning 만 남긴다 (토큰/채널 ID 는 로그에 남기지 않음)"""
    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.post(
                f"{TG_API}{token}/sendMessage",
                json={"chat_id": channel_id, "text": text, "parse_mode": "HTML"},
                timeout=aiohttp.ClientTimeout(total=10),
            )
            status = getattr(resp, "status", 200)
            if status != 200:
                logger.warning(f"텔레그램 채널 전송 실패: HTTP {status}")
                return False
        return True
    except Exception as e:
        detail = str(e)
        for secret in (token, channel_id):
            if secret:
                detail = detail.replace(secret, "***")
        logger.warning(f"텔레그램 채널 전송 실패: {type(e).__name__}: {detail}")
        return False


async def send_report(text: str):
    """보고서 전송. TELEGRAM_CHANNEL_ID 가 있으면 채널로(STARK_BOT_TOKEN 우선, 없으면 TELEGRAM_TOKEN),
    비어 있으면 send_stock 으로 폴백. 4000자 초과 시 여러 메시지로 분할. 어떤 실패도 예외로 번지지 않는다."""
    try:
        chunks = split_text(text)
        channel_id = os.getenv("TELEGRAM_CHANNEL_ID", "").strip()
        if not channel_id:
            for chunk in chunks:
                await send_stock(chunk)
            return
        token = config.STARK_BOT_TOKEN or config.TELEGRAM_TOKEN
        if not token:
            logger.warning("텔레그램 채널 전송 스킵: 봇 토큰 없음")
            return
        for chunk in chunks:
            if not await _post_channel(token, channel_id, chunk):
                return  # 실패 시 나머지 조각은 보내지 않는다
    except Exception as e:
        logger.warning(f"보고서 전송 실패: {type(e).__name__}")


def fill_summary_line(*, name: str, symbol: str, side_kr: str, qty, price: float,
                       pnl_rate: float = None) -> str:
    """매수·매도 체결 알림의 채널용 한 줄 요약 — 금액(평가손익 등) 없이 가격·수량·손익률(%)만
    ([AT] feat/telegram-routing). 체결가는 종목 단가(공개 시세)라 계좌 잔고 규모를 드러내지
    않는다."""
    emoji = "📈" if side_kr == "매수" else "📉"
    line = f"{emoji} {name}({symbol}) {side_kr} {qty}주 @ {price:,.0f}원"
    if pnl_rate is not None:
        line += f" ({pnl_rate:+.1f}%)"
    return line


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
