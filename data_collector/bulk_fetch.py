"""
과거 OHLCV 데이터 일괄 적재 - KIS API 사용
pykrx/외부URL 불필요, Railway 내부에서 동작
"""
import asyncio
import logging
import sys
import os
from datetime import datetime

sys.path.insert(0, "/app")
from common.database import db
from common.config import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("bulk-fetch")

START_DATE = os.getenv("FETCH_START", "20260601")
END_DATE   = os.getenv("FETCH_END",   datetime.now().strftime("%Y%m%d"))


async def fetch_and_save(trader, symbol: str) -> int:
    candles = await trader.get_daily_ohlcv(symbol, START_DATE, END_DATE)

    if not candles:
        logger.warning(f"[{symbol}] 데이터 없음")
        return 0

    rows = [
        (symbol,
         datetime.strptime(c["date"], "%Y%m%d"),
         c["open"], c["high"], c["low"], c["close"],
         c["volume"], c["change_rate"])
        for c in candles
    ]

    async with db.pool.acquire() as conn:
        await conn.executemany("""
            INSERT INTO stock_daily_ohlcv
                (symbol, ts, open, high, low, close, volume, change_rate)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (symbol, ts) DO UPDATE
            SET open=$3, high=$4, low=$5, close=$6, volume=$7, change_rate=$8
        """, rows)

    logger.info(f"✅ [{symbol}] {len(rows)}개 저장")
    return len(rows)


async def main():
    import aiohttp
    await db.connect()

    # KIS 토큰 발급
    from stock_trader.kis_trader import KISTrader
    session = aiohttp.ClientSession()
    trader = KISTrader()
    trader.session = session
    await trader._get_token()
    logger.info("✅ KIS 토큰 발급 완료")

    symbols = await db.get_watchlist_symbols()
    if not symbols:
        symbols = [
            "005930","000660","005380","035420","051910",
            "006400","035720","207940","068270","028260",
            "105560","055550","096770","017670","030200",
            "003550","066570","012330","009150","011200",
        ]
        logger.info(f"⚠️ watchlist 비어있음 → 기본 대형주 {len(symbols)}종목")
    else:
        logger.info(f"✅ watchlist {len(symbols)}종목 로드")

    logger.info(f"🚀 적재 시작: {START_DATE} ~ {END_DATE} / {len(symbols)}종목")

    total, failed = 0, []
    for i, symbol in enumerate(symbols, 1):
        try:
            count = await fetch_and_save(trader, symbol)
            total += count
        except Exception as e:
            logger.error(f"❌ [{symbol}] 실패: {e}")
            failed.append(symbol)
        await asyncio.sleep(0.3)  # KIS API 레이트 리밋

        if i % 5 == 0:
            logger.info(f"진행: {i}/{len(symbols)} ({i/len(symbols)*100:.0f}%) — 저장 {total}개")

    await session.close()
    await db.disconnect()

    logger.info("=" * 40)
    logger.info(f"🎉 완료! 총 {total}개 캔들 저장")
    logger.info(f"성공: {len(symbols)-len(failed)}종목 / 실패: {len(failed)}종목")
    if failed:
        logger.info(f"실패: {failed}")
    logger.info("이제 자동매매 가능! 🚀")


if __name__ == "__main__":
    asyncio.run(main())
