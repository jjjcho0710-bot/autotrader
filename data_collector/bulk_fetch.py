"""
과거 OHLCV 데이터 일괄 적재 - pykrx 직접 import 방식
"""
import asyncio
import logging
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, "/app")

from common.config import config
from common.database import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("bulk-fetch")

START_DATE = os.getenv("FETCH_START", "20260601")
END_DATE   = os.getenv("FETCH_END",   datetime.now().strftime("%Y%m%d"))


async def fetch_and_save(symbol: str, start: str, end: str) -> int:
    loop = asyncio.get_event_loop()

    def _fetch():
        # setuptools 없이 pykrx 직접 import
        import importlib, types
        # pkg_resources mock
        if 'pkg_resources' not in sys.modules:
            mock = types.ModuleType('pkg_resources')
            mock.require = lambda *a, **k: None
            mock.DistributionNotFound = Exception
            sys.modules['pkg_resources'] = mock

        from pykrx import stock as pykrx_stock
        return pykrx_stock.get_market_ohlcv(start, end, symbol)

    df = await loop.run_in_executor(None, _fetch)

    if df is None or df.empty:
        logger.warning(f"[{symbol}] 데이터 없음")
        return 0

    candles = []
    for date, row in df.iterrows():
        close = int(row.get("종가", 0))
        if close <= 0:
            continue
        candles.append((
            symbol,
            datetime.strptime(date.strftime("%Y%m%d"), "%Y%m%d"),
            int(row.get("시가", 0)),
            int(row.get("고가", 0)),
            int(row.get("저가", 0)),
            close,
            int(row.get("거래량", 0)),
            float(row.get("등락률", 0)),
        ))

    if not candles:
        return 0

    async with db.pool.acquire() as conn:
        await conn.executemany("""
            INSERT INTO stock_daily_ohlcv
                (symbol, ts, open, high, low, close, volume, change_rate)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (symbol, ts) DO UPDATE
            SET open=$3, high=$4, low=$5, close=$6, volume=$7, change_rate=$8
        """, candles)

    logger.info(f"✅ [{symbol}] {len(candles)}개 저장")
    return len(candles)


async def main():
    await db.connect()

    symbols = await db.get_watchlist_symbols()
    if not symbols:
        symbols = [
            "005930", "000660", "005380", "035420", "051910",
            "006400", "035720", "207940", "068270", "028260",
            "105560", "055550", "096770", "017670", "030200",
            "003550", "066570", "012330", "009150", "011200",
        ]
        logger.info(f"⚠️ watchlist 비어있음 → 기본 대형주 {len(symbols)}종목")
    else:
        logger.info(f"✅ watchlist {len(symbols)}종목 로드")

    logger.info(f"🚀 적재 시작: {START_DATE} ~ {END_DATE} / {len(symbols)}종목")

    total = 0
    failed = []
    for i, symbol in enumerate(symbols, 1):
        try:
            count = await fetch_and_save(symbol, START_DATE, END_DATE)
            total += count
        except Exception as e:
            logger.error(f"❌ [{symbol}] 실패: {e}")
            failed.append(symbol)
        await asyncio.sleep(0.3)

        if i % 5 == 0:
            logger.info(f"진행: {i}/{len(symbols)} ({i/len(symbols)*100:.0f}%) — 저장 {total}개")

    await db.disconnect()
    logger.info("=" * 40)
    logger.info(f"🎉 완료! 총 {total}개 캔들 저장")
    logger.info(f"성공: {len(symbols)-len(failed)}종목 / 실패: {len(failed)}종목")
    if failed:
        logger.info(f"실패: {failed}")


if __name__ == "__main__":
    asyncio.run(main())
