"""
과거 OHLCV 데이터 일괄 적재 스크립트
Railway 콘솔에서: python data_collector/bulk_fetch.py
또는 자동 실행용
"""
import asyncio
import logging
import sys
import os
from datetime import datetime

sys.path.insert(0, "/app")

from common.config import config
from common.database import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("bulk-fetch")

# ── 기간 설정 ──────────────────────────────────────────
START_DATE = os.getenv("FETCH_START", "20260601")
END_DATE   = os.getenv("FETCH_END",   datetime.now().strftime("%Y%m%d"))


async def fetch_and_save(symbol: str, start: str, end: str) -> int:
    from pykrx import stock as pykrx_stock
    loop = asyncio.get_event_loop()

    # pykrx는 blocking → executor로
    df = await loop.run_in_executor(
        None, lambda: pykrx_stock.get_market_ohlcv(start, end, symbol)
    )

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

    # watchlist 종목 가져오기 (없으면 기본 종목)
    symbols = await db.get_watchlist_symbols()
    if not symbols:
        # 기본 대형주 20종목
        symbols = [
            "005930",  # 삼성전자
            "000660",  # SK하이닉스
            "005380",  # 현대차
            "035420",  # NAVER
            "051910",  # LG화학
            "006400",  # 삼성SDI
            "035720",  # 카카오
            "207940",  # 삼성바이오로직스
            "068270",  # 셀트리온
            "028260",  # 삼성물산
            "105560",  # KB금융
            "055550",  # 신한지주
            "096770",  # SK이노베이션
            "017670",  # SK텔레콤
            "030200",  # KT
            "003550",  # LG
            "066570",  # LG전자
            "012330",  # 현대모비스
            "009150",  # 삼성전기
            "011200",  # HMM
        ]
        logger.info(f"⚠️ watchlist 비어있음 → 기본 대형주 {len(symbols)}종목 사용")
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
        await asyncio.sleep(0.5)  # pykrx 레이트 리밋

        # 진행률 10종목마다 출력
        if i % 10 == 0:
            logger.info(f"진행: {i}/{len(symbols)} ({i/len(symbols)*100:.0f}%)")

    await db.disconnect()

    logger.info("=" * 40)
    logger.info(f"🎉 완료! 총 {total}개 캔들 저장")
    logger.info(f"성공: {len(symbols)-len(failed)}종목 / 실패: {len(failed)}종목")
    if failed:
        logger.info(f"실패 목록: {failed}")
    logger.info("이제 자동매매 가능! 🚀")


if __name__ == "__main__":
    asyncio.run(main())
