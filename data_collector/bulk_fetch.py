"""
과거 OHLCV 데이터 일괄 적재 - pykrx 없이 KRX 직접 HTTP 호출
"""
import asyncio
import logging
import sys
import os
import json
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.parse import urlencode

sys.path.insert(0, "/app")
from common.database import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("bulk-fetch")

START_DATE = os.getenv("FETCH_START", "20260601")
END_DATE   = os.getenv("FETCH_END",   datetime.now().strftime("%Y%m%d"))


def fetch_ohlcv_krx(symbol: str, start: str, end: str) -> list:
    """KRX 정보데이터시스템 직접 호출 (pykrx 없이)"""
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    params = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT01701",
        "locale": "ko_KR",
        "isuCd": symbol,
        "isuCd2": "",
        "strtDd": start,
        "endDd": end,
        "adjStkPrc_ind": "1",
        "adjStkPrc": "2",
        "outputFileType": "JSON",
        "pagePath": "/contents/MDC/STAT/standard/MDCSTAT01701",
        "kindOfDate": "D",
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "http://data.krx.co.kr/",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    req = Request(url, data=urlencode(params).encode(), headers=headers, method="POST")
    with urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    candles = []
    for row in data.get("output", []):
        try:
            date_str = row.get("TRD_DD", "").replace("/", "").replace("-", "").strip()
            if len(date_str) != 8:
                continue
            close = int(str(row.get("TDD_CLSPRC", "0")).replace(",", "") or 0)
            if close <= 0:
                continue
            candles.append({
                "date":   date_str,
                "open":   int(str(row.get("TDD_OPNPRC",  "0")).replace(",", "") or 0),
                "high":   int(str(row.get("TDD_HGPRC",   "0")).replace(",", "") or 0),
                "low":    int(str(row.get("TDD_LWPRC",   "0")).replace(",", "") or 0),
                "close":  close,
                "volume": int(str(row.get("ACC_TRDVOL",  "0")).replace(",", "") or 0),
                "change_rate": float(str(row.get("FLUC_RT", "0")).replace(",", "") or 0),
            })
        except Exception:
            continue
    return candles


async def fetch_and_save(symbol: str, start: str, end: str) -> int:
    loop = asyncio.get_event_loop()
    candles = await loop.run_in_executor(None, fetch_ohlcv_krx, symbol, start, end)

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
    await db.connect()

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
            count = await fetch_and_save(symbol, START_DATE, END_DATE)
            total += count
        except Exception as e:
            logger.error(f"❌ [{symbol}] 실패: {e}")
            failed.append(symbol)
        await asyncio.sleep(0.5)

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
