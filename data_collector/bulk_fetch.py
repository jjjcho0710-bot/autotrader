"""
과거 OHLCV 일괄 적재 - KIS API 일봉 조회
pykrx/setuptools 불필요, KIS openapi만 사용
"""
import asyncio, logging, sys, os
from datetime import datetime, timedelta

sys.path.insert(0, "/app")
from common.config import config
from common.database import db
import aiohttp

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] — %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("bulk-fetch")

START_DATE = os.getenv("FETCH_START", "20260601")
END_DATE   = os.getenv("FETCH_END",   datetime.now().strftime("%Y%m%d"))
BASE_URL   = config.kis_base_url  # paper or real


async def get_kis_token(session: aiohttp.ClientSession) -> str:
    res = await session.post(f"{BASE_URL}/oauth2/tokenP", json={
        "grant_type": "client_credentials",
        "appkey":     config.KIS_APP_KEY,
        "appsecret":  config.KIS_APP_SECRET,
    })
    data = await res.json()
    return data.get("access_token", "")


async def fetch_ohlcv_kis(session: aiohttp.ClientSession, token: str, symbol: str) -> list:
    """KIS 일봉 조회 (최대 100일)"""
    tr_id = "FHKST01010400"  # 모의/실전 동일
    headers = {
        "authorization": f"Bearer {token}",
        "appkey":        config.KIS_APP_KEY,
        "appsecret":     config.KIS_APP_SECRET,
        "tr_id":         tr_id,
        "custtype":      "P",
    }
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD":         symbol,
        "FID_INPUT_DATE_1":       START_DATE,
        "FID_INPUT_DATE_2":       END_DATE,
        "FID_PERIOD_DIV_CODE":    "D",
        "FID_ORG_ADJ_PRC":        "0",
    }
    res = await session.get(
        f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        headers=headers, params=params
    )
    data = await res.json()
    output = data.get("output2", [])
    candles = []
    for row in output:
        date_str = row.get("stck_bsop_date", "")
        close    = int(row.get("stck_clpr", 0) or 0)
        if len(date_str) != 8 or close <= 0:
            continue
        if not (START_DATE <= date_str <= END_DATE):
            continue
        candles.append((
            symbol,
            datetime.strptime(date_str, "%Y%m%d"),
            int(row.get("stck_oprc", 0) or 0),
            int(row.get("stck_hgpr", 0) or 0),
            int(row.get("stck_lwpr", 0) or 0),
            close,
            int(row.get("acml_vol", 0) or 0),
            float(row.get("prdy_ctrt", 0) or 0),
        ))
    return candles


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

    logger.info(f"🚀 KIS API 적재 시작: {START_DATE} ~ {END_DATE} / {len(symbols)}종목")

    total, failed = 0, []
    async with aiohttp.ClientSession() as session:
        token = await get_kis_token(session)
        if not token:
            logger.error("❌ KIS 토큰 발급 실패! KIS_APP_KEY/SECRET 환경변수 확인")
            await db.disconnect()
            return
        logger.info("✅ KIS 토큰 발급 완료")

        for i, symbol in enumerate(symbols, 1):
            try:
                candles = await fetch_ohlcv_kis(session, token, symbol)
                if candles:
                    async with db.pool.acquire() as conn:
                        await conn.executemany("""
                            INSERT INTO stock_daily_ohlcv
                                (symbol,ts,open,high,low,close,volume,change_rate)
                            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                            ON CONFLICT (symbol,ts) DO UPDATE
                            SET open=$3,high=$4,low=$5,close=$6,volume=$7,change_rate=$8
                        """, candles)
                    total += len(candles)
                    logger.info(f"✅ [{symbol}] {len(candles)}개 저장 ({i}/{len(symbols)})")
                else:
                    logger.warning(f"⚠️ [{symbol}] 데이터 없음")
                    failed.append(symbol)
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
    logger.info("이제 자동매매 가능! 🚀")


if __name__ == "__main__":
    asyncio.run(main())
