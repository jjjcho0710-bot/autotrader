"""
과거 데이터 일괄 수집 스크립트 (pykrx 버전)
2021-01-01 부터 오늘까지 일봉 데이터 한 번에 수집
KIS API 불필요 — 한국거래소 공식 데이터

실행법:
  cd /app && python data_collector/historical_fetch.py
"""
import asyncio
import logging
import sys
from datetime import datetime

sys.path.insert(0, "/app")

from common.config import config
from common.database import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("historical-fetch")

START_DATE = "20210101"
END_DATE   = datetime.now().strftime("%Y%m%d")


class HistoricalFetcher:

    async def fetch_ohlcv(self, symbol: str) -> list:
        """pykrx로 일봉 데이터 조회"""
        try:
            from pykrx import stock
            df = stock.get_market_ohlcv(START_DATE, END_DATE, symbol)
            if df is None or df.empty:
                return []

            candles = []
            for date, row in df.iterrows():
                candles.append({
                    "date":        date.strftime("%Y%m%d"),
                    "open":        int(row.get("시가", 0)),
                    "high":        int(row.get("고가", 0)),
                    "low":         int(row.get("저가", 0)),
                    "close":       int(row.get("종가", 0)),
                    "volume":      int(row.get("거래량", 0)),
                    "change_rate": float(row.get("등락률", 0)),
                })
            logger.info(f"  [{symbol}] pykrx {len(candles)}개 수집 완료")
            return candles
        except Exception as e:
            logger.error(f"pykrx 조회 실패 [{symbol}]: {e}")
            return []

    async def save_ohlcv(self, symbol: str, candles: list):
        """DB에 저장"""
        if not candles:
            return 0
        count = 0
        async with db.pool.acquire() as conn:
            for c in candles:
                if len(c["date"]) != 8 or c["close"] <= 0:
                    continue
                ts = datetime.strptime(c["date"], "%Y%m%d")
                await conn.execute("""
                    INSERT INTO stock_daily_ohlcv
                        (symbol, ts, open, high, low, close, volume, change_rate)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT (symbol, ts) DO UPDATE
                    SET open=$3, high=$4, low=$5, close=$6, volume=$7, change_rate=$8
                """, symbol, ts,
                    c["open"], c["high"], c["low"], c["close"],
                    c["volume"], c["change_rate"])
                count += 1
        logger.info(f"✅ [{symbol}] {count}개 저장 완료")
        return count

    async def save_indicators(self, symbol: str, candles: list):
        """기술적 지표 계산 후 DB 저장"""
        if len(candles) < 30:
            logger.warning(f"⚠️ [{symbol}] 지표 계산 데이터 부족 ({len(candles)}개)")
            return
        try:
            from ml.indicators import (
                sma, ema, rsi as calc_rsi,
                macd as calc_macd, bollinger_bands, atr, stochastic
            )
            closes  = [float(c["close"]) for c in candles]
            highs   = [float(c["high"]) for c in candles]
            lows    = [float(c["low"]) for c in candles]

            rsi14     = calc_rsi(closes, 14)
            macd_vals = calc_macd(closes, 12, 26, 9)
            bb        = bollinger_bands(closes, 20, 2.0)
            atr14     = atr(highs, lows, closes, 14)
            stoch     = stochastic(highs, lows, closes, 14, 3)
            sma5      = sma(closes, 5)
            sma20     = sma(closes, 20)
            sma60     = sma(closes, 60)
            ema12     = ema(closes, 12)
            ema26     = ema(closes, 26)

            async with db.pool.acquire() as conn:
                for i in range(len(candles)):
                    if len(candles[i]["date"]) != 8:
                        continue
                    ts = datetime.strptime(candles[i]["date"], "%Y%m%d")
                    await conn.execute("""
                        INSERT INTO stock_indicators
                            (symbol, ts, rsi14, macd, macd_signal, macd_hist,
                             bb_upper, bb_middle, bb_lower, bb_pct,
                             atr14, stoch_k, stoch_d,
                             sma5, sma20, sma60, ema12, ema26,
                             golden_cross, dead_cross)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
                        ON CONFLICT (symbol, ts) DO UPDATE
                        SET rsi14=$3, macd=$4, macd_signal=$5, macd_hist=$6,
                            bb_upper=$7, bb_middle=$8, bb_lower=$9, bb_pct=$10,
                            atr14=$11, stoch_k=$12, stoch_d=$13,
                            sma5=$14, sma20=$15, sma60=$16, ema12=$17, ema26=$18,
                            golden_cross=$19, dead_cross=$20
                    """,
                        symbol, ts,
                        rsi14[i], macd_vals["macd"][i], macd_vals["signal"][i], macd_vals["histogram"][i],
                        bb["upper"][i], bb["middle"][i], bb["lower"][i], bb["percent_b"][i],
                        atr14[i], stoch["k"][i], stoch["d"][i],
                        sma5[i], sma20[i], sma60[i], ema12[i], ema26[i],
                        bool(sma5[i] and sma20[i] and sma5[i] > sma20[i] and
                             i > 0 and sma5[i-1] and sma20[i-1] and sma5[i-1] <= sma20[i-1]),
                        bool(sma5[i] and sma20[i] and sma5[i] < sma20[i] and
                             i > 0 and sma5[i-1] and sma20[i-1] and sma5[i-1] >= sma20[i-1]),
                    )
            logger.info(f"✅ [{symbol}] 지표 저장 완료")
        except Exception as e:
            logger.error(f"지표 저장 실패 [{symbol}]: {e}")

    async def run(self):
        await db.connect()

        # DB watchlist에서 종목 읽기 (없으면 환경변수 fallback)
        symbols = await db.get_watchlist_symbols()
        if not symbols:
            symbols = config.STOCK_SYMBOLS
            logger.info("⚠️ watchlist 비어있음 → 환경변수 STOCK_SYMBOLS 사용")
        else:
            logger.info(f"✅ DB watchlist에서 종목 로드: {symbols}")
        logger.info(f"🚀 과거 데이터 수집 시작: {START_DATE} ~ {END_DATE} / {len(symbols)}종목")
        logger.info(f"📋 대상 종목: {symbols}")

        total_saved = 0
        for symbol in symbols:
            logger.info(f"\n{'='*40}")
            logger.info(f"📈 [{symbol}] 수집 시작...")
            candles = await self.fetch_ohlcv(symbol)
            if candles:
                count = await self.save_ohlcv(symbol, candles)
                await self.save_indicators(symbol, candles)
                total_saved += count
            else:
                logger.warning(f"⚠️ [{symbol}] 데이터 없음")
            await asyncio.sleep(1)   # pykrx 레이트 리밋 방지

        await db.disconnect()
        logger.info(f"\n{'='*40}")
        logger.info(f"🎉 전체 완료! 총 {total_saved}개 캔들 저장")


if __name__ == "__main__":
    asyncio.run(HistoricalFetcher().run())
