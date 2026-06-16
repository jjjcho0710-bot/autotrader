"""
과거 데이터 일괄 수집 스크립트
2025-01-01 부터 오늘까지 일봉 데이터 한 번에 수집
Railway에서 한 번만 실행하면 됨

실행법:
  python historical_fetch.py
"""
import asyncio
import logging
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, "/app")   # ml 모듈 경로 추가

import aiohttp

sys.path.insert(0, "/app")
from common.config import config
from common.database import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("historical-fetch")

# ── 설정 ──────────────────────────────────────────────
START_DATE = "20250101"   # 수집 시작일 (여기서 조정)
BASE_URL   = config.kis_base_url


class HistoricalFetcher:

    def __init__(self):
        self.access_token = ""
        self.session: aiohttp.ClientSession = None

    async def start(self):
        self.session = aiohttp.ClientSession()
        await self._get_token()

    async def stop(self):
        if self.session:
            await self.session.close()

    async def _get_token(self):
        url = f"{BASE_URL}/oauth2/tokenP"
        payload = {
            "grant_type": "client_credentials",
            "appkey": config.KIS_APP_KEY,
            "appsecret": config.KIS_APP_SECRET,
        }
        async with self.session.post(url, json=payload) as resp:
            data = await resp.json()
            self.access_token = data.get("access_token", "")
            logger.info("✅ KIS 토큰 발급 완료")

    def _headers(self, tr_id: str) -> dict:
        return {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.access_token}",
            "appkey": config.KIS_APP_KEY,
            "appsecret": config.KIS_APP_SECRET,
            "tr_id": tr_id,
            "custtype": "P",
        }

    async def fetch_period(self, symbol: str, start: str, end: str) -> list:
        """특정 기간 일봉 조회 (KIS API 1회 호출 = 최대 100개)"""
        url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
            "FID_INPUT_DATE_1": start,
            "FID_INPUT_DATE_2": end,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
        }
        try:
            async with self.session.get(
                url,
                headers=self._headers("FHKST03010100"),
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()
                candles = []
                for row in data.get("output2", []):
                    close = int(row.get("stck_clpr", 0))
                    if close <= 0:
                        continue
                    candles.append({
                        "date":        row.get("stck_bsop_date", ""),
                        "open":        int(row.get("stck_oprc", 0)),
                        "high":        int(row.get("stck_hgpr", 0)),
                        "low":         int(row.get("stck_lwpr", 0)),
                        "close":       close,
                        "volume":      int(row.get("acml_vol", 0)),
                        "change_rate": float(row.get("prdy_ctrt", 0)),
                    })
                return list(reversed(candles))
        except Exception as e:
            logger.error(f"조회 실패 [{symbol} {start}~{end}]: {e}")
            return []

    async def fetch_all_history(self, symbol: str) -> list:
        """START_DATE ~ 오늘까지 100일 단위로 나눠서 전체 수집"""
        all_candles = []
        seen_dates = set()

        start_dt = datetime.strptime(START_DATE, "%Y%m%d")
        end_dt   = datetime.now()

        # 100일씩 슬라이딩
        cursor = start_dt
        while cursor < end_dt:
            chunk_end = min(cursor + timedelta(days=100), end_dt)
            start_str = cursor.strftime("%Y%m%d")
            end_str   = chunk_end.strftime("%Y%m%d")

            candles = await self.fetch_period(symbol, start_str, end_str)

            for c in candles:
                if c["date"] not in seen_dates:
                    seen_dates.add(c["date"])
                    all_candles.append(c)

            logger.info(f"  [{symbol}] {start_str}~{end_str}: {len(candles)}개 수집")
            cursor = chunk_end + timedelta(days=1)
            await asyncio.sleep(0.3)   # API 레이트 리밋 방지

        # 날짜 오름차순 정렬
        all_candles.sort(key=lambda x: x["date"])
        return all_candles

    async def save_ohlcv(self, symbol: str, candles: list):
        """DB에 저장 (중복 무시)"""
        if not candles:
            return
        async with db.pool.acquire() as conn:
            for c in candles:
                date_str = c["date"]
                if len(date_str) != 8:
                    continue
                ts = datetime.strptime(date_str, "%Y%m%d")
                await conn.execute("""
                    INSERT INTO stock_daily_ohlcv
                        (symbol, ts, open, high, low, close, volume, change_rate)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT (symbol, ts) DO UPDATE
                    SET open=$3, high=$4, low=$5, close=$6, volume=$7, change_rate=$8
                """, symbol, ts,
                    c["open"], c["high"], c["low"], c["close"],
                    c["volume"], c["change_rate"])
        logger.info(f"✅ [{symbol}] {len(candles)}개 저장 완료")

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
                    date_str = candles[i]["date"]
                    if len(date_str) != 8:
                        continue
                    ts = datetime.strptime(date_str, "%Y%m%d")
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
                        atr14[i], stoch[i]["k"] if isinstance(stoch, list) else stoch["k"][i],
                        stoch[i]["d"] if isinstance(stoch, list) else stoch["d"][i],
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
        await self.start()

        symbols = config.STOCK_SYMBOLS
        logger.info(f"🚀 과거 데이터 수집 시작: {START_DATE} ~ 오늘 / {len(symbols)}종목")
        logger.info(f"📋 대상 종목: {symbols}")

        total_saved = 0
        for symbol in symbols:
            logger.info(f"\n{'='*40}")
            logger.info(f"📈 [{symbol}] 수집 시작...")
            candles = await self.fetch_all_history(symbol)
            if candles:
                await self.save_ohlcv(symbol, candles)
                await self.save_indicators(symbol, candles)
                total_saved += len(candles)
                logger.info(f"✅ [{symbol}] 총 {len(candles)}개 완료")
            else:
                logger.warning(f"⚠️ [{symbol}] 데이터 없음")
            await asyncio.sleep(1)   # 종목 간 딜레이

        await self.stop()
        await db.disconnect()
        logger.info(f"\n{'='*40}")
        logger.info(f"🎉 전체 완료! 총 {total_saved}개 캔들 저장")


if __name__ == "__main__":
    asyncio.run(HistoricalFetcher().run())
