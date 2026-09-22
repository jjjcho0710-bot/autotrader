"""
일봉 데이터 수집기
KIS API로 주식 일봉 OHLCV + 기술적 지표 계산 후 DB 저장
하루 1번 장 마감 후 실행 (15:30 이후)
"""
import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp

from common.config import config
from common.database import db, cache

logger = logging.getLogger(__name__)


class DailyCollector:
    """일봉 데이터 + 기술적 지표 수집기"""

    BASE_URL = config.kis_base_url

    def __init__(self):
        self.access_token: str = ""
        self.session: aiohttp.ClientSession = None

    async def start(self):
        self.session = aiohttp.ClientSession()
        await self._get_token()
        logger.info("✅ DailyCollector 시작")

    async def stop(self):
        if self.session:
            await self.session.close()

    async def _get_token(self):
        # Redis에서 토큰 재사용 (kis_collector와 공유). 모의/실전 계좌 토큰은 서로 호환되지
        # 않으므로 키를 분리 — 분리하지 않으면 이 프로세스가 다른 모드로 발급받은 토큰을
        # 실주문 프로세스와 같은 키로 공유하게 되어 "모의투자 주문이 불가한 계좌입니다" 류의
        # 주문 거부를 유발할 수 있다.
        redis_key = "kis:paper_token" if config.KIS_IS_PAPER else "kis:access_token"
        try:
            cached = await cache.client.get(redis_key)
            if cached:
                if isinstance(cached, bytes):
                    cached = cached.decode('utf-8')
                self.access_token = cached
                logger.info("✅ KIS 일봉 토큰 Redis에서 복원")
                return
        except Exception:
            pass

        # 새 토큰 발급
        url = f"{self.BASE_URL}/oauth2/tokenP"
        payload = {
            "grant_type": "client_credentials",
            "appkey": config.kis_app_key,
            "appsecret": config.kis_app_secret,
        }
        try:
            async with self.session.post(url, json=payload) as resp:
                data = await resp.json()
                token = data.get("access_token", "")
                if token:
                    self.access_token = token
                    # Redis에 저장 (23시간)
                    try:
                        await cache.client.setex(redis_key, 23 * 3600, token)
                    except Exception:
                        pass
                    logger.info("✅ KIS 일봉 토큰 발급 완료")
        except Exception as e:
            logger.error(f"토큰 발급 실패: {e}")

    def _headers(self, tr_id: str) -> dict:
        return {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.access_token}",
            "appkey": config.kis_app_key,
            "appsecret": config.kis_app_secret,
            "tr_id": tr_id,
            "custtype": "P",
        }

    async def get_daily_ohlcv(self, symbol: str, days: int = 100) -> list:
        """주식 일봉 OHLCV 조회 (최근 N일)"""
        url = f"{self.BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")

        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
            "FID_INPUT_DATE_1": start_date,
            "FID_INPUT_DATE_2": end_date,
            "FID_PERIOD_DIV_CODE": "D",  # 일봉
            "FID_ORG_ADJ_PRC": "0",
        }
        try:
            async with self.session.get(
                url,
                headers=self._headers("FHKST03010100"),
                params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()
                candles = []
                for row in data.get("output2", []):
                    close = int(row.get("stck_clpr", 0))
                    if close <= 0:
                        continue
                    candles.append({
                        "date": row.get("stck_bsop_date", ""),
                        "open":   int(row.get("stck_oprc", 0)),
                        "high":   int(row.get("stck_hgpr", 0)),
                        "low":    int(row.get("stck_lwpr", 0)),
                        "close":  close,
                        "volume": int(row.get("acml_vol", 0)),
                        "change_rate": float(row.get("prdy_ctrt", 0)),
                    })
                return list(reversed(candles))  # 오래된 것부터
        except Exception as e:
            logger.error(f"일봉 조회 실패 [{symbol}]: {e}")
            return []

    async def save_daily_ohlcv(self, symbol: str, candles: list):
        """일봉 데이터 DB 저장"""
        if not candles:
            return
        try:
            async with db.pool.acquire() as conn:
                for c in candles:
                    date_str = c["date"]
                    if len(date_str) == 8:
                        ts = datetime.strptime(date_str, "%Y%m%d")
                    else:
                        continue
                    await conn.execute("""
                        INSERT INTO stock_daily_ohlcv
                            (symbol, ts, open, high, low, close, volume, change_rate)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                        ON CONFLICT (symbol, ts) DO UPDATE
                        SET open=$3, high=$4, low=$5, close=$6, volume=$7, change_rate=$8
                    """, symbol, ts,
                        c["open"], c["high"], c["low"], c["close"],
                        c["volume"], c["change_rate"])
            logger.info(f"✅ 일봉 저장: {symbol} {len(candles)}일")
        except Exception as e:
            logger.error(f"일봉 저장 실패 [{symbol}]: {e}")

    async def save_indicators(self, symbol: str, candles: list):
        """기술적 지표 계산 후 DB 저장"""
        if len(candles) < 30:
            return
        try:
            import sys
            sys.path.insert(0, "/app")
            from ml.indicators import (
                sma, ema, rsi as calc_rsi,
                macd as calc_macd, bollinger_bands, atr, stochastic
            )

            closes  = [float(c["close"]) for c in candles]
            highs   = [float(c["high"]) for c in candles]
            lows    = [float(c["low"]) for c in candles]
            volumes = [float(c["volume"]) for c in candles]

            rsi14      = calc_rsi(closes, 14)
            macd_vals  = calc_macd(closes, 12, 26, 9)
            bb         = bollinger_bands(closes, 20, 2.0)
            atr14      = atr(highs, lows, closes, 14)
            stoch      = stochastic(highs, lows, closes, 14, 3)
            sma5       = sma(closes, 5)
            sma20      = sma(closes, 20)
            sma60      = sma(closes, 60)
            ema12      = ema(closes, 12)
            ema26      = ema(closes, 26)

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
                        atr14[i], stoch["k"][i], stoch["d"][i],
                        sma5[i], sma20[i], sma60[i], ema12[i], ema26[i],
                        bool(sma5[i] and sma20[i] and sma5[i] > sma20[i] and
                             i > 0 and sma5[i-1] and sma20[i-1] and sma5[i-1] <= sma20[i-1]),
                        bool(sma5[i] and sma20[i] and sma5[i] < sma20[i] and
                             i > 0 and sma5[i-1] and sma20[i-1] and sma5[i-1] >= sma20[i-1]),
                    )
            logger.info(f"✅ 지표 저장: {symbol}")
        except Exception as e:
            logger.error(f"지표 저장 실패 [{symbol}]: {e}")

    async def collect_all(self):
        """전체 종목 일봉 + 지표 수집"""
        try:
            symbols = await db.get_watchlist_symbols()
            if not symbols:
                symbols = config.STOCK_SYMBOLS
        except Exception:
            symbols = config.STOCK_SYMBOLS
        logger.info(f"📅 일봉 수집 시작 — {len(symbols)}종목")
        for symbol in symbols:
            try:
                candles = await self.get_daily_ohlcv(symbol, days=200)
                if candles:
                    await self.save_daily_ohlcv(symbol, candles)
                    await self.save_indicators(symbol, candles)
                await asyncio.sleep(0.5)  # API 레이트 리밋
            except Exception as e:
                logger.error(f"일봉 수집 실패 [{symbol}]: {e}")
        logger.info("✅ 일봉 수집 완료")
