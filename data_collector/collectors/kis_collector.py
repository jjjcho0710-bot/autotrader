import asyncio
import logging
from datetime import datetime

import aiohttp

from common.config import config
from common.database import db, cache

logger = logging.getLogger(__name__)


class KISCollector:
    """한국투자증권 KIS API — 주식 시세 수집"""

    BASE_URL = config.kis_base_url

    def __init__(self):
        self.access_token: str = ""
        self.token_expires: datetime = datetime.min
        self.session: aiohttp.ClientSession = None

    async def start(self):
        self.session = aiohttp.ClientSession()
        await self._get_token()
        logger.info("✅ KIS Collector 시작")

    async def stop(self):
        if self.session:
            await self.session.close()

    # ── 인증 ───────────────────────────────────────────
    async def _get_token(self):
        url = f"{self.BASE_URL}/oauth2/tokenP"
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

    # ── 현재가 조회 ─────────────────────────────────────
    async def get_price(self, symbol: str) -> dict:
        """주식 현재가 조회"""
        url = f"{self.BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
        }
        try:
            async with self.session.get(
                url,
                headers=self._headers("FHKST01010100"),
                params=params,
            ) as resp:
                data = await resp.json()
                output = data.get("output", {})
                return {
                    "symbol": symbol,
                    "price": int(output.get("stck_prpr", 0)),       # 현재가
                    "open":  int(output.get("stck_oprc", 0)),       # 시가
                    "high":  int(output.get("stck_hgpr", 0)),       # 고가
                    "low":   int(output.get("stck_lwpr", 0)),       # 저가
                    "volume":int(output.get("acml_vol", 0)),        # 누적거래량
                    "change_rate": float(output.get("prdy_ctrt", 0)),  # 전일대비율
                    "ts": datetime.now().isoformat(),
                }
        except Exception as e:
            logger.error(f"KIS 현재가 조회 실패 [{symbol}]: {e}")
            return {}

    # ── 1분봉 OHLCV 조회 ──────────────────────────────
    async def get_minute_ohlcv(self, symbol: str) -> list:
        """1분봉 OHLCV 조회"""
        url = f"{self.BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
        params = {
            "FID_ETC_CLS_CODE": "",
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
            "FID_INPUT_HOUR_1": datetime.now().strftime("%H%M%S"),
            "FID_PW_DATA_INCU_YN": "Y",
        }
        try:
            async with self.session.get(
                url,
                headers=self._headers("FHKST03010200"),
                params=params,
            ) as resp:
                data = await resp.json()
                candles = []
                for row in data.get("output2", []):
                    candles.append({
                        "ts":     row.get("stck_bsop_date", "") + row.get("stck_cntg_hour", ""),
                        "open":   int(row.get("stck_oprc", 0)),
                        "high":   int(row.get("stck_hgpr", 0)),
                        "low":    int(row.get("stck_lwpr", 0)),
                        "close":  int(row.get("stck_prpr", 0)),
                        "volume": int(row.get("cntg_vol", 0)),
                    })
                return candles
        except Exception as e:
            logger.error(f"KIS 1분봉 조회 실패 [{symbol}]: {e}")
            return []

    # ── 전체 수집 루프 ──────────────────────────────────
    async def collect_all(self):
        """설정된 전체 종목 수집 → DB 저장 + Redis 캐시"""
        try:
            symbols = await db.get_watchlist_symbols()
            if not symbols:
                symbols = config.STOCK_SYMBOLS
        except Exception:
            symbols = config.STOCK_SYMBOLS
        logger.info(f"📊 주식 수집 시작 — {len(symbols)}종목")

        tasks = [self._collect_one(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        success = sum(1 for r in results if not isinstance(r, Exception))
        logger.info(f"✅ 주식 수집 완료 — {success}/{len(symbols)}종목")

    async def _collect_one(self, symbol: str):
        """단일 종목 수집"""
        # 현재가
        price_data = await self.get_price(symbol)
        if not price_data:
            return

        # Redis 캐시 저장 (실시간)
        await cache.set_price(f"stock:price:{symbol}", price_data, ttl=120)

        # DB 저장 (1분봉)
        now = datetime.now().replace(second=0, microsecond=0)
        await db.insert_stock_ohlcv(
            symbol=symbol,
            ts=now,
            o=price_data["open"],
            h=price_data["high"],
            l=price_data["low"],
            c=price_data["price"],
            v=price_data["volume"],
        )

        # API 레이트 리밋 방지
        await asyncio.sleep(0.1)
