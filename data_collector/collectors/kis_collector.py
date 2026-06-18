import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp

from common.config import config
from common.database import db, cache

logger = logging.getLogger(__name__)


class KISCollector:
    """한국투자증권 KIS API — 주식 시세 수집"""

    BASE_URL = config.kis_base_url
    _shared_token: str = ""
    _token_expires: datetime = datetime.min

    def __init__(self):
        self.session: aiohttp.ClientSession = None

    async def start(self):
        self.session = aiohttp.ClientSession()
        await self._get_token()
        logger.info("✅ KIS Collector 시작")

    async def stop(self):
        if self.session:
            await self.session.close()

    @property
    def access_token(self):
        return KISCollector._shared_token

    # ── 인증 ───────────────────────────────────────────
    async def _get_token(self):
        # Redis에서 토큰 확인 (재시작해도 재사용)
        try:
            cached = await cache.client.get("kis:access_token")
            if cached:
                # bytes → str 변환
                if isinstance(cached, bytes):
                    cached = cached.decode('utf-8')
                KISCollector._shared_token = cached
                logger.info("✅ KIS 토큰 Redis에서 복원")
                return
        except Exception as e:
            logger.warning(f"Redis 토큰 조회 실패: {e}")

        # 클래스 변수 확인
        now = datetime.now()
        if KISCollector._shared_token and KISCollector._token_expires > now:
            logger.info(f"✅ KIS 토큰 재사용 (만료까지 {int((KISCollector._token_expires - now).total_seconds() / 60)}분)")
            return

        # 새 토큰 발급
        try:
            url = f"{self.BASE_URL}/oauth2/tokenP"
            payload = {
                "grant_type": "client_credentials",
                "appkey": config.KIS_APP_KEY,
                "appsecret": config.KIS_APP_SECRET,
            }
            async with self.session.post(url, json=payload) as resp:
                data = await resp.json()
                token = data.get("access_token", "")
                if token:
                    KISCollector._shared_token = token
                    KISCollector._token_expires = now + timedelta(hours=23)
                    try:
                        await cache.client.setex("kis:access_token", 23 * 3600, token)
                    except Exception as e:
                        logger.warning(f"Redis 토큰 저장 실패: {e}")
                    logger.info("✅ KIS 토큰 발급 완료 (23시간 유효)")
                else:
                    logger.error(f"❌ KIS 토큰 발급 실패: {data}")
        except Exception as e:
            logger.error(f"❌ KIS 토큰 발급 오류: {e}")

    def _headers(self, tr_id: str) -> dict:
        return {
            "Content-Type": "application/json",
            "authorization": f"Bearer {KISCollector._shared_token}",
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

        success = 0
        for symbol in symbols:
            try:
                await self._collect_one(symbol)
                success += 1
            except Exception as e:
                logger.error(f"수집 실패 [{symbol}]: {e}")
            await asyncio.sleep(0.3)  # 레이트 리밋 방지

        logger.info(f"✅ 주식 수집 완료 — {success}/{len(symbols)}종목")

    async def _collect_one(self, symbol: str):
        """단일 종목 수집"""
        # 현재가
        price_data = await self.get_price(symbol)
        if not price_data:
            return

        # Redis 캐시 저장 (실시간)
        await cache.set_price(f"stock:price:{symbol}", price_data, ttl=120)

        # price가 0이면 저장 안함
        if price_data["price"] <= 0:
            logger.warning(f"⚠️ [{symbol}] 시세 0 — DB 저장 스킵")
            return

        # DB 저장 (1분봉)
        from datetime import timezone, timedelta
        KST = timezone(timedelta(hours=9))
        now = datetime.now(KST).replace(second=0, microsecond=0, tzinfo=None)
        await db.insert_stock_ohlcv(
            symbol=symbol,
            ts=now,
            o=price_data["open"],
            h=price_data["high"],
            l=price_data["low"],
            c=price_data["price"],
            v=price_data["volume"],
        )

        # API 레이트 리밋 방지 (실전 API: 초당 20건 제한)
        await asyncio.sleep(0.2)
