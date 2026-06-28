import asyncio
import logging
from datetime import datetime

import aiohttp

from common.config import config
from common.database import db, cache

logger = logging.getLogger(__name__)

UPBIT_API = "https://api.upbit.com/v1"


class UpbitCollector:
    """업비트 API — 코인 시세 수집 (인증 불필요 — 공개 API)"""

    def __init__(self):
        self.session: aiohttp.ClientSession = None

    async def start(self):
        self.session = aiohttp.ClientSession(
            headers={"Accept": "application/json"}
        )
        logger.info("✅ Upbit Collector 시작")

    async def stop(self):
        if self.session:
            await self.session.close()

    # ── 현재가 (ticker) ────────────────────────────────
    async def get_ticker(self, pairs: list) -> list:
        """여러 페어 현재가 한번에 조회"""
        markets = ",".join(pairs)
        url = f"{UPBIT_API}/ticker"
        try:
            async with self.session.get(url, params={"markets": markets}) as resp:
                data = await resp.json()
                return data
        except Exception as e:
            logger.error(f"업비트 ticker 조회 실패: {e}")
            return []

    # ── 1분봉 OHLCV ────────────────────────────────────
    async def get_minute_candles(self, pair: str, count: int = 5) -> list:
        """1분봉 캔들 조회"""
        url = f"{UPBIT_API}/candles/minutes/1"
        params = {"market": pair, "count": count}
        try:
            async with self.session.get(url, params=params) as resp:
                data = await resp.json()
                candles = []
                for row in data:
                    candles.append({
                        "ts":     row.get("candle_date_time_kst"),
                        "open":   float(row.get("opening_price", 0)),
                        "high":   float(row.get("high_price", 0)),
                        "low":    float(row.get("low_price", 0)),
                        "close":  float(row.get("trade_price", 0)),
                        "volume": float(row.get("candle_acc_trade_volume", 0)),
                    })
                return candles
        except Exception as e:
            logger.error(f"업비트 1분봉 조회 실패 [{pair}]: {e}")
            return []

    # ── 전체 수집 루프 ──────────────────────────────────
    async def collect_all(self):
        """설정된 전체 페어 수집 → DB 저장 + Redis 캐시"""
        # Redis에서 TOP 20 코인 목록 읽기 (crypto-trader가 설정)
        try:
            import json as _json
            cached = await cache.client.get("crypto:top_pairs")
            pairs = _json.loads(cached) if cached else config.CRYPTO_PAIRS
        except:
            pairs = config.CRYPTO_PAIRS
        logger.info(f"₿ 코인 수집 시작 — {len(pairs)}페어")

        # 현재가 한번에 조회
        tickers = await self.get_ticker(pairs)
        ticker_map = {t["market"]: t for t in tickers}

        # 각 페어 저장
        tasks = [self._collect_one(pair, ticker_map.get(pair)) for pair in pairs]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        success = sum(1 for r in results if not isinstance(r, Exception))
        logger.info(f"✅ 코인 수집 완료 — {success}/{len(pairs)}페어")

    async def _collect_one(self, pair: str, ticker: dict):
        """단일 페어 수집"""
        if not ticker:
            return

        # Redis 캐시
        price_data = {
            "pair":         pair,
            "price":        ticker.get("trade_price", 0),
            "open":         ticker.get("opening_price", 0),
            "high":         ticker.get("high_price", 0),
            "low":          ticker.get("low_price", 0),
            "volume":       ticker.get("acc_trade_volume_24h", 0),
            "change_rate":  ticker.get("signed_change_rate", 0) * 100,
            "ts":           datetime.now().isoformat(),
        }
        await cache.set_price(f"crypto:price:{pair}", price_data, ttl=120)

        # DB 저장 (1분봉)
        candles = await self.get_minute_candles(pair, count=1)
        if candles:
            c = candles[0]
            ts_str = c["ts"]  # "2026-06-13T14:32:00"
            ts = datetime.fromisoformat(ts_str)
            await db.insert_crypto_ohlcv(
                pair=pair,
                ts=ts,
                o=c["open"],
                h=c["high"],
                l=c["low"],
                c=c["close"],
                v=c["volume"],
            )

        await asyncio.sleep(0.05)
