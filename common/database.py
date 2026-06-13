import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import asyncpg
import redis.asyncio as aioredis

from common.config import config

logger = logging.getLogger(__name__)


# ── PostgreSQL ──────────────────────────────────────────
class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            host=config.DB_HOST,
            port=config.DB_PORT,
            database=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASS,
            min_size=2,
            max_size=10,
        )
        logger.info("✅ PostgreSQL 연결 완료")
        await self._create_tables()

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL 연결 종료")

    async def _create_tables(self):
        """테이블 없으면 자동 생성"""
        async with self.pool.acquire() as conn:
            # 주식 OHLCV
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_ohlcv (
                    id          BIGSERIAL PRIMARY KEY,
                    symbol      VARCHAR(10) NOT NULL,
                    ts          TIMESTAMPTZ NOT NULL,
                    open        BIGINT,
                    high        BIGINT,
                    low         BIGINT,
                    close       BIGINT,
                    volume      BIGINT,
                    created_at  TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_ohlcv_symbol_ts
                    ON stock_ohlcv (symbol, ts);
            """)

            # 코인 OHLCV
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS crypto_ohlcv (
                    id          BIGSERIAL PRIMARY KEY,
                    pair        VARCHAR(20) NOT NULL,
                    ts          TIMESTAMPTZ NOT NULL,
                    open        NUMERIC(20,2),
                    high        NUMERIC(20,2),
                    low         NUMERIC(20,2),
                    close       NUMERIC(20,2),
                    volume      NUMERIC(20,8),
                    created_at  TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_crypto_ohlcv_pair_ts
                    ON crypto_ohlcv (pair, ts);
            """)

            # 매매 이력
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS trade_history (
                    id          BIGSERIAL PRIMARY KEY,
                    bot         VARCHAR(20) NOT NULL,   -- stock_trader / crypto_trader
                    asset_type  VARCHAR(10) NOT NULL,   -- stock / crypto
                    symbol      VARCHAR(20) NOT NULL,
                    side        VARCHAR(5) NOT NULL,    -- BUY / SELL
                    price       NUMERIC(20,2),
                    quantity    NUMERIC(20,8),
                    amount      NUMERIC(20,2),
                    strategy    VARCHAR(50),
                    pnl         NUMERIC(20,2),
                    ts          TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # 잔고 스냅샷
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS balance_snapshot (
                    id          BIGSERIAL PRIMARY KEY,
                    bot         VARCHAR(20) NOT NULL,
                    total_krw   NUMERIC(20,2),
                    cash_krw    NUMERIC(20,2),
                    eval_krw    NUMERIC(20,2),
                    pnl_today   NUMERIC(20,2),
                    ts          TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            logger.info("✅ DB 테이블 확인 완료")

    @asynccontextmanager
    async def acquire(self):
        async with self.pool.acquire() as conn:
            yield conn

    async def insert_stock_ohlcv(self, symbol: str, ts: datetime, o: int, h: int, l: int, c: int, v: int):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO stock_ohlcv (symbol, ts, open, high, low, close, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (symbol, ts) DO UPDATE
                SET open=$3, high=$4, low=$5, close=$6, volume=$7
            """, symbol, ts, o, h, l, c, v)

    async def insert_crypto_ohlcv(self, pair: str, ts: datetime, o, h, l, c, v):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO crypto_ohlcv (pair, ts, open, high, low, close, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (pair, ts) DO UPDATE
                SET open=$3, high=$4, low=$5, close=$6, volume=$7
            """, pair, ts, float(o), float(h), float(l), float(c), float(v))

    async def insert_trade(self, bot: str, asset_type: str, symbol: str,
                           side: str, price: float, quantity: float,
                           amount: float, strategy: str, pnl: float = None):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO trade_history
                    (bot, asset_type, symbol, side, price, quantity, amount, strategy, pnl)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """, bot, asset_type, symbol, side, price, quantity, amount, strategy, pnl)

    async def get_recent_ohlcv(self, symbol: str, limit: int = 60, asset: str = "stock"):
        table = "stock_ohlcv" if asset == "stock" else "crypto_ohlcv"
        col = "symbol" if asset == "stock" else "pair"
        async with self.pool.acquire() as conn:
            return await conn.fetch(f"""
                SELECT * FROM {table}
                WHERE {col} = $1
                ORDER BY ts DESC
                LIMIT $2
            """, symbol, limit)


# ── Redis ──────────────────────────────────────────────
class Cache:
    def __init__(self):
        self.client: Optional[aioredis.Redis] = None

    async def connect(self):
        self.client = aioredis.from_url(
            config.redis_url,
            encoding="utf-8",
            decode_responses=True,
        )
        await self.client.ping()
        logger.info("✅ Redis 연결 완료")

    async def disconnect(self):
        if self.client:
            await self.client.close()

    async def set_price(self, key: str, data: dict, ttl: int = 120):
        """실시간 시세 캐시 (기본 2분 TTL)"""
        await self.client.setex(key, ttl, json.dumps(data, ensure_ascii=False))

    async def get_price(self, key: str) -> Optional[dict]:
        val = await self.client.get(key)
        return json.loads(val) if val else None

    async def push_signal(self, channel: str, signal: dict):
        """매매 신호 큐에 발행"""
        await self.client.publish(channel, json.dumps(signal, ensure_ascii=False))

    async def set_bot_status(self, bot: str, status: dict):
        await self.client.hset("bot:status", bot, json.dumps(status, ensure_ascii=False))

    async def get_all_bot_status(self) -> dict:
        raw = await self.client.hgetall("bot:status")
        return {k: json.loads(v) for k, v in raw.items()}


# 싱글톤
db = Database()
cache = Cache()
