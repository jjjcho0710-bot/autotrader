import json
import logging
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
            host=config.DB_HOST, port=config.DB_PORT,
            database=config.DB_NAME, user=config.DB_USER,
            password=config.DB_PASS, min_size=2, max_size=10,
        )
        logger.info("✅ PostgreSQL 연결 완료")
        await self._create_tables()

    async def disconnect(self):
        if self.pool:
            await self.pool.close()

    async def _create_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_ohlcv (
                    id BIGSERIAL PRIMARY KEY, symbol VARCHAR(10) NOT NULL,
                    ts TIMESTAMPTZ NOT NULL, open BIGINT, high BIGINT,
                    low BIGINT, close BIGINT, volume BIGINT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_ohlcv_symbol_ts ON stock_ohlcv (symbol, ts);

                CREATE TABLE IF NOT EXISTS crypto_ohlcv (
                    id BIGSERIAL PRIMARY KEY, pair VARCHAR(20) NOT NULL,
                    ts TIMESTAMPTZ NOT NULL, open NUMERIC(20,2), high NUMERIC(20,2),
                    low NUMERIC(20,2), close NUMERIC(20,2), volume NUMERIC(20,8),
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_crypto_ohlcv_pair_ts ON crypto_ohlcv (pair, ts);

                CREATE TABLE IF NOT EXISTS trade_history (
                    id BIGSERIAL PRIMARY KEY, bot VARCHAR(20) NOT NULL,
                    asset_type VARCHAR(10) NOT NULL, symbol VARCHAR(20) NOT NULL,
                    side VARCHAR(5) NOT NULL, price NUMERIC(20,2), quantity NUMERIC(20,8),
                    amount NUMERIC(20,2), strategy VARCHAR(50), pnl NUMERIC(20,2),
                    ts TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS balance_snapshot (
                    id BIGSERIAL PRIMARY KEY, bot VARCHAR(20) NOT NULL,
                    total_krw NUMERIC(20,2), cash_krw NUMERIC(20,2),
                    eval_krw NUMERIC(20,2), pnl_today NUMERIC(20,2),
                    ts TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS stock_daily_ohlcv (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    ts DATE NOT NULL,
                    open BIGINT, high BIGINT, low BIGINT, close BIGINT,
                    volume BIGINT, change_rate NUMERIC(8,2),
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_daily_symbol_ts
                    ON stock_daily_ohlcv (symbol, ts);

                CREATE TABLE IF NOT EXISTS stock_indicators (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    ts DATE NOT NULL,
                    rsi14 NUMERIC(8,2),
                    macd NUMERIC(12,2), macd_signal NUMERIC(12,2), macd_hist NUMERIC(12,2),
                    bb_upper NUMERIC(12,2), bb_middle NUMERIC(12,2), bb_lower NUMERIC(12,2),
                    bb_pct NUMERIC(8,4),
                    atr14 NUMERIC(12,2),
                    stoch_k NUMERIC(8,2), stoch_d NUMERIC(8,2),
                    sma5 NUMERIC(12,2), sma20 NUMERIC(12,2), sma60 NUMERIC(12,2),
                    ema12 NUMERIC(12,2), ema26 NUMERIC(12,2),
                    golden_cross BOOLEAN, dead_cross BOOLEAN,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_indicators_symbol_ts
                    ON stock_indicators (symbol, ts);

                CREATE TABLE IF NOT EXISTS ml_predictions (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    ts TIMESTAMPTZ NOT NULL,
                    model_name VARCHAR(50),
                    buy_prob NUMERIC(6,4),
                    sell_prob NUMERIC(6,4),
                    signal VARCHAR(10),
                    features JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS strategy_config (
                    id SERIAL PRIMARY KEY, bot VARCHAR(20) NOT NULL,
                    name VARCHAR(50) NOT NULL, is_active BOOLEAN DEFAULT FALSE,
                    params JSONB DEFAULT '{}', updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(bot, name)
                );
                CREATE TABLE IF NOT EXISTS watchlist (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL UNIQUE,
                    name VARCHAR(50),
                    added_by VARCHAR(20) DEFAULT 'manual',
                    reason TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS stock_supply (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    foreign_net BIGINT DEFAULT 0,
                    institution_net BIGINT DEFAULT 0,
                    individual_net BIGINT DEFAULT 0,
                    foreign_hold_ratio NUMERIC(6,2) DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(symbol, date)
                );
                CREATE TABLE IF NOT EXISTS stock_disclosure (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10),
                    corp_name VARCHAR(100),
                    report_name VARCHAR(200),
                    rcept_dt VARCHAR(20),
                    rcept_no VARCHAR(20) UNIQUE,
                    is_important BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS stock_news_sentiment (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    sentiment_score INTEGER DEFAULT 0,
                    signal VARCHAR(10) DEFAULT 'NEUTRAL',
                    summary TEXT,
                    news_count INTEGER DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(symbol, date)
                );

                INSERT INTO strategy_config (bot, name, is_active, params) VALUES
                ('stock_trader','MA크로스',true,'{"short":5,"long":20,"stop_loss":-2,"take_profit":5,"buy_amount":500000,"max_positions":5}'),
                ('stock_trader','RSI반등',false,'{"period":14,"entry":30,"exit":60,"stop_loss":-2,"buy_amount":500000}'),
                ('stock_trader','볼린저밴드',false,'{"period":20,"std":2,"stop_loss":-2,"buy_amount":500000}'),
                ('crypto_trader','MACD',true,'{"fast":12,"slow":26,"signal":9,"candle_min":60,"stop_loss":-3,"take_profit":7,"buy_amount":500000}'),
                ('crypto_trader','변동성돌파',false,'{"k":0.5,"candle_min":1440,"stop_loss":-3}'),
                ('crypto_trader','RSI과매도',false,'{"period":14,"entry":25,"exit":65,"stop_loss":-3,"buy_amount":500000}')
                ON CONFLICT (bot, name) DO NOTHING;
            """)
            logger.info("✅ DB 테이블 확인 완료")

    async def insert_stock_ohlcv(self, symbol, ts, o, h, l, c, v):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO stock_ohlcv (symbol, ts, open, high, low, close, volume)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT (symbol, ts) DO UPDATE
                SET open=$3, high=$4, low=$5, close=$6, volume=$7
            """, symbol, ts, o, h, l, c, v)

    async def insert_crypto_ohlcv(self, pair, ts, o, h, l, c, v):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO crypto_ohlcv (pair, ts, open, high, low, close, volume)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT (pair, ts) DO UPDATE
                SET open=$3, high=$4, low=$5, close=$6, volume=$7
            """, pair, ts, float(o), float(h), float(l), float(c), float(v))

    async def insert_trade(self, bot, asset_type, symbol, side, price, quantity, amount, strategy, pnl=None):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO trade_history (bot,asset_type,symbol,side,price,quantity,amount,strategy,pnl)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """, bot, asset_type, symbol, side, price, quantity, amount, strategy, pnl)

    async def get_recent_ohlcv(self, symbol, limit=60, asset="stock", daily=False):
        if daily:
            table = "stock_daily_ohlcv"
            col = "symbol"
        else:
            table = "stock_ohlcv" if asset == "stock" else "crypto_ohlcv"
            col = "symbol" if asset == "stock" else "pair"
        async with self.pool.acquire() as conn:
            return await conn.fetch(f"""
                SELECT * FROM {table} WHERE {col}=$1 ORDER BY ts ASC LIMIT $2
            """, symbol, limit)


# ── Redis ──────────────────────────────────────────────
class Cache:
    def __init__(self):
        self.client: Optional[aioredis.Redis] = None

    async def connect(self):
        self.client = aioredis.from_url(
            config.redis_url, encoding="utf-8", decode_responses=True
        )
        await self.client.ping()
        logger.info("✅ Redis 연결 완료")

    async def disconnect(self):
        if self.client:
            await self.client.close()

    async def set_price(self, key: str, data: dict, ttl: int = 120):
        await self.client.setex(key, ttl, json.dumps(data, ensure_ascii=False))

    async def get_price(self, key: str):
        val = await self.client.get(key)
        return json.loads(val) if val else None

    async def push_signal(self, channel: str, signal: dict):
        await self.client.publish(channel, json.dumps(signal, ensure_ascii=False))

    async def set_bot_status(self, bot: str, status: dict):
        await self.client.hset("bot:status", bot, json.dumps(status, ensure_ascii=False))

    async def get_all_bot_status(self):
        raw = await self.client.hgetall("bot:status")
        return {k: json.loads(v) for k, v in raw.items()}


# 싱글톤
db = Database()
cache = Cache()


# ── Database watchlist 메서드 동적 추가 ──────────────────
import types

async def _get_watchlist(self, active_only: bool = True) -> list:
    async with self.pool.acquire() as conn:
        if active_only:
            rows = await conn.fetch("SELECT * FROM watchlist WHERE is_active=TRUE ORDER BY created_at")
        else:
            rows = await conn.fetch("SELECT * FROM watchlist ORDER BY created_at")
    return [dict(r) for r in rows]

async def _get_watchlist_symbols(self) -> list:
    rows = await self.get_watchlist(active_only=True)
    return [r["symbol"] for r in rows]

async def _add_watchlist(self, symbol: str, name: str = None, added_by: str = "jarvis", reason: str = None) -> bool:
    try:
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO watchlist (symbol, name, added_by, reason, is_active)
                VALUES ($1, $2, $3, $4, TRUE)
                ON CONFLICT (symbol) DO UPDATE
                SET is_active=TRUE, name=COALESCE($2, watchlist.name),
                    added_by=$3, reason=$4, updated_at=NOW()
            """, symbol, name, added_by, reason)
        return True
    except Exception as e:
        return False

async def _remove_watchlist(self, symbol: str) -> bool:
    try:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE watchlist SET is_active=FALSE, updated_at=NOW() WHERE symbol=$1", symbol
            )
        return True
    except Exception as e:
        return False

Database.get_watchlist         = _get_watchlist
Database.get_watchlist_symbols = _get_watchlist_symbols
Database.add_watchlist         = _add_watchlist
Database.remove_watchlist      = _remove_watchlist
