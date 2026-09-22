import json
import logging
from datetime import datetime
from typing import Optional

import asyncpg
import redis.asyncio as aioredis

from common.config import config
from common.migrations import run_migrations

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
        await self._run_migrations()

    async def disconnect(self):
        if self.pool:
            await self.pool.close()

    async def _run_migrations(self):
        """스키마 정본은 migrations/*.sql 이다 (common/migrations.py 러너 사용).
        이전에는 여기서 CREATE TABLE IF NOT EXISTS를 직접 실행했으나,
        STARK v2 DB 마이그레이션 정본화 작업으로 migrations/ 디렉터리의
        순차 마이그레이션 파일로 이관했다."""
        applied = await run_migrations(self.pool)
        if applied:
            logger.info(f"✅ 신규 마이그레이션 적용: {', '.join(applied)}")
        else:
            logger.info("✅ DB 스키마 최신 상태 (신규 마이그레이션 없음)")

    async def insert_stock_ohlcv(self, symbol, ts, o, h, l, c, v):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO stock_ohlcv (symbol, ts, open, high, low, close, volume)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT (symbol, ts) DO UPDATE
                SET open=$3, high=$4, low=$5, close=$6, volume=$7
            """, symbol, ts, o, h, l, c, v)
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
            table = "stock_ohlcv"
            col = "symbol"
        async with self.pool.acquire() as conn:
            # 최신 N개를 DESC로 가져온 뒤 ASC로 뒤집기
            rows = await conn.fetch(f"""
                SELECT * FROM (
                    SELECT * FROM {table} WHERE {col}=$1 ORDER BY ts DESC LIMIT $2
                ) sub ORDER BY ts ASC
            """, symbol, limit)
            return rows


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
