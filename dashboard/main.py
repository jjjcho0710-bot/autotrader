"""
AutoTrader Dashboard — FastAPI 서버
실시간 DB/Redis 데이터를 API로 제공
"""
import json
import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncpg
import redis.asyncio as aioredis

from common.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard")

app = FastAPI(title="AutoTrader Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# DB / Redis 연결
db_pool: Optional[asyncpg.Pool] = None
redis_client: Optional[aioredis.Redis] = None


@app.on_event("startup")
async def startup():
    global db_pool, redis_client
    db_pool = await asyncpg.create_pool(
        host=config.DB_HOST, port=config.DB_PORT,
        database=config.DB_NAME, user=config.DB_USER,
        password=config.DB_PASS, min_size=2, max_size=5,
    )
    redis_client = aioredis.from_url(config.redis_url, decode_responses=True)
    logger.info("✅ Dashboard 서버 시작")


@app.on_event("shutdown")
async def shutdown():
    if db_pool:
        await db_pool.close()
    if redis_client:
        await redis_client.close()


# ── 정적 파일 ───────────────────────────────────────────
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def root():
    with open("static/jarvis.html", encoding="utf-8") as f:
        return f.read()

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    with open("static/dashboard-C.html", encoding="utf-8") as f:
        return f.read()

@app.get("/stock", response_class=HTMLResponse)
async def stock():
    with open("static/stock.html", encoding="utf-8") as f:
        return f.read()

@app.get("/crypto", response_class=HTMLResponse)
async def crypto():
    with open("static/crypto.html", encoding="utf-8") as f:
        return f.read()

@app.get("/crypto.html", response_class=HTMLResponse)
async def crypto_html():
    with open("static/crypto.html", encoding="utf-8") as f:
        return f.read()

@app.get("/strategy", response_class=HTMLResponse)
async def strategy():
    with open("static/strategy.html", encoding="utf-8") as f:
        return f.read()

@app.get("/logs", response_class=HTMLResponse)
async def logs():
    with open("static/logs.html", encoding="utf-8") as f:
        return f.read()


@app.get("/stock.html", response_class=HTMLResponse)
async def stock_html():
    with open("static/stock.html", encoding="utf-8") as f:
        return f.read()

@app.get("/strategy.html", response_class=HTMLResponse)
async def strategy_html():
    with open("static/strategy.html", encoding="utf-8") as f:
        return f.read()

@app.get("/logs.html", response_class=HTMLResponse)
async def logs_html():
    with open("static/logs.html", encoding="utf-8") as f:
        return f.read()

@app.get("/dashboard-C.html", response_class=HTMLResponse)
async def dashboard_html():
    with open("static/dashboard-C.html", encoding="utf-8") as f:
        return f.read()

@app.get("/jarvis.html", response_class=HTMLResponse)
async def jarvis_html():
    with open("static/jarvis.html", encoding="utf-8") as f:
        return f.read()

# ── API 엔드포인트 ──────────────────────────────────────

@app.get("/api/status")
async def get_status():
    """봇 상태 조회 (Redis)"""
    try:
        raw = await redis_client.hgetall("bot:status")
        status = {k: json.loads(v) for k, v in raw.items()}
        return {"success": True, "data": status}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/prices/stock")
async def get_stock_prices():
    """주식 실시간 시세 (Redis)"""
    try:
        prices = {}
        for symbol in config.STOCK_SYMBOLS:
            val = await redis_client.get(f"stock:price:{symbol}")
            if val:
                prices[symbol] = json.loads(val)
        return {"success": True, "data": prices}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/prices/crypto")
async def get_crypto_prices():
    """코인 실시간 시세 (Redis)"""
    try:
        prices = {}
        for pair in config.CRYPTO_PAIRS:
            val = await redis_client.get(f"crypto:price:{pair}")
            if val:
                prices[pair] = json.loads(val)
        return {"success": True, "data": prices}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/trades")
async def get_trades(limit: int = 50, bot: str = None):
    """매매 이력 조회 (PostgreSQL)"""
    try:
        async with db_pool.acquire() as conn:
            if bot:
                rows = await conn.fetch("""
                    SELECT * FROM trade_history
                    WHERE bot = $1
                    ORDER BY ts DESC LIMIT $2
                """, bot, limit)
            else:
                rows = await conn.fetch("""
                    SELECT * FROM trade_history
                    ORDER BY ts DESC LIMIT $1
                """, limit)

            trades = []
            for r in rows:
                trades.append({
                    "id":         r["id"],
                    "bot":        r["bot"],
                    "asset_type": r["asset_type"],
                    "symbol":     r["symbol"],
                    "side":       r["side"],
                    "price":      float(r["price"] or 0),
                    "quantity":   float(r["quantity"] or 0),
                    "amount":     float(r["amount"] or 0),
                    "strategy":   r["strategy"],
                    "pnl":        float(r["pnl"] or 0) if r["pnl"] else None,
                    "ts":         r["ts"].isoformat() if r["ts"] else None,
                })
            return {"success": True, "data": trades}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/ohlcv/stock/{symbol}")
async def get_stock_ohlcv(symbol: str, limit: int = 60):
    """주식 OHLCV 조회"""
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM stock_ohlcv
                WHERE symbol = $1
                ORDER BY ts DESC LIMIT $2
            """, symbol, limit)
            data = [{"ts": r["ts"].isoformat(), "o": r["open"], "h": r["high"],
                     "l": r["low"], "c": r["close"], "v": r["volume"]} for r in rows]
            return {"success": True, "data": list(reversed(data))}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/ohlcv/crypto/{pair}")
async def get_crypto_ohlcv(pair: str, limit: int = 60):
    """코인 OHLCV 조회"""
    try:
        pair = pair.replace("-", "/")
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM crypto_ohlcv
                WHERE pair = $1
                ORDER BY ts DESC LIMIT $2
            """, pair, limit)
            data = [{"ts": r["ts"].isoformat(), "o": float(r["open"]), "h": float(r["high"]),
                     "l": float(r["low"]), "c": float(r["close"]), "v": float(r["volume"])} for r in rows]
            return {"success": True, "data": list(reversed(data))}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/summary")
async def get_summary():
    """전체 요약 (오늘 수익, 체결 수 등)"""
    try:
        today = datetime.now().date()
        async with db_pool.acquire() as conn:
            # 오늘 매매
            today_trades = await conn.fetchrow("""
                SELECT COUNT(*) as cnt,
                       SUM(CASE WHEN pnl IS NOT NULL THEN pnl ELSE 0 END) as total_pnl
                FROM trade_history
                WHERE ts::date = $1
            """, today)

            # 전체 누적
            total = await conn.fetchrow("""
                SELECT SUM(CASE WHEN pnl IS NOT NULL THEN pnl ELSE 0 END) as total_pnl
                FROM trade_history
            """)

            # 봇별 오늘 수익
            bot_pnl = await conn.fetch("""
                SELECT bot, SUM(CASE WHEN pnl IS NOT NULL THEN pnl ELSE 0 END) as pnl
                FROM trade_history
                WHERE ts::date = $1
                GROUP BY bot
            """, today)

        return {
            "success": True,
            "data": {
                "today_trades": today_trades["cnt"],
                "today_pnl":   float(today_trades["total_pnl"] or 0),
                "total_pnl":   float(total["total_pnl"] or 0),
                "bot_pnl":     {r["bot"]: float(r["pnl"] or 0) for r in bot_pnl},
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/health")
async def health():
    return {"status": "ok", "ts": datetime.now().isoformat()}


# ── 보유 포지션 API ──────────────────────────────────────

@app.get("/api/positions/stock")
async def get_stock_positions():
    """KIS API - 주식 보유 포지션 실시간 조회"""
    try:
        import aiohttp as http
        base = config.kis_base_url

        # 토큰 발급
        async with http.ClientSession() as session:
            token_res = await session.post(f"{base}/oauth2/tokenP", json={
                "grant_type": "client_credentials",
                "appkey": config.KIS_APP_KEY,
                "appsecret": config.KIS_APP_SECRET,
            })
            token_data = await token_res.json()
            token = token_data.get("access_token", "")

            headers = {
                "authorization": f"Bearer {token}",
                "appkey": config.KIS_APP_KEY,
                "appsecret": config.KIS_APP_SECRET,
                "tr_id": "VTTC8434R" if config.KIS_IS_PAPER else "TTTC8434R",
                "custtype": "P",
            }
            acct = config.KIS_ACCOUNT_NO.split("-")
            params = {
                "CANO": acct[0],
                "ACNT_PRDT_CD": acct[1] if len(acct) > 1 else "01",
                "AFHR_FLPR_YN": "N", "OFL_YN": "",
                "INQR_DVSN": "02", "UNPR_DVSN": "01",
                "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N",
                "PRCS_DVSN": "01", "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
            }
            res = await session.get(
                f"{base}/uapi/domestic-stock/v1/trading/inquire-balance",
                headers=headers, params=params
            )
            data = await res.json()
            positions = []
            for row in data.get("output1", []):
                qty = int(row.get("hldg_qty", 0))
                if qty <= 0:
                    continue
                positions.append({
                    "symbol":    row.get("pdno"),
                    "name":      row.get("prdt_name"),
                    "qty":       qty,
                    "avg_price": int(row.get("pchs_avg_pric", 0)),
                    "cur_price": int(row.get("prpr", 0)),
                    "pnl":       int(row.get("evlu_pfls_amt", 0)),
                    "pnl_rate":  float(row.get("evlu_pfls_rt", 0)),
                })
            return {"success": True, "data": positions}
    except Exception as e:
        return {"success": False, "error": str(e), "data": []}


@app.get("/api/positions/crypto")
async def get_crypto_positions():
    """업비트 API - 코인 보유 포지션 실시간 조회"""
    try:
        import aiohttp as http
        import jwt, uuid, hashlib

        payload = {
            "access_key": config.UPBIT_ACCESS_KEY,
            "nonce": str(uuid.uuid4()),
        }
        token = jwt.encode(payload, config.UPBIT_SECRET_KEY, algorithm="HS256")
        headers = {"Authorization": f"Bearer {token}"}

        async with http.ClientSession() as session:
            res = await session.get("https://api.upbit.com/v1/accounts", headers=headers)
            balances = await res.json()

            positions = []
            for b in balances:
                if b["currency"] == "KRW":
                    continue
                qty = float(b.get("balance", 0))
                if qty < 0.00001:
                    continue
                avg = float(b.get("avg_buy_price", 0))
                pair = f"KRW-{b['currency']}"

                # 현재가 조회
                price_res = await session.get(
                    "https://api.upbit.com/v1/ticker",
                    params={"markets": pair}
                )
                price_data = await price_res.json()
                cur = float(price_data[0].get("trade_price", 0)) if price_data else 0

                positions.append({
                    "pair":      pair,
                    "currency":  b["currency"],
                    "qty":       qty,
                    "avg_price": avg,
                    "cur_price": cur,
                    "pnl":       (cur - avg) * qty,
                    "pnl_rate":  (cur - avg) / avg * 100 if avg > 0 else 0,
                })
            return {"success": True, "data": positions}
    except Exception as e:
        return {"success": False, "error": str(e), "data": []}


@app.get("/api/balance/stock")
async def get_stock_balance():
    """KIS API - 주식 잔고 조회"""
    try:
        import aiohttp as http
        base = config.kis_base_url
        async with http.ClientSession() as session:
            token_res = await session.post(f"{base}/oauth2/tokenP", json={
                "grant_type": "client_credentials",
                "appkey": config.KIS_APP_KEY,
                "appsecret": config.KIS_APP_SECRET,
            })
            token_data = await token_res.json()
            token = token_data.get("access_token", "")
            headers = {
                "authorization": f"Bearer {token}",
                "appkey": config.KIS_APP_KEY,
                "appsecret": config.KIS_APP_SECRET,
                "tr_id": "VTTC8908R" if config.KIS_IS_PAPER else "TTTC8908R",
                "custtype": "P",
            }
            acct = config.KIS_ACCOUNT_NO.split("-")
            params = {
                "CANO": acct[0],
                "ACNT_PRDT_CD": acct[1] if len(acct) > 1 else "01",
                "PDNO": "005930", "ORD_UNPR": "0",
                "ORD_DVSN": "01", "CMA_EVLU_AMT_ICLD_YN": "Y", "OVRS_ICLD_YN": "N",
            }
            res = await session.get(
                f"{base}/uapi/domestic-stock/v1/trading/inquire-psbl-order",
                headers=headers, params=params
            )
            data = await res.json()
            output = data.get("output", {})
            return {
                "success": True,
                "data": {
                    "cash":  int(output.get("ord_psbl_cash", 0)),
                    "total": int(output.get("tot_evlu_amt", 0)),
                }
            }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/balance/crypto")
async def get_crypto_balance():
    """업비트 - KRW 잔고 조회"""
    try:
        import aiohttp as http
        import jwt, uuid
        payload = {"access_key": config.UPBIT_ACCESS_KEY, "nonce": str(uuid.uuid4())}
        token = jwt.encode(payload, config.UPBIT_SECRET_KEY, algorithm="HS256")
        async with http.ClientSession() as session:
            res = await session.get(
                "https://api.upbit.com/v1/accounts",
                headers={"Authorization": f"Bearer {token}"}
            )
            balances = await res.json()
            krw = next((float(b["balance"]) for b in balances if b["currency"] == "KRW"), 0)
            return {"success": True, "data": {"krw": krw}}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ── 전략 관리 API ──────────────────────────────────────

@app.get("/api/strategies")
async def get_strategies(bot: str = None):
    """전략 설정 조회"""
    try:
        import json
        async with db_pool.acquire() as conn:
            if bot:
                rows = await conn.fetch("SELECT * FROM strategy_config WHERE bot=$1 ORDER BY id", bot)
            else:
                rows = await conn.fetch("SELECT * FROM strategy_config ORDER BY bot, id")
            data = []
            for r in rows:
                data.append({
                    "id":        r["id"],
                    "bot":       r["bot"],
                    "name":      r["name"],
                    "is_active": r["is_active"],
                    "params":    dict(r["params"]) if r["params"] else {},
                    "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
                })
            return {"success": True, "data": data}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/strategies/update")
async def update_strategy(body: dict):
    """전략 ON/OFF + 파라미터 저장"""
    try:
        import json
        bot     = body.get("bot")
        name    = body.get("name")
        active  = body.get("is_active", False)
        params  = body.get("params", {})

        async with db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE strategy_config
                SET is_active=$1, params=$2, updated_at=NOW()
                WHERE bot=$3 AND name=$4
            """, active, json.dumps(params), bot, name)

            # Redis에 전략 변경 알림
            await redis_client.publish("strategy:update", json.dumps({
                "bot": bot, "name": name, "is_active": active, "params": params
            }))

        return {"success": True, "message": f"{name} 전략 {'활성화' if active else '비활성화'} 완료"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.on_event("startup")
async def create_strategy_table():
    """서버 시작 시 전략 테이블 초기화"""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS strategy_config (
                    id          SERIAL PRIMARY KEY,
                    bot         VARCHAR(20) NOT NULL,
                    name        VARCHAR(50) NOT NULL,
                    is_active   BOOLEAN DEFAULT FALSE,
                    params      JSONB DEFAULT '{}',
                    updated_at  TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(bot, name)
                );
                INSERT INTO strategy_config (bot, name, is_active, params) VALUES
                ('stock_trader','MA크로스',true,'{"short":5,"long":20,"stop_loss":-2,"take_profit":5,"buy_amount":500000,"max_positions":5}'),
                ('stock_trader','RSI반등',false,'{"period":14,"entry":30,"exit":60,"stop_loss":-2,"buy_amount":500000}'),
                ('stock_trader','볼린저밴드',false,'{"period":20,"std":2,"stop_loss":-2,"buy_amount":500000}'),
                ('crypto_trader','MACD',true,'{"fast":12,"slow":26,"signal":9,"candle_min":60,"stop_loss":-3,"take_profit":7,"buy_amount":500000}'),
                ('crypto_trader','변동성돌파',false,'{"k":0.5,"candle_min":1440,"close_time":"09:00","stop_loss":-3}'),
                ('crypto_trader','RSI과매도',false,'{"period":14,"entry":25,"exit":65,"stop_loss":-3,"buy_amount":500000}')
                ON CONFLICT (bot, name) DO NOTHING;
            """)
            logger.info("✅ strategy_config 테이블 초기화 완료")
    except Exception as e:
        logger.error(f"strategy_config 초기화 실패: {e}")


@app.post("/api/strategies/add")
async def add_strategy(body: dict):
    """새 전략 추가"""
    try:
        import json
        bot     = body.get("bot")
        name    = body.get("name", "").strip()
        active  = body.get("is_active", False)
        params  = body.get("params", {})

        if not name:
            return {"success": False, "message": "전략명을 입력해주세요"}

        async with db_pool.acquire() as conn:
            exists = await conn.fetchrow(
                "SELECT id FROM strategy_config WHERE bot=$1 AND name=$2", bot, name
            )
            if exists:
                return {"success": False, "message": f"'{name}' 전략이 이미 존재해요"}

            await conn.execute("""
                INSERT INTO strategy_config (bot, name, is_active, params)
                VALUES ($1, $2, $3, $4)
            """, bot, name, active, json.dumps(params))

        return {"success": True, "message": f"{name} 전략 추가 완료"}
    except Exception as e:
        return {"success": False, "message": str(e)}
