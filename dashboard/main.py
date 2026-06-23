"""
AutoTrader Dashboard — FastAPI 서버
실시간 DB/Redis 데이터를 API로 제공
"""
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

import fastapi
from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncpg
import redis.asyncio as aioredis

from common.config import config

import time as _time

class KSTFormatter(logging.Formatter):
    def converter(self, timestamp):
        return _time.gmtime(timestamp + 9 * 3600)

_fmt = KSTFormatter(
    fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logging.basicConfig(level=logging.INFO)
logging.root.handlers[0].setFormatter(_fmt)
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



# ── KIS 토큰 캐시 ──────────────────────────────────────
import aiohttp as _aiohttp
_kis_token_cache: dict = {"token": "", "expires": 0}

async def get_kis_token() -> str:
    """KIS 액세스 토큰 — Redis 캐시 우선 (재시작해도 재사용)"""
    import time
    now = time.time()

    # 1. 메모리 캐시 확인
    if _kis_token_cache["token"] and now < _kis_token_cache["expires"]:
        return _kis_token_cache["token"]

    # 2. Redis 캐시 확인
    try:
        if redis_client:
            cached = await redis_client.get("kis:access_token")
            if cached:
                token = cached if isinstance(cached, str) else cached.decode('utf-8')
                _kis_token_cache["token"] = token
                _kis_token_cache["expires"] = now + 82800  # 23시간
                return token
    except Exception:
        pass

    # 3. 새 토큰 발급
    try:
        base = config.kis_base_url
        async with _aiohttp.ClientSession() as session:
            res = await session.post(f"{base}/oauth2/tokenP", json={
                "grant_type": "client_credentials",
                "appkey": config.KIS_APP_KEY,
                "appsecret": config.KIS_APP_SECRET,
            }, timeout=_aiohttp.ClientTimeout(total=10))
            data = await res.json()
            token = data.get("access_token", "")
            if token:
                _kis_token_cache["token"] = token
                _kis_token_cache["expires"] = now + 82800  # 23시간
                # Redis에 저장
                try:
                    if redis_client:
                        await redis_client.setex("kis:access_token", 82800, token)
                except Exception:
                    pass
            return token
    except Exception as e:
        logger.error(f"KIS 토큰 발급 실패: {e}")
        return _kis_token_cache.get("token", "")

# 전체 종목 코드+이름 캐시 (서버 시작 시 로드)
_stock_name_cache: dict = {}  # {종목명: 종목코드}
_stock_code_cache: dict = {}  # {종목코드: 종목명}

async def _load_stock_cache():
    """pykrx로 전체 종목 목록 메모리 로드"""
    global _stock_name_cache, _stock_code_cache
    try:
        import asyncio
        from pykrx import stock as pykrx_stock
        loop = asyncio.get_event_loop()

        def _fetch():
            result = {}
            for market in ["KOSPI", "KOSDAQ"]:
                tickers = pykrx_stock.get_market_ticker_list(market=market)
                for ticker in tickers:
                    name = pykrx_stock.get_market_ticker_name(ticker)
                    if name:
                        result[name] = ticker
            return result

        name_map = await loop.run_in_executor(None, _fetch)
        _stock_name_cache = name_map
        _stock_code_cache = {v: k for k, v in name_map.items()}
        logger.info(f"✅ 전체 종목 캐시 로드 완료: {len(name_map)}개")
    except Exception as e:
        logger.warning(f"종목 캐시 로드 실패 (무시): {e}")

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

    # ML 테이블 생성
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ml_models (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    model_name VARCHAR(50) NOT NULL,
                    model_data TEXT NOT NULL,
                    accuracy NUMERIC(6,2),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(symbol, model_name)
                );
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
                CREATE TABLE IF NOT EXISTS stock_daily_ohlcv (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    ts DATE NOT NULL,
                    open BIGINT, high BIGINT, low BIGINT, close BIGINT,
                    volume BIGINT, change_rate NUMERIC(8,2),
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(symbol, ts)
                );
                CREATE TABLE IF NOT EXISTS stock_indicators (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    ts DATE NOT NULL,
                    rsi14 NUMERIC(8,2), macd NUMERIC(12,2),
                    macd_signal NUMERIC(12,2), macd_hist NUMERIC(12,2),
                    bb_upper NUMERIC(12,2), bb_middle NUMERIC(12,2),
                    bb_lower NUMERIC(12,2), bb_pct NUMERIC(8,4),
                    atr14 NUMERIC(12,2), stoch_k NUMERIC(8,2), stoch_d NUMERIC(8,2),
                    sma5 NUMERIC(12,2), sma20 NUMERIC(12,2), sma60 NUMERIC(12,2),
                    ema12 NUMERIC(12,2), ema26 NUMERIC(12,2),
                    golden_cross BOOLEAN, dead_cross BOOLEAN,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(symbol, ts)
                );
            """)
        logger.info("✅ ML 테이블 확인 완료")
    except Exception as e:
        logger.warning(f"ML 테이블 생성 오류 (무시): {e}")

    # 텔레그램 webhook 자동 등록
    await _auto_register_webhook()

    # 전체 종목 캐시 백그라운드 로드 (시작 지연 없이)
    import asyncio
    asyncio.create_task(_load_stock_cache())
    asyncio.create_task(_jarvis_scheduler())


async def _auto_register_webhook():
    """서버 시작 시 텔레그램 webhook 자동 등록"""
    import os
    import aiohttp as http
    token = config.JARVIS_ANALYST_TOKEN or config.TELEGRAM_TOKEN
    public_url = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
    if not token or not public_url:
        logger.warning("텔레그램 webhook 자동 등록 스킵 (토큰 또는 도메인 없음)")
        return
    webhook_url = f"https://{public_url}/api/telegram/webhook"
    try:
        async with http.ClientSession() as session:
            res = await session.post(
                f"https://api.telegram.org/bot{token}/setWebhook",
                json={"url": webhook_url, "drop_pending_updates": True},
            )
            data = await res.json()
            logger.info(f"텔레그램 webhook 자동 등록: {webhook_url} → {data}")
    except Exception as e:
        logger.warning(f"텔레그램 webhook 자동 등록 실패: {e}")


async def _jarvis_stock_scanner():
    """08:30 — 코스피/코스닥 전종목 스캔 → 유망 종목 watchlist 자동 추가"""
    try:
        from pykrx import stock as pykrx_stock
        import asyncio
        from datetime import datetime, timedelta

        from datetime import timezone, timedelta
        KST = timezone(timedelta(hours=9))
        now_kst = datetime.now(KST)
        logger.info("🔍 Jarvis 전종목 스캔 시작...")
        today = now_kst.strftime("%Y%m%d")
        d30 = (now_kst - timedelta(days=45)).strftime("%Y%m%d")

        loop = asyncio.get_event_loop()

        def _scan():
            results = []
            for market in ["KOSPI", "KOSDAQ"]:
                try:
                    tickers = pykrx_stock.get_market_ticker_list(market=market)
                    for ticker in tickers:  # 전체 종목
                        try:
                            # 30일 데이터로 MA크로스 체크
                            df = pykrx_stock.get_market_ohlcv(d30, today, ticker)
                            if df is None or len(df) < 22:
                                continue

                            close = df["종가"].iloc[-1]
                            if close < 1000:  # 동전주 제외
                                continue

                            vol = df["거래량"].iloc[-1]
                            vol_avg = df["거래량"].iloc[-6:-1].mean()  # 5일 평균 거래량
                            change = df["등락률"].iloc[-1]

                            # MA 크로스 계산
                            closes = df["종가"].tolist()
                            ma5 = sum(closes[-5:]) / 5
                            ma20 = sum(closes[-20:]) / 20
                            ma5_prev = sum(closes[-6:-1]) / 5
                            ma20_prev = sum(closes[-21:-1]) / 20

                            # 골든크로스: 5일선이 20일선 상향 돌파
                            golden_cross = ma5_prev < ma20_prev and ma5 > ma20

                            # 거래량 조건: 평균 대비 1.5배 이상
                            vol_ok = vol > vol_avg * 1.5 if vol_avg > 0 else False

                            score = 0
                            if golden_cross:
                                score += 3
                            if vol_ok:
                                score += 2
                            if change > 1:
                                score += 1
                            if change > 3:
                                score += 1

                            if score >= 2:  # 조건 완화 (3→2)
                                name = pykrx_stock.get_market_ticker_name(ticker)
                                results.append({
                                    "symbol": ticker,
                                    "name": name,
                                    "change": change,
                                    "vol_ratio": vol / vol_avg if vol_avg > 0 else 1,
                                    "golden_cross": golden_cross,
                                    "score": score,
                                    "close": close,
                                })
                        except:
                            continue
                except:
                    continue
            return sorted(results, key=lambda x: x["score"], reverse=True)[:20]

        candidates = await loop.run_in_executor(None, _scan)

        if not candidates:
            logger.info("🔍 스캔 완료: 유망 종목 없음")
            await _send_telegram(f"🔍 Jarvis 스캔 [{now_kst.strftime('%m/%d %H:%M')}]\n유망 종목 없음")
            return

        # watchlist에 자동 추가
        added = []
        async with db_pool.acquire() as conn:
            existing = [r["symbol"] for r in await conn.fetch(
                "SELECT symbol FROM watchlist WHERE is_active=TRUE"
            )]
            for c in candidates:
                if c["symbol"] not in existing:
                    reason = f"{'골든크로스+' if c['golden_cross'] else ''}거래량{c['vol_ratio']:.1f}배 등락률{c['change']:+.1f}%"
                    await conn.execute("""
                        INSERT INTO watchlist (symbol, name, added_by, reason, is_active)
                        VALUES ($1, $2, 'jarvis_scanner', $3, TRUE)
                        ON CONFLICT (symbol) DO UPDATE
                        SET is_active=TRUE, added_by='jarvis_scanner', reason=$3, updated_at=NOW()
                    """, c["symbol"], c["name"], reason)
                    gc = "🌟골든크로스 " if c["golden_cross"] else ""
                    added.append(f"  {gc}{c['name']}({c['symbol']}) {c['close']:,}원 {c['change']:+.1f}%")

        msg = f"🔍 Jarvis 스캔 [{now_kst.strftime('%m/%d %H:%M')}]\n"
        msg += f"총 {len(candidates)}종목 발굴"
        if added:
            msg += f", {len(added)}종목 신규 추가:\n" + "\n".join(added[:10])
        else:
            msg += " (모두 기존 watchlist에 있음)"
        await _send_telegram(msg)
        logger.info(f"✅ 스캐너 완료: {len(candidates)}종목 발굴, {len(added)}종목 추가")

    except Exception as e:
        logger.error(f"Jarvis 스캐너 실패: {e}")


async def _jarvis_auto_analysis():
    """Jarvis 자동 분석 — 감시 종목 전체 ML 예측 후 텔레그램 리포트"""
    try:
        from ml.model import MLModelManager
        manager = MLModelManager(db_pool)

        async with db_pool.acquire() as conn:
            watchlist = await conn.fetch("SELECT symbol, name FROM watchlist WHERE is_active=TRUE")
            symbols = [r["symbol"] for r in watchlist]
            name_map = {r["symbol"]: r["name"] for r in watchlist}

        if not symbols:
            return

        buy_list, sell_list, hold_list = [], [], []

        for symbol in symbols:
            try:
                async with db_pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT * FROM stock_daily_ohlcv WHERE symbol=$1 AND close>0 ORDER BY ts ASC",
                        symbol
                    )
                if len(rows) < 70:
                    continue
                ohlcv = [{"ts": str(r["ts"]), "open": float(r["open"]), "high": float(r["high"]),
                          "low": float(r["low"]), "close": float(r["close"]), "volume": float(r["volume"])}
                         for r in rows]
                result = await manager.predict(symbol, ohlcv)
                if not result.get("success"):
                    continue
                name = name_map.get(symbol, symbol)
                signal = result.get("signal", "HOLD")
                prob = result.get("buy_prob", 0.5)
                if signal == "BUY":
                    buy_list.append(f"  🟢 {name}({symbol}) {prob:.0%}")
                elif signal == "SELL":
                    sell_list.append(f"  🔴 {name}({symbol})")
                else:
                    hold_list.append(name)
            except:
                continue

        # 텔레그램 리포트
        now = datetime.now(timezone(timedelta(hours=9))).strftime("%m/%d %H:%M")
        msg = f"🤖 Jarvis 자동 분석 [{now}]\n"
        msg += f"총 {len(symbols)}종목 분석\n\n"
        if buy_list:
            msg += "📈 매수 신호:\n" + "\n".join(buy_list) + "\n\n"
        if sell_list:
            msg += "📉 매도 신호:\n" + "\n".join(sell_list) + "\n\n"
        msg += f"🟡 관망: {len(hold_list)}종목"

        await _send_telegram(msg)
        logger.info(f"✅ Jarvis 자동 분석 완료: BUY {len(buy_list)}, SELL {len(sell_list)}, HOLD {len(hold_list)}")

    except Exception as e:
        logger.error(f"Jarvis 자동 분석 실패: {e}")


async def _jarvis_scheduler():
    """Jarvis 자동 분석 스케줄러 — 08:30 장 시작 전 / 15:40 장 마감 후"""
    import asyncio
    from datetime import time as dtime
    logger.info("🕐 Jarvis 스케줄러 시작")
    last_morning = None
    last_closing = None

    while True:
        await asyncio.sleep(60)
        from datetime import timezone, timedelta
        KST = timezone(timedelta(hours=9))
        now = datetime.now(KST)
        today = now.date()
        cur_time = now.time().replace(tzinfo=None)

        if now.weekday() >= 5:
            continue

        if dtime(8, 30) <= cur_time <= dtime(8, 35) and last_morning != today:
            last_morning = today
            logger.info("🌅 Jarvis 장 시작 전 루틴")
            await _jarvis_stock_scanner()
            await _jarvis_auto_analysis()
            asyncio.create_task(_manual_collect())  # 뉴스 감성 수집

            # 오늘 공시 확인
            try:
                from data_collector.collectors.dart_collector import DARTCollector
                dart = DARTCollector()
                async with db_pool.acquire() as conn:
                    symbols = [r["symbol"] for r in await conn.fetch(
                        "SELECT symbol FROM watchlist WHERE is_active=TRUE"
                    )]
                await dart.collect_and_alert(symbols, telegram_func=_send_telegram)
            except Exception as e:
                logger.warning(f"공시 확인 실패: {e}")

        if dtime(15, 40) <= cur_time <= dtime(15, 45) and last_closing != today:
            last_closing = today
            logger.info("🌆 Jarvis 장 마감 후 자동 분석")
            await _jarvis_auto_analysis()
            asyncio.create_task(_manual_collect())  # 장 마감 후 뉴스 수집


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

@app.get("/analysis", response_class=HTMLResponse)
async def analysis_page():
    with open("/app/dashboard/static/analysis.html") as f:
        return f.read()

@app.get("/analysis.html", response_class=HTMLResponse)
async def analysis_page2():
    with open("/app/dashboard/static/analysis.html") as f:
        return f.read()

# ── ML / 백테스트 API ──────────────────────────────────
@app.get("/api/ml/indicators/{symbol}")
async def get_indicators(symbol: str, limit: int = 100):
    """종목의 기술적 지표 계산"""
    try:
        from ml.indicators import calculate_all
        if not db_pool:
            return {"success": False, "error": "DB 연결 없음"}
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM stock_ohlcv WHERE symbol=$1 ORDER BY ts DESC LIMIT $2",
                symbol, limit
            )
        if not rows:
            return {"success": False, "error": "데이터 없음"}
        ohlcv = [{"ts": str(r["ts"]), "open": r["open"], "high": r["high"],
                  "low": r["low"], "close": r["close"], "volume": r["volume"]}
                 for r in reversed(rows)]
        indicators = calculate_all(ohlcv)
        return {"success": True, "symbol": symbol, "data": indicators}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/ml/backtest")
async def run_backtest(request: Request):
    """백테스트 실행"""
    try:
        from ml.backtest import Backtest

        body = await request.json()
        symbol   = body.get("symbol", "005930")
        strategy = body.get("strategy", "ma_cross")
        params   = body.get("params", {})
        capital  = int(body.get("capital", 10_000_000))

        if not db_pool:
            return {"success": False, "error": "DB 연결 없음"}
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM stock_daily_ohlcv WHERE symbol=$1 AND close > 0 ORDER BY ts ASC",
                symbol
            )
        if len(rows) < 60:
            return {"success": False, "error": f"데이터 부족 ({len(rows)}개, 최소 60개 필요)"}
        ohlcv = [{"ts": str(r["ts"]), "open": float(r["open"]), "high": float(r["high"]),
                  "low": float(r["low"]), "close": float(r["close"]), "volume": float(r["volume"])}
                 for r in rows]

        bt = Backtest(initial_capital=capital)

        if strategy == "ma_cross":
            result = bt.run_ma_cross(
                ohlcv,
                short=int(params.get("short", 5)),
                long=int(params.get("long", 20)),
                stop_loss=float(params.get("stop_loss", -0.02)),
                take_profit=float(params.get("take_profit", 0.05)),
                buy_amount=int(params.get("buy_amount", 500000))
            )
        elif strategy == "rsi":
            result = bt.run_rsi(
                ohlcv,
                period=int(params.get("period", 14)),
                entry=float(params.get("entry", 30)),
                exit_=float(params.get("exit", 70)),
                stop_loss=float(params.get("stop_loss", -0.03)),
                buy_amount=int(params.get("buy_amount", 500000))
            )
        elif strategy == "optimize":
            result = bt.optimize_ma_cross(ohlcv, buy_amount=int(params.get("buy_amount", 500000)))
            return {"success": True, "strategy": "optimize", "data": result}
        else:
            return {"success": False, "error": f"알 수 없는 전략: {strategy}"}

        return {"success": True, "strategy": strategy, "symbol": symbol,
                "data": result.to_dict()}

    except Exception as e:
        logger.error(f"백테스트 오류: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/ml/features/{symbol}")
async def get_features(symbol: str, limit: int = 200):
    """피처 엔지니어링 결과"""
    try:
        from ml.features import build_features, feature_summary

        if not db_pool:
            return {"success": False, "error": "DB 연결 없음"}
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM stock_ohlcv WHERE symbol=$1 ORDER BY ts DESC LIMIT $2",
                symbol, limit
            )
        if len(rows) < 70:
            return {"success": False, "error": f"데이터 부족 ({len(rows)}개)"}
        ohlcv = [{"ts": str(r["ts"]), "open": float(r["open"]), "high": float(r["high"]),
                  "low": float(r["low"]), "close": float(r["close"]), "volume": float(r["volume"])}
                 for r in reversed(rows)]

        features = build_features(ohlcv)
        summary = feature_summary(features)

        return {"success": True, "symbol": symbol,
                "summary": summary, "data": features[-20:]}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/ml/train/{symbol}")
async def train_model(symbol: str):
    """종목 ML 모델 학습"""
    try:
        from ml.model import MLModelManager
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM stock_daily_ohlcv WHERE symbol=$1 AND close > 0 ORDER BY ts ASC",
                symbol
            )
        if len(rows) < 70:
            return {"success": False, "error": f"데이터 부족 ({len(rows)}개, 최소 70개 필요)"}

        ohlcv = [{"ts": str(r["ts"]), "open": float(r["open"]), "high": float(r["high"]),
                  "low": float(r["low"]), "close": float(r["close"]), "volume": float(r["volume"])}
                 for r in rows]

        manager = MLModelManager(db_pool)
        result = await manager.train(symbol, ohlcv)
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/ml/train-all")
async def train_all_models():
    """전체 감시 종목 ML 모델 일괄 학습 + 예측"""
    try:
        from ml.model import MLModelManager

        # watchlist에서 종목 조회
        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT symbol FROM watchlist WHERE is_active=TRUE ORDER BY created_at")
            symbols = [r["symbol"] for r in rows]
        except Exception:
            symbols = []

        if not symbols:
            symbols = config.STOCK_SYMBOLS

        manager = MLModelManager(db_pool)
        results = []

        for symbol in symbols:
            try:
                async with db_pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT * FROM stock_daily_ohlcv WHERE symbol=$1 AND close > 0 ORDER BY ts ASC",
                        symbol
                    )
                if len(rows) < 70:
                    results.append({"symbol": symbol, "success": False, "error": f"데이터 부족 ({len(rows)}개)"})
                    continue

                ohlcv = [{"ts": str(r["ts"]), "open": float(r["open"]), "high": float(r["high"]),
                          "low": float(r["low"]), "close": float(r["close"]), "volume": float(r["volume"])}
                         for r in rows]

                # 학습
                train_result = await manager.train(symbol, ohlcv)
                if not train_result.get("success"):
                    results.append({"symbol": symbol, **train_result})
                    continue

                # 예측 (학습 직후 바로 실행 → DB 저장)
                pred_result = await manager.predict(symbol, ohlcv)
                signal = pred_result.get("signal", "-") if pred_result.get("success") else "-"
                buy_prob = pred_result.get("buy_prob", 0) if pred_result.get("success") else 0

                results.append({
                    "symbol": symbol,
                    "success": True,
                    "accuracy": train_result.get("accuracy", 0),
                    "samples": train_result.get("samples", 0),
                    "signal": signal,
                    "buy_prob": buy_prob,
                })
                logger.info(f"✅ [{symbol}] 학습+예측 완료: {train_result.get('accuracy')}% / {signal}")

            except Exception as e:
                results.append({"symbol": symbol, "success": False, "error": str(e)})
                logger.error(f"❌ [{symbol}] 실패: {e}")

        success_count = sum(1 for r in results if r.get("success"))
        return {
            "success": True,
            "total": len(symbols),
            "trained": success_count,
            "failed": len(symbols) - success_count,
            "results": results
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/ml/predict/{symbol}")
async def predict_signal(symbol: str):
    """종목 매수/매도 신호 예측"""
    try:
        from ml.model import MLModelManager
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM stock_daily_ohlcv WHERE symbol=$1 AND close > 0 ORDER BY ts ASC",
                symbol
            )
        if len(rows) < 70:
            return {"success": False, "error": f"데이터 부족 ({len(rows)}개)"}

        ohlcv = [{"ts": str(r["ts"]), "open": float(r["open"]), "high": float(r["high"]),
                  "low": float(r["low"]), "close": float(r["close"]), "volume": float(r["volume"])}
                 for r in rows]

        manager = MLModelManager(db_pool)
        result = await manager.predict(symbol, ohlcv)
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/ml/predictions")
async def get_all_predictions():
    """모든 종목 최신 ML 예측"""
    try:
        from ml.model import MLModelManager
        manager = MLModelManager(db_pool)
        predictions = await manager.get_all_predictions()
        return {"success": True, "data": predictions}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/learn/stats")
async def learn_stats():
    """학습 데이터 현황 통계"""
    try:
        result = {}
        if db_pool:
            async with db_pool.acquire() as conn:
                # 주식 OHLCV
                stock_cnt = await conn.fetchval("SELECT COUNT(*) FROM stock_ohlcv")
                stock_min = await conn.fetchval("SELECT MIN(ts) FROM stock_ohlcv")
                stock_max = await conn.fetchval("SELECT MAX(ts) FROM stock_ohlcv")
                stock_syms = await conn.fetchval("SELECT COUNT(DISTINCT symbol) FROM stock_ohlcv")

                # 코인 OHLCV
                crypto_cnt = await conn.fetchval("SELECT COUNT(*) FROM crypto_ohlcv")
                crypto_min = await conn.fetchval("SELECT MIN(ts) FROM crypto_ohlcv")
                crypto_max = await conn.fetchval("SELECT MAX(ts) FROM crypto_ohlcv")
                crypto_syms = await conn.fetchval("SELECT COUNT(DISTINCT pair) FROM crypto_ohlcv")

            result = {
                "stock_candles": stock_cnt or 0,
                "stock_symbols": stock_syms or 0,
                "stock_period": f"{stock_min.strftime('%m/%d') if stock_min else '-'} ~ {stock_max.strftime('%m/%d') if stock_max else '-'}",
                "stock_last": stock_max.strftime('%H:%M') if stock_max else '-',
                "crypto_candles": crypto_cnt or 0,
                "crypto_symbols": crypto_syms or 0,
                "crypto_period": f"{crypto_min.strftime('%m/%d') if crypto_min else '-'} ~ {crypto_max.strftime('%m/%d') if crypto_max else '-'}",
                "crypto_last": crypto_max.strftime('%H:%M') if crypto_max else '-',
            }
        return {"success": True, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e)}


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
    """주식 실시간 시세 (Redis) — watchlist 기준"""
    try:
        # watchlist에서 종목 조회
        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT symbol, name FROM watchlist WHERE is_active=TRUE ORDER BY created_at"
                )
            symbols = [(r["symbol"], r["name"]) for r in rows]
        except Exception:
            symbols = [(s, s) for s in config.STOCK_SYMBOLS]

        if not symbols:
            symbols = [(s, s) for s in config.STOCK_SYMBOLS]

        prices = {}
        for symbol, name in symbols:
            val = await redis_client.get(f"stock:price:{symbol}")
            if val:
                data = json.loads(val)
                data["name"] = name or symbol
                prices[symbol] = data
            else:
                # Redis에 없으면 DB에서 최신 시세 조회
                try:
                    async with db_pool.acquire() as conn:
                        row = await conn.fetchrow(
                            "SELECT close, open, high, low, volume FROM stock_ohlcv WHERE symbol=$1 AND close>0 ORDER BY ts DESC LIMIT 1",
                            symbol
                        )
                    if row:
                        prices[symbol] = {
                            "symbol": symbol,
                            "name": name or symbol,
                            "price": row["close"],
                            "open": row["open"],
                            "high": row["high"],
                            "low": row["low"],
                            "volume": row["volume"],
                            "change_rate": 0,
                        }
                except Exception:
                    pass

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


@app.get("/api/data/supply")
async def get_supply_data():
    """외국인/기관 수급 최신 데이터"""
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (s.symbol)
                    s.symbol, s.date, s.foreign_net, s.institution_net,
                    s.individual_net, s.foreign_hold_ratio, w.name
                FROM stock_supply s
                LEFT JOIN watchlist w ON s.symbol=w.symbol
                ORDER BY s.symbol, s.date DESC
            """)
        return {"success": True, "data": [dict(r) for r in rows], "count": len(rows)}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/data/disclosure")
async def get_disclosure_data():
    """최근 공시 목록"""
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT symbol, corp_name, report_name, rcept_dt, is_important
                FROM stock_disclosure
                ORDER BY rcept_dt DESC LIMIT 20
            """)
        return {"success": True, "data": [dict(r) for r in rows], "count": len(rows)}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/data/sentiment")
async def get_sentiment_data():
    """뉴스 감성 분석 최신 결과"""
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (s.symbol)
                    s.symbol, s.date, s.sentiment_score, s.signal,
                    s.summary, s.news_count, w.name
                FROM stock_news_sentiment s
                LEFT JOIN watchlist w ON s.symbol=w.symbol
                ORDER BY s.symbol, s.date DESC
            """)
        return {"success": True, "data": [dict(r) for r in rows], "count": len(rows)}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/data/collect")
async def trigger_collect(background_tasks: fastapi.background.BackgroundTasks):
    """수동 데이터 수집 트리거 (수급/공시/뉴스)"""
    async def _collect():
        try:
            async with db_pool.acquire() as conn:
                symbols = [r["symbol"] for r in await conn.fetch(
                    "SELECT symbol FROM watchlist WHERE is_active=TRUE"
                )]
            if not symbols:
                return

            import aiohttp, os
            openwebui_url = os.getenv("OPENWEBUI_URL", "")
            openwebui_token = os.getenv("OPENWEBUI_API_TOKEN", "")
            jarvis_model = os.getenv("JARVIS_MODEL", "autotrader-jarvis")

            # 뉴스 감성 분석
            for symbol in symbols[:10]:  # 10종목만
                try:
                    async with db_pool.acquire() as conn:
                        name_row = await conn.fetchrow(
                            "SELECT name FROM watchlist WHERE symbol=$1", symbol
                        )
                    name = name_row["name"] if name_row else symbol

                    prompt = f"""다음 주식 뉴스를 분석해서 JSON만 출력해줘. 다른 말은 하지 마.
종목: {name}({symbol})
요청: {name} 주식 최신 뉴스 감성 분석
출력형식: {{"score": 1, "signal": "BUY", "summary": "긍정적"}}"""

                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            f"{openwebui_url}/api/chat/completions",
                            headers={"Authorization": f"Bearer {openwebui_token}", "Content-Type": "application/json"},
                            json={"model": jarvis_model, "messages": [{"role": "user", "content": prompt}], "stream": False},
                            timeout=aiohttp.ClientTimeout(total=30)
                        ) as resp:
                            data = await resp.json()

                    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                    import json, re
                    content = re.sub(r'```json|```', '', content).strip()
                    match = re.search(r'\{[^{}]*\}', content, re.DOTALL)
                    if match:
                        result = json.loads(match.group())
                        score = max(-2, min(2, int(result.get("score", 0))))
                        signal = result.get("signal", "NEUTRAL").upper()
                        if signal not in ["BUY", "SELL", "NEUTRAL"]:
                            signal = "NEUTRAL"
                        summary = result.get("summary", "")

                        async with db_pool.acquire() as conn:
                            await conn.execute("""
                                INSERT INTO stock_news_sentiment
                                (symbol, date, sentiment_score, signal, summary, news_count)
                                VALUES ($1, $2, $3, $4, $5, 1)
                                ON CONFLICT (symbol, date) DO UPDATE
                                SET sentiment_score=$3, signal=$4, summary=$5
                            """, symbol, datetime.now().date(), score, signal, summary)
                        logger.info(f"📰 {name}: {signal} ({score:+d}) - {summary}")

                except Exception as e:
                    logger.error(f"뉴스 수집 실패 [{symbol}]: {e}")
                    continue

        except Exception as e:
            logger.error(f"수동 수집 오류: {e}")

    background_tasks.add_task(_collect)
    return {"success": True, "message": "수집 시작! /api/data/sentiment 에서 결과 확인하세요"}


@app.get("/api/market/checklist")
async def market_checklist():
    """장중 테스트 체크리스트"""
    from datetime import time as dtime
    now = datetime.now()
    cur_time = now.time()
    is_market = dtime(9, 0) <= cur_time <= dtime(15, 30) and now.weekday() < 5

    checks = {}

    # 1. KIS 시세 확인
    try:
        async with db_pool.acquire() as conn:
            latest = await conn.fetchrow(
                "SELECT ts, close FROM stock_ohlcv ORDER BY ts DESC LIMIT 1"
            )
        if latest:
            ts_diff = (now - latest["ts"].replace(tzinfo=None)).total_seconds()
            checks["kis_realtime"] = {
                "ok": ts_diff < 300,
                "msg": f"최근 시세: {latest['ts'].strftime('%H:%M:%S')} ({int(ts_diff)}초 전)"
            }
        else:
            checks["kis_realtime"] = {"ok": False, "msg": "시세 데이터 없음"}
    except Exception as e:
        checks["kis_realtime"] = {"ok": False, "msg": str(e)}

    # 2. ML 모델 확인
    try:
        async with db_pool.acquire() as conn:
            ml_count = await conn.fetchval("SELECT COUNT(*) FROM ml_models")
        checks["ml_models"] = {
            "ok": ml_count > 0,
            "msg": f"학습된 모델 {ml_count}개"
        }
    except Exception as e:
        checks["ml_models"] = {"ok": False, "msg": str(e)}

    # 3. watchlist 확인
    try:
        async with db_pool.acquire() as conn:
            wl_count = await conn.fetchval("SELECT COUNT(*) FROM watchlist WHERE is_active=TRUE")
        checks["watchlist"] = {
            "ok": wl_count > 0,
            "msg": f"감시 종목 {wl_count}개"
        }
    except Exception as e:
        checks["watchlist"] = {"ok": False, "msg": str(e)}

    # 4. 봇 상태
    try:
        bot_status = await cache.get_all_bot_status()
        checks["bots"] = {
            "ok": len(bot_status) > 0,
            "msg": ", ".join([f"{k}:{v.get('status','?')}" for k, v in bot_status.items()])
        }
    except Exception as e:
        checks["bots"] = {"ok": False, "msg": str(e)}

    return {
        "success": True,
        "is_market_hours": is_market,
        "market_status": "장중" if is_market else "장외",
        "checks": checks
    }

# ── Jarvis AI (Gemini 2.5 Flash) ────────────────────────────────

import google.generativeai as genai
from datetime import datetime

# 대화 히스토리 (메모리)
_jarvis_history: list = []

JARVIS_SYSTEM_PROMPT = """너는 AutoTrader의 AI 집사 Jarvis야. 주인님(민기)을 섬기는 똑똑하고 친근한 AI야.

## 성격
- 집사처럼 격식 있지만 친근하게
- 트레이딩 전문가이면서 일상 대화도 자연스럽게
- 유머 감각 있고 때로는 위트 있게
- 주인님 기분에 맞춰 대화 톤 조절

## 대화 규칙
- 한국어로 답변
- 트레이딩 질문 → 핵심 위주 간결하게 (3-5문장)
- 일상/잡담 → 자연스럽고 친근하게 (길이 자유)
- 이모지 적절히 사용 (과하지 않게)
- 인사말/면책문구 금지
- 중요 수치는 **볼드** 처리

## 트레이딩 권한
- 주식/코인 매수/매도 실행 가능
- 매매 전 반드시 확인 요청
- 감시 종목 추가/제거 가능
- ML 예측, 시세, 포트폴리오 분석 가능
- 확실하지 않은 정보는 추측이라고 명시

## 알고 있는 것
- 현재 포트폴리오 및 보유 종목
- 감시 종목 목록 및 ML 예측 결과
- 최근 매매 이력
- 실시간 코인 시세
- 활성 전략 상태
"""

async def get_portfolio_context() -> str:
    """현재 포트폴리오 데이터를 Gemini 컨텍스트로 변환"""
    ctx_parts = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    ctx_parts.append(f"[현재 시각: {now}]")

    try:
        # 주식 포지션
        stock_pos = await get_stock_positions()
        if stock_pos.get("success") and stock_pos.get("data"):
            positions = stock_pos["data"]
            account = stock_pos.get("account", {})
            ctx_parts.append(f"\n[주식 계좌]")
            ctx_parts.append(f"총평가금액: {account.get('total_eval', 0):,}원")
            ctx_parts.append(f"주식평가금액: {account.get('stock_eval', 0):,}원")
            ctx_parts.append(f"예수금: {account.get('cash', 0):,}원")
            ctx_parts.append(f"평가손익: {account.get('pnl', 0):+,}원 ({account.get('pnl_rate', 0):+.2f}%)")
            if positions:
                ctx_parts.append(f"보유종목 {len(positions)}개:")
                for p in positions:
                    ctx_parts.append(
                        f"  - {p['name']}({p['symbol']}): {p['qty']}주 "
                        f"평균{p['avg_price']:,} 현재{p['cur_price']:,} "
                        f"손익{p['pnl']:+,}원({p['pnl_rate']:+.1f}%)"
                    )
    except Exception as e:
        ctx_parts.append(f"[주식 데이터 조회 실패: {e}]")

    try:
        # 코인 포지션
        crypto_pos = await get_crypto_positions()
        if crypto_pos.get("success") and crypto_pos.get("data"):
            positions = crypto_pos["data"]
            ctx_parts.append(f"\n[코인 계좌]")
            if positions:
                ctx_parts.append(f"보유코인 {len(positions)}개:")
                for p in positions:
                    ctx_parts.append(
                        f"  - {p['name']}({p['pair']}): {p['qty']:.4f} "
                        f"평균{p['avg_price']:,} 현재{p['cur_price']:,} "
                        f"손익{p['pnl']:+,.0f}원({p['pnl_rate']:+.1f}%)"
                    )
    except Exception as e:
        ctx_parts.append(f"[코인 데이터 조회 실패: {e}]")

    try:
        # 코인 시세
        crypto_prices = await get_crypto_prices()
        if crypto_prices.get("success") and crypto_prices.get("data"):
            prices = crypto_prices["data"]
            ctx_parts.append(f"\n[실시간 코인 시세]")
            for pair, p in list(prices.items())[:5]:
                ctx_parts.append(
                    f"  {pair}: {p.get('price', 0):,}원 ({p.get('change_rate', 0):+.2f}%)"
                )
    except:
        pass

    try:
        # 전략 상태
        strategies = await get_strategies()
        if strategies.get("success") and strategies.get("data"):
            active = [s for s in strategies["data"] if s["is_active"]]
            ctx_parts.append(f"\n[활성 전략 {len(active)}개]")
            for s in active:
                ctx_parts.append(f"  - {s['bot']}: {s['name']} ON")
    except:
        pass

    try:
        # 최근 매매 이력
        trades_res = await get_trades(limit=5)
        if trades_res.get("success") and trades_res.get("data"):
            trades = trades_res["data"]
            ctx_parts.append(f"\n[최근 매매 {len(trades)}건]")
            for t in trades:
                ts = t["ts"][:16] if t["ts"] else "-"
                pnl_str = f" 손익:{t['pnl']:+,.0f}원" if t.get("pnl") else " 보유중"
                ctx_parts.append(
                    f"  {ts} [{t['bot']}] {t['side']} {t['symbol']} "
                    f"{t['price']:,}×{t['quantity']}{pnl_str}"
                )
    except:
        pass

    try:
        # 감시 종목 목록
        async with db_pool.acquire() as conn:
            watchlist = await conn.fetch(
                "SELECT symbol, name FROM watchlist WHERE is_active=TRUE ORDER BY created_at"
            )
        if watchlist:
            ctx_parts.append(f"\n[감시 종목 {len(watchlist)}개]")
            ctx_parts.append("  " + ", ".join([f"{r['name'] or r['symbol']}({r['symbol']})" for r in watchlist]))
    except:
        pass

    try:
        # ML 예측 결과 (최신)
        async with db_pool.acquire() as conn:
            preds = await conn.fetch("""
                SELECT DISTINCT ON (p.symbol)
                    p.symbol, p.signal, p.buy_prob, p.ts,
                    m.accuracy, w.name
                FROM ml_predictions p
                LEFT JOIN ml_models m ON p.symbol=m.symbol AND m.model_name='naive_bayes'
                LEFT JOIN watchlist w ON p.symbol=w.symbol
                ORDER BY p.symbol, p.ts DESC
            """)
        if preds:
            buy_list = [r for r in preds if r['signal'] == 'BUY']
            sell_list = [r for r in preds if r['signal'] == 'SELL']
            ctx_parts.append(f"\n[ML 예측 결과 ({len(preds)}종목 분석)]")
            if buy_list:
                ctx_parts.append(f"  🟢 매수 신호: " + ", ".join(
                    [f"{r['name'] or r['symbol']}({r['buy_prob']:.0%})" for r in buy_list]
                ))
            if sell_list:
                ctx_parts.append(f"  🔴 매도 신호: " + ", ".join(
                    [f"{r['name'] or r['symbol']}" for r in sell_list]
                ))
            hold_list = [r for r in preds if r['signal'] == 'HOLD']
            if hold_list:
                ctx_parts.append(f"  🟡 관망: {len(hold_list)}종목")
    except:
        pass

    try:
        # 주식 최신 시세 (stock_daily_ohlcv 최근일)
        async with db_pool.acquire() as conn:
            latest_prices = await conn.fetch("""
                SELECT DISTINCT ON (o.symbol)
                    o.symbol, o.close, o.change_rate, o.ts, w.name
                FROM stock_daily_ohlcv o
                LEFT JOIN watchlist w ON o.symbol=w.symbol
                WHERE w.is_active=TRUE
                ORDER BY o.symbol, o.ts DESC
            """)
        if latest_prices:
            ctx_parts.append(f"\n[감시 종목 최근 시세]")
            for r in latest_prices[:10]:
                rate = float(r['change_rate'] or 0)
                emoji = "📈" if rate > 0 else "📉" if rate < 0 else "➡️"
                ctx_parts.append(
                    f"  {emoji} {r['name'] or r['symbol']}: {r['close']:,}원 ({rate:+.2f}%) [{str(r['ts'])[:10]}]"
                )
    except:
        pass

    try:
        # 수급 데이터 (외국인/기관 순매수)
        async with db_pool.acquire() as conn:
            supply_rows = await conn.fetch("""
                SELECT DISTINCT ON (s.symbol)
                    s.symbol, s.foreign_net, s.institution_net, s.date, w.name
                FROM stock_supply s
                LEFT JOIN watchlist w ON s.symbol=w.symbol
                WHERE w.is_active=TRUE
                ORDER BY s.symbol, s.date DESC
            """)
        if supply_rows:
            buy_supply = [r for r in supply_rows if r['foreign_net'] > 0 or r['institution_net'] > 0]
            sell_supply = [r for r in supply_rows if r['foreign_net'] < 0 and r['institution_net'] < 0]
            ctx_parts.append(f"\n[수급 동향]")
            if buy_supply:
                ctx_parts.append("  🟢 외국인/기관 순매수: " + ", ".join(
                    [f"{r['name'] or r['symbol']}(외:{r['foreign_net']:+,})" for r in buy_supply[:5]]
                ))
            if sell_supply:
                ctx_parts.append("  🔴 외국인/기관 순매도: " + ", ".join(
                    [f"{r['name'] or r['symbol']}" for r in sell_supply[:5]]
                ))
    except:
        pass

    try:
        # 최근 중요 공시
        async with db_pool.acquire() as conn:
            disclosures = await conn.fetch("""
                SELECT d.symbol, d.corp_name, d.report_name, d.rcept_dt
                FROM stock_disclosure d
                JOIN watchlist w ON d.symbol=w.symbol
                WHERE w.is_active=TRUE AND d.is_important=TRUE
                ORDER BY d.rcept_dt DESC LIMIT 5
            """)
        if disclosures:
            ctx_parts.append(f"\n[최근 중요 공시]")
            for d in disclosures:
                ctx_parts.append(f"  📋 {d['corp_name']}({d['symbol']}): {d['report_name']} [{d['rcept_dt']}]")
    except:
        pass

    try:
        # 뉴스 감성 분석
        async with db_pool.acquire() as conn:
            sentiments = await conn.fetch("""
                SELECT DISTINCT ON (s.symbol)
                    s.symbol, s.sentiment_score, s.signal, s.summary, s.date, w.name
                FROM stock_news_sentiment s
                JOIN watchlist w ON s.symbol=w.symbol
                WHERE w.is_active=TRUE
                ORDER BY s.symbol, s.date DESC
            """)
        if sentiments:
            positive = [r for r in sentiments if r['sentiment_score'] > 0]
            negative = [r for r in sentiments if r['sentiment_score'] < 0]
            ctx_parts.append(f"\n[뉴스 감성 분석]")
            if positive:
                ctx_parts.append("  📰 긍정: " + ", ".join(
                    [f"{r['name'] or r['symbol']}({r['sentiment_score']:+d})" for r in positive[:5]]
                ))
            if negative:
                ctx_parts.append("  📰 부정: " + ", ".join(
                    [f"{r['name'] or r['symbol']}({r['sentiment_score']:+d})" for r in negative[:5]]
                ))
    except:
        pass

    return "\n".join(ctx_parts)


# 종목명 → 코드 매핑
STOCK_NAME_MAP = {
    "삼성전자": ("005930", "삼성전자"),
    "sk하이닉스": ("000660", "SK하이닉스"),
    "sk하이닉스": ("000660", "SK하이닉스"),
    "하이닉스": ("000660", "SK하이닉스"),
    "naver": ("035420", "NAVER"),
    "네이버": ("035420", "NAVER"),
    "카카오": ("035720", "카카오"),
    "현대차": ("005380", "현대차"),
    "현대자동차": ("005380", "현대차"),
    "셀트리온": ("068270", "셀트리온"),
    "lg에너지솔루션": ("373220", "LG에너지솔루션"),
    "카카오뱅크": ("323410", "카카오뱅크"),
    "삼성바이오로직스": ("207940", "삼성바이오로직스"),
    "삼성바이오": ("207940", "삼성바이오로직스"),
    "lg화학": ("051910", "LG화학"),
    "포스코": ("005490", "POSCO홀딩스"),
    "기아": ("000270", "기아"),
    "삼성sdi": ("006400", "삼성SDI"),
    "sk이노베이션": ("096770", "SK이노베이션"),
    "한국전력": ("015760", "한국전력"),
}

async def _manual_collect():
    """수동 데이터 수집 (뉴스 감성) + Jarvis 세션에 기록"""
    try:
        import aiohttp, os, json, re
        openwebui_url = os.getenv("OPENWEBUI_URL", "")
        openwebui_token = os.getenv("OPENWEBUI_API_TOKEN", "")
        jarvis_model = os.getenv("JARVIS_MODEL", "autotrader-jarvis")
        shared_session = os.getenv("JARVIS_ANALYST_CHAT_ID", "jarvis_main")

        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT symbol, name FROM watchlist WHERE is_active=TRUE LIMIT 10")

        results = []
        for r in rows:
            symbol, name = r["symbol"], r["name"] or r["symbol"]
            try:
                prompt = f"""다음 주식 뉴스를 분석해서 JSON만 출력해줘. 다른 말은 하지 마.
종목: {name}({symbol})
요청: {name} 주식 최신 뉴스 감성 분석
출력형식: {{"score": 1, "signal": "BUY", "summary": "긍정적"}}"""

                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{openwebui_url}/api/chat/completions",
                        headers={"Authorization": f"Bearer {openwebui_token}", "Content-Type": "application/json"},
                        json={"model": jarvis_model, "messages": [{"role": "user", "content": prompt}], "stream": False},
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as resp:
                        data = await resp.json()

                content_raw = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                content_clean = re.sub(r'```json|```', '', content_raw).strip()
                match = re.search(r'\{[^{}]*\}', content_clean, re.DOTALL)
                if match:
                    result = json.loads(match.group())
                    score = max(-2, min(2, int(result.get("score", 0))))
                    signal = result.get("signal", "NEUTRAL").upper()
                    if signal not in ["BUY", "SELL", "NEUTRAL"]:
                        signal = "NEUTRAL"
                    summary = result.get("summary", "")

                    async with db_pool.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO stock_news_sentiment
                            (symbol, date, sentiment_score, signal, summary, news_count)
                            VALUES ($1, $2, $3, $4, $5, 1)
                            ON CONFLICT (symbol, date) DO UPDATE
                            SET sentiment_score=$3, signal=$4, summary=$5
                        """, symbol, datetime.now().date(), score, signal, summary)

                    emoji = "🟢" if signal == "BUY" else "🔴" if signal == "SELL" else "🟡"
                    results.append(f"{emoji} {name}: {summary}")
                    logger.info(f"📰 {name}: {signal} ({score:+d}) - {summary}")

            except Exception as e:
                logger.error(f"수집 실패 [{symbol}]: {e}")

        # Jarvis Open-WebUI 세션에 뉴스 분석 결과 기록 (기억)
        if results:
            from datetime import timezone, timedelta
            KST = timezone(timedelta(hours=9))
            now_kst = datetime.now(KST).strftime("%m/%d %H:%M")
            memory_msg = f"""[시스템: 자동 뉴스 분석 {now_kst}]
오늘 감시 종목 뉴스 감성 분석 결과야. 이 내용을 기억하고 투자 조언에 활용해줘:

{chr(10).join(results)}"""

            try:
                async with aiohttp.ClientSession() as session:
                    await session.post(
                        f"{openwebui_url}/api/chat/completions",
                        headers={"Authorization": f"Bearer {openwebui_token}", "Content-Type": "application/json"},
                        json={
                            "model": jarvis_model,
                            "messages": [{"role": "user", "content": memory_msg}],
                            "stream": False,
                            "session_id": shared_session,
                        },
                        timeout=aiohttp.ClientTimeout(total=30)
                    )
                logger.info("✅ Jarvis 뉴스 기억 완료")
            except Exception as e:
                logger.warning(f"Jarvis 기억 실패: {e}")

        logger.info(f"✅ 수동 수집 완료: {len(results)}종목")
    except Exception as e:
        logger.error(f"수동 수집 오류: {e}")


async def _handle_watchlist_command(msg: str) -> str | None:
    """감시 종목 추가/삭제/조회 명령 감지 후 실행"""
    msg_lower = msg.lower().strip()

    # ── 조회 ──────────────────────────────────────────
    if any(k in msg_lower for k in ["감시 종목 보여", "감시종목 보여", "감시 종목 목록", "watchlist"]):
        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT symbol, name FROM watchlist WHERE is_active=TRUE ORDER BY created_at")
            if not rows:
                return "📋 현재 감시 종목이 없어요."
            lines = [f"  {r['symbol']} {r['name'] or ''}" for r in rows]
            return "📋 **현재 감시 종목**\n" + "\n".join(lines)
        except Exception as e:
            return f"❌ 조회 실패: {e}"

    # ── 추가 ──────────────────────────────────────────
    is_add = any(k in msg_lower for k in ["감시 종목 추가", "감시종목 추가", "추가해줘", "추가해", "등록해", "감시해줘"])
    if is_add:
        # STOCK_NAME_MAP에서 종목명 매칭
        matched_symbol, matched_name = None, None
        for name_key, (symbol, name) in STOCK_NAME_MAP.items():
            if name_key in msg_lower:
                matched_symbol, matched_name = symbol, name
                break

        # MAP에 없으면 메모리 캐시에서 빠른 검색
        if not matched_symbol:
            # 정확한 종목명 매칭
            for stock_name, ticker in _stock_name_cache.items():
                if stock_name in msg:
                    matched_symbol = ticker
                    matched_name = stock_name
                    break
            # 부분 매칭 (앞 2글자 이상)
            if not matched_symbol:
                for stock_name, ticker in _stock_name_cache.items():
                    if len(stock_name) >= 2 and stock_name[:2] in msg and len(stock_name) >= 2:
                        words = [w for w in msg_lower.split() if len(w) >= 2]
                        if any(stock_name.startswith(w) or w in stock_name for w in words):
                            matched_symbol = ticker
                            matched_name = stock_name
                            break

        if matched_symbol:
            try:
                async with db_pool.acquire() as conn:
                    existing = [r["symbol"] for r in await conn.fetch(
                        "SELECT symbol FROM watchlist WHERE is_active=TRUE"
                    )]
                    if matched_symbol in existing:
                        return f"📋 **{matched_name}**({matched_symbol})은 이미 감시 종목이에요."
                    await conn.execute("""
                        INSERT INTO watchlist (symbol, name, added_by, reason, is_active)
                        VALUES ($1, $2, 'jarvis', $3, TRUE)
                        ON CONFLICT (symbol) DO UPDATE
                        SET is_active=TRUE, name=$2, added_by='jarvis', reason=$3, updated_at=NOW()
                    """, matched_symbol, matched_name, msg)
                return f"✅ **{matched_name}**({matched_symbol})을 감시 종목에 추가했어요!"
            except Exception as e:
                return f"❌ 추가 실패: {e}"

        # 6자리 코드 직접 입력
        import re
        codes = re.findall(r'\b\d{6}\b', msg)
        if codes:
            results = []
            for code in codes:
                try:
                    async with db_pool.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO watchlist (symbol, added_by, reason, is_active)
                            VALUES ($1, 'jarvis', $2, TRUE)
                            ON CONFLICT (symbol) DO UPDATE
                            SET is_active=TRUE, added_by='jarvis', updated_at=NOW()
                        """, code, msg)
                    results.append(f"✅ {code} 추가")
                except Exception as e:
                    results.append(f"❌ {code} 실패: {e}")
            return "\n".join(results)

        return None  # Gemini로 넘김

    # ── 삭제/제거 ──────────────────────────────────────
    is_remove = any(k in msg_lower for k in ["감시 종목 제거", "감시종목 제거", "제거해줘", "삭제해줘", "빼줘"])
    if is_remove:
        for name_key, (symbol, name) in STOCK_NAME_MAP.items():
            if name_key in msg_lower:
                try:
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE watchlist SET is_active=FALSE, updated_at=NOW() WHERE symbol=$1", symbol
                        )
                    return f"🗑️ **{name}**({symbol})을 감시 종목에서 제거했어요."
                except Exception as e:
                    return f"❌ {name} 제거 실패: {e}"

        import re
        codes = re.findall(r'\b\d{6}\b', msg)
        if codes:
            results = []
            for code in codes:
                try:
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE watchlist SET is_active=FALSE, updated_at=NOW() WHERE symbol=$1", code
                        )
                    results.append(f"🗑️ {code} 제거")
                except Exception as e:
                    results.append(f"❌ {code} 실패: {e}")
            return "\n".join(results)

        return "❓ 종목명을 찾지 못했어요. 예: '삼성전자 감시 종목 제거해줘'"

    return None  # 일반 채팅으로 처리


@app.post("/api/jarvis/chat")
async def jarvis_chat(body: dict):
    """Jarvis AI 채팅 — Open-WebUI 통해서 (텔레그램과 대화 공유)"""
    user_msg = body.get("message", "").strip()
    session_id = body.get("session_id", None)
    # 웹/텔레그램 같은 세션 공유 (JARVIS_ANALYST_CHAT_ID 기준)
    if not session_id:
        session_id = os.getenv("JARVIS_ANALYST_CHAT_ID", "jarvis_main")
    if not user_msg:
        return {"success": False, "error": "메시지가 없어요"}

    try:
        # 감시 종목 추가/삭제 명령 감지 (Open-WebUI 거치지 않고 직접 처리)
        action_result = await _handle_watchlist_command(user_msg)
        if action_result:
            return {"success": True, "reply": action_result, "context_used": False}

        # 수동 수집 명령
        if any(k in user_msg for k in ["수동 수집", "뉴스 수집", "감성 수집", "데이터 수집"]):
            asyncio.create_task(_manual_collect())
            return {"success": True, "reply": "📰 데이터 수집 시작했어요! 1~2분 후 `/api/data/sentiment` 에서 결과 확인하세요.", "context_used": False}

        # 포트폴리오 컨텍스트 추가
        portfolio_ctx = await get_portfolio_context()
        full_msg = f"{user_msg}\n\n---\n현재 데이터:\n{portfolio_ctx}"

        # Open-WebUI 통해서 호출 (텔레그램과 같은 경로)
        reply = await _ask_openwebui(full_msg, session_id=session_id)

        logger.info(f"Jarvis 웹 응답: {reply[:100]}...")
        return {"success": True, "reply": reply, "context_used": True}

    except Exception as e:
        logger.error(f"Jarvis 오류: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/jarvis/analyze")
async def jarvis_analyze():
    """정기 포트폴리오 분석 — 자동 호출용"""
    try:
        api_key = config.GEMINI_API_KEY
        if not api_key:
            return {"success": False, "error": "GEMINI_API_KEY 없음"}

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            system_instruction=JARVIS_SYSTEM_PROMPT,
        )

        portfolio_ctx = await get_portfolio_context()
        prompt = f"""현재 포트폴리오를 분석하고 간단한 리포트를 작성해주세요.\n다음을 포함하세요:
1. 전체 수익/손실 현황 요약
2. 가장 주목할 종목 1-2개
3. 리스크 경고 (있다면)
4. 단기 액션 제안

포트폴리오 데이터:
{portfolio_ctx}"""

        response = model.generate_content(prompt)
        reply = response.text

        # 텔레그램 전송
        tg_msg = f"📊 <b>Jarvis 정기 분석</b>\n\n{reply}"
        await _send_telegram(tg_msg)

        return {"success": True, "reply": reply}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/jarvis/history")
async def jarvis_history():
    """대화 히스토리 조회"""
    history = []
    for msg in _jarvis_history:
        history.append({
            "role": msg.role,
            "text": msg.parts[0].text if msg.parts else "",
        })
    return {"success": True, "data": history}


@app.delete("/api/jarvis/history")
async def jarvis_clear_history():
    """대화 히스토리 초기화"""
    global _jarvis_history
    _jarvis_history = []
    return {"success": True, "message": "대화 초기화 완료"}


# ── 텔레그램 Webhook ─────────────────────────────────────────────

async def _typing_action(chat_id: str, token: str = None):
    """텔레그램 상단 '입력 중...' 표시"""
    _token = token or config.JARVIS_ANALYST_TOKEN or config.TELEGRAM_TOKEN
    if not _token or not chat_id:
        return
    try:
        import aiohttp as http
        async with http.ClientSession() as session:
            await session.post(
                f"https://api.telegram.org/bot{_token}/sendChatAction",
                json={"chat_id": chat_id, "action": "typing"},
                timeout=http.ClientTimeout(total=5),
            )
    except:
        pass


async def _send_telegram(text: str, chat_id: str = None, token: str = None):
    """텔레그램 메시지 전송 (내부용)"""
    _token = token or config.TELEGRAM_TOKEN
    cid = chat_id or config.TELEGRAM_CHAT_ID
    if not _token or not cid:
        return
    try:
        import aiohttp as http
        async with http.ClientSession() as session:
            await session.post(
                f"https://api.telegram.org/bot{_token}/sendMessage",
                json={"chat_id": cid, "text": text, "parse_mode": "HTML"},
                timeout=http.ClientTimeout(total=10),
            )
    except Exception as e:
        logger.warning(f"텔레그램 전송 실패: {e}")


# 텔레그램 채팅별 대화 히스토리 (Redis 저장)
async def _get_chat_history(chat_id: str, max_turns: int = 8) -> list:
    """Redis에서 대화 히스토리 로드"""
    if not redis_client:
        return []
    try:
        key = f"jarvis:history:{chat_id}"
        raw = await redis_client.get(key)
        if raw:
            return json.loads(raw)[-max_turns*2:]  # 최근 N턴
    except:
        pass
    return []


async def _save_chat_history(chat_id: str, role: str, content: str):
    """Redis에 대화 히스토리 저장"""
    if not redis_client:
        return
    try:
        key = f"jarvis:history:{chat_id}"
        raw = await redis_client.get(key)
        history = json.loads(raw) if raw else []
        history.append({"role": role, "content": content})
        # 최근 20턴만 보관
        if len(history) > 40:
            history = history[-40:]
        await redis_client.setex(key, 86400, json.dumps(history))  # 24시간 보관
    except Exception as e:
        logger.warning(f"히스토리 저장 실패: {e}")


async def _ask_openwebui(message: str, session_id: str = "telegram") -> str:
    """Open-WebUI Jarvis 모델 호출 — Tools + 대화 히스토리 포함"""
    import aiohttp as http
    import os
    openwebui_url   = os.getenv("OPENWEBUI_URL", "https://open-webui-production-5843.up.railway.app")
    openwebui_token = os.getenv("OPENWEBUI_API_TOKEN", "")
    jarvis_model    = os.getenv("JARVIS_MODEL", "autotrader-jarvis")

    if not openwebui_token:
        return await _ask_gemini_direct(message)

    try:
        # 이전 대화 히스토리 로드
        history = await _get_chat_history(session_id)

        # 현재 메시지 추가
        messages = history + [{"role": "user", "content": message}]

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {openwebui_token}",
        }
        payload = {
            "model": jarvis_model,
            "messages": [{"role": "system", "content": JARVIS_SYSTEM_PROMPT}] + messages,
            "stream": False,
        }
        async with http.ClientSession() as session:
            async with session.post(
                f"{openwebui_url}/api/chat/completions",
                json=payload,
                headers=headers,
                timeout=http.ClientTimeout(total=60),
            ) as res:
                data = await res.json()
                reply = data["choices"][0]["message"]["content"]

                # 대화 히스토리 저장
                await _save_chat_history(session_id, "user", message)
                await _save_chat_history(session_id, "assistant", reply)

                return reply
    except Exception as e:
        logger.error(f"Open-WebUI 호출 실패: {e}")
        return await _ask_gemini_direct(message)


async def _ask_gemini_direct(message: str) -> str:
    """Gemini 직접 호출 (Open-WebUI fallback)"""
    try:
        portfolio_ctx = await get_portfolio_context()
        full_msg = f"{message}\n\n---\n현재 데이터:\n{portfolio_ctx}"
        genai.configure(api_key=config.GEMINI_API_KEY)
        model = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            system_instruction=JARVIS_SYSTEM_PROMPT,
        )
        response = model.generate_content(full_msg)
        return response.text
    except Exception as e:
        return f"❌ AI 오류: {e}"


@app.post("/api/telegram/webhook")
async def telegram_webhook(body: dict):
    """텔레그램 Bot webhook — 메시지 수신 → Jarvis 처리"""
    try:
        message = body.get("message", {})
        chat_id = str(message.get("chat", {}).get("id", ""))
        text = message.get("text", "").strip()

        if not text or not chat_id:
            return {"ok": True}

        logger.info(f"텔레그램 수신: {text} (chat_id: {chat_id})")

        # 명령어 처리
        if text == "/start":
            token = config.JARVIS_ANALYST_TOKEN or config.TELEGRAM_TOKEN
            await _send_telegram(
                "🤖 <b>TradeJarvis입니다!</b>\n\n"
                "AI 트레이딩 어시스턴트예요. 실시간 데이터로 분석해드려요!\n\n"
                "<b>명령어:</b>\n"
                "/analyze — 포트폴리오 종합 분석\n"
                "/positions — 보유 포지션\n"
                "/status — 봇 상태\n"
                "/history — 매매 이력\n\n"
                "또는 자유롭게 질문하세요! 💬", chat_id, token
            )
            return {"ok": True}

        elif text == "/analyze":
            token = config.JARVIS_ANALYST_TOKEN or config.TELEGRAM_TOKEN
            await _typing_action(chat_id, token)
            reply = await _ask_openwebui(
                "현재 포트폴리오를 종합 분석하고 리스크와 액션 포인트를 알려줘",
                session_id=chat_id
            )
            await _send_telegram(f"📊 <b>TradeJarvis 분석</b>\n\n{reply}", chat_id, token)
            return {"ok": True}

        elif text == "/status":
            token = config.JARVIS_ANALYST_TOKEN or config.TELEGRAM_TOKEN
            reply = await _ask_openwebui("현재 봇 상태 알려줘", session_id=chat_id)
            await _send_telegram(f"🤖 <b>TradeJarvis</b>\n\n{reply}", chat_id, token)
            return {"ok": True}

        elif text == "/positions":
            token = config.JARVIS_ANALYST_TOKEN or config.TELEGRAM_TOKEN
            await _typing_action(chat_id, token)
            reply = await _ask_openwebui("현재 보유 포지션 현황 알려줘", session_id=chat_id)
            await _send_telegram(f"🤖 <b>TradeJarvis</b>\n\n{reply}", chat_id, token)
            return {"ok": True}

        elif text == "/history":
            try:
                trades_res = await get_trades(limit=10)
                trades = trades_res.get("data", [])
                if not trades:
                    await _send_telegram("📋 오늘 매매 이력 없음", chat_id)
                else:
                    lines = [f"📋 <b>최근 매매 {len(trades)}건</b>\n"]
                    for t in trades:
                        ts = t["ts"][11:16] if t["ts"] else "-"
                        side = "매수" if t["side"] == "BUY" else "매도"
                        pnl = f" {t['pnl']:+,}원" if t.get("pnl") else ""
                        lines.append(f"  {ts} {side} {t['symbol']}{pnl}")
                    await _send_telegram("\n".join(lines), chat_id)
            except Exception as e:
                await _send_telegram(f"❌ 이력 조회 실패: {e}", chat_id)
            return {"ok": True}

        else:
            # 수동 수집 명령 감지
            if any(k in text for k in ["뉴스 수집", "수동 수집", "감성 수집", "데이터 수집"]):
                token = config.JARVIS_ANALYST_TOKEN or config.TELEGRAM_TOKEN
                await _send_telegram("📰 데이터 수집 시작했어요! 잠시 기다려주세요...", chat_id, token)
                asyncio.create_task(_manual_collect())
                return {"ok": True}

            # 자유 대화 → Open-WebUI Jarvis (Tools + 메모리 포함)
            # 웹과 같은 세션 공유
            shared_session = os.getenv("JARVIS_ANALYST_CHAT_ID", "jarvis_main")
            token = config.JARVIS_ANALYST_TOKEN or config.TELEGRAM_TOKEN
            await _typing_action(chat_id, token)
            reply = await _ask_openwebui(text, session_id=shared_session)
            if len(reply) > 3800:
                reply = reply[:3800] + "...\n(내용이 길어 일부 생략됨)"
            await _send_telegram(f"🤖 <b>TradeJarvis</b>\n\n{reply}", chat_id, token)

        return {"ok": True}
    except Exception as e:
        logger.error(f"텔레그램 webhook 오류: {e}")
        return {"ok": True}


@app.get("/api/telegram/set-webhook")
async def set_telegram_webhook(request: fastapi.Request):
    """텔레그램 webhook URL 등록"""
    try:
        import aiohttp as http
        token = config.JARVIS_ANALYST_TOKEN or config.TELEGRAM_TOKEN
        if not token:
            return {"success": False, "error": "JARVIS_ANALYST_TOKEN 없음"}

        # 현재 서버 URL 자동 감지
        base_url = str(request.base_url).rstrip("/")
        # Railway는 항상 HTTPS
        base_url = base_url.replace("http://", "https://")
        webhook_url = f"{base_url}/api/telegram/webhook"

        async with http.ClientSession() as session:
            res = await session.post(
                f"https://api.telegram.org/bot{token}/setWebhook",
                json={"url": webhook_url, "drop_pending_updates": True},
            )
            data = await res.json()

        logger.info(f"텔레그램 webhook 등록: {webhook_url} → {data}")
        return {"success": data.get("ok"), "webhook_url": webhook_url, "result": data}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/debug/stock-account")
async def debug_stock_account():
    """KIS output2 raw 데이터 확인용 (디버그)"""
    try:
        import aiohttp as http
        base = config.kis_base_url
        token = await get_kis_token()
        if not token:
            return {"success": False, "error": "KIS 토큰 발급 실패"}
        async with http.ClientSession() as session:
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
                headers=headers, params=params,
                timeout=http.ClientTimeout(total=10),
            )
            raw = await res.json()
            out2 = raw.get("output2", [{}])
            summary = out2[0] if out2 else {}
            return {"success": True, "output2_keys": list(summary.keys()), "output2_data": summary}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ── 보유 포지션 API ──────────────────────────────────────

@app.get("/api/positions/stock")
async def get_stock_positions():
    """KIS API - 주식 보유 포지션 실시간 조회"""
    try:
        import aiohttp as http
        base = config.kis_base_url
        token = await get_kis_token()
        if not token:
            return {"success": False, "error": "KIS 토큰 발급 실패", "data": [], "account": {}}

        async with http.ClientSession() as session:
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
                headers=headers, params=params,
                timeout=http.ClientTimeout(total=10),
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
            # output2: 계좌 총평가 요약
            out2 = data.get("output2", [{}])
            summary = out2[0] if out2 else {}
            # 예수금: 필드명 후보 순서대로 시도
            cash_val = (
                int(summary.get("dnca_tot_amt", 0)) or
                int(summary.get("ord_psbl_cash", 0)) or
                int(summary.get("cma_evlu_amt", 0)) or
                int(summary.get("thdt_buyable_qty", 0))
            )
            account = {
                "total_eval":   int(summary.get("tot_evlu_amt", 0)),
                "stock_eval":   int(summary.get("scts_evlu_amt", 0)),
                "cash":         cash_val,
                "buy_amount":   int(summary.get("pchs_amt_smtl_amt", 0)),
                "pnl":          int(summary.get("evlu_pfls_smtl_amt", 0)),
                "pnl_rate":     float(summary.get("asst_icdc_erng_rt", 0) or 0),
                "_raw_keys":    list(summary.keys()),  # 디버그용
            }
            return {"success": True, "data": positions, "account": account}
    except Exception as e:
        return {"success": False, "error": str(e), "data": []}


@app.get("/api/positions/crypto")
async def get_crypto_positions():
    """업비트 코인 보유 포지션 - Redis 캐시에서 조회"""
    try:
        # crypto-trader가 저장한 포지션 캐시 조회
        cached = await redis_client.get("crypto:positions")
        if cached:
            import json
            positions = json.loads(cached)
            return {"success": True, "data": positions}

        # 캐시 없으면 빈 데이터
        return {"success": True, "data": [], "message": "포지션 데이터 없음 (crypto-trader 실행 중인지 확인)"}
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
                params = r["params"]
                if isinstance(params, str):
                    try:
                        params = json.loads(params)
                    except:
                        params = {}
                elif params is None:
                    params = {}
                data.append({
                    "id":        r["id"],
                    "bot":       r["bot"],
                    "name":      r["name"],
                    "is_active": r["is_active"],
                    "params":    params,
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


# ── 모의 데이터 API ──────────────────────────────────────
import random, math
from datetime import datetime, timedelta

# 종목/코인 기준 가격 (실제와 비슷한 수준)
_STOCK_BASE = {
    "005930": {"name": "삼성전자",    "price": 74200},
    "000660": {"name": "SK하이닉스",  "price": 198000},
    "035420": {"name": "NAVER",       "price": 215000},
    "035720": {"name": "카카오",      "price": 43500},
    "005380": {"name": "현대차",      "price": 232000},
    "068270": {"name": "셀트리온",    "price": 178000},
    "373220": {"name": "LG에너지솔루션", "price": 310000},
    "323410": {"name": "카카오뱅크",  "price": 24800},
}

_CRYPTO_BASE = {
    "KRW-BTC": {"name": "비트코인",  "price": 142_850_000},
    "KRW-ETH": {"name": "이더리움",  "price": 5_230_000},
    "KRW-SOL": {"name": "솔라나",    "price": 318_000},
    "KRW-XRP": {"name": "리플",      "price": 3_250},
    "KRW-ADA": {"name": "에이다",    "price": 810},
}

def _jitter(base: float, pct: float = 0.015) -> float:
    """기준가에서 ±pct 범위 랜덤 변동"""
    return round(base * (1 + random.uniform(-pct, pct)))

def _change_rate(base: float, cur: float) -> float:
    return round((cur - base) / base * 100, 2)


@app.get("/api/watchlist")
async def get_watchlist():
    """감시 종목 전체 조회"""
    try:
        rows = await db.get_watchlist(active_only=False)
        for r in rows:
            if r.get("created_at"):
                r["created_at"] = r["created_at"].isoformat()
            if r.get("updated_at"):
                r["updated_at"] = r["updated_at"].isoformat()
        return {"success": True, "data": rows}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/watchlist")
async def add_watchlist(body: dict):
    """감시 종목 추가 — Jarvis 또는 수동"""
    symbol  = body.get("symbol", "").strip().upper()
    name    = body.get("name", "")
    added_by = body.get("added_by", "manual")
    reason  = body.get("reason", "")
    if not symbol:
        return {"success": False, "error": "종목 코드 필요"}
    ok = await db.add_watchlist(symbol, name, added_by, reason)
    if ok:
        return {"success": True, "message": f"{name or symbol} 감시 종목 추가 완료"}
    return {"success": False, "error": "추가 실패"}


@app.delete("/api/watchlist/{symbol}")
async def remove_watchlist(symbol: str):
    """감시 종목 제거"""
    ok = await db.remove_watchlist(symbol.upper())
    if ok:
        return {"success": True, "message": f"{symbol} 감시 종목 제거 완료"}
    return {"success": False, "error": "제거 실패"}


@app.post("/api/jarvis/signal")
async def jarvis_signal(request: Request):
    """
    stock-trader/crypto-trader가 매매 신호 발생시 Jarvis에게 전달
    Jarvis가 분석 후 자동 실행 + 텔레그램 보고
    """
    try:
        body = await request.json()
        bot       = body.get("bot", "stock_trader")
        action    = body.get("action", "buy")   # buy / sell
        symbol    = body.get("symbol", "")
        name      = body.get("name", symbol)
        price     = body.get("price", 0)
        qty       = body.get("qty", 0)
        strategy  = body.get("strategy", "")
        reason    = body.get("reason", "")

        if not symbol:
            return {"success": False, "error": "종목코드 없음"}

        action_kr = "매수" if action == "buy" else "매도"
        token = config.JARVIS_ANALYST_TOKEN or config.TELEGRAM_TOKEN
        chat_id = config.JARVIS_ANALYST_CHAT_ID or config.TELEGRAM_CHAT_ID

        # 1. Jarvis에게 분석 요청
        analysis_prompt = f"""\n{name}({symbol}) {action_kr} 신호 발생!
전략: {strategy}
현재가: {price:,}원
수량: {qty}주
금액: {price*qty:,}원
이유: {reason}

시장 상황을 분석하고 이 매매를 실행해야 할지 판단해줘.
실행 여부를 결정하고 EXECUTE 또는 SKIP으로 시작해서 이유를 한 줄로 설명해줘.
"""
        jarvis_reply = await _ask_openwebui(analysis_prompt, session_id="signal")

        # 2. Jarvis 판단 결과 확인
        should_execute = jarvis_reply.upper().startswith("EXECUTE") or "실행" in jarvis_reply[:30]

        if should_execute:
            # 3. 실제 매매 실행 (주식 vs 코인 분기)
            import aiohttp as http

            if bot == "crypto_trader":
                # 코인 매매
                from upbit_trader import UpbitTrader
                upbit = UpbitTrader()
                upbit.session = http.ClientSession()
                await upbit.start()
                amount = body.get("amount", price * qty)

                if action == "BUY" or action == "buy":
                    result = await upbit.buy_market(symbol, amount)
                else:
                    result = await upbit.sell_market(symbol, qty)

                await upbit.session.close()

                if result.get("success"):
                    if db_pool:
                        async with db_pool.acquire() as conn:
                            await conn.execute("""
                                INSERT INTO trade_history (bot,asset_type,symbol,side,price,quantity,amount,strategy)
                                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                            """, bot, "crypto", symbol, action.upper(), float(price), float(qty), float(amount), strategy)

                    emoji = "📈" if action in ["buy", "BUY"] else "📉"
                    msg = (
                        f"{emoji} <b>{name} {action_kr} 완료</b>\n"
                        f"코인: {symbol}\n"
                        f"금액: {amount:,.0f}원\n"
                        f"전략: {strategy}\n"
                        f"Jarvis: {jarvis_reply[:80]}"
                    )
                    await _send_telegram(msg, chat_id, token)
                    logger.info(f"✅ Jarvis 코인 {action_kr}: {symbol} {amount:,.0f}원")
                    return {"success": True, "executed": True, "jarvis_reply": jarvis_reply}
                else:
                    await _send_telegram(f"❌ {name} 코인 {action_kr} 실패\n{result.get('error')}", chat_id, token)
                    return {"success": False, "executed": False, "error": result.get("error")}

            else:
                # 주식 매매
                from stock_trader.kis_trader import KISTrader
                trader = KISTrader()
                trader.session = http.ClientSession()
                await trader._get_token()

                if action in ["buy", "BUY"]:
                    result = await trader.buy(symbol, price, qty)
                else:
                    result = await trader.sell(symbol, price, qty)

                await trader.session.close()

            if result.get("success"):
                # DB에 매매 기록
                if db_pool:
                    async with db_pool.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO trade_history (bot,asset_type,symbol,side,price,quantity,amount,strategy)
                            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                        """, bot, "stock", symbol, action.upper(), float(price), float(qty), float(price*qty), strategy)

                # 텔레그램 보고
                emoji = "📈" if action == "buy" else "📉"
                msg = (
                    f"{emoji} <b>{name} {action_kr} 완료</b>\n"
                    f"가격: {price:,}원 × {qty}주\n"
                    f"금액: {price*qty:,}원\n"
                    f"전략: {strategy}\n"
                    f"Jarvis 판단: {jarvis_reply[:80]}"
                )
                await _send_telegram(msg, chat_id, token)
                logger.info(f"✅ Jarvis 자동 {action_kr}: {symbol} {price:,}원 × {qty}주")
                return {"success": True, "executed": True, "jarvis_reply": jarvis_reply}
            else:
                await _send_telegram(
                    f"❌ {name} {action_kr} 실패\n{result.get('error')}",
                    chat_id, token
                )
                return {"success": False, "executed": False, "error": result.get("error")}
        else:
            # 4. 건너뜀 보고
            msg = f"⏭️ <b>{name} {action_kr} 신호 건너뜀</b>\nJarvis 판단: {jarvis_reply[:100]}"
            await _send_telegram(msg, chat_id, token)
            logger.info(f"⏭️ Jarvis가 {action_kr} 신호 건너뜀: {symbol}")
            return {"success": True, "executed": False, "jarvis_reply": jarvis_reply}

    except Exception as e:
        logger.error(f"Jarvis 신호 처리 오류: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/trade/execute")
async def execute_trade(request: Request):
    """Jarvis가 직접 매수/매도 명령"""
    try:
        body = await request.json()
        bot    = body.get("bot", "stock")
        action = body.get("action", "buy")
        symbol = body.get("symbol", "")
        amount = int(body.get("amount", 500000))

        if not symbol:
            return {"success": False, "error": "종목코드 없음"}

        if bot == "stock":
            from stock_trader.kis_trader import KISTrader
            trader = KISTrader()
            trader.session = __import__('aiohttp').ClientSession()
            await trader._get_token()
            price = await trader.get_current_price(symbol)
            if price <= 0:
                return {"success": False, "error": "시세 조회 실패 (장외 시간일 수 있음)"}
            qty = max(1, amount // price)
            if action == "buy":
                result = await trader.buy(symbol, price, qty)
            else:
                result = await trader.sell(symbol, price, qty)
            await trader.session.close()
            return {"success": result["success"], "data": result, "price": price, "qty": qty}

        elif bot == "crypto":
            return {"success": False, "error": "코인 거래는 Static IP 설정 후 가능합니다"}

        return {"success": False, "error": "알 수 없는 봇"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/mock/prices/stock")
async def mock_stock_prices():
    """모의 주식 실시간 시세"""
    prices = {}
    for sym, info in _STOCK_BASE.items():
        cur = _jitter(info["price"])
        prev = _jitter(info["price"], 0.01)
        prices[sym] = {
            "name":        info["name"],
            "price":       cur,
            "prev":        prev,
            "change":      cur - prev,
            "change_rate": _change_rate(prev, cur),
            "volume":      random.randint(500_000, 30_000_000),
        }
    return {"success": True, "data": prices}


@app.get("/api/mock/prices/crypto")
async def mock_crypto_prices():
    """모의 코인 실시간 시세 (업비트 공개 API 시도 → 실패시 모의)"""
    import aiohttp as http
    prices = {}
    pairs = list(_CRYPTO_BASE.keys())
    try:
        async with http.ClientSession() as session:
            res = await session.get(
                "https://api.upbit.com/v1/ticker",
                params={"markets": ",".join(pairs)},
                timeout=http.ClientTimeout(total=4),
            )
            tickers = await res.json()
            for t in tickers:
                pair = t["market"]
                info = _CRYPTO_BASE.get(pair, {})
                prices[pair] = {
                    "name":        info.get("name", pair),
                    "price":       float(t.get("trade_price", 0)),
                    "prev":        float(t.get("prev_closing_price", 0)),
                    "change":      float(t.get("signed_change_price", 0)),
                    "change_rate": float(t.get("signed_change_rate", 0)) * 100,
                    "volume":      float(t.get("acc_trade_volume_24h", 0)),
                    "high":        float(t.get("high_price", 0)),
                    "low":         float(t.get("low_price", 0)),
                    "source":      "live",
                }
        return {"success": True, "data": prices}
    except Exception as e:
        logger.warning(f"업비트 실시간 실패, 모의 데이터 사용: {e}")
        for pair, info in _CRYPTO_BASE.items():
            cur = _jitter(info["price"], 0.008)
            prev = _jitter(info["price"], 0.005)
            prices[pair] = {
                "name":        info["name"],
                "price":       cur,
                "prev":        prev,
                "change":      cur - prev,
                "change_rate": _change_rate(prev, cur),
                "volume":      random.uniform(100, 5000),
                "high":        round(cur * 1.02),
                "low":         round(cur * 0.98),
                "source":      "mock",
            }
        return {"success": True, "data": prices}


@app.get("/api/mock/summary")
async def mock_summary():
    """모의 요약 데이터"""
    stock_pnl  = random.randint(-50000, 300000)
    crypto_pnl = random.randint(-80000, 500000)
    total_pnl  = stock_pnl + crypto_pnl
    today_trades = random.randint(0, 12)
    return {
        "success": True,
        "data": {
            "today_trades": today_trades,
            "today_pnl":    total_pnl,
            "total_pnl":    total_pnl + random.randint(500000, 3000000),
            "bot_pnl": {
                "stock_trader":  stock_pnl,
                "crypto_trader": crypto_pnl,
            },
            "source": "mock",
        }
    }


@app.get("/api/mock/positions/stock")
async def mock_stock_positions():
    """모의 주식 보유 포지션"""
    # 3~5개 랜덤 종목 보유
    symbols = random.sample(list(_STOCK_BASE.keys()), k=random.randint(2, 5))
    positions = []
    for sym in symbols:
        info = _STOCK_BASE[sym]
        avg  = round(info["price"] * random.uniform(0.88, 1.05))
        cur  = _jitter(info["price"])
        qty  = random.randint(5, 50)
        pnl  = (cur - avg) * qty
        positions.append({
            "symbol":    sym,
            "name":      info["name"],
            "qty":       qty,
            "avg_price": avg,
            "cur_price": cur,
            "pnl":       round(pnl),
            "pnl_rate":  round((cur - avg) / avg * 100, 2),
        })
    return {"success": True, "data": positions}


@app.get("/api/mock/positions/crypto")
async def mock_crypto_positions():
    """모의 코인 보유 포지션"""
    pairs = random.sample(list(_CRYPTO_BASE.keys()), k=random.randint(1, 3))
    positions = []
    for pair in pairs:
        info = _CRYPTO_BASE[pair]
        avg  = round(info["price"] * random.uniform(0.85, 1.08))
        cur  = _jitter(info["price"])
        qty  = round(random.uniform(0.001, 0.5) if "BTC" in pair else random.uniform(0.1, 10), 6)
        pnl  = (cur - avg) * qty
        currency = pair.replace("KRW-", "")
        positions.append({
            "pair":      pair,
            "currency":  currency,
            "name":      info["name"],
            "qty":       qty,
            "avg_price": avg,
            "cur_price": cur,
            "pnl":       round(pnl),
            "pnl_rate":  round((cur - avg) / avg * 100, 2),
        })
    return {"success": True, "data": positions}


@app.get("/api/mock/trades")
async def mock_trades(limit: int = 10, bot: str = None):
    """모의 매매 이력"""
    all_trades = []
    now = datetime.now()
    stock_syms  = list(_STOCK_BASE.items())
    crypto_syms = list(_CRYPTO_BASE.items())
    for i in range(limit):
        is_stock = (bot == "stock_trader") or (bot is None and random.random() > 0.4)
        if is_stock:
            sym, info = random.choice(stock_syms)
            price = _jitter(info["price"])
            qty   = random.randint(1, 20)
            b     = "stock_trader"
            atype = "stock"
        else:
            pair, info = random.choice(crypto_syms)
            sym   = pair
            price = _jitter(info["price"])
            qty   = round(random.uniform(0.0001, 0.05) if "BTC" in pair else random.uniform(0.01, 2), 6)
            b     = "crypto_trader"
            atype = "crypto"
        side = random.choice(["BUY", "SELL"])
        pnl  = round(random.uniform(-50000, 150000)) if side == "SELL" else None
        ts   = now - timedelta(minutes=i * random.randint(5, 30))
        all_trades.append({
            "id":         i + 1,
            "bot":        b,
            "asset_type": atype,
            "symbol":     sym,
            "side":       side,
            "price":      price,
            "quantity":   qty,
            "amount":     round(price * qty),
            "strategy":   random.choice(["MA크로스", "MACD", "RSI반등", "볼린저밴드"]),
            "pnl":        pnl,
            "ts":         ts.isoformat(),
        })
    return {"success": True, "data": all_trades}


@app.get("/api/mock/status")
async def mock_status():
    """모의 봇 상태"""
    return {
        "success": True,
        "data": {
            "stock_trader":  {"status": "running", "last_tick": datetime.now().isoformat()},
            "crypto_trader": {"status": "running", "last_tick": datetime.now().isoformat()},
            "data_collector":{"status": "running", "last_tick": datetime.now().isoformat()},
        }
    }
