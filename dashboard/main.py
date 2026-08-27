"""
AutoTrader Dashboard — FastAPI 서버
실시간 DB/Redis 데이터를 API로 제공
"""
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

KST = timezone(timedelta(hours=9))

import fastapi
from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
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

# pykrx 내부의 깨진 logging.info(args, kwargs) 호출이 KRX 차단 시
# 'Logging error' 트레이스백을 대량 발생 → pykrx 발 레코드 차단
class _PykrxNoiseFilter(logging.Filter):
    def filter(self, record):
        try:
            if "pykrx" in (record.pathname or ""):
                return False
            if record.name == "root" and "Expecting value" in str(record.msg):
                return False
        except Exception:
            pass
        return True

for _h in logging.root.handlers:
    _h.addFilter(_PykrxNoiseFilter())
logging.raiseExceptions = False  # 포맷 오류 트레이스백 출력 억제
logging.root.handlers[0].setFormatter(_fmt)
logger = logging.getLogger("dashboard")

app = FastAPI(title="AutoTrader Dashboard")

# SSE 이벤트 큐 (실시간 알림)
import queue
sse_clients: list = []

async def push_event(event_type: str, data: dict):
    """SSE 클라이언트에게 이벤트 푸시"""
    import json
    msg = f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
    for q in sse_clients[:]:
        try:
            await q.put(msg)
        except:
            sse_clients.remove(q)

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

async def get_kis_token(force_new: bool = False) -> str:
    """KIS 액세스 토큰 — Redis 캐시 우선 (force_new=True면 강제 재발급)"""
    import time
    now = time.time()

    redis_key = "kis:paper_token" if config.KIS_IS_PAPER else "kis:access_token"
    if force_new:
        _kis_token_cache["token"] = None
        _kis_token_cache["expires"] = 0
        try:
            if redis_client:
                await redis_client.delete(redis_key)
        except Exception:
            pass

    # 1. 메모리 캐시 확인
    if _kis_token_cache["token"] and now < _kis_token_cache["expires"]:
        return _kis_token_cache["token"]

    # 2. Redis 캐시 확인 (모의투자/실전 구분)
    try:
        if redis_client and not force_new:
            cached = await redis_client.get(redis_key)
            if cached:
                token = cached if isinstance(cached, str) else cached.decode('utf-8')
                _kis_token_cache["token"] = token
                _kis_token_cache["expires"] = now + 82800  # 23시간
                return token
    except Exception:
        pass

    # 3. 새 토큰 발급
    try:
        import ssl as _ssl_mod
        _ssl_ctx = _ssl_mod.create_default_context()
        _ssl_ctx.check_hostname = False
        _ssl_ctx.verify_mode = _ssl_mod.CERT_NONE
        base = config.kis_base_url
        async with _aiohttp.ClientSession(connector=_aiohttp.TCPConnector(ssl=_ssl_ctx)) as session:
            res = await session.post(f"{base}/oauth2/tokenP", json={
                "grant_type": "client_credentials",
                "appkey": config.kis_app_key,
                "appsecret": config.kis_app_secret,
            }, timeout=_aiohttp.ClientTimeout(total=10))
            data = await res.json()
            token = data.get("access_token", "")
            if token:
                _kis_token_cache["token"] = token
                _kis_token_cache["expires"] = now + 82800  # 23시간
                # Redis에 저장
                try:
                    if redis_client:
                        save_key = "kis:paper_token" if config.KIS_IS_PAPER else "kis:access_token"
                        await redis_client.setex(save_key, 82800, token)
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


async def _get_price_ceiling() -> int:
    """활성 지시에서 'N만원 이하' 가격 상한 파싱 (없으면 0)"""
    try:
        import re as _re
        txt = await _get_active_directives(20)
        m = _re.search(r"(\d+)\s*만\s*원?\s*이하", txt or "")
        return int(m.group(1)) * 10000 if m else 0
    except Exception:
        return 0


async def _kis_scan_candidates() -> list:
    """KRX(pykrx) 차단 시 폴백: KIS 거래량순위 → KIS 일봉으로 스코어링"""
    import asyncio as _asyncio
    token = await get_kis_token()
    if not token:
        logger.error("🔍 KIS 폴백 스캔: 토큰 없음")
        return None  # 하드 실패

    import ssl as _ssl
    ctx = _ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = _ssl.CERT_NONE
    conn = _aiohttp.TCPConnector(ssl=ctx)

    def _hdr(tr_id):
        return {"authorization": f"Bearer {token}", "appkey": config.kis_app_key,
                "appsecret": config.kis_app_secret, "tr_id": tr_id, "custtype": "P"}

    # 1) 거래량 순위로 후보 유니버스 (코스피 0001 / 코스닥 1001, 각 상위 30)
    # 모의투자 서버는 순위 TR 미지원일 수 있어 실전 시세 도메인도 시도
    bases = [config.kis_base_url]
    if config.KIS_IS_PAPER:
        bases.append("https://openapi.koreainvestment.com:9443")
    universe = []
    async with _aiohttp.ClientSession(connector=conn) as sess:
        for mkt_code in ["0001", "1001"]:
            got = []
            for base in bases:
                try:
                    r = await sess.get(
                        f"{base}/uapi/domestic-stock/v1/quotations/volume-rank",
                        headers=_hdr("FHPST01710000"),
                        params={"FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "20171",
                                "FID_INPUT_ISCD": mkt_code, "FID_DIV_CLS_CODE": "0",
                                "FID_BLNG_CLS_CODE": "0", "FID_TRGT_CLS_CODE": "111111111",
                                "FID_TRGT_EXLS_CLS_CODE": "0000000000", "FID_INPUT_PRICE_1": "",
                                "FID_INPUT_PRICE_2": "", "FID_VOL_CNT": "", "FID_INPUT_DATE_1": ""},
                        timeout=_aiohttp.ClientTimeout(total=10))
                    data = await r.json()
                    rows = data.get("output", []) or []
                    if rows:
                        got = [(x.get("mksc_shrn_iscd"), x.get("hts_kor_isnm")) for x in rows[:30]]
                        break
                except Exception as e:
                    logger.debug(f"거래량순위 실패({base}): {e}")
            universe.extend([(s, n) for s, n in got if s])
            await _asyncio.sleep(0.3)

        if not universe:
            logger.error("🔍 KIS 폴백: 거래량순위 조회 실패")
            return None  # 하드 실패
        logger.info(f"🔍 KIS 폴백 유니버스: {len(universe)}종목")

        # 2) 각 종목 일봉 30개로 스코어링 (기존 pykrx 로직과 동일)
        price_ceiling = await _get_price_ceiling()
        if price_ceiling:
            logger.info(f"🔍 지시 반영: 주당 {price_ceiling:,}원 이하만 스캔")
        from datetime import timedelta as _td
        end = datetime.now(KST).strftime("%Y%m%d")
        start = (datetime.now(KST) - _td(days=60)).strftime("%Y%m%d")
        results = []
        for symbol, name in universe:
            try:
                # 특수증권 제외: 6자리 숫자 보통주(끝 0)만, 스팩 제외
                if not (symbol and symbol.isdigit() and len(symbol) == 6 and symbol.endswith("0")):
                    continue
                if name and ("스팩" in name or "SPAC" in name.upper()):
                    continue
                r = await sess.get(
                    f"{config.kis_base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
                    headers=_hdr("FHKST03010100"),
                    params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol,
                            "FID_INPUT_DATE_1": start, "FID_INPUT_DATE_2": end,
                            "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "1"},
                    timeout=_aiohttp.ClientTimeout(total=10))
                data = await r.json()
                rows = [x for x in (data.get("output2") or []) if x.get("stck_clpr")]
                if len(rows) < 22:
                    continue
                rows.reverse()  # 과거→최신
                closes = [float(x["stck_clpr"]) for x in rows]
                vols   = [float(x.get("acml_vol", 0) or 0) for x in rows]
                close = closes[-1]
                if close < 1000:
                    continue
                if price_ceiling and close > price_ceiling:
                    continue  # 주인 지시: 가격 상한
                vol, vol_avg = vols[-1], (sum(vols[-6:-1]) / 5 if len(vols) >= 6 else 0)
                # 등락률: 마지막 서로 다른 두 종가 기준 (중복 캔들 0.0% 버그 방지)
                change = 0.0
                for k in range(len(closes) - 2, -1, -1):
                    if closes[k] != closes[-1]:
                        change = (closes[-1] / closes[k] - 1) * 100
                        break
                ma5 = sum(closes[-5:]) / 5; ma20 = sum(closes[-20:]) / 20
                ma5p = sum(closes[-6:-1]) / 5; ma20p = sum(closes[-21:-1]) / 20
                golden_cross = ma5p < ma20p and ma5 > ma20
                ma_trend_ok = ma5 > ma20
                vol_ok = vol > vol_avg * 1.5 if vol_avg > 0 else False
                gains = [max(closes[i] - closes[i-1], 0) for i in range(-14, 0)]
                losses = [max(closes[i-1] - closes[i], 0) for i in range(-14, 0)]
                ag, al = sum(gains) / 14, sum(losses) / 14
                rsi = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
                momentum_5d = (closes[-1] / closes[-6] - 1) * 100 if len(closes) >= 6 else 0
                # 폭락 종목 제외 (떨어지는 칼날)
                if momentum_5d <= -5:
                    continue
                score = (3 if golden_cross else 0) + (2 if vol_ok else 0)
                score += (1 if change > 1 else 0) + (1 if change > 3 else 0)
                score += (2 if 30 <= rsi <= 55 else 0) + (1 if 50 < rsi <= 70 else 0)
                score += (1 if momentum_5d > 1.5 else 0)
                score += (1 if (ma_trend_ok and not golden_cross) else 0)
                if score >= 4:
                    results.append({"symbol": symbol, "name": name or symbol, "change": change,
                                    "vol_ratio": vol / vol_avg if vol_avg > 0 else 1,
                                    "golden_cross": golden_cross, "score": score, "close": int(close),
                                    "rsi": round(rsi, 1), "momentum_5d": round(momentum_5d, 2)})
            except Exception:
                pass
            await _asyncio.sleep(0.5)  # 모의투자 rate limit (초당 2건)

    logger.info(f"🔍 KIS 폴백 스캔 완료: {len(results)}종목 통과")
    # 종목명 캐시 보강 (pykrx 실패 시에도 이름 표시 가능)
    try:
        for r_ in results:
            if r_.get("name"):
                _stock_name_cache[r_["name"]] = r_["symbol"]
                _stock_code_cache[r_["symbol"]] = r_["name"]
    except Exception:
        pass
    return sorted(results, key=lambda x: x["score"], reverse=True)[:20]


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

        # 08:30 스캔은 장 시작 전이라 '오늘' 데이터가 아직 없음.
        # pykrx가 실제 보유한 최근 거래일 기준으로 분석해야 함.
        # 넉넉히 today까지 요청하되, 최근 거래일을 pykrx로 확인.
        today = now_kst.strftime("%Y%m%d")
        d30 = (now_kst - timedelta(days=45)).strftime("%Y%m%d")
        try:
            # 삼성전자(005930)로 실제 마지막 거래일 확인
            _probe = pykrx_stock.get_market_ohlcv(d30, today, "005930")
            if _probe is not None and len(_probe) > 0:
                last_trading_day = _probe.index[-1].strftime("%Y%m%d")
                logger.info(f"🔍 최근 거래일: {last_trading_day} (오늘: {today})")
                today = last_trading_day  # 분석 종료일을 실제 마지막 거래일로
            else:
                logger.warning("🔍 pykrx 프로브 데이터 없음 — 기본 날짜 사용")
        except Exception as e:
            logger.warning(f"🔍 거래일 확인 실패: {e}")

        loop = asyncio.get_event_loop()

        def _scan():
            import time as _t
            results = []
            stats = {"total": 0, "no_data": 0, "low_price": 0, "scored": 0, "err": 0}
            for market in ["KOSPI", "KOSDAQ"]:
                # KRX가 간헐적으로 빈 응답 반환 → 최대 3회 재시도
                tickers = []
                for attempt in range(3):
                    try:
                        # 날짜 명시 → pykrx 내부 '최근 영업일 탐색'(불안정) 우회
                        tickers = pykrx_stock.get_market_ticker_list(today, market=market)
                        if tickers:
                            break
                    except Exception as e:
                        logger.warning(f"🔍 {market} 티커 조회 {attempt+1}차 실패: {e}")
                    _t.sleep(3)
                logger.info(f"🔍 {market} 종목 수: {len(tickers)}")
                if not tickers:
                    logger.error(f"🔍 {market} 티커 목록 조회 최종 실패 — 스킵")
                    continue
                try:
                    for ticker in tickers:  # 전체 종목
                        stats["total"] += 1
                        try:
                            # 30일 데이터로 MA크로스 체크
                            df = pykrx_stock.get_market_ohlcv(d30, today, ticker)
                            if df is None or len(df) < 22:
                                stats["no_data"] += 1
                                continue

                            close = df["종가"].iloc[-1]
                            if close < 1000:  # 동전주 제외
                                stats["low_price"] += 1
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
                            # MA 정배열
                            ma_trend_ok = ma5 > ma20

                            # 거래량 조건: 평균 대비 1.5배 이상
                            vol_ok = vol > vol_avg * 1.5 if vol_avg > 0 else False

                            # RSI 계산 (14일)
                            rsi = 50.0
                            if len(closes) >= 15:
                                gains, losses = [], []
                                for i in range(-14, 0):
                                    d = closes[i] - closes[i - 1]
                                    gains.append(max(d, 0))
                                    losses.append(max(-d, 0))
                                avg_gain = sum(gains) / 14
                                avg_loss = sum(losses) / 14
                                if avg_loss > 0:
                                    rs = avg_gain / avg_loss
                                    rsi = 100 - (100 / (1 + rs))
                                else:
                                    rsi = 100.0
                            rsi_bounce = 30 <= rsi <= 55  # 과매도 반등
                            rsi_strong = 50 < rsi <= 70   # 강세 추세

                            # 모멘텀: 5일 수익률
                            momentum_5d = (closes[-1] / closes[-6] - 1) * 100 if len(closes) >= 6 else 0
                            momentum_ok = momentum_5d > 1.5

                            score = 0
                            if golden_cross:
                                score += 3
                            if vol_ok:
                                score += 2
                            if change > 1:
                                score += 1
                            if change > 3:
                                score += 1
                            if rsi_bounce:
                                score += 2
                            if rsi_strong:
                                score += 1
                            if momentum_ok:
                                score += 1
                            if ma_trend_ok and not golden_cross:
                                score += 1

                            if score >= 4:  # 강화된 기준
                                if momentum_5d <= -5:  # 폭락 종목 제외
                                    continue
                                if not (ticker.isdigit() and ticker.endswith("0")):  # 특수증권 제외
                                    continue
                                name = pykrx_stock.get_market_ticker_name(ticker)
                                if name and "스팩" in name:
                                    continue
                                stats["scored"] += 1
                                results.append({
                                    "symbol": ticker,
                                    "name": name,
                                    "change": change,
                                    "vol_ratio": vol / vol_avg if vol_avg > 0 else 1,
                                    "golden_cross": golden_cross,
                                    "score": score,
                                    "close": close,
                                    "rsi": round(rsi, 1),
                                    "momentum_5d": round(momentum_5d, 2),
                                })
                        except Exception:
                            stats["err"] += 1
                            continue
                except Exception as e:
                    logger.error(f"🔍 {market} 스캔 오류: {e}")
                    continue
            logger.info(f"🔍 스캔 통계: 전체 {stats['total']} · 데이터부족 {stats['no_data']} · "
                        f"동전주 {stats['low_price']} · 통과 {stats['scored']} · 오류 {stats['err']}")
            return sorted(results, key=lambda x: x["score"], reverse=True)[:20]

        candidates = await loop.run_in_executor(None, _scan)

        if not candidates:
            logger.warning("🔍 pykrx 스캔 실패/0종목 → KIS API 폴백 스캔 시도")
            candidates = await _kis_scan_candidates()

        if candidates is None:
            # 스캔 자체 실패 → 기존 watchlist 보존
            logger.error("🔍 스캔 실패 — 기존 watchlist 유지")
            await _send_telegram(f"🔍 Jarvis 스캔 [{now_kst.strftime('%m/%d %H:%M')}]\n⚠️ 스캔 실패 (기존 감시종목 유지)")
            return

        if not candidates:
            # 스캔은 성공했으나 통과 종목 없음 → 잔재 정리
            cleaned = await _cleanup_scanner_watchlist()
            logger.info(f"🔍 스캔 완료: 유망 종목 없음 (잔재 {cleaned}개 정리)")
            await _send_telegram(
                f"🔍 Jarvis 스캔 [{now_kst.strftime('%m/%d %H:%M')}]\n유망 종목 없음"
                + (f" · 기존 {cleaned}종목 해제" if cleaned else "")
            )
            return

        # ── watchlist 갱신 ──────────────────────────────────
        # 매일 재선정: 스캐너가 넣은 기존 종목은 비활성화하되,
        # 현재 보유 중인 종목과 수동 추가 종목은 유지
        held_symbols = set()
        try:
            pos_res = await get_stock_positions()
            if pos_res.get("success"):
                held_symbols = {p["symbol"] for p in pos_res.get("data", []) if p.get("symbol")}
        except Exception as e:
            logger.warning(f"보유 종목 조회 실패(무시): {e}")

        today_symbols = {c["symbol"] for c in candidates}
        added = []
        async with db_pool.acquire() as conn:
            # 1) 스캐너가 넣었던 종목 중 오늘 안 뽑혔고 보유중도 아닌 것 → 비활성화
            old_scanner = await conn.fetch(
                "SELECT symbol FROM watchlist WHERE is_active=TRUE AND added_by='jarvis_scanner'"
            )
            deactivated = 0
            for r in old_scanner:
                sym = r["symbol"]
                if sym not in today_symbols and sym not in held_symbols:
                    await conn.execute(
                        "UPDATE watchlist SET is_active=FALSE, updated_at=NOW() WHERE symbol=$1", sym
                    )
                    deactivated += 1

            # 2) 오늘 뽑은 종목 활성화 (신규는 추가, 기존은 갱신)
            existing_active = {r["symbol"] for r in await conn.fetch(
                "SELECT symbol FROM watchlist WHERE is_active=TRUE"
            )}
            for c in candidates:
                gc = "🌟" if c["golden_cross"] else ""
                rsi_tag = f" RSI{c.get('rsi',50):.0f}"
                mom_tag = f" 5d{c.get('momentum_5d',0):+.1f}%"
                reason = (f"{'골든크로스+' if c['golden_cross'] else ''}"
                          f"거래량{c['vol_ratio']:.1f}배 등락률{c['change']:+.1f}%"
                          f" RSI{c.get('rsi',50):.0f} 모멘텀{c.get('momentum_5d',0):+.1f}%")
                await conn.execute("""
                    INSERT INTO watchlist (symbol, name, added_by, reason, is_active)
                    VALUES ($1, $2, 'jarvis_scanner', $3, TRUE)
                    ON CONFLICT (symbol) DO UPDATE
                    SET is_active=TRUE, added_by='jarvis_scanner', reason=$3, updated_at=NOW()
                """, c["symbol"], c["name"], reason)
                if c["symbol"] not in existing_active:
                    added.append(f"  {gc}{c['name']}({c['symbol']}) {c['close']:,}원 {c['change']:+.1f}%{rsi_tag}{mom_tag}")

        msg = f"🔍 Jarvis 스캔 [{now_kst.strftime('%m/%d %H:%M')}]\n"
        msg += f"총 {len(candidates)}종목 선정 (재선정)"
        if deactivated:
            msg += f" · 기존 {deactivated}종목 해제"
        if held_symbols:
            msg += f" · 보유 {len(held_symbols)}종목 유지"
        if added:
            msg += f"\n신규 {len(added)}종목:\n" + "\n".join(added[:10])
        await _send_telegram(msg)
        logger.info(f"✅ 스캐너 완료: {len(candidates)}종목 선정, 신규 {len(added)}, 해제 {deactivated}")

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


async def _trigger_ohlcv_collect():
    """watchlist 종목에 대한 OHLCV 수집을 data-collector에 트리거"""
    try:
        import aiohttp
        import os
        from datetime import timezone, timedelta
        KST = timezone(timedelta(hours=9))

        if not db_pool:
            return

        async with db_pool.acquire() as conn:
            symbols = [r["symbol"] for r in await conn.fetch(
                "SELECT symbol FROM watchlist WHERE is_active=TRUE"
            )]

        if not symbols:
            return

        # data-collector API 호출 (내부 통신)
        collector_url = os.getenv("COLLECTOR_URL", "http://autotrader.railway.internal:8000")
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                await session.post(f"{collector_url}/api/collect/ohlcv",
                                   json={"symbols": symbols[:50]})  # 최대 50종목
            logger.info(f"📡 OHLCV 수집 트리거: {len(symbols)}종목")
        except Exception:
            # data-collector 연결 실패 시 직접 pykrx로 수집
            logger.info(f"📡 data-collector 미연결 — 직접 수집 스킵 (stock-trader가 처리)")
    except Exception as e:
        logger.warning(f"OHLCV 수집 트리거 실패: {e}")


async def _jarvis_closing_report():
    """장 마감 후 오늘 거래 결과 + 내일 전략 업데이트 텔레그램 리포트"""
    try:
        from datetime import timezone, timedelta
        KST = timezone(timedelta(hours=9))
        now_kst = datetime.now(KST)
        today_str = now_kst.strftime("%Y-%m-%d")

        if not db_pool:
            return

        async with db_pool.acquire() as conn:
            # 오늘 거래 실적
            trades = await conn.fetch("""
                SELECT symbol, side, price, quantity, amount, pnl, strategy, created_at
                FROM trade_history
                WHERE DATE(created_at AT TIME ZONE 'Asia/Seoul') = $1
                  AND asset_type = 'stock'
                ORDER BY created_at DESC
            """, now_kst.date())

            # watchlist 현황
            wl_count = await conn.fetchval("SELECT COUNT(*) FROM watchlist WHERE is_active=TRUE")

            # 보유 포지션 (trades 기반 집계)
            positions = await conn.fetch("""
                SELECT symbol,
                       SUM(CASE WHEN side='BUY' THEN quantity ELSE -quantity END) as net_qty,
                       AVG(CASE WHEN side='BUY' THEN price END) as avg_buy
                FROM trade_history
                WHERE asset_type='stock'
                GROUP BY symbol
                HAVING SUM(CASE WHEN side='BUY' THEN quantity ELSE -quantity END) > 0
            """)

        # 오늘 거래 요약
        buy_trades = [t for t in trades if t["side"] == "BUY"]
        sell_trades = [t for t in trades if t["side"] == "SELL"]
        total_pnl = sum(float(t["pnl"] or 0) for t in sell_trades)

        msg = f"📊 Jarvis 일일 결산 [{now_kst.strftime('%m/%d')}]\n"
        msg += f"{'='*25}\n"

        if trades:
            msg += f"매수 {len(buy_trades)}건 / 매도 {len(sell_trades)}건\n"
            if sell_trades:
                pnl_emoji = "📈" if total_pnl >= 0 else "📉"
                msg += f"{pnl_emoji} 실현손익: {total_pnl:+,.0f}원\n"
            if buy_trades:
                buy_list = "\n".join([f"  🟢 {t['symbol']} {int(t['price']):,}원×{int(t['quantity'])}주" 
                                       for t in buy_trades[:5]])
                msg += f"신규 매수:\n{buy_list}\n"
        else:
            msg += "오늘 거래 없음\n"

        msg += f"\n📋 watchlist: {wl_count}종목"
        if positions:
            msg += f" | 보유: {len(positions)}종목"

        # 내일 전략 방향
        msg += f"\n\n🔮 내일 전략 [{(now_kst + timedelta(days=1)).strftime('%m/%d')}]\n"
        msg += f"• 08:30 전종목 스캔 (RSI+모멘텀+거래량)\n"
        msg += f"• ML 재학습 완료 종목 우선 매매\n"
        msg += f"• 손절 -2% / MA 데드크로스 매도 유지\n"
        msg += f"• 최대 보유 10종목 제한\n"
        msg += f"\n✅ ML 자동 학습 진행 중..."

        await _send_telegram(msg)
        logger.info("✅ Jarvis 마감 리포트 전송 완료")

    except Exception as e:
        logger.error(f"Jarvis 마감 리포트 실패: {e}")


async def _get_jarvis_lessons(limit: int = 5) -> str:
    """최근 복기 교훈 로드 (아침 작전 수립용)"""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS jarvis_notes (
                    id SERIAL PRIMARY KEY,
                    category VARCHAR(30) DEFAULT 'note',
                    content TEXT NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )""")
            rows = await conn.fetch(
                "SELECT content FROM jarvis_notes WHERE category='lesson' ORDER BY created_at DESC LIMIT $1",
                limit)
        return "\n".join(f"- {r['content']}" for r in rows) if rows else "(아직 없음)"
    except Exception:
        return "(로드 실패)"


async def _jarvis_daily_plan():
    """아침 작전 수립 → Redis 캐시 (장중 빠른 판단의 컨텍스트 1장)"""
    try:
        async with db_pool.acquire() as conn:
            wl = await conn.fetch(
                "SELECT symbol, name, reason FROM watchlist WHERE is_active=TRUE LIMIT 20")
        wl_txt = "\n".join(f"- {r['name']}({r['symbol']}): {r['reason'] or ''}" for r in wl) or "(없음)"
        lessons = await _get_jarvis_lessons()
        directives_txt = await _get_active_directives()
        now_str = datetime.now(KST).strftime("%m/%d")

        prompt = f"""너는 한국 주식 단타 전문 트레이더다. 오늘({now_str}) 장중 매매 작전을 수립하라.

[오늘의 감시종목]
{wl_txt}

[최근 복기 교훈]
{lessons}

[주인 지시사항 — 작전에 반드시 반영]
{directives_txt or '(없음)'}


[매매 규칙 — 반드시 준수]
- 재매수 금지: 손절한 종목은 5일간 재매수 불가(2회 손절 시 14일). 단, 손절가 +5% 위 신호는 추세전환으로 허용
- 당일 2회 손절 발생 시 그날 신규 매수 전면 중단
- 신호 등급: 강한 복합 신호(골든크로스+거래량급증+수급양호)는 스윙 관점 허용, 약한 신호는 단타로 짧게
- 하락장(코스피 -1.5%↓)에서는 강한 신호만 매수

아래 형식으로 500자 이내 '오늘의 작전'을 작성하라. 장중 매수/매도 판단 시 이 작전만 보고 즉시 결정한다.
1. 시장 스탠스: (공격/중립/보수 중 하나와 한줄 이유)
2. 우선 종목: (감시종목 중 주목할 2~3개와 이유 한줄씩)
3. 회피 조건: (오늘 매수를 피할 상황)
4. 리스크 한도: (연속 손절 시 대응)"""

        plan = await _ask_openwebui(prompt, session_id="daily_plan")
        if plan and not plan.startswith("❌"):
            await redis_client.setex("jarvis:daily_plan", 60 * 60 * 12, plan)
            logger.info("🧭 오늘의 작전 캐시 완료")
            await _send_telegram(f"🧭 자비스 오늘의 작전 [{now_str}]\n{plan[:900]}")
        else:
            logger.warning(f"작전 수립 실패(AI 응답 불가): {str(plan)[:100]}")
    except Exception as e:
        logger.error(f"작전 수립 오류: {e}")


async def _score_journal() -> str:
    """오늘의 판단(SKIP/EXECUTE)을 당일 종가로 채점 → 요약 반환"""
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, symbol, name, action, jarvis_decision, price
                FROM trade_journal
                WHERE DATE(ts AT TIME ZONE 'Asia/Seoul') = (NOW() AT TIME ZONE 'Asia/Seoul')::date
                  AND bot='stock_trader' AND eval_at IS NULL AND price > 0
            """)
        if not rows:
            # 오늘 판단 자체가 0건인지 확인 → 침묵 대신 명시 보고
            async with db_pool.acquire() as conn:
                total_today = await conn.fetchval("""
                    SELECT COUNT(*) FROM trade_journal
                    WHERE DATE(ts AT TIME ZONE 'Asia/Seoul') = (NOW() AT TIME ZONE 'Asia/Seoul')::date
                      AND bot='stock_trader'""")
            if not total_today:
                await _send_telegram("📝 오늘 판단 채점: 기록 0건\n(전략 신호 미발생 — 매수 시도 자체가 없었음)")
            return ""
        token = await get_kis_token()
        if not token:
            return ""
        import ssl as _ssl
        _c = _ssl.create_default_context(); _c.check_hostname = False; _c.verify_mode = _ssl.CERT_NONE
        scored = {"exec_hit": 0, "exec_miss": 0, "skip_good": 0, "skip_missed": 0}
        lines = []
        async with _aiohttp.ClientSession(connector=_aiohttp.TCPConnector(ssl=_c)) as sess:
            # 종목별 종가 1회 조회
            closes = {}
            for sym in {r["symbol"] for r in rows}:
                try:
                    pr = await sess.get(
                        f"{config.kis_base_url}/uapi/domestic-stock/v1/quotations/inquire-price",
                        headers={"authorization": f"Bearer {token}", "appkey": config.kis_app_key,
                                 "appsecret": config.kis_app_secret,
                                 "tr_id": "FHKST01010100", "custtype": "P"},
                        params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": sym},
                        timeout=_aiohttp.ClientTimeout(total=8))
                    o = (await pr.json()).get("output", {})
                    closes[sym] = int(o.get("stck_prpr", 0) or 0)
                except Exception:
                    closes[sym] = 0
                await asyncio.sleep(0.3)
        async with db_pool.acquire() as conn:
            for r in rows:
                close = closes.get(r["symbol"], 0)
                if close <= 0:
                    continue
                rate = (close - float(r["price"])) / float(r["price"]) * 100
                await conn.execute("""
                    UPDATE trade_journal SET eval_price=$1, eval_pnl_rate=$2, eval_at=NOW()
                    WHERE id=$3
                """, float(close), round(rate, 2), r["id"])
                dec = r["jarvis_decision"]
                nm = r["name"] or r["symbol"]
                if dec in ("EXECUTE", "EXECUTE_SMALL") and r["action"] == "buy":
                    if rate >= 0.5: scored["exec_hit"] += 1; tag = "✅적중"
                    else: scored["exec_miss"] += 1; tag = "❌빗나감"
                    lines.append(f"매수 {nm}: 신호가 대비 {rate:+.1f}% {tag}")
                elif dec == "SKIP" and r["action"] == "buy":
                    if rate >= 1.0: scored["skip_missed"] += 1; tag = "⚠️기회놓침"
                    else: scored["skip_good"] += 1; tag = "✅잘거름"
                    lines.append(f"SKIP {nm}: 이후 {rate:+.1f}% {tag}")
        total = sum(scored.values())
        if total == 0:
            return ""
        summary = (f"📝 오늘 판단 채점 ({total}건)\n"
                   f"매수 적중 {scored['exec_hit']} / 빗나감 {scored['exec_miss']}\n"
                   f"SKIP 잘거름 {scored['skip_good']} / 기회놓침 {scored['skip_missed']}\n"
                   + "\n".join(lines[:8]))
        try:
            await redis_client.setex("jarvis:score_today", 3600 * 6, summary)
        except Exception:
            pass
        await _send_telegram(summary)
        logger.info("📝 판단 채점 완료: %s건", total)
        return summary
    except Exception as e:
        logger.error(f"판단 채점 오류: {e}")
        return ""


async def _score_then_review():
    """마감 후: 채점 → 복기 (채점 결과를 복기에 반영)"""
    await _score_journal()
    await _jarvis_evening_review()


async def _jarvis_evening_review():
    """저녁 복기 → 교훈을 jarvis_memory에 저장 (내일 작전에 반영)"""
    try:
        today = datetime.now(KST).date()
        async with db_pool.acquire() as conn:
            trades = await conn.fetch("""
                SELECT symbol, side, amount, pnl, strategy
                FROM trade_history
                WHERE bot='stock_trader'
                  AND DATE(created_at AT TIME ZONE 'Asia/Seoul') = $1
                ORDER BY created_at""", today)
        if not trades:
            logger.info("🌙 복기: 오늘 매매 없음 — 스킵")
            return
        t_txt = "\n".join(
            f"- {t['side']} {t['symbol']} {float(t['amount']):,.0f}원"
            + (f" 손익 {float(t['pnl'] or 0):+,.0f}원" if t['pnl'] is not None else "")
            + f" ({t['strategy']})" for t in trades)
        total_pnl = sum(float(t['pnl'] or 0) for t in trades)
        plan = ""
        try:
            cached = await redis_client.get("jarvis:daily_plan")
            plan = (cached if isinstance(cached, str) else (cached or b"").decode())[:500]
        except Exception:
            pass

        score_txt = ""
        try:
            sc = await redis_client.get("jarvis:score_today")
            score_txt = sc if isinstance(sc, str) else (sc or b"").decode()
        except Exception:
            pass

        prompt = f"""너는 한국 주식 단타 트레이더다. 오늘 매매를 복기하라.

[아침 작전]
{plan or '(없음)'}

[오늘 매매 기록] (총 손익 {total_pnl:+,.0f}원)
{t_txt}

[오늘 판단 채점표]
{score_txt or '(채점 없음)'}

내일 매매에 반영할 핵심 교훈을 딱 1~2줄로 작성하라. 형식: "교훈: ..." """

        review = await _ask_openwebui(prompt, session_id="daily_plan")
        if review and not review.startswith("❌"):
            lesson = review.strip()[:300]
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO jarvis_notes (category, content) VALUES ('lesson', $1)", lesson)
            await _send_telegram(f"🌙 자비스 복기\n{lesson}")
            logger.info("🌙 복기 교훈 저장 완료")
    except Exception as e:
        logger.error(f"복기 오류: {e}")


async def _jarvis_unified_daily_report():
    """관리자 자비스 통합 일일보고: 코인봇+주식봇 보고 종합 → 텔레그램"""
    try:
        today = datetime.now(KST).date()
        async with db_pool.acquire() as conn:
            # 두 봇의 오늘 매매 보고
            stock_trades = await conn.fetch("""
                SELECT symbol, side, amount, pnl, strategy FROM trade_history
                WHERE bot='stock_trader' AND DATE(created_at AT TIME ZONE 'Asia/Seoul')=$1
                ORDER BY created_at""", today)
            crypto_trades = await conn.fetch("""
                SELECT symbol, side, amount, pnl, strategy FROM trade_history
                WHERE bot='crypto_trader' AND DATE(created_at AT TIME ZONE 'Asia/Seoul')=$1
                ORDER BY created_at""", today)
            journal = await conn.fetchrow("""
                SELECT COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE jarvis_decision IN ('EXECUTE','EXECUTE_SMALL')) AS ex,
                       COUNT(*) FILTER (WHERE jarvis_decision='SKIP') AS sk
                FROM trade_journal
                WHERE DATE(ts AT TIME ZONE 'Asia/Seoul')=$1""", today)

        def _fmt(trades):
            if not trades:
                return "매매 없음", 0.0
            pnl = sum(float(t["pnl"] or 0) for t in trades)
            buys = sum(1 for t in trades if t["side"] == "BUY")
            sells = len(trades) - buys
            lines = "\n".join(
                f"  · {t['side']} {t['symbol'].replace('KRW-','')} "
                f"{float(t['amount']):,.0f}원"
                + (f" ({float(t['pnl']):+,.0f}원)" if t["pnl"] is not None else "")
                for t in trades[:6])
            more = f"\n  ...외 {len(trades)-6}건" if len(trades) > 6 else ""
            return f"매수 {buys} / 매도 {sells} (손익 {pnl:+,.0f}원)\n{lines}{more}", pnl

        stock_txt, stock_pnl = _fmt(stock_trades)
        crypto_txt, crypto_pnl = _fmt(crypto_trades)
        total_pnl = stock_pnl + crypto_pnl

        raw = (f"[주식봇 보고]\n{stock_txt}\n\n"
               f"[코인봇 보고]\n{crypto_txt}\n\n"
               f"[자비스 판단 활동] 판단 {journal['total']}건 (실행 {journal['ex']} / 보류 {journal['sk']})\n"
               f"[오늘 총 손익] {total_pnl:+,.0f}원")

        # 자비스 총평 (AI 1회)
        comment = ""
        try:
            reply = await _ask_openwebui(
                f"너는 트레이딩 시스템 총괄 관리자다. 아래 두 봇의 오늘 보고를 보고 "
                f"주인에게 전할 총평을 2~3문장으로 작성하라. 솔직하고 간결하게.\n\n{raw}",
                session_id="daily_report")
            if reply and not reply.startswith("❌"):
                comment = f"\n\n💬 자비스 총평:\n{reply.strip()[:400]}"
        except Exception:
            pass

        msg = (f"📊 <b>자비스 일일보고</b> [{today.strftime('%m/%d')}]\n\n{raw}{comment}")
        await _send_telegram(msg)
        logger.info("📊 통합 일일보고 발송 완료")
    except Exception as e:
        logger.error(f"통합 일일보고 오류: {e}")


@app.api_route("/api/jarvis/daily-report/run", methods=["GET", "POST"])
async def run_daily_report_now():
    try:
        await _jarvis_unified_daily_report()
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def _intraday_scan():
    """장중 감시종목 보충: 그 시점 거래량 상위에서 조건 통과 종목 추가
    (아침 종목 유지, 신규만 추가 — 기준 동일: score≥4, 스팩/칼날 제외)"""
    try:
        now_kst = datetime.now(KST)
        logger.info("🔍 장중 보충 스캔 시작 [%s]", now_kst.strftime("%H:%M"))
        candidates = await _kis_scan_candidates()
        if not candidates:
            logger.info("🔍 장중 스캔: 신규 후보 없음")
            return
        added = []
        async with db_pool.acquire() as conn:
            existing = {r["symbol"] for r in await conn.fetch(
                "SELECT symbol FROM watchlist WHERE is_active=TRUE")}
            for c in candidates:
                if c["symbol"] in existing:
                    continue
                reason = (f"[장중{now_kst.strftime('%H:%M')}] 거래량{c['vol_ratio']:.1f}배 "
                          f"등락{c['change']:+.1f}% RSI{c.get('rsi',50):.0f} score{c['score']}")
                await conn.execute("""
                    INSERT INTO watchlist (symbol, name, added_by, reason, is_active)
                    VALUES ($1, $2, 'jarvis_scanner', $3, TRUE)
                    ON CONFLICT (symbol) DO UPDATE
                    SET is_active=TRUE, added_by='jarvis_scanner', reason=$3, updated_at=NOW()
                """, c["symbol"], c["name"], reason)
                added.append(f"· {c['name']}({c['symbol']}) {c['close']:,}원 {c['change']:+.1f}%")
        if added:
            await _send_telegram(
                f"🔍 장중 보충 스캔 [{now_kst.strftime('%H:%M')}]\n"
                f"신규 감시 {len(added)}종목:\n" + "\n".join(added[:8]))
            logger.info("🔍 장중 스캔: %d종목 추가", len(added))
        else:
            logger.info("🔍 장중 스캔: 전부 기존 감시 중")
    except Exception as e:
        logger.error(f"장중 스캔 오류: {e}")


@app.api_route("/api/scan/intraday", methods=["GET", "POST"])
async def run_intraday_scan_now():
    """장중 보충 스캔 수동 실행"""
    try:
        await _intraday_scan()
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def _jarvis_scheduler():
    """Jarvis 자동 분석 스케줄러 — 08:30 장 시작 전 / 15:40 장 마감 후"""
    import asyncio
    from datetime import time as dtime
    logger.info("🕐 Jarvis 스케줄러 시작")
    last_morning = None
    last_closing = None
    last_daily_report = None
    last_scan_1030 = None
    last_scan_1300 = None

    while True:
        await asyncio.sleep(60)
        from datetime import timezone, timedelta
        KST = timezone(timedelta(hours=9))
        now = datetime.now(KST)
        today = now.date()
        cur_time = now.time().replace(tzinfo=None)

        # 통합 일일보고 (매일 21:00, 주말 포함 — 코인 반영)
        if dtime(21, 0) <= cur_time <= dtime(21, 5) and last_daily_report != today:
            last_daily_report = today
            asyncio.create_task(_jarvis_unified_daily_report())
            asyncio.create_task(_summarize_old_chats())  # 장기 기억 이관 (하루 1일치)

        if now.weekday() >= 5:
            continue

        # 장중 보충 스캔 (10:30 / 13:00) — 새 거래량 상위 종목 감시 추가
        if dtime(10, 30) <= cur_time <= dtime(10, 35) and last_scan_1030 != today:
            last_scan_1030 = today
            asyncio.create_task(_intraday_scan())
        if dtime(13, 0) <= cur_time <= dtime(13, 5) and last_scan_1300 != today:
            last_scan_1300 = today
            asyncio.create_task(_intraday_scan())

        if dtime(8, 30) <= cur_time <= dtime(8, 35) and last_morning != today:
            last_morning = today
            logger.info("🌅 Jarvis 장 시작 전 루틴")
            await _jarvis_stock_scanner()        # 1. 전종목 스캔 → watchlist 업데이트
            await _jarvis_auto_analysis()         # 2. watchlist ML 예측
            await _jarvis_daily_plan()            # 3. 오늘의 작전 수립 → 캐시
            asyncio.create_task(_manual_collect())  # 4. 뉴스 감성 수집
            asyncio.create_task(_trigger_ohlcv_collect())  # 5. OHLCV 수집 트리거

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
            asyncio.create_task(_jarvis_closing_report())  # 마감 리포트 + 내일 전략
            asyncio.create_task(_score_then_review())  # 채점 → 복기 → 교훈 저장


@app.on_event("shutdown")
async def shutdown():
    if db_pool:
        await db_pool.close()
    if redis_client:
        await redis_client.close()


# ── 정적 파일 ───────────────────────────────────────────
app.mount("/static", StaticFiles(directory="static"), name="static")


async def _cleanup_scanner_watchlist() -> int:
    """스캐너가 넣은 종목 중 보유하지 않은 것 비활성화. 반환: 해제 수"""
    held = set()
    try:
        pos_res = await get_stock_positions()
        if pos_res.get("success"):
            held = {p["symbol"] for p in pos_res.get("data", []) if p.get("symbol")}
    except Exception:
        pass
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT symbol FROM watchlist WHERE is_active=TRUE AND added_by='jarvis_scanner'"
        )
        n = 0
        for r in rows:
            if r["symbol"] not in held:
                await conn.execute(
                    "UPDATE watchlist SET is_active=FALSE, updated_at=NOW() WHERE symbol=$1", r["symbol"]
                )
                n += 1
    return n


@app.api_route("/api/jarvis/plan/run", methods=["GET", "POST"])
async def run_daily_plan_now():
    """오늘의 작전 수동 수립 (점심 테스트용)"""
    try:
        await _jarvis_daily_plan()
        cached = await redis_client.get("jarvis:daily_plan")
        plan = cached if isinstance(cached, str) else (cached or b"").decode()
        return {"success": bool(plan), "plan": plan[:1500]}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.api_route("/api/journal/score/run", methods=["GET", "POST"])
async def run_score_now():
    """오늘 판단 채점 수동 실행"""
    try:
        summary = await _score_journal()
        return {"success": bool(summary), "summary": summary or "채점 대상 없음"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.api_route("/api/jarvis/review/run", methods=["GET", "POST"])
async def run_review_now():
    """복기 수동 실행"""
    try:
        await _jarvis_evening_review()
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.api_route("/api/watchlist/cleanup", methods=["GET", "POST"])
async def watchlist_cleanup():
    """잔재 감시종목 즉시 정리 (보유 종목 제외)"""
    try:
        n = await _cleanup_scanner_watchlist()
        async with db_pool.acquire() as conn:
            remain = await conn.fetchval("SELECT COUNT(*) FROM watchlist WHERE is_active=TRUE")
        await _send_telegram(f"🧹 감시종목 정리: {n}개 해제, 활성 {remain}개 남음")
        return {"success": True, "deactivated": n, "remaining": remain}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.api_route("/api/scan/run", methods=["GET", "POST"])
async def run_scan_now():
    """수동 스캔 트리거 — 08:30 안 기다리고 즉시 실행"""
    try:
        await _jarvis_stock_scanner()
        # 결과 조회
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT symbol, name, reason FROM watchlist WHERE is_active=TRUE ORDER BY updated_at DESC LIMIT 30"
            )
        # 텔레그램 결과 전송 (수동 스캔용)
        try:
            now_str = datetime.now(KST).strftime("%m/%d %H:%M")
            if rows:
                msg = f"📋 수동 스캔 결과 [{now_str}]\n감시종목 {len(rows)}개:\n"
                for r in rows[:15]:
                    msg += f"· {r['name']}({r['symbol']})\n"
                if len(rows) > 15:
                    msg += f"...외 {len(rows)-15}개"
            else:
                msg = f"📋 수동 스캔 결과 [{now_str}]\n감시종목 없음"
            await _send_telegram(msg)
        except Exception as te:
            logger.warning(f"수동 스캔 텔레그램 전송 실패: {te}")
        return {"success": True, "count": len(rows),
                "watchlist": [{"symbol": r["symbol"], "name": r["name"], "reason": r["reason"]} for r in rows]}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/", response_class=HTMLResponse)
async def root():
    with open("static/home.html", encoding="utf-8") as f:
        return f.read()

@app.get("/home", response_class=HTMLResponse)
async def home_page():
    with open("static/home.html", encoding="utf-8") as f:
        return f.read()

@app.get("/jarvis", response_class=HTMLResponse)
async def jarvis_page():
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
    """모든 종목 최신 ML 예측 (종목명 포함)"""
    try:
        from ml.model import MLModelManager
        manager = MLModelManager(db_pool)
        predictions = await manager.get_all_predictions()

        # watchlist에서 종목명 매핑
        try:
            async with db_pool.acquire() as conn:
                wl = await conn.fetch("SELECT symbol, name FROM watchlist WHERE is_active=TRUE")
            name_map = {r["symbol"]: r["name"] for r in wl}
            for p in predictions:
                if not p.get("name"):
                    p["name"] = name_map.get(p["symbol"], p["symbol"])
        except Exception:
            pass

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


@app.get("/api/price/{symbol}")
async def get_single_price(symbol: str):
    """단일 종목/코인 실시간 시세 조회 (Jarvis Tool용)"""
    try:
        symbol = symbol.upper()

        # 코인 (KRW- 포함)
        if "KRW-" in symbol or symbol in ["BTC","ETH","XRP","SOL","ADA","DOGE"]:
            pair = symbol if "KRW-" in symbol else f"KRW-{symbol}"
            cached = await redis_client.get("crypto:prices")
            if cached:
                prices = json.loads(cached)
                if pair in prices:
                    p = prices[pair]
                    return {
                        "success": True,
                        "symbol": pair,
                        "price": p["price"],
                        "change_rate": p.get("change_rate", 0),
                        "type": "crypto"
                    }

        # 주식 - KIS API 직접 호출 (stock_trader 모듈 없이)
        import aiohttp as http
        base_url = config.kis_base_url

        # 토큰 발급
        async with http.ClientSession() as sess:
            token_res = await sess.post(f"{base_url}/oauth2/tokenP", json={
                "grant_type": "client_credentials",
                "appkey": config.KIS_APP_KEY,
                "appsecret": config.KIS_APP_SECRET,
            })
            token_data = await token_res.json()
            token = token_data.get("access_token", "")

            if not token:
                return {"success": False, "error": "KIS 토큰 발급 실패"}

            # 현재가 조회
            headers = {
                "authorization": f"Bearer {token}",
                "appkey": config.KIS_APP_KEY,
                "appsecret": config.KIS_APP_SECRET,
                "tr_id": "FHKST01010100",
                "custtype": "P",
            }
            params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}
            price_res = await sess.get(
                f"{base_url}/uapi/domestic-stock/v1/quotations/inquire-price",
                headers=headers, params=params
            )
            data = await price_res.json()
            output = data.get("output", {})
            price = int(output.get("stck_prpr", 0))
            change_rate = float(output.get("prdy_ctrt", 0))
            name = output.get("hts_kor_isnm", symbol)

        if price > 0:
            return {
                "success": True,
                "symbol": symbol,
                "name": name,
                "price": price,
                "change_rate": change_rate,
                "type": "stock"
            }

        return {"success": False, "error": "시세 조회 실패"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/market/index")
async def get_market_index():
    """코스피/코스닥 지수 조회 (KIS API)"""
    try:
        import aiohttp as http
        import ssl

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        connector = http.TCPConnector(ssl=ssl_ctx)

        # 지수 조회는 실전 API만 가능 (모의투자 불가)
        real_url = "https://openapi.koreainvestment.com:9443"
        real_key = config.KIS_APP_KEY
        real_secret = config.KIS_APP_SECRET

        async with http.ClientSession(connector=connector) as sess:
            # 실전 토큰 Redis 캐시 우선
            token = ""
            try:
                cached = await redis_client.get("kis:real_token")
                if cached:
                    token = cached if isinstance(cached, str) else cached.decode()
            except:
                pass

            if not token:
                res = await sess.post(f"{real_url}/oauth2/tokenP", json={
                    "grant_type": "client_credentials",
                    "appkey": real_key,
                    "appsecret": real_secret,
                })
                token = (await res.json()).get("access_token", "")
                if token:
                    try:
                        await redis_client.setex("kis:real_token", 82800, token)
                    except:
                        pass
            if not token:
                return {"success": False, "error": "토큰 발급 실패"}

            headers = {
                "authorization": f"Bearer {token}",
                "appkey": real_key,
                "appsecret": real_secret,
                "tr_id": "FHPUP02100000",
                "custtype": "P",
            }

            result = {}
            for code, name in [("0001", "KOSPI"), ("1001", "KOSDAQ")]:
                params = {
                    "FID_COND_MRKT_DIV_CODE": "U",
                    "FID_INPUT_ISCD": code,
                }
                r = await sess.get(
                    f"{real_url}/uapi/domestic-stock/v1/quotations/inquire-index-price",
                    headers=headers, params=params
                )
                data = await r.json()
                output = data.get("output", {})
                price = float(output.get("bstp_nmix_prpr", 0) or 0)
                change = float(output.get("bstp_nmix_prdy_ctrt", 0) or 0)
                if price > 0:
                    result[name] = {
                        "price": round(price, 2),
                        "change_rate": round(change, 2),
                        "change": float(output.get("bstp_nmix_prdy_vrss", 0) or 0),
                    }

        return {"success": True, "data": result}
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


COIN_NAMES = {'KRW-BTC': '비트코인', 'KRW-ETH': '이더리움', 'KRW-SOL': '솔라나', 'KRW-XRP': '리플', 'KRW-ADA': '에이다', 'KRW-DOGE': '도지코인', 'KRW-AVAX': '아발란체', 'KRW-DOT': '폴카닷', 'KRW-MATIC': '폴리곤', 'KRW-LINK': '체인링크', 'KRW-SUI': '수이', 'KRW-TRX': '트론', 'KRW-SHIB': '시바이누', 'KRW-ARB': '아비트럼', 'KRW-OP': '옵티미즘', 'KRW-NEAR': '니어', 'KRW-APT': '앱토스', 'KRW-FIL': '파일코인', 'KRW-SAND': '샌드박스', 'KRW-AXS': '엑시인피니티'}

@app.get("/api/prices/crypto")
async def get_crypto_prices():
    """코인 실시간 시세 (Redis) + 한글명"""
    try:
        import json as _json
        val = await redis_client.get("crypto:prices")
        if val:
            data = _json.loads(val)
        else:
            data = {}
            for pair in config.CRYPTO_PAIRS:
                v = await redis_client.get(f"crypto:price:{pair}")
                if v:
                    data[pair] = _json.loads(v)

        # 한글명 추가
        for pair in data:
            data[pair]["name"] = COIN_NAMES.get(pair, pair.replace("KRW-", ""))

        return {"success": True, "data": data}
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
        from datetime import timezone, timedelta
        KST = timezone(timedelta(hours=9))
        today = datetime.now(KST).date()
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



@app.get("/api/crypto/trade-mode")
async def get_trade_mode():
    mode = await redis_client.get("crypto:trade_mode")
    return {"mode": mode.decode() if mode else "scalping"}

@app.post("/api/crypto/trade-mode")
async def set_trade_mode(request: Request):
    body = await request.json()
    mode = body.get("mode", "scalping")
    if mode not in ["scalping", "swing"]:
        return {"success": False, "error": "모드는 scalping 또는 swing"}
    await redis_client.set("crypto:trade_mode", mode)
    return {"success": True, "mode": mode, "message": f"{'단타' if mode=='scalping' else '스윙'} 모드로 전환!"}

@app.get("/api/health")
async def health():
    return {"status": "ok", "ts": datetime.now().isoformat()}


@app.get("/api/health/full")
async def full_health_check():
    """전체 시스템 상태 점검"""
    import json as _json
    from datetime import timezone, timedelta
    KST = timezone(timedelta(hours=9))
    now = datetime.now(KST)
    result = {"timestamp": now.isoformat(), "checks": {}}

    # 1. DB
    try:
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        result["checks"]["database"] = {"status": "ok"}
    except Exception as e:
        result["checks"]["database"] = {"status": "error", "error": str(e)}

    # 2. Redis
    try:
        await redis_client.ping()
        result["checks"]["redis"] = {"status": "ok"}
    except Exception as e:
        result["checks"]["redis"] = {"status": "error", "error": str(e)}

    # 3. stock-trader
    try:
        stk = await redis_client.hget("bot:status", "stock_trader")
        if stk:
            stk_data = _json.loads(stk)
            last = stk_data.get("last_cycle", "")
            diff = 9999
            if last:
                try:
                    last_dt = datetime.fromisoformat(last)
                    diff = (now - last_dt.astimezone(KST)).total_seconds()
                except: pass
            status = "ok" if diff < 120 else "warning" if diff < 300 else "error"
            result["checks"]["stock_trader"] = {
                "status": status, "seconds_ago": int(diff),
                "positions": stk_data.get("positions", 0),
                "strategy": stk_data.get("strategy", "-"),
            }
        else:
            result["checks"]["stock_trader"] = {"status": "error", "error": "응답없음"}
    except Exception as e:
        result["checks"]["stock_trader"] = {"status": "error", "error": str(e)}

    # 4. crypto-trader
    try:
        cry = await redis_client.hget("bot:status", "crypto_trader")
        if cry:
            cry_data = _json.loads(cry)
            last = cry_data.get("last_cycle", "")
            diff = 9999
            if last:
                try:
                    last_dt = datetime.fromisoformat(last)
                    diff = (now - last_dt.astimezone(KST)).total_seconds()
                except: pass
            status = "ok" if diff < 120 else "warning" if diff < 300 else "error"
            result["checks"]["crypto_trader"] = {
                "status": status, "seconds_ago": int(diff),
                "positions": cry_data.get("positions", 0),
                "krw_balance": round(cry_data.get("krw_balance", 0)),
            }
        else:
            result["checks"]["crypto_trader"] = {"status": "error", "error": "응답없음"}
    except Exception as e:
        result["checks"]["crypto_trader"] = {"status": "error", "error": str(e)}

    # 5. 주식 계좌
    try:
        pos_data = await get_stock_positions()
        acct = pos_data.get("account", {})
        result["checks"]["stock_account"] = {
            "status": "ok",
            "total_eval": acct.get("total_eval", 0),
            "cash": acct.get("cash", 0),
            "pnl": acct.get("pnl", 0),
            "pnl_rate": acct.get("pnl_rate", 0),
            "positions": len(pos_data.get("data", [])),
        }
    except Exception as e:
        result["checks"]["stock_account"] = {"status": "error", "error": str(e)}

    # 6. 코인 계좌
    try:
        krw = await redis_client.get("crypto:krw_balance")
        pos_raw = await redis_client.get("crypto:positions")
        positions = _json.loads(pos_raw) if pos_raw else []
        result["checks"]["crypto_account"] = {
            "status": "ok",
            "krw_balance": round(float(krw)) if krw else 0,
            "positions": len(positions),
            "coins": [p.get("symbol","") for p in positions],
        }
    except Exception as e:
        result["checks"]["crypto_account"] = {"status": "error", "error": str(e)}

    # 7. 오늘 매매
    try:
        async with db_pool.acquire() as conn:
            trades = await conn.fetch("""
                SELECT bot, side, symbol, amount, pnl, created_at
                FROM trade_history
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                ORDER BY created_at DESC LIMIT 20
            """)
        total_pnl = sum(float(t["pnl"] or 0) for t in trades)
        result["checks"]["today_trades"] = {
            "status": "ok",
            "count": len(trades),
            "total_pnl": round(total_pnl, 2),
            "recent": [{"bot": t["bot"], "side": t["side"],
                        "symbol": t["symbol"],
                        "amount": round(float(t["amount"] or 0)),
                        "pnl": round(float(t["pnl"] or 0), 2)}
                       for t in trades[:5]],
        }
    except Exception as e:
        result["checks"]["today_trades"] = {"status": "error", "error": str(e)}

    # 8. KIS 토큰
    try:
        paper = await redis_client.get("kis:paper_token")
        real = await redis_client.get("kis:real_token")
        result["checks"]["kis_tokens"] = {
            "status": "ok" if paper else "warning",
            "paper_token": "있음" if paper else "없음",
            "real_token": "있음" if real else "없음",
        }
    except Exception as e:
        result["checks"]["kis_tokens"] = {"status": "error", "error": str(e)}

    # 전체 상태
    statuses = [v.get("status") for v in result["checks"].values()]
    result["overall"] = "ok" if all(s=="ok" for s in statuses) else                         "error" if any(s=="error" for s in statuses) else "warning"
    return result


@app.get("/api/events")
async def sse_events(request: Request):
    """SSE - 실시간 이벤트 스트림"""
    import asyncio
    q = asyncio.Queue()
    sse_clients.append(q)

    async def generate():
        try:
            yield "data: connected\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=30)
                    yield msg
                except asyncio.TimeoutError:
                    yield ": ping\n\n"  # 연결 유지
        finally:
            if q in sse_clients:
                sse_clients.remove(q)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )


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




@app.post("/api/collect/crypto/reset")
async def reset_crypto_pairs(background_tasks: fastapi.background.BackgroundTasks):
    """코인 TOP 20을 메이저 코인으로 초기화 + OHLCV 수집"""
    MAJOR_PAIRS = [
        "KRW-BTC","KRW-ETH","KRW-XRP","KRW-SOL","KRW-ADA",
        "KRW-DOGE","KRW-AVAX","KRW-LINK","KRW-DOT","KRW-SUI",
        "KRW-TRX","KRW-NEAR","KRW-MATIC","KRW-ARB","KRW-SHIB",
        "KRW-APT","KRW-SAND","KRW-ATOM","KRW-FIL","KRW-AXS"
    ]

    async def _reset_and_collect():
        import aiohttp, json as _json
        # Redis TOP 20 메이저 코인으로 교체
        await redis_client.setex("crypto:top_pairs", 86400, _json.dumps(MAJOR_PAIRS))
        logger.info(f"✅ TOP 20 메이저 코인으로 초기화: {len(MAJOR_PAIRS)}개")

        # OHLCV 수집
        total = 0
        async with aiohttp.ClientSession() as s:
            for pair in MAJOR_PAIRS:
                try:
                    r = await s.get(
                        "https://api.upbit.com/v1/candles/minutes/1",
                        params={"market": pair, "count": 200},
                        timeout=aiohttp.ClientTimeout(total=10)
                    )
                    candles = await r.json()
                    if isinstance(candles, list) and candles:
                        from datetime import datetime as _dt
                        rows = [(pair,
                                 _dt.fromisoformat(c["candle_date_time_kst"]),
                                 c["opening_price"], c["high_price"],
                                 c["low_price"], c["trade_price"],
                                 c["candle_acc_trade_volume"]) for c in candles]
                        async with db_pool.acquire() as conn:
                            await conn.executemany("""
                                INSERT INTO crypto_ohlcv(symbol,ts,open,high,low,close,volume)
                                VALUES($1,$2,$3,$4,$5,$6,$7)
                                ON CONFLICT(symbol,ts) DO NOTHING
                            """, rows)
                        total += len(rows)
                        logger.info(f"✅ {pair}: {len(rows)}개")
                    await asyncio.sleep(0.2)
                except Exception as e:
                    logger.error(f"❌ {pair}: {e}")
        logger.info(f"🎉 완료: {total}개")

    background_tasks.add_task(_reset_and_collect)
    return {"success": True, "message": f"메이저 코인 {len(MAJOR_PAIRS)}개 초기화 + 데이터 수집 시작!"}

@app.post("/api/collect/crypto/ohlcv")
async def collect_crypto_ohlcv(background_tasks: fastapi.background.BackgroundTasks):
    """코인 OHLCV 데이터 수집 (TOP 20 코인 200개 캔들)"""
    async def _collect():
        try:
            import aiohttp, json as _json
            cached = await redis_client.get("crypto:top_pairs")
            pairs = _json.loads(cached) if cached else [
                "KRW-BTC","KRW-ETH","KRW-XRP","KRW-SOL","KRW-ADA",
                "KRW-DOGE","KRW-AVAX","KRW-DOT","KRW-LINK","KRW-SUI",
                "KRW-TRX","KRW-SHIB","KRW-ARB","KRW-NEAR","KRW-MATIC",
            ]
            logger.info(f"📊 코인 OHLCV 수집 시작: {len(pairs)}개")
            total = 0
            async with aiohttp.ClientSession() as s:
                for pair in pairs:
                    try:
                        r = await s.get(
                            "https://api.upbit.com/v1/candles/minutes/1",
                            params={"market": pair, "count": 200},
                            timeout=aiohttp.ClientTimeout(total=10)
                        )
                        candles = await r.json()
                        if isinstance(candles, list) and candles:
                            rows = [(pair, c["candle_date_time_kst"],
                                     c["opening_price"], c["high_price"],
                                     c["low_price"], c["trade_price"],
                                     c["candle_acc_trade_volume"]) for c in candles]
                            async with db_pool.acquire() as conn:
                                await conn.executemany("""
                                    INSERT INTO crypto_ohlcv(pair,ts,open,high,low,close,volume)
                                    VALUES($1,$2,$3,$4,$5,$6,$7)
                                    ON CONFLICT(pair,ts) DO NOTHING
                                """, rows)
                            total += len(rows)
                            logger.info(f"✅ {pair}: {len(rows)}개")
                        await asyncio.sleep(0.2)
                    except Exception as e:
                        logger.error(f"❌ {pair}: {e}")
            logger.info(f"🎉 코인 OHLCV 수집 완료: {total}개")
        except Exception as e:
            logger.error(f"코인 OHLCV 수집 오류: {e}")

    background_tasks.add_task(_collect)
    return {"success": True, "message": "코인 OHLCV 수집 시작! 잠시 후 완료됩니다."}

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

            # 뉴스 감성 분석 (Jarvis 웹 검색 기반 - 실제 뉴스)
            for symbol in symbols[:10]:  # 10종목만
                try:
                    async with db_pool.acquire() as conn:
                        name_row = await conn.fetchrow(
                            "SELECT name FROM watchlist WHERE symbol=$1", symbol
                        )
                    name = name_row["name"] if name_row else symbol

                    prompt = f"""웹 검색으로 {name}({symbol}) 주식의 오늘 최신 뉴스를 찾아서 감성 분석해줘.
실제 뉴스 제목과 내용을 기반으로 분석하고, JSON만 출력해줘. 다른 말은 하지 마.
출력형식: {{"score": 1, "signal": "BUY", "summary": "실제 뉴스 기반 요약 (뉴스 제목 포함)"}}
score는 -2~+2, signal은 BUY/SELL/NEUTRAL"""

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

                        # Jarvis 메모리에 뉴스 분석 결과 저장 (학습용)
                        from datetime import timezone, timedelta
                        KST = timezone(timedelta(hours=9))
                        now_str = datetime.now(KST).strftime("%Y-%m-%d")
                        memory_content = (
                            f"[뉴스분석 {now_str}] {name}({symbol}): {signal} "
                            f"감성점수 {score:+d} - {summary}"
                        )
                        session_id = os.getenv("JARVIS_ANALYST_CHAT_ID", "jarvis_main")
                        await _save_chat_history(session_id, "system", memory_content)

                except Exception as e:
                    logger.error(f"뉴스 수집 실패 [{symbol}]: {e}")
                    continue

        except Exception as e:
            logger.error(f"수동 수집 오류: {e}")

    async def _collect_supply_dart():
        """data-collector에 수급/공시 수집 트리거"""
        try:
            import aiohttp
            collector_url = os.getenv("COLLECTOR_URL", "http://autotrader.railway.internal:8000")
            async with aiohttp.ClientSession() as s:
                await s.post(f"{collector_url}/api/collect/supply",
                             timeout=aiohttp.ClientTimeout(total=10))
            logger.info("✅ data-collector 수급/공시 수집 트리거 완료")
        except Exception as e:
            logger.warning(f"data-collector 수집 트리거 실패: {e}")

    background_tasks.add_task(_collect)
    background_tasks.add_task(_collect_supply_dart)
    return {"success": True, "message": "수집 시작! 뉴스감성 + 수급 + 공시 수집 중..."}


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

## 추가 능력
- 웹 검색으로 날씨, 뉴스, 실시간 정보 조회 가능
- 날씨 물어보면 웹 검색해서 답변
- 주식 뉴스, 경제 지표도 검색 가능

## 매매 신호 판단 규칙
- 신호 판단 시 반드시 EXECUTE 또는 SKIP으로 시작
- EXECUTE 조건: 잔고 충분 + 포지션 미중복 + 시장 흐름 일치
- SKIP 조건: 잔고 부족 / 이미 보유 중 / 시장 흐름 반대
- 판단 이유 한 줄로 간결하게
- 확신 없으면 SKIP 우선
"""

async def _get_watchlist_prices_context() -> str:
    """감시종목 현재가·등락률 (KIS, Redis 30초 캐시)"""
    try:
        cached = await redis_client.get("jarvis:wl_prices")
        if cached:
            return cached if isinstance(cached, str) else cached.decode()
    except Exception:
        pass
    try:
        token = await get_kis_token()
        if not token:
            return ""
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT symbol, name FROM watchlist WHERE is_active=TRUE LIMIT 10")
        if not rows:
            return ""
        import ssl as _ssl
        ctx = _ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = _ssl.CERT_NONE
        lines = []
        async with _aiohttp.ClientSession(connector=_aiohttp.TCPConnector(ssl=ctx)) as sess:
            for r in rows:
                try:
                    pr = await sess.get(
                        f"{config.kis_base_url}/uapi/domestic-stock/v1/quotations/inquire-price",
                        headers={"authorization": f"Bearer {token}", "appkey": config.kis_app_key,
                                 "appsecret": config.kis_app_secret,
                                 "tr_id": "FHKST01010100", "custtype": "P"},
                        params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": r["symbol"]},
                        timeout=_aiohttp.ClientTimeout(total=5))
                    o = (await pr.json()).get("output", {})
                    p = int(o.get("stck_prpr", 0) or 0)
                    cr = float(o.get("prdy_ctrt", 0) or 0)
                    if p > 0:
                        lines.append(f"- {r['name'] or r['symbol']}({r['symbol']}): {p:,}원 ({cr:+.1f}%)")
                except Exception:
                    pass
                await asyncio.sleep(0.25)
        if not lines:
            return ""
        result = "\n[감시종목 현황 (실시간)]\n" + "\n".join(lines)
        try:
            await redis_client.setex("jarvis:wl_prices", 30, result)
        except Exception:
            pass
        return result
    except Exception as e:
        logger.debug(f"감시종목 시세 조회 실패: {e}")
        return ""


async def get_portfolio_context() -> str:
    """현재 포트폴리오 데이터를 Gemini 컨텍스트로 변환"""
    ctx_parts = []
    _now = datetime.now(KST)
    _wd = ["월", "화", "수", "목", "금", "토", "일"][_now.weekday()]
    _t = _now.time()
    if _now.weekday() >= 5:
        _session = "주말 휴장"
    elif _t >= datetime.strptime("09:00", "%H:%M").time() and _t <= datetime.strptime("15:30", "%H:%M").time():
        _session = "한국 주식시장 장중 (개장 상태)"
    elif _t < datetime.strptime("09:00", "%H:%M").time():
        _session = "장 시작 전 (09:00 개장)"
    else:
        _session = "장 마감 후"
    now = f"{_now.strftime('%Y-%m-%d')}({_wd}) {_now.strftime('%H:%M')} KST — {_session}"
    ctx_parts.append(f"[현재 시각: {now}]")

    try:
        # 주식 포지션
        stock_pos = await get_stock_positions()
        if stock_pos.get("success"):
            positions = stock_pos.get("data") or []
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
            else:
                ctx_parts.append("보유종목: 없음 (신규 매수 가능)")
        else:
            ctx_parts.append(f"[주식 계좌 조회 실패: {stock_pos.get('error', '알 수 없음')}]")
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

    # 감시종목 실시간 시세 (30초 캐시)
    try:
        wl_ctx = await _get_watchlist_prices_context()
        if wl_ctx:
            ctx_parts.append(wl_ctx)
    except Exception:
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


async def _kis_stock_order(symbol: str, price: int, qty: int, is_buy: bool,
                            _retry: bool = False) -> dict:
    """KIS 주식 주문 (dashboard 내장) — 토큰 만료 시 1회 자동 재발급·재시도"""
    token = await get_kis_token(force_new=_retry)
    if not token:
        return {"success": False, "error": "KIS 토큰 없음"}
    acct = (config.KIS_ACCOUNT_NO or "").split("-")
    cano = acct[0] if acct else ""
    prdt = acct[1] if len(acct) > 1 else "01"
    if config.KIS_IS_PAPER:
        tr_id = "VTTC0802U" if is_buy else "VTTC0801U"
    else:
        tr_id = "TTTC0802U" if is_buy else "TTTC0801U"
    payload = {"CANO": cano, "ACNT_PRDT_CD": prdt, "PDNO": symbol,
               "ORD_DVSN": "00", "ORD_QTY": str(qty), "ORD_UNPR": str(price)}
    import ssl as _ssl
    _c = _ssl.create_default_context(); _c.check_hostname = False; _c.verify_mode = _ssl.CERT_NONE
    try:
        async with _aiohttp.ClientSession(connector=_aiohttp.TCPConnector(ssl=_c)) as sess:
            async with sess.post(
                f"{config.kis_base_url}/uapi/domestic-stock/v1/trading/order-cash",
                headers={"Content-Type": "application/json",
                         "authorization": f"Bearer {token}", "appkey": config.kis_app_key,
                         "appsecret": config.kis_app_secret, "tr_id": tr_id, "custtype": "P"},
                json=payload, timeout=_aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
        if data.get("rt_cd") == "0":
            return {"success": True, "order_no": data.get("output", {}).get("ODNO")}
        err = data.get("msg1", "주문 실패")
        # 토큰 만료 → 강제 재발급 후 1회 재시도
        if not _retry and ("token" in err.lower() or "만료" in err):
            logger.warning(f"토큰 만료 감지 → 재발급 후 재주문 [{symbol}]")
            return await _kis_stock_order(symbol, price, qty, is_buy, _retry=True)
        return {"success": False, "error": err}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def _resolve_stock_symbol(text: str) -> tuple:
    """메시지에서 종목 식별 → (symbol, name). 실패 시 (None, None)"""
    import re as _re
    m = _re.search(r"\b(\d{6})\b", text)
    if m:
        code = m.group(1)
        try:
            async with db_pool.acquire() as conn:
                nm = await conn.fetchval("SELECT name FROM watchlist WHERE symbol=$1", code)
            return code, (nm or code)
        except Exception:
            return code, code
    # 이름으로 찾기: watchlist → 캐시
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT symbol, name FROM watchlist WHERE name IS NOT NULL")
        for r in rows:
            if r["name"] and r["name"] in text:
                return r["symbol"], r["name"]
    except Exception:
        pass
    try:
        for nm, code in _stock_name_cache.items():
            if nm and nm in text:
                return code, nm
    except Exception:
        pass
    return None, None


async def _handle_trade_command(user_msg: str):
    """채팅에서 '종목 N주 매수/매도' 명령 → 실제 KIS 주문 실행. 해당 없으면 None"""
    import re as _re
    msg = user_msg.strip()
    is_buy = bool(_re.search(r"(매수|사자|사줘|사라)", msg))
    is_sell = bool(_re.search(r"(매도|팔아|팔자|팔아줘)", msg))
    if not (is_buy or is_sell):
        return None
    qty_m = _re.search(r"(\d+)\s*주", msg)
    all_sell = "전량" in msg or "다 팔" in msg
    if not qty_m and not all_sell:
        return None  # 수량 없는 문장은 일반 대화로

    symbol, name = await _resolve_stock_symbol(msg)
    if not symbol:
        return "⚠️ 종목을 특정할 수 없어요. 종목코드 6자리 또는 감시종목 이름으로 다시 지시해주세요. (예: 000660 2주 매수)"

    action = "buy" if is_buy else "sell"
    action_kr = "매수" if is_buy else "매도"

    # 현재가 (검증된 KIS 직접 조회 경로)
    price = 0
    try:
        token = await get_kis_token()
        if token:
            import ssl as _ssl
            _c = _ssl.create_default_context(); _c.check_hostname = False; _c.verify_mode = _ssl.CERT_NONE
            async with _aiohttp.ClientSession(connector=_aiohttp.TCPConnector(ssl=_c)) as sess:
                pr = await sess.get(
                    f"{config.kis_base_url}/uapi/domestic-stock/v1/quotations/inquire-price",
                    headers={"authorization": f"Bearer {token}", "appkey": config.kis_app_key,
                             "appsecret": config.kis_app_secret,
                             "tr_id": "FHKST01010100", "custtype": "P"},
                    params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol},
                    timeout=_aiohttp.ClientTimeout(total=8))
                o = (await pr.json()).get("output", {})
                price = int(o.get("stck_prpr", 0) or 0)
                if o.get("hts_kor_isnm"):
                    name = o.get("hts_kor_isnm")
    except Exception as e:
        logger.warning(f"수동주문 현재가 조회 실패 [{symbol}]: {e}")
    if price <= 0:
        return f"⚠️ {name}({symbol}) 현재가 조회 실패 — 주문 불가"

    # 수량
    if all_sell and not qty_m:
        try:
            pos = await get_stock_positions()
            qty = next((int(p["qty"]) for p in pos.get("data", []) if p["symbol"] == symbol), 0)
        except Exception:
            qty = 0
        if qty <= 0:
            return f"⚠️ {name}({symbol}) 보유 수량이 없어요"
    else:
        qty = int(qty_m.group(1))

    # 실제 주문 (dashboard 내장 함수 — 모듈 의존 없음)
    result = await _kis_stock_order(symbol, price, qty, is_buy)

    if result.get("success"):
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO trade_history (bot,asset_type,symbol,side,price,quantity,amount,strategy)
                    VALUES ('stock_trader','stock',$1,$2,$3,$4,$5,'수동지시')
                """, symbol, action.upper(), float(price), float(qty), float(price * qty))
        except Exception:
            pass
        await _send_telegram(
            f"{'📈' if is_buy else '📉'} <b>{name} {action_kr} 체결 (수동지시)</b>\n"
            f"가격: {price:,}원 × {qty}주 = {price*qty:,}원")
        await _log_journal("stock_trader", symbol, name, action, "수동지시",
                           user_msg[:200], "MANUAL", "사용자 직접 지시",
                           True, True, price, qty, source="chat")
        return (f"✅ [실제 체결] {name}({symbol}) {qty}주 {action_kr} 완료 — "
                f"{price:,}원 × {qty}주 = {price*qty:,}원")
    else:
        return f"❌ {name}({symbol}) {action_kr} 주문 실패: {result.get('error', '알 수 없음')}"


async def _get_active_directives(limit: int = 10) -> str:
    """활성 지시사항 텍스트 (판단·작전 프롬프트 주입용)"""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "ALTER TABLE jarvis_notes ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE")
            rows = await conn.fetch("""
                SELECT id, content FROM jarvis_notes
                WHERE category='directive' AND is_active=TRUE
                ORDER BY created_at DESC LIMIT $1""", limit)
        if not rows:
            return ""
        return "\n".join(f"- (#{r['id']}) {r['content']}" for r in rows)
    except Exception:
        return ""


async def _handle_directive_command(user_msg: str):
    """지시사항 저장/목록/취소. 해당 없으면 None"""
    import re as _re
    msg = user_msg.strip()

    async with db_pool.acquire() as _c:
        await _c.execute("""CREATE TABLE IF NOT EXISTS jarvis_notes (
            id SERIAL PRIMARY KEY, category VARCHAR(30) DEFAULT 'note',
            content TEXT NOT NULL, is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW())""")

    # 목록
    if msg in ("지시 목록", "지시목록", "지시사항 목록", "지시사항"):
        txt = await _get_active_directives(20)
        return f"📌 활성 지시사항:\n{txt}" if txt else "📌 활성 지시사항이 없습니다."

    # 취소: "지시 취소 12" / "지시 삭제 12"
    m = _re.match(r"지시\s*(취소|삭제)\s*#?(\d+)", msg)
    if m:
        did = int(m.group(2))
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE jarvis_notes SET is_active=FALSE WHERE id=$1 AND category='directive'", did)
        return f"🗑️ 지시 #{did} 를 해제했습니다."

    # 저장: "지시: ..." / "지시 ..." / "앞으로 ..." / "내일부터 ..."
    directive = None
    if msg.startswith("지시:"):
        directive = msg[3:].strip()
    elif msg.startswith("지시 ") and len(msg) > 4:
        directive = msg[3:].strip()
    elif msg.startswith(("앞으로 ", "내일부터 ", "오늘부터 ")):
        directive = msg
    if directive and len(directive) >= 4:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "ALTER TABLE jarvis_notes ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE")
            did = await conn.fetchval(
                "INSERT INTO jarvis_notes (category, content, is_active) VALUES ('directive', $1, TRUE) RETURNING id",
                directive[:300])
        return (f"📌 지시 #{did} 저장 완료 — 다음 매매 판단부터 즉시 반영됩니다.\n"
                f"\"{directive[:100]}\"\n(해제: '지시 취소 {did}')")
    return None


async def _handle_setting_command(user_msg: str):
    """전략 설정 실시간 변경 (배포 없음). 해당 없으면 None"""
    import re as _re
    msg = user_msg.replace(",", "").strip()

    # 패턴: 손절 -7% / 익절 3% / 매수금액 100만원(또는 1000000원) + 변경/바꿔/설정/해줘
    if not _re.search(r"(변경|바꿔|바꾸|설정|해줘|올려|내려|조정)", msg):
        return None
    m_sl = _re.search(r"손절[을를]?\s*(-?\d+(?:\.\d+)?)\s*%", msg)
    m_tp = _re.search(r"익절[을를]?\s*(\+?\d+(?:\.\d+)?)\s*%", msg)
    m_amt = _re.search(r"매수\s*금액[을를]?\s*(\d+(?:\.\d+)?)\s*(만원|원)", msg)
    if not (m_sl or m_tp or m_amt):
        return None

    changes = {}
    if m_sl:
        v = -abs(float(m_sl.group(1)))
        if not (-15 <= v <= -0.5):
            return f"⚠️ 손절 {v}%는 허용 범위(-0.5% ~ -15%)를 벗어나 적용하지 않았습니다."
        changes["stop_loss"] = v
    if m_tp:
        v = abs(float(m_tp.group(1)))
        if not (0.5 <= v <= 20):
            return f"⚠️ 익절 {v}%는 허용 범위(0.5% ~ 20%)를 벗어나 적용하지 않았습니다."
        changes["take_profit"] = v
    if m_amt:
        v = float(m_amt.group(1)) * (10000 if m_amt.group(2) == "만원" else 1)
        if not (50000 <= v <= 5000000):
            return f"⚠️ 매수금액 {v:,.0f}원은 허용 범위(5만~500만원)를 벗어나 적용하지 않았습니다."
        changes["buy_amount"] = int(v)

    applied = []
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, is_active, params FROM strategy_config WHERE bot='stock_trader'")
        for r in rows:
            params = r["params"] if isinstance(r["params"], dict) else json.loads(r["params"] or "{}")
            for k, v in changes.items():
                # 기존 단위 관례 유지 (|기존값|<=1 이면 소수 단위로 저장)
                old = params.get(k)
                if k in ("stop_loss", "take_profit") and old is not None and abs(float(old)) <= 1:
                    params[k] = v / 100.0
                else:
                    params[k] = v
            await conn.execute(
                "UPDATE strategy_config SET params=$1, updated_at=NOW() WHERE id=$2",
                json.dumps(params), r["id"])
            try:
                await redis_client.publish("strategy:update", json.dumps({
                    "bot": "stock_trader", "name": r["name"],
                    "is_active": r["is_active"], "params": params}))
            except Exception:
                pass
            applied.append(r["name"])

    desc = " · ".join(
        [f"손절 {changes['stop_loss']}%" if "stop_loss" in changes else "",
         f"익절 {changes['take_profit']}%" if "take_profit" in changes else "",
         f"매수금액 {changes['buy_amount']:,}원" if "buy_amount" in changes else ""])
    desc = " · ".join([d for d in desc.split(" · ") if d])
    await _send_telegram(f"⚙️ 전략 설정 변경 (채팅 지시)\n{desc}\n적용 전략: {', '.join(applied)}")
    return (f"⚙️ 설정 변경 완료 — {desc}\n"
            f"적용: {', '.join(applied)} (봇이 1분 내 자동 반영, 배포 없음)")


async def _apply_strategy_settings(changes: dict) -> list:
    """전략 설정 변경 공용 적용기 (검증된 changes만 받음). 반환: 적용 전략명"""
    applied = []
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, is_active, params FROM strategy_config WHERE bot='stock_trader'")
        for r in rows:
            params = r["params"] if isinstance(r["params"], dict) else json.loads(r["params"] or "{}")
            for k, v in changes.items():
                old = params.get(k)
                if k in ("stop_loss", "take_profit") and old is not None and abs(float(old)) <= 1:
                    params[k] = v / 100.0
                else:
                    params[k] = v
            await conn.execute(
                "UPDATE strategy_config SET params=$1, updated_at=NOW() WHERE id=$2",
                json.dumps(params), r["id"])
            try:
                await redis_client.publish("strategy:update", json.dumps({
                    "bot": "stock_trader", "name": r["name"],
                    "is_active": r["is_active"], "params": params}))
            except Exception:
                pass
            applied.append(r["name"])
    return applied


def _validate_setting(k: str, v) -> tuple:
    """(ok, normalized_value or 오류메시지)"""
    try:
        v = float(v)
    except Exception:
        return False, f"{k} 값이 숫자가 아님"
    if k == "stop_loss":
        v = -abs(v)
        return ((-15 <= v <= -0.5), v if -15 <= v <= -0.5 else "손절 허용범위 -0.5~-15%")
    if k == "take_profit":
        v = abs(v)
        return ((0.5 <= v <= 20), v if 0.5 <= v <= 20 else "익절 허용범위 0.5~20%")
    if k == "buy_amount":
        return ((50000 <= v <= 5000000), int(v) if 50000 <= v <= 5000000 else "매수금액 허용범위 5만~500만원")
    return False, f"알 수 없는 설정 {k}"


async def _search_past_chats(query: str, limit: int = 5) -> str:
    """과거 대화 키워드 검색 → 관련 대화 발췌 ('그때 그거' 기억)"""
    try:
        import re as _re
        words = [w for w in _re.findall(r"[가-힣A-Za-z0-9]{2,}", query)
                 if w not in ("자비스", "그때", "저번", "예전", "우리", "했던", "말한", "얘기")][:4]
        if not words:
            return ""
        conds = " OR ".join(f"content ILIKE ${i+1}" for i in range(len(words)))
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT role, content, created_at FROM jarvis_memory
                WHERE ({conds}) AND created_at < NOW() - INTERVAL '10 minutes'
                ORDER BY created_at DESC LIMIT {int(limit)}
            """, *[f"%{w}%" for w in words])
        if not rows:
            return ""
        lines = []
        for r in reversed(rows):
            who = "주인" if r["role"] == "user" else "자비스"
            lines.append(f"[{r['created_at'].strftime('%m/%d')}] {who}: {r['content'][:150]}")
        return "\n".join(lines)
    except Exception:
        return ""


async def _get_chat_summaries(limit: int = 3) -> str:
    """장기 기억: 과거 대화 요약본"""
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT content FROM jarvis_notes
                WHERE category='chat_summary' ORDER BY created_at DESC LIMIT $1""", limit)
        return "\n".join(r["content"] for r in rows) if rows else ""
    except Exception:
        return ""


async def _summarize_old_chats():
    """7일 지난 대화를 일 단위로 요약해 장기 기억으로 이관"""
    try:
        async with db_pool.acquire() as conn:
            day = await conn.fetchval("""
                SELECT DATE(created_at AT TIME ZONE 'Asia/Seoul') FROM jarvis_memory
                WHERE created_at < NOW() - INTERVAL '7 days'
                ORDER BY created_at LIMIT 1""")
            if not day:
                return
            rows = await conn.fetch("""
                SELECT role, content FROM jarvis_memory
                WHERE DATE(created_at AT TIME ZONE 'Asia/Seoul') = $1
                ORDER BY created_at LIMIT 200""", day)
        if not rows:
            return
        convo = "\n".join(f"{'주인' if r['role']=='user' else '자비스'}: {r['content'][:200]}" for r in rows)[:6000]
        summary = await _ask_openwebui(
            f"다음은 {day} 하루의 주인-자비스 대화다. 나중에 참조할 핵심(결정사항, 지시, 전략 논의, 중요 사실)만 "
            f"500자 이내로 요약하라. 잡담은 제외.\n\n{convo}", session_id="summarizer")
        if summary and not summary.startswith("❌"):
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO jarvis_notes (category, content) VALUES ('chat_summary', $1)",
                    f"[{day}] {summary.strip()[:600]}")
                await conn.execute("""
                    DELETE FROM jarvis_memory
                    WHERE DATE(created_at AT TIME ZONE 'Asia/Seoul') = $1""", day)
            logger.info(f"🧠 대화 요약 이관 완료: {day}")
    except Exception as e:
        logger.error(f"대화 요약 오류: {e}")


@app.post("/api/jarvis/chat")
async def jarvis_chat(body: dict):
    """Jarvis AI 채팅 — Open-WebUI 통해서 (텔레그램과 대화 공유)"""
    user_msg = body.get("message", "").strip()
    # 텔레그램과 완전히 같은 세션 공유
    session_id = os.getenv("JARVIS_ANALYST_CHAT_ID", "jarvis_main")
    if not user_msg:
        return {"success": False, "error": "메시지가 없어요"}

    try:
        # 지시사항 저장/목록/취소
        directive_result = await _handle_directive_command(user_msg)
        if directive_result:
            return {"success": True, "reply": directive_result, "context_used": False}

        # 전략 설정 실시간 변경 (손절/익절/매수금액)
        setting_result = await _handle_setting_command(user_msg)
        if setting_result:
            return {"success": True, "reply": setting_result, "context_used": False}

        # 감시 종목 추가/삭제 명령 감지 (Open-WebUI 거치지 않고 직접 처리)
        action_result = await _handle_watchlist_command(user_msg)
        if action_result:
            return {"success": True, "reply": action_result, "context_used": False}

        # 매매 지시 감지 → 실제 KIS 주문 실행 (자비스 경유 X)
        trade_result = await _handle_trade_command(user_msg)
        if trade_result:
            return {"success": True, "reply": trade_result, "context_used": False}

        # 수동 수집 명령
        if any(k in user_msg for k in ["수동 수집", "뉴스 수집", "감성 수집", "데이터 수집"]):
            asyncio.create_task(_manual_collect())
            return {"success": True, "reply": "📰 데이터 수집 시작했어요! 1~2분 후 `/api/data/sentiment` 에서 결과 확인하세요.", "context_used": False}

        # 포트폴리오 컨텍스트 추가
        portfolio_ctx = await get_portfolio_context()

        # 기억 주입: 관련 과거 대화 + 장기 기억 요약 + 활성 지시
        past_ctx = await _search_past_chats(user_msg)
        summaries = await _get_chat_summaries()
        directives_now = await _get_active_directives()
        memory_block = ""
        if summaries:
            memory_block += f"\n[장기 기억 — 과거 대화 요약]\n{summaries}\n"
        if past_ctx:
            memory_block += f"\n[관련 과거 대화 발췌]\n{past_ctx}\n"
        if directives_now:
            memory_block += f"\n[현재 활성 지시사항]\n{directives_now}\n"

        full_msg = (f"{user_msg}\n\n---\n현재 데이터:\n{portfolio_ctx}\n{memory_block}\n"
                    "[시스템 주의] 너는 이 대화에서 직접 주문을 실행할 수 없다. "
                    "매매는 사용자가 '종목명(또는 코드) N주 매수/매도' 형식으로 지시하면 시스템이 직접 체결하고 결과를 표시한다. "
                    "네가 '매수 완료/체결'이라고 단정하지 마라.\n"
                    "[액션 프로토콜] 사용자의 말에 앞으로 계속 적용해야 할 지시(매매 원칙·선호·제한)나 "
                    "전략 설정 변경(손절%/익절%/매수금액)이 담겨 있으면, 자연스러운 답변 후 마지막 줄에 딱 한 줄로:\n"
                    '[[ACTION]]{"directive": "저장할 지시 요약(있으면)", "settings": {"stop_loss": -7}}\n'
                    "형식으로 출력하라. settings 키는 stop_loss/take_profit/buy_amount만 가능. "
                    "해당 없으면 [[ACTION]] 줄을 출력하지 마라. 일회성 질문·잡담엔 절대 출력 금지.\n"
                    "[응답 형식 — 반드시 준수] 최종 결론만 출력하라. 최대 4문장. "
                    "사고 과정, 규칙/지시 인용, 검토 중얼거림, '~라고 답변해야 한다' 류 초안, 같은 내용 반복을 절대 출력하지 마라. "
                    "근거는 핵심 1~2개만 짧게.")

        # Open-WebUI 통해서 호출 (텔레그램과 같은 경로)
        reply = await _ask_openwebui(full_msg, session_id=session_id)

        # 액션 프로토콜 파싱: 자연어 지시/설정을 자동 저장·적용
        try:
            import re as _re
            m = _re.search(r"\[\[ACTION\]\]\s*(\{.*\})", reply, _re.DOTALL)
            if m:
                action_raw = m.group(1).strip()
                reply = reply[:m.start()].rstrip()  # 표시용 답변에서 액션 줄 제거
                try:
                    action = json.loads(action_raw)
                except Exception:
                    action = {}
                notes = []
                # 지시 저장
                d = (action.get("directive") or "").strip()
                if d and len(d) >= 4:
                    async with db_pool.acquire() as conn:
                        await conn.execute("""CREATE TABLE IF NOT EXISTS jarvis_notes (
                            id SERIAL PRIMARY KEY, category VARCHAR(30) DEFAULT 'note',
                            content TEXT NOT NULL, is_active BOOLEAN DEFAULT TRUE,
                            created_at TIMESTAMPTZ DEFAULT NOW())""")
                        did = await conn.fetchval(
                            "INSERT INTO jarvis_notes (category, content, is_active) "
                            "VALUES ('directive', $1, TRUE) RETURNING id", d[:300])
                    notes.append(f"📌 지시 #{did} 저장됨 (해제: '지시 취소 {did}')")
                # 설정 적용
                st = action.get("settings") or {}
                valid = {}
                for k, v in st.items():
                    ok, nv = _validate_setting(k, v)
                    if ok:
                        valid[k] = nv
                    else:
                        notes.append(f"⚠️ {k} 변경 거부: {nv}")
                if valid:
                    applied = await _apply_strategy_settings(valid)
                    desc = ", ".join(f"{k}={v}" for k, v in valid.items())
                    notes.append(f"⚙️ 설정 적용됨 [{desc}] → {', '.join(applied)}")
                    await _send_telegram(f"⚙️ 전략 설정 변경 (대화 인식)\n{desc}")
                if notes:
                    reply = reply + "\n\n" + "\n".join(notes)
        except Exception as ae:
            logger.warning(f"액션 파싱 오류(무시): {ae}")

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


async def _store_notification(text: str):
    """시스템 알림센터 저장 (배지용)"""
    try:
        import re as _re
        clean = _re.sub(r"<[^>]+>", "", text or "").strip()
        if not clean:
            return
        title = clean.split("\n")[0][:80]
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS notifications (
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(120), body TEXT,
                    is_read BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )""")
            await conn.execute(
                "INSERT INTO notifications (title, body) VALUES ($1, $2)",
                title, clean[:1500])
    except Exception as e:
        logger.debug(f"알림 저장 실패(무시): {e}")


@app.get("/api/notifications")
async def get_notifications(limit: int = 30):
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, title, body, is_read, created_at FROM notifications "
                "ORDER BY created_at DESC LIMIT $1", limit)
            unread = await conn.fetchval(
                "SELECT COUNT(*) FROM notifications WHERE is_read=FALSE")
        return {"success": True, "unread": unread,
                "data": [dict(r) | {"created_at": r["created_at"].isoformat()} for r in rows]}
    except Exception as e:
        return {"success": False, "error": str(e), "unread": 0, "data": []}


@app.post("/api/notifications/read")
async def mark_notifications_read():
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE notifications SET is_read=TRUE WHERE is_read=FALSE")
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def _send_telegram(text: str, chat_id: str = None, token: str = None):
    """텔레그램 메시지 전송 (내부용) + 시스템 알림센터 저장"""
    try:
        await _store_notification(text)
    except Exception:
        pass
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
    """대화 히스토리 로드 — Redis 캐시 우선, 없으면 PostgreSQL"""
    # Redis 캐시 먼저 (빠름)
    if redis_client:
        try:
            key = f"jarvis:history:{chat_id}"
            raw = await redis_client.get(key)
            if raw:
                return json.loads(raw)[-(max_turns * 2):]
        except:
            pass

    # Redis 없으면 PostgreSQL에서 로드 (영구 메모리)
    try:
        if db_pool:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT role, content FROM jarvis_memory
                    WHERE session_id = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                """, chat_id, max_turns * 2)
            history = [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]
            # Redis에 캐시 복원
            if redis_client and history:
                key = f"jarvis:history:{chat_id}"
                await redis_client.setex(key, 604800, json.dumps(history))
            return history
    except Exception as e:
        logger.debug(f"PostgreSQL 히스토리 로드 실패: {e}")

    return []


async def _save_chat_history(chat_id: str, role: str, content: str):
    """대화 히스토리 저장 — PostgreSQL(영구) + Redis(캐시)"""
    # PostgreSQL 영구 저장
    try:
        if db_pool:
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO jarvis_memory (session_id, role, content, created_at)
                    VALUES ($1, $2, $3, NOW())
                """, chat_id, role, content)
    except Exception as e:
        logger.debug(f"메모리 DB 저장 실패(테이블 없을 수 있음): {e}")

    # Redis 캐시 (최근 40턴, 빠른 조회용)
    if not redis_client:
        return
    try:
        key = f"jarvis:history:{chat_id}"
        raw = await redis_client.get(key)
        history = json.loads(raw) if raw else []
        history.append({"role": role, "content": content})
        if len(history) > 40:
            history = history[-40:]
        await redis_client.setex(key, 604800, json.dumps(history))  # 7일 보관
    except Exception as e:
        logger.warning(f"히스토리 Redis 저장 실패: {e}")


async def _save_trade_memory(symbol: str, action: str, price: float,
                              amount: float, result: str, pnl: float = 0,
                              reason: str = ""):
    """매매 결과를 Jarvis 메모리에 저장 (학습용)"""
    now = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M")
    pnl_str = f" PnL: {pnl:+,.0f}원" if pnl != 0 else ""
    memory_content = (
        f"[매매기록 {now}] {action} {symbol} "
        f"{amount:,.0f}원 @ {price:,.0f}원 → {result}{pnl_str}"
        f"{f' ({reason})' if reason else ''}"
    )
    # 메인 Jarvis 세션에 기록
    session_id = os.getenv("JARVIS_ANALYST_CHAT_ID", "jarvis_main")
    await _save_chat_history(session_id, "system", memory_content)
    logger.info(f"🧠 Jarvis 메모리 저장: {memory_content}")


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
            "messages": messages,  # Open-WebUI 모델 프롬프트 사용 (중복 제거)
            "stream": False,
        }
        # 사고과정(THINK) 유출 차단: 최종답변 표식 프로토콜
        message = (message + "\n\n[출력 프로토콜] 사고 과정이 필요하면 내부적으로만 하라. "
                   "출력의 맨 마지막에 [[FINAL]] 표식 뒤에 최종 답변만 써라. "
                   "[[FINAL]] 이전의 모든 내용은 사용자에게 표시되지 않는다.")
        data = None
        last_err = None
        for _attempt in range(2):  # 1회 재시도
            try:
                async with http.ClientSession() as session:
                    async with session.post(
                        f"{openwebui_url}/api/chat/completions",
                        json=payload,
                        headers=headers,
                        timeout=http.ClientTimeout(total=90),
                    ) as res:
                        data = await res.json()
                if data and data.get("choices"):
                    break
                last_err = Exception(f"응답 없음: {data.get('error', data) if data else 'no data'}")
            except Exception as _e:
                last_err = _e
                await asyncio.sleep(2)
        if not data or not data.get("choices"):
            raise last_err or Exception("응답 없음")
        if True:
            if True:
                reply = data["choices"][0]["message"]["content"]
                # [[FINAL]] 이후만 사용 (사고과정 제거)
                if "[[FINAL]]" in reply:
                    reply = reply.split("[[FINAL]]")[-1].strip()
                else:
                    # 메타 유출 감지: THINK / [최종 답변 구성] / "규칙을 지킨다" 등
                    _head = reply.strip()[:300]
                    _meta_markers = ("THINK", "[최종", "답변 구성", "라고 답변", "규칙을 지킨")
                    if any(m in _head for m in _meta_markers):
                        parts = [p_.strip() for p_ in reply.replace("\r","").split("\n\n") if p_.strip()]
                        # 뒤에서부터 메타 아닌 첫 문단 선택
                        for cand in reversed(parts):
                            if not any(m in cand[:80] for m in _meta_markers) and not cand.startswith('"'):
                                reply = cand
                                break

                # 대화 히스토리 저장
                await _save_chat_history(session_id, "user", message)
                await _save_chat_history(session_id, "assistant", reply)

                return reply
    except Exception as e:
        logger.error(f"Open-WebUI 호출 실패: {e}")
        return await _ask_gemini_direct(message)


async def _ask_gemini_direct(message: str) -> str:
    """Gemini 직접 호출 (Open-WebUI fallback) — 시세 조회 포함"""
    try:
        portfolio_ctx = await get_portfolio_context()

        # 시세 관련 키워드 감지 → KIS API 직접 조회
        price_ctx = ""
        keywords = ["시세", "현재가", "얼마", "가격", "주가", "시가"]
        stock_map = {"삼성전자": "005930", "SK하이닉스": "000660", "현대차": "005380",
                     "NAVER": "035420", "카카오": "035720", "LG화학": "051910",
                     "삼성SDI": "006400", "셀트리온": "068270"}

        if any(k in message for k in keywords):
            for name, code in stock_map.items():
                if name in message or code in message:
                    try:
                        import aiohttp as http
                        base_url = config.kis_base_url
                        async with http.ClientSession() as sess:
                            tr = await sess.post(f"{base_url}/oauth2/tokenP", json={
                                "grant_type": "client_credentials",
                                "appkey": config.KIS_APP_KEY,
                                "appsecret": config.KIS_APP_SECRET,
                            })
                            token = (await tr.json()).get("access_token", "")
                            hdrs = {"authorization": f"Bearer {token}",
                                    "appkey": config.KIS_APP_KEY,
                                    "appsecret": config.KIS_APP_SECRET,
                                    "tr_id": "FHKST01010100", "custtype": "P"}
                            pr = await sess.get(
                                f"{base_url}/uapi/domestic-stock/v1/quotations/inquire-price",
                                headers=hdrs, params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code}
                            )
                            output = (await pr.json()).get("output", {})
                            price = int(output.get("stck_prpr", 0))
                            change = float(output.get("prdy_ctrt", 0))
                        if price > 0:
                            price_ctx = f"\n[실시간 시세] {name}({code}): {price:,}원 ({change:+.2f}%)"
                    except:
                        pass
                    break

        full_msg = f"{message}\n\n---\n현재 데이터:\n{portfolio_ctx}{price_ctx}"
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
        import ssl as _ssl
        _ssl_ctx = _ssl.create_default_context()
        _ssl_ctx.check_hostname = False
        _ssl_ctx.verify_mode = _ssl.CERT_NONE
        _connector = http.TCPConnector(ssl=_ssl_ctx)

        async with http.ClientSession(connector=_connector) as session:
            headers = {
                "authorization": f"Bearer {token}",
                "appkey": config.kis_app_key,
                "appsecret": config.kis_app_secret,
                "tr_id": "VTTC8434R" if config.KIS_IS_PAPER else "TTTC8434R",
                "custtype": "P",
            }
            acct = config.KIS_ACCOUNT_NO.replace("-", "")
            params = {
                "CANO": acct[:8],
                "ACNT_PRDT_CD": acct[8:] if len(acct) > 8 else "01",
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

        import ssl as _ssl
        _ssl_ctx = _ssl.create_default_context()
        _ssl_ctx.check_hostname = False
        _ssl_ctx.verify_mode = _ssl.CERT_NONE
        _connector = http.TCPConnector(ssl=_ssl_ctx)

        async with http.ClientSession(connector=_connector) as session:
            headers = {
                "authorization": f"Bearer {token}",
                "appkey": config.kis_app_key,
                "appsecret": config.kis_app_secret,
                "tr_id": "VTTC8434R" if config.KIS_IS_PAPER else "TTTC8434R",
                "custtype": "P",
            }
            acct = config.KIS_ACCOUNT_NO.replace("-", "")
            params = {
                "CANO": acct[:8],
                "ACNT_PRDT_CD": acct[8:] if len(acct) > 8 else "01",
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
                    "avg_price": int(float(row.get("pchs_avg_pric", 0) or 0)),
                    "cur_price": int(row.get("prpr", 0)),
                    "pnl":       int(row.get("evlu_pfls_amt", 0)),
                    "pnl_rate":  float(row.get("evlu_pfls_rt", 0) or 0),
                })
            # output2: 계좌 총평가 요약
            out2 = data.get("output2", [{}])
            summary = out2[0] if out2 else {}
            total_eval = int(summary.get("tot_evlu_amt", 0) or 0)
            stock_eval = int(summary.get("evlu_amt_smtl_amt", 0) or 0)  # 평가금액합계
            # 예수금: D+2 정산 예수금(실제 가용) 우선 — 매수해도 dnca_tot_amt는
            # D+2 결제 전까지 안 줄어 혼동 유발
            cash_val = int(summary.get("prvs_rcdl_excc_amt", 0) or 0)   # D+2 예수금
            if cash_val == 0:
                cash_val = int(summary.get("nxdy_excc_amt", 0) or 0)    # D+1 예수금
            if cash_val == 0:
                cash_val = int(summary.get("dnca_tot_amt", 0) or 0)     # 예수금총액
            if cash_val == 0:
                cash_val = total_eval - stock_eval

            account = {
                "total_eval":   total_eval,
                "stock_eval":   stock_eval,
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
    """업비트 코인 보유 포지션 + 계좌 요약"""
    try:
        import json as _json

        # 포지션 캐시
        positions = []
        cached = await redis_client.get("crypto:positions")
        if cached:
            positions = _json.loads(cached)

        # KRW 잔고
        krw_balance = 0.0
        krw_cached = await redis_client.get("crypto:krw_balance")
        if krw_cached:
            krw_balance = float(krw_cached)

        # 코인 평가금액 + 손익 계산
        coin_eval = sum(float(p.get("cur_price",0)) * float(p.get("qty",0)) for p in positions)
        buy_amount = sum(float(p.get("avg_price",0)) * float(p.get("qty",0)) for p in positions)
        pnl = coin_eval - buy_amount
        pnl_rate = (pnl / buy_amount * 100) if buy_amount > 0 else 0.0

        account = {
            "krw_balance": krw_balance,
            "coin_eval":   round(coin_eval, 2),
            "total_assets": round(krw_balance + coin_eval, 2),
            "pnl":         round(pnl, 2),
            "pnl_rate":    round(pnl_rate, 2),
        }

        return {"success": True, "data": positions, "account": account}
    except Exception as e:
        return {"success": False, "error": str(e), "data": [], "account": {}}


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
            acct = config.KIS_ACCOUNT_NO.replace("-", "")
            params = {
                "CANO": acct[:8],
                "ACNT_PRDT_CD": acct[8:] if len(acct) > 8 else "01",
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


async def _log_journal(bot: str, symbol: str, name: str, action: str,
                        strategy: str, signal_reason: str,
                        jarvis_decision: str, jarvis_reason: str,
                        executed: bool, order_success: bool,
                        price: float, qty: float, source: str = "auto"):
    """매매일지: 모든 판단(EXECUTE/SKIP 포함)을 구조화 기록 — 학습의 원재료"""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS trade_journal (
                    id SERIAL PRIMARY KEY,
                    ts TIMESTAMPTZ DEFAULT NOW(),
                    bot VARCHAR(20), symbol VARCHAR(15), name VARCHAR(50),
                    action VARCHAR(10), strategy VARCHAR(50),
                    signal_reason TEXT,
                    jarvis_decision VARCHAR(10), jarvis_reason TEXT,
                    executed BOOLEAN, order_success BOOLEAN,
                    price NUMERIC, qty NUMERIC,
                    source VARCHAR(10) DEFAULT 'auto',
                    eval_price NUMERIC, eval_pnl_rate NUMERIC, eval_at TIMESTAMPTZ
                )""")
            await conn.execute("""
                INSERT INTO trade_journal
                (bot,symbol,name,action,strategy,signal_reason,
                 jarvis_decision,jarvis_reason,executed,order_success,price,qty,source)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            """, bot, symbol, name, action, strategy, (signal_reason or "")[:500],
                 jarvis_decision, (jarvis_reason or "")[:300],
                 executed, order_success, float(price or 0), float(qty or 0), source)
    except Exception as e:
        logger.warning(f"매매일지 기록 실패: {e}")


@app.get("/api/training/summary")
async def training_summary(days: int = 14):
    """트레이닝 페이지용: 교훈 목록 + 일별 채점 성적"""
    try:
        async with db_pool.acquire() as conn:
            lessons = await conn.fetch("""
                SELECT content, created_at FROM jarvis_notes
                WHERE category='lesson' ORDER BY created_at DESC LIMIT 20""")
            daily = await conn.fetch("""
                SELECT DATE(ts AT TIME ZONE 'Asia/Seoul') AS d,
                       COUNT(*) FILTER (WHERE jarvis_decision IN ('EXECUTE','EXECUTE_SMALL') AND eval_pnl_rate >= 0.5)  AS exec_hit,
                       COUNT(*) FILTER (WHERE jarvis_decision IN ('EXECUTE','EXECUTE_SMALL') AND eval_pnl_rate < 0.5)   AS exec_miss,
                       COUNT(*) FILTER (WHERE jarvis_decision='SKIP' AND eval_pnl_rate >= 1.0)     AS skip_missed,
                       COUNT(*) FILTER (WHERE jarvis_decision='SKIP' AND eval_pnl_rate < 1.0)      AS skip_good,
                       COUNT(*) AS total
                FROM trade_journal
                WHERE ts >= NOW() - ($1 || ' days')::interval AND eval_at IS NOT NULL
                GROUP BY 1 ORDER BY 1 DESC""", str(days))
        return {"success": True,
                "lessons": [{"content": r["content"], "ts": r["created_at"].isoformat()} for r in lessons],
                "daily": [{"date": str(r["d"]), "exec_hit": r["exec_hit"], "exec_miss": r["exec_miss"],
                            "skip_good": r["skip_good"], "skip_missed": r["skip_missed"],
                            "total": r["total"]} for r in daily]}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/training", response_class=HTMLResponse)
async def training_page():
    with open("static/training.html", encoding="utf-8") as f:
        return f.read()


@app.get("/api/journal")
async def get_journal(days: int = 7):
    """매매일지 조회 + 요약 (EXECUTE율, 체결수, SKIP수)"""
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT ts, bot, symbol, name, action, strategy,
                       jarvis_decision, jarvis_reason, executed, order_success,
                       price, qty, source
                FROM trade_journal
                WHERE ts >= NOW() - ($1 || ' days')::interval
                ORDER BY ts DESC LIMIT 200
            """, str(days))
        total = len(rows)
        executes = sum(1 for r in rows if r["executed"])
        fills = sum(1 for r in rows if r["order_success"])
        return {"success": True,
                "summary": {"total_signals": total, "executes": executes,
                             "skips": total - executes, "fills": fills,
                             "execute_rate": round(executes / total * 100, 1) if total else 0},
                "data": [dict(r) | {"ts": r["ts"].isoformat()} for r in rows]}
    except Exception as e:
        return {"success": False, "error": str(e)}


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

        # 1. DB 컨텍스트 수집
        ctx = await get_portfolio_context()

        # 1-1. 오늘의 작전 (아침에 캐시된 1장 — 빠른 판단용)
        daily_plan = ""
        try:
            cached_plan = await redis_client.get("jarvis:daily_plan")
            if cached_plan:
                daily_plan = cached_plan if isinstance(cached_plan, str) else cached_plan.decode()
        except Exception:
            pass

        # 1-1b. 주인 지시사항 (최우선)
        directives = await _get_active_directives()

        # 1-2. 오늘 이 종목에 대한 내 판단 이력 (기회놓침 반복 방지)
        self_history = ""
        try:
            async with db_pool.acquire() as conn:
                hist = await conn.fetch("""
                    SELECT jarvis_decision, price, ts
                    FROM trade_journal
                    WHERE symbol=$1 AND bot=$2
                      AND DATE(ts AT TIME ZONE 'Asia/Seoul') = (NOW() AT TIME ZONE 'Asia/Seoul')::date
                    ORDER BY ts
                """, symbol, bot)
            if hist:
                skips = [h for h in hist if h["jarvis_decision"] == "SKIP"]
                first_price = float(hist[0]["price"] or 0)
                drift = ((float(price) - first_price) / first_price * 100) if first_price > 0 else 0
                self_history = (
                    f"\n[오늘 이 종목에 대한 내 판단 이력]\n"
                    f"- 오늘 판단 {len(hist)}회 (SKIP {len(skips)}회)\n"
                    f"- 첫 판단가 {first_price:,.0f}원 → 현재가 {price:,.0f}원 ({drift:+.1f}%)\n"
                )
                if len(skips) >= 2 and drift >= 1.0:
                    self_history += ("⚠️ 주의: 반복 SKIP 중 가격이 계속 상승. 추세가 확인되면 "
                                     "과거 SKIP에 얽매이지 말고 재평가하라. 놓친 기회의 반복은 손실과 같다.\n")
        except Exception:
            pass

        # 2. Jarvis에게 분석 요청 (DB 데이터 포함)
        analysis_prompt = f"""[매매 신호 발생]
종목: {name}({symbol})
방향: {action_kr}
전략: {strategy}
현재가: {price:,}원
수량: {qty}
매수금액: {price * (qty if isinstance(qty, (int,float)) else 0):,.0f}원
신호 이유: {reason}

[오늘의 작전]
{daily_plan or '(작전 없음 — 일반 기준으로 판단)'}

[주인 지시사항 — 최우선 준수]
{directives or '(없음)'}
{self_history}
[현재 포트폴리오 현황]
{ctx}

[매매 규칙] 손절종목 재매수 금지(쿨다운은 시스템이 이미 체크함) · 당일 2회 손절 시 신규중단 · 약한 신호는 단타, 강한 복합신호만 스윙 관점

오늘의 작전과 위 데이터 기준으로 이 {action_kr} 신호를 즉시 판단하라.
반드시 다음 중 하나로 시작해서 이유를 한 줄로:
- EXECUTE: 조건 대부분 충족, 강한 확신
- EXECUTE_SMALL: 일부 조건(2~3개) 충족, 리스크 제한적 → 절반 금액 진입
- SKIP: 근거 부족
완벽하지 않다는 이유만으로 전부 SKIP하지 마라. 애매하면 EXECUTE_SMALL로 소액 검증하라."""

        # 3. Jarvis 판단 (+ AI 장애 시 ML 폴백)
        jarvis_reply = await _ask_openwebui(analysis_prompt, session_id="signal")
        logger.info(f"🤖 Jarvis 판단 [{symbol}]: {jarvis_reply[:150]}")
        import re as _re2
        _m = _re2.search(r"\b(EXECUTE_SMALL|EXECUTE|SKIP)\b", jarvis_reply.upper())
        _verdict = _m.group(1) if _m else ""
        is_small = _verdict == "EXECUTE_SMALL"
        should_execute = _verdict in ("EXECUTE", "EXECUTE_SMALL")
        if is_small and action in ["buy", "BUY"]:
            try:
                qty = max(1, int(float(qty) // 2))  # 절반 금액 진입
            except Exception:
                pass

        # 429/오류 폴백: AI 응답 불가 시 신호의 ML 확률로 규칙 판단 (봇 생존)
        ai_failed = (not jarvis_reply) or jarvis_reply.startswith("❌") or "429" in jarvis_reply[:200]
        if ai_failed:
            import re as _re
            m = _re.search(r"ML매수확률[:\s]*([0-9]+)%", reason or "")
            ml_prob = int(m.group(1)) if m else 0
            should_execute = (action == "buy" and ml_prob >= 70)
            jarvis_reply = (f"[AI폴백] ML확률 {ml_prob}% 기준 "
                            f"{'EXECUTE' if should_execute else 'SKIP'} (Gemini 응답 불가)")
            logger.warning(f"🤖 AI 폴백 판단 [{symbol}]: {jarvis_reply}")

        if should_execute:
            # 3. 실제 매매 실행 (주식 vs 코인 분기)
            import aiohttp as http

            if bot == "crypto_trader":
                # 코인 매매
                from crypto_trader.upbit_trader import UpbitTrader
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

                    # Jarvis 메모리에 매매 기록 저장
                    await _save_trade_memory(
                        symbol=symbol, action=action_kr,
                        price=float(price), amount=float(amount),
                        result="성공", reason=reason
                    )

                    # SSE 실시간 알림
                    await push_event("trade", {
                        "type": "trade",
                        "action": action.upper(),
                        "symbol": symbol,
                        "name": name,
                        "amount": float(amount),
                        "price": float(price),
                        "strategy": strategy,
                        "jarvis": jarvis_reply[:80],
                        "ts": datetime.now().isoformat(),
                    })

                    return {"success": True, "executed": True, "jarvis_reply": jarvis_reply}
                else:
                    await _send_telegram(f"❌ {name} 코인 {action_kr} 실패\n{result.get('error')}", chat_id, token)
                    return {"success": False, "executed": False, "error": result.get("error")}

            else:
                # 주식 매매 (dashboard 내장 주문 — 모듈 의존 없음)
                result = await _kis_stock_order(symbol, int(price), int(qty), action in ["buy", "BUY"])

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

                # Jarvis 메모리에 매매 기록 저장
                await _save_trade_memory(
                    symbol=symbol, action=action_kr,
                    price=float(price), amount=float(price*qty),
                    result="성공", reason=reason
                )
                await _log_journal(bot, symbol, name, action, strategy, reason,
                                   "EXECUTE_SMALL" if is_small else "EXECUTE",
                                   jarvis_reply, True, True, price, qty)
                return {"success": True, "executed": True, "jarvis_reply": jarvis_reply}
            else:
                await _send_telegram(
                    f"❌ {name} {action_kr} 실패\n{result.get('error')}",
                    chat_id, token
                )
                await _log_journal(bot, symbol, name, action, strategy, reason,
                                   "EXECUTE_SMALL" if is_small else "EXECUTE",
                                   jarvis_reply, True, False, price, qty)
                return {"success": False, "executed": False, "error": result.get("error")}
        else:
            # 4. 건너뜀 보고
            msg = f"⏭️ <b>{name} {action_kr} 신호 건너뜀</b>\nJarvis 판단: {jarvis_reply[:100]}"
            await _send_telegram(msg, chat_id, token)
            logger.info(f"⏭️ Jarvis가 {action_kr} 신호 건너뜀: {symbol}")
            await _log_journal(bot, symbol, name, action, strategy, reason,
                               "SKIP", jarvis_reply, False, False, price, qty)
            return {"success": True, "executed": False, "jarvis_reply": jarvis_reply}

    except Exception as e:
        logger.error(f"Jarvis 신호 처리 오류: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/jarvis/memory")
async def save_jarvis_memory(request: Request):
    """외부 서비스(stock-trader 등)에서 Jarvis 메모리 저장"""
    try:
        body = await request.json()
        content = body.get("content", "")
        mem_type = body.get("type", "general")  # ml_training / trade / strategy
        if not content:
            return {"success": False, "error": "content 없음"}

        session_id = os.getenv("JARVIS_ANALYST_CHAT_ID", "jarvis_main")
        await _save_chat_history(session_id, "system", f"[{mem_type}] {content}")
        logger.info(f"🧠 Jarvis 메모리 저장: [{mem_type}] {content[:60]}...")
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/admin/fetch-historical")
async def fetch_historical_data(request: Request):
    """watchlist 전종목 과거 OHLCV 데이터 일괄 수집 (백그라운드)"""
    try:
        body = await request.json()
        start_date = body.get("start_date", "20260601")
        end_date   = body.get("end_date", datetime.now().strftime("%Y%m%d"))

        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT symbol, name FROM watchlist WHERE is_active=TRUE")
        symbols = [r["symbol"] for r in rows]
        if not symbols:
            return {"success": False, "error": "watchlist가 비어있어요. 먼저 스캐너를 실행하세요."}

        asyncio.create_task(_run_historical_fetch(symbols, start_date, end_date))
        return {
            "success": True,
            "message": f"{len(symbols)}종목 과거 데이터 적재 시작 (백그라운드)",
            "symbols": symbols,
            "period": f"{start_date} ~ {end_date}",
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


async def _run_historical_fetch(symbols: list, start_date: str, end_date: str):
    """백그라운드로 pykrx 과거 데이터 수집 후 DB 저장"""
    import asyncio
    from pykrx import stock as pykrx_stock

    logger.info(f"🚀 과거 데이터 적재 시작: {start_date}~{end_date} / {len(symbols)}종목")
    await _send_telegram(f"📥 과거 데이터 적재 시작\n기간: {start_date[:4]}.{start_date[4:6]}.{start_date[6:]} ~ {end_date[:4]}.{end_date[4:6]}.{end_date[6:]}\n종목: {len(symbols)}개")

    total_saved = 0
    failed = []

    loop = asyncio.get_event_loop()

    for i, symbol in enumerate(symbols):
        try:
            def _fetch(sym, sd, ed):
                df = pykrx_stock.get_market_ohlcv(sd, ed, sym)
                return df

            df = await loop.run_in_executor(None, _fetch, symbol, start_date, end_date)

            if df is None or df.empty:
                logger.warning(f"[{symbol}] 데이터 없음")
                failed.append(symbol)
                continue

            candles = []
            for date, row in df.iterrows():
                if int(row.get("종가", 0)) <= 0:
                    continue
                candles.append({
                    "date":   date.strftime("%Y%m%d"),
                    "open":   int(row.get("시가", 0)),
                    "high":   int(row.get("고가", 0)),
                    "low":    int(row.get("저가", 0)),
                    "close":  int(row.get("종가", 0)),
                    "volume": int(row.get("거래량", 0)),
                    "change_rate": float(row.get("등락률", 0)),
                })

            if not candles:
                continue

            async with db_pool.acquire() as conn:
                for c in candles:
                    ts = datetime.strptime(c["date"], "%Y%m%d")
                    await conn.execute("""
                        INSERT INTO stock_daily_ohlcv
                            (symbol, ts, open, high, low, close, volume, change_rate)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                        ON CONFLICT (symbol, ts) DO UPDATE
                        SET open=$3, high=$4, low=$5, close=$6, volume=$7, change_rate=$8
                    """, symbol, ts,
                        c["open"], c["high"], c["low"], c["close"],
                        c["volume"], c["change_rate"])
            total_saved += len(candles)
            logger.info(f"✅ [{symbol}] {len(candles)}개 저장 ({i+1}/{len(symbols)})")

        except Exception as e:
            logger.error(f"❌ [{symbol}] 적재 실패: {e}")
            failed.append(symbol)

        await asyncio.sleep(0.5)  # pykrx 레이트 리밋

    msg = f"✅ 과거 데이터 적재 완료\n총 {total_saved}개 캔들 저장\n성공: {len(symbols)-len(failed)}종목"
    if failed:
        msg += f"\n실패: {len(failed)}종목 ({', '.join(failed[:5])})"
    msg += "\n\n이제 자동매매 시작 가능! 🚀"
    await _send_telegram(msg)
    logger.info(f"🎉 과거 데이터 적재 완료: {total_saved}개 저장, 실패 {len(failed)}종목")


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
