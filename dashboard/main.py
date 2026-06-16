"""
AutoTrader Dashboard — FastAPI 서버
실시간 DB/Redis 데이터를 API로 제공
"""
import json
import logging
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



# ── KIS 토큰 캐시 ──────────────────────────────────────
import aiohttp as _aiohttp
_kis_token_cache: dict = {"token": "", "expires": 0}

async def get_kis_token() -> str:
    """KIS 액세스 토큰 발급 (1시간 캐싱)"""
    import time
    now = time.time()
    if _kis_token_cache["token"] and now < _kis_token_cache["expires"]:
        return _kis_token_cache["token"]
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
                _kis_token_cache["expires"] = now + 3600
            return token
    except Exception as e:
        logger.error(f"KIS 토큰 발급 실패: {e}")
        return _kis_token_cache.get("token", "")

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

    # 텔레그램 webhook 자동 등록
    await _auto_register_webhook()


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

# ── Jarvis AI (Gemini 2.5 Flash) ────────────────────────────────

import google.generativeai as genai
from datetime import datetime

# 대화 히스토리 (메모리)
_jarvis_history: list = []

JARVIS_SYSTEM_PROMPT = """
당신은 AutoTrader의 AI 트레이딩 어시스턴트 **Jarvis**입니다.

## 역할
- 실시간 포트폴리오 데이터를 분석하고 인사이트를 제공합니다
- 주식/코인 매매 전략에 대해 조언합니다
- 리스크를 감지하고 경고합니다
- 사용자의 명령을 이해하고 봇 제어를 도웁니다

## 성격
- 전문적이지만 친근하게 대화합니다
- 데이터 기반으로 명확하게 분석합니다
- 중요한 리스크는 반드시 짚어줍니다
- 답변은 간결하게, 핵심만 먼저 말합니다

## 답변 형식
- 한국어로 답변합니다
- 숫자는 한국 원화 형식(,구분자)으로 표시합니다
- 텔레그램 메시지에 적합하게 이모지를 적절히 사용합니다
- 중요 수치는 **볼드** 처리합니다
- 답변은 3-5문장 이내로 간결하게 (길면 핵심만)

## 제한사항
- 투자는 항상 본인 책임임을 인지시킵니다
- 확실하지 않은 정보는 추측이라고 명시합니다
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

    return "\n".join(ctx_parts)


@app.post("/api/jarvis/chat")
async def jarvis_chat(body: dict):
    """Jarvis AI 채팅 — Gemini 2.5 Flash"""
    global _jarvis_history
    user_msg = body.get("message", "").strip()
    if not user_msg:
        return {"success": False, "error": "메시지가 없어요"}

    try:
        api_key = config.GEMINI_API_KEY
        if not api_key:
            return {"success": False, "error": "GEMINI_API_KEY 환경변수가 없어요"}

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            system_instruction=JARVIS_SYSTEM_PROMPT,
        )

        # 포트폴리오 컨텍스트 수집
        portfolio_ctx = await get_portfolio_context()

        # 사용자 메시지에 컨텍스트 첨부
        full_user_msg = f"{user_msg}\n\n---\n현재 포트폴리오 데이터:\n{portfolio_ctx}"

        # 대화 히스토리 유지 (최근 10턴)
        if len(_jarvis_history) > 20:
            _jarvis_history = _jarvis_history[-20:]

        # Gemini 채팅
        chat = model.start_chat(history=_jarvis_history)
        response = chat.send_message(full_user_msg)
        reply = response.text

        # 히스토리 업데이트
        _jarvis_history = list(chat.history)

        # 텔레그램으로도 전송
        tg_msg = f"🤖 <b>Jarvis 분석</b>\n\n질문: {user_msg}\n\n{reply}"
        await _send_telegram(tg_msg)

        logger.info(f"Jarvis 응답: {reply[:100]}...")
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
        prompt = f"""현재 포트폴리오를 분석하고 간단한 리포트를 작성해주세요.
다음을 포함하세요:
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


async def _ask_openwebui(message: str, session_id: str = "telegram") -> str:
    """Open-WebUI Jarvis 모델 호출 — Tools + 메모리 포함"""
    import aiohttp as http
    import os
    openwebui_url   = os.getenv("OPENWEBUI_URL", "https://open-webui-production-5843.up.railway.app")
    openwebui_token = os.getenv("OPENWEBUI_API_TOKEN", "")
    jarvis_model    = os.getenv("JARVIS_MODEL", "autotrader-jarvis")

    if not openwebui_token:
        # fallback: Gemini 직접 호출
        return await _ask_gemini_direct(message)

    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {openwebui_token}",
        }
        payload = {
            "model": jarvis_model,
            "messages": [{"role": "user", "content": message}],
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
                return data["choices"][0]["message"]["content"]
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
            await _send_telegram("🔍 분석 중...", chat_id, token)
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
            await _send_telegram("📊 포지션 조회 중...", chat_id, token)
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
            # 자유 대화 → Open-WebUI Jarvis (Tools + 메모리 포함)
            token = config.JARVIS_ANALYST_TOKEN or config.TELEGRAM_TOKEN
            await _send_telegram("💭 생각 중...", chat_id, token)
            reply = await _ask_openwebui(text, session_id=chat_id)
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
