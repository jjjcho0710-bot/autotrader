import asyncio
import logging
import re
import time as _time
from datetime import datetime, timedelta

import aiohttp

from common.config import config
from common.database import db, cache

logger = logging.getLogger(__name__)

# [AT] fix/collector-observability
REQUEST_TIMEOUT_SEC = 10          # get_price 단건 HTTP 타임아웃(aiohttp 기본 약 300초에 의존하지 않는다)
RATE_LIMIT_RETRIES = 2            # EGW00201(초당 거래건수 초과) 백오프 재시도 횟수
RATE_LIMIT_BACKOFF_SEC = 1.5      # 재시도 n회차 대기 = BACKOFF * n
TOKEN_EXPIRED_CODES = ("EGW00121", "EGW00123")
TOKEN_REISSUE_COOLDOWN_SEC = 60   # KIS 토큰 발급은 분당 1회 제한 — 종목마다 재발급하지 않도록 쿨다운
_MASK = "********"
_BEARER_RE = re.compile(r"(?i)bearer\s+[A-Za-z0-9._\-]+")
_JWT_RE = re.compile(r"eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+")


def mask_secrets(text) -> str:
    """로그에 남길 문자열에서 토큰·앱키·시크릿·계좌번호를 가린다."""
    out = str(text)
    candidates = []
    try:
        acct = config.kis_account_no or ""
        candidates += [KISCollector._shared_token, config.kis_app_key, config.kis_app_secret,
                       acct, acct.split("-")[0], acct[:8]]
    except Exception:
        pass
    for secret in candidates:
        if secret and len(secret) >= 6:
            out = out.replace(secret, _MASK)
    out = _BEARER_RE.sub("Bearer " + _MASK, out)
    out = _JWT_RE.sub(_MASK, out)
    return out


class KISCollector:
    """한국투자증권 KIS API — 주식 시세 수집"""

    BASE_URL = config.kis_base_url
    _shared_token: str = ""
    _token_expires: datetime = datetime.min
    _last_reissue_at: float = -1e9      # monotonic
    _last_reissue_ok: bool = False

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
    async def _get_token(self, force_new: bool = False) -> bool:
        """토큰 확보. 반환: 사용 가능한 토큰이 있으면 True.
        force_new=True면 Redis 키와 메모리 토큰을 버리고 새로 발급한다(만료 오류 복구용)."""
        # 모의/실전 계좌 토큰은 서로 호환되지 않으므로 키를 분리 — 분리하지 않으면
        # 이 프로세스가 다른 모드로 발급받은 토큰을 실주문 프로세스와 같은 키로 공유하게 되어
        # "모의투자 주문이 불가한 계좌입니다" 류의 주문 거부를 유발할 수 있다.
        redis_key = "kis:paper_token" if config.KIS_IS_PAPER else "kis:access_token"
        now = datetime.now()
        if force_new:
            KISCollector._shared_token = ""
            KISCollector._token_expires = datetime.min
            try:
                await cache.client.delete(redis_key)
            except Exception as e:
                logger.warning(f"Redis 토큰 삭제 실패: {mask_secrets(e)}")
        else:
            # Redis에서 토큰 확인 (재시작해도 재사용)
            try:
                cached = await cache.client.get(redis_key)
                if cached:
                    # bytes → str 변환
                    if isinstance(cached, bytes):
                        cached = cached.decode('utf-8')
                    KISCollector._shared_token = cached
                    logger.info("✅ KIS 토큰 Redis에서 복원")
                    return True
            except Exception as e:
                logger.warning(f"Redis 토큰 조회 실패: {mask_secrets(e)}")

            # 클래스 변수 확인
            if KISCollector._shared_token and KISCollector._token_expires > now:
                logger.info(f"✅ KIS 토큰 재사용 (만료까지 {int((KISCollector._token_expires - now).total_seconds() / 60)}분)")
                return True

        # 새 토큰 발급
        try:
            url = f"{self.BASE_URL}/oauth2/tokenP"
            payload = {
                "grant_type": "client_credentials",
                "appkey": config.kis_app_key,
                "appsecret": config.kis_app_secret,
            }
            async with self.session.post(url, json=payload) as resp:
                data = await resp.json()
                token = data.get("access_token", "")
                if token:
                    KISCollector._shared_token = token
                    KISCollector._token_expires = now + timedelta(hours=23)
                    try:
                        await cache.client.setex(redis_key, 23 * 3600, token)
                    except Exception as e:
                        logger.warning(f"Redis 토큰 저장 실패: {mask_secrets(e)}")
                    logger.info("✅ KIS 토큰 발급 완료 (23시간 유효)")
                    return True
                logger.error(f"❌ KIS 토큰 발급 실패: {mask_secrets(data)}")
        except Exception as e:
            logger.error(f"❌ KIS 토큰 발급 오류: {mask_secrets(e)}")
        return False

    async def _refresh_token(self) -> bool:
        """만료 오류 시 재발급. 쿨다운 안에서는 직전 재발급 결과를 그대로 돌려준다
        (여러 종목이 연달아 만료 오류를 만나도 토큰 발급 API를 두드리지 않기 위함)."""
        now = _time.monotonic()
        if now - KISCollector._last_reissue_at < TOKEN_REISSUE_COOLDOWN_SEC:
            return KISCollector._last_reissue_ok and bool(KISCollector._shared_token)
        KISCollector._last_reissue_at = now
        ok = await self._get_token(force_new=True)
        KISCollector._last_reissue_ok = bool(ok)
        return bool(ok)

    def _headers(self, tr_id: str) -> dict:
        return {
            "Content-Type": "application/json",
            "authorization": f"Bearer {KISCollector._shared_token}",
            "appkey": config.kis_app_key,
            "appsecret": config.kis_app_secret,
            "tr_id": tr_id,
            "custtype": "P",
        }

    # ── 현재가 조회 ─────────────────────────────────────
    async def get_price(self, symbol: str) -> dict:
        """주식 현재가 조회. 실패하면 {} (오류 응답의 rt_cd/msg_cd/msg1은 마스킹 후 로그에 남긴다)."""
        url = f"{self.BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
        }
        rate_retry = 0
        reissued = False
        while True:
            try:
                async with self.session.get(
                    url,
                    headers=self._headers("FHKST01010100"),
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SEC),
                ) as resp:
                    data = await resp.json()
                    http_status = getattr(resp, "status", None)
            except Exception as e:
                logger.error(f"KIS 현재가 조회 실패 [{symbol}]: {mask_secrets(e) or type(e).__name__}")
                return {}

            if not isinstance(data, dict):
                data = {}
            rt_cd = data.get("rt_cd")
            output = data.get("output")
            if rt_cd in (None, "0") and isinstance(output, dict) and output:
                break  # 정상 응답

            msg_cd = data.get("msg_cd", "")
            msg1 = data.get("msg1", "")
            logger.warning(
                f"⚠️ KIS 현재가 오류 [{symbol}] http={http_status} rt_cd={rt_cd} "
                f"msg_cd={msg_cd} msg1={mask_secrets(msg1)}"
            )

            if (msg_cd == "EGW00201" or "초당" in str(msg1)) and rate_retry < RATE_LIMIT_RETRIES:
                rate_retry += 1
                await asyncio.sleep(RATE_LIMIT_BACKOFF_SEC * rate_retry)
                continue

            if msg_cd in TOKEN_EXPIRED_CODES and not reissued:
                reissued = True
                logger.warning(f"⚠️ KIS 토큰 만료 감지(msg_cd={msg_cd}) → Redis 키 삭제 후 재발급·1회 재시도 [{symbol}]")
                if await self._refresh_token():
                    continue
            return {}

        try:
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
        except (ValueError, TypeError) as e:
            logger.error(f"KIS 현재가 파싱 실패 [{symbol}]: {mask_secrets(e)}")
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
    async def collect_all(self) -> int:
        """설정된 전체 종목 수집 → DB 저장 + Redis 캐시. 반환: 실제 DB insert(upsert) 성공 종목 수."""
        try:
            symbols = await db.get_watchlist_symbols()
            if not symbols:
                symbols = config.STOCK_SYMBOLS
        except Exception:
            symbols = config.STOCK_SYMBOLS
        logger.info(f"📊 주식 수집 시작 — {len(symbols)}종목")

        inserted = 0
        failed = 0
        for symbol in symbols:
            try:
                if await self._collect_one(symbol):
                    inserted += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                logger.error(f"수집 실패 [{symbol}]: {mask_secrets(e)}")
            await asyncio.sleep(0.5)  # 레이트 리밋 방지 (실전 API: 초당 20건)

        if symbols and inserted == 0:
            logger.error(f"❌ 주식 수집 사이클 insert 0건 — {len(symbols)}종목 전부 실패/스킵")
        else:
            logger.info(f"✅ 주식 수집 완료 — insert {inserted}/{len(symbols)}종목 (실패·스킵 {failed})")
        return inserted

    async def _collect_one(self, symbol: str) -> bool:
        """단일 종목 수집. 반환: DB insert까지 성공하면 True, 시세 조회 실패·0원 스킵이면 False."""
        # 현재가
        price_data = await self.get_price(symbol)
        if not price_data:
            return False

        # price가 0이면 1번 재시도
        if price_data.get("price", 0) <= 0:
            await asyncio.sleep(0.5)
            price_data = await self.get_price(symbol)
            if not price_data or price_data.get("price", 0) <= 0:
                logger.warning(f"⚠️ [{symbol}] 시세 0 — DB 저장 스킵")
                return False

        # Redis 캐시 저장 (실시간) — 실패해도 DB 저장은 계속한다
        try:
            await cache.set_price(f"stock:price:{symbol}", price_data, ttl=120)
        except Exception as e:
            logger.warning(f"Redis 시세 캐시 저장 실패 [{symbol}]: {mask_secrets(e)}")

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

        # API 레이트 리밋 방지
        await asyncio.sleep(0.3)
        return True
