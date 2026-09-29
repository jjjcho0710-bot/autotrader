"""
KIS API — 주문 실행 모듈
매수 / 매도 / 잔고조회 / 보유종목 조회
"""
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import aiohttp

from common.config import config
from common.database import cache

logger = logging.getLogger(__name__)


class KISTrader:
    """한국투자증권 주문 실행"""

    BASE_URL = config.kis_base_url

    def __init__(self):
        self.access_token: str = ""
        self._last_cash: int = 0
        self.session: aiohttp.ClientSession = None
        self._balance_lock: Optional[asyncio.Lock] = None
        self._balance_cache: Optional[dict] = None
        self._balance_cache_ts: float = 0.0
        self._balance_cache_ttl: float = 20.0

    def invalidate_balance_cache(self):
        """잔고 캐시 즉시 무효화 (주문 체결 시 호출)"""
        self._balance_cache = None
        self._balance_cache_ts = 0.0

    async def start(self):
        # 키 설정 진단
        mode = "모의투자" if config.KIS_IS_PAPER else "실전투자"
        key = config.kis_app_key
        secret = config.kis_app_secret
        acct = config.kis_account_no
        logger.info("🔑 KIS 설정: %s | 서버=%s", mode, self.BASE_URL)
        logger.info("🔑 앱키=%s | 시크릿=%s | 계좌=%s",
                    (key[:6] + "..." if key else "❌미설정"),
                    ("설정됨" if secret else "❌미설정"),
                    (acct if acct else "❌미설정"))
        if not key or not secret:
            logger.error("❌ KIS 앱키/시크릿 미설정 — Railway 환경변수 확인 필요"
                         " (모의투자면 KIS_APP_KEY_PAPER / KIS_APP_SECRET_PAPER)")
        if not acct:
            logger.error("❌ KIS 계좌번호 미설정 — 계좌번호 확인 필요 (config.kis_account_no)")

        await self._get_token()
        if self.access_token:
            logger.info("✅ KISTrader 시작 (토큰 정상)")
        else:
            logger.error("❌ KISTrader 토큰 발급 실패 — 매매 불가 상태")

    def _new_session(self):
        """매 요청마다 새 세션 생성 (Server disconnected 방지)"""
        import ssl
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        return aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_ctx))

    async def stop(self):
        if self.session:
            await self.session.close()

    async def _get_token(self):
        # Redis에서 토큰 재사용 (모의/실전 계좌 토큰은 서로 호환되지 않으므로 키를 분리 —
        # 분리하지 않으면 다른 프로세스가 모의투자 토큰을 같은 키에 써서 실전 주문이
        # "모의투자 주문이 불가한 계좌입니다" 오류로 거부될 수 있다)
        redis_key = "kis:paper_token" if config.KIS_IS_PAPER else "kis:access_token"
        try:
            cached = await cache.client.get(redis_key)
            if cached:
                self.access_token = cached if isinstance(cached, str) else cached.decode('utf-8')
                logger.info("✅ KIS 토큰 Redis에서 복원")
                return
        except Exception:
            pass

        # 새 토큰 발급
        url = f"{self.BASE_URL}/oauth2/tokenP"
        payload = {
            "grant_type": "client_credentials",
            "appkey": config.kis_app_key,
            "appsecret": config.kis_app_secret,
        }
        try:
            async with self._new_session() as sess:
              async with sess.post(url, json=payload) as resp:
                data = await resp.json()
                token = data.get("access_token", "")
                if token:
                    self.access_token = token
                    try:
                        await cache.client.setex(redis_key, 82800, token)
                    except Exception:
                        pass
                    logger.info("✅ KIS 토큰 발급 완료 (23시간 유효)")
                else:
                    # 실패 원인 로그 (KIS는 error_description 반환)
                    err = data.get("error_description") or data.get("error_code") or data.get("msg1") or str(data)[:200]
                    logger.error("❌ KIS 토큰 발급 실패: %s", err)
        except Exception as e:
            logger.error("❌ KIS 연결 실패: %s — Railway Static IP를 KIS에 등록했는지 확인"
                         " (한국투자 개발자센터 > 마이페이지 > 사용 IP 등록)", e)

    @property
    def _cano(self) -> str:
        """계좌번호 앞자리"""
        parts = config.kis_account_no.split("-")
        return parts[0] if parts else config.kis_account_no

    @property
    def _acnt_prdt_cd(self) -> str:
        """계좌번호 뒷자리 (상품코드)"""
        parts = config.kis_account_no.split("-")
        return parts[1] if len(parts) > 1 else "01"

    def _headers(self, tr_id: str) -> dict:
        return {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.access_token}",
            "appkey": config.kis_app_key,
            "appsecret": config.kis_app_secret,
            "tr_id": tr_id,
            "custtype": "P",
        }

    # ── 현재가 조회 ─────────────────────────────────────
    async def get_current_price(self, symbol: str) -> int:
        # Redis 캐시에서 먼저 조회 (data-collector가 1분마다 업데이트)
        try:
            from common.database import cache
            import json
            cached = await cache.client.get(f"stock:price:{symbol}")
            if cached:
                data = json.loads(cached)
                price = int(data.get("price", 0))
                if price > 0:
                    return price
        except Exception:
            pass
        # Redis 없으면 KIS API 직접 조회
        url = f"{self.BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}
        async with self._new_session() as sess:
          async with sess.get(
            url, headers=self._headers("FHKST01010100"), params=params
          ) as resp:
            data = await resp.json()
            return int(data.get("output", {}).get("stck_prpr", 0))

    # ── 잔고 조회 ───────────────────────────────────────
    async def get_balance(self) -> dict:
        """예수금 + 총평가금액 조회 (동시 호출 직렬화 + 20초 캐시 + 10초 타임아웃)"""
        now = time.time()
        if self._balance_cache is not None and (now - self._balance_cache_ts) < self._balance_cache_ttl:
            return dict(self._balance_cache)

        if self._balance_lock is None:
            self._balance_lock = asyncio.Lock()

        async with self._balance_lock:
            now = time.time()
            if self._balance_cache is not None and (now - self._balance_cache_ts) < self._balance_cache_ttl:
                return dict(self._balance_cache)

            url = f"{self.BASE_URL}/uapi/domestic-stock/v1/trading/inquire-psbl-order"
            params = {
                "CANO": self._cano,
                "ACNT_PRDT_CD": self._acnt_prdt_cd,
                "PDNO": "005930",
                "ORD_UNPR": "0",
                "ORD_DVSN": "01",
                "CMA_EVLU_AMT_ICLD_YN": "Y",
                "OVRS_ICLD_YN": "N",
            }
            tr_id = "VTTC8908R" if config.KIS_IS_PAPER else "TTTC8908R"
            timeout = aiohttp.ClientTimeout(total=10)

            try:
                async with self._new_session() as sess:
                    async with sess.get(
                        url, headers=self._headers(tr_id), params=params, timeout=timeout
                    ) as resp:
                        status = resp.status
                        data = await resp.json()

                        if status != 200 or data.get("rt_cd") != "0":
                            rt_cd = data.get("rt_cd", "None")
                            msg_cd = data.get("msg_cd", "None")
                            msg1 = data.get("msg1", "")
                            if self._cano:
                                msg1 = msg1.replace(self._cano, "********")
                            logger.error(
                                f"❌ KIS 잔고 조회 실패: HTTP {status} | rt_cd={rt_cd} | msg_cd={msg_cd} | msg1={msg1}"
                            )
                            return {
                                "cash": self._last_cash,
                                "total": 0,
                                "error": msg1,
                                "status": status,
                                "rt_cd": rt_cd,
                                "msg_cd": msg_cd,
                            }

                        output = data.get("output", {})
                        cash = (int(output.get("ord_psbl_cash", 0)) or
                                int(output.get("dnca_tot_amt", 0)) or
                                int(output.get("nass_amt", 0)) or
                                self._last_cash)  # get_positions에서 읽은 잔고 fallback
                        if cash > 0:
                            self._last_cash = cash
                        total = int(output.get("tot_evlu_amt", 0) or 0)
                        res = {
                            "cash": cash,
                            "total": total,
                        }
                        self._balance_cache = dict(res)
                        self._balance_cache_ts = time.time()
                        return res

            except (asyncio.TimeoutError, TimeoutError):
                logger.error("❌ KIS 잔고 조회 타임아웃 (10초 초과)")
                return {"cash": self._last_cash, "total": 0, "error": "타임아웃(10초)"}
            except Exception as e:
                logger.error(f"❌ KIS 잔고 조회 예외 발생: {type(e).__name__}: {e}")
                return {"cash": self._last_cash, "total": 0, "error": str(e)}

    # ── 보유 종목 조회 ──────────────────────────────────
    async def get_positions(self) -> list:
        url = f"{self.BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
        params = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "02",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }
        tr_id = "VTTC8434R" if config.KIS_IS_PAPER else "TTTC8434R"
        for attempt in range(2):
            async with self._new_session() as sess:
              async with sess.get(
                url, headers=self._headers(tr_id), params=params
              ) as resp:
                data = await resp.json()
                if attempt == 0 and await self._refresh_token_if_expired(data):
                    continue
                positions = []
                for row in data.get("output1", []):
                    qty = int(row.get("hldg_qty", 0))
                    if qty <= 0:
                        continue
                    # 주문가능수량: 미체결/정산중 물량은 보유수량엔 잡혀도 실제로는 매도 불가
                    try:
                        sellable = int(row.get("ord_psbl_qty", qty) or qty)
                    except Exception:
                        sellable = qty
                    positions.append({
                        "symbol":    row.get("pdno"),
                        "name":      row.get("prdt_name"),
                        "qty":       qty,
                        "sellable_qty": sellable,
                        "avg_price": int(float(row.get("pchs_avg_pric", 0) or 0)),
                        "cur_price": int(row.get("prpr", 0)),
                        "pnl":       int(row.get("evlu_pfls_amt", 0)),
                        "pnl_rate":  float(row.get("evlu_pfls_rt", 0) or 0),
                    })
                # 잔고도 같이 읽기
                out2 = data.get("output2", [{}])
                if out2:
                    s = out2[0]
                    self._last_cash = int(s.get("dnca_tot_amt", 0) or 0)
                return positions
        return []

    # ── 매수 주문 ───────────────────────────────────────
    async def _refresh_token_if_expired(self, data: dict) -> bool:
        """토큰 만료 확인 후 자동 재발급, 재발급 성공 시 True"""
        msg = data.get("msg1", "")
        if data.get("rt_cd") == "1" and ("만료" in msg or "token" in msg.lower() or "EGW00" in data.get("msg_cd", "")):
            logger.warning("🔄 KIS 토큰 만료 → 자동 재발급")
            try:
                redis_key = "kis:paper_token" if config.KIS_IS_PAPER else "kis:access_token"
                await cache.client.delete(redis_key)
            except:
                pass
            self.access_token = ""
            await self._get_token()
            return True
        return False

    async def _ensure_session(self):
        """세션 끊김 시 자동 재시작"""
        if self.session is None or self.session.closed:
            logger.warning("🔄 KIS 세션 재시작")
            await self.start()

    async def buy(self, symbol: str, price: int, qty: int) -> dict:
        """지정가 매수 (토큰 만료 시 자동 재시도)
        주문 접수(rt_cd=0)는 '접수'만 의미하고 실제 체결을 보장하지 않으므로,
        접수 후 실제 체결 수량을 재조회해 진짜 성공 여부를 판정한다."""
        url = f"{self.BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        tr_id = "VTTC0802U" if config.KIS_IS_PAPER else "TTTC0802U"
        payload = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "PDNO": symbol,
            "ORD_DVSN": "00",
            "ORD_QTY": str(qty),
            "ORD_UNPR": str(price),
        }
        for attempt in range(2):
            async with self._new_session() as sess:
              async with sess.post(
                url, headers=self._headers(tr_id), json=payload
              ) as resp:
                data = await resp.json()
                rt_cd = data.get("rt_cd")
                if rt_cd != "0":
                    # 토큰 만료 → 재발급 후 재시도
                    if attempt == 0 and await self._refresh_token_if_expired(data):
                        continue
                    logger.error(f"❌ 매수 주문 접수 실패: {symbol} — {data.get('msg1')}")
                    return {"success": False, "error": data.get("msg1")}

                order_no = data.get("output", {}).get("ODNO")
                logger.info(f"📝 매수 주문 접수: {symbol} {price:,}원 × {qty}주 (주문번호 {order_no}) — 체결 확인 중")

            # 접수 성공 ≠ 체결 성공. 잠시 대기 후 실제 체결 수량을 재조회해 확정한다.
            import asyncio as _aio
            await _aio.sleep(1.5)
            filled_qty = await self._get_filled_qty(order_no, symbol, side="02")
            if filled_qty is None:
                # 체결 조회 자체가 실패하면 판정 불가 — 접수는 됐으니 보수적으로 성공 처리하되 표시
                self.invalidate_balance_cache()
                logger.warning(f"⚠️ 체결 확인 API 실패 [{symbol}] — 접수 결과만으로 판정")
                return {"success": True, "order_no": order_no, "fill_unconfirmed": True}
            if filled_qty <= 0:
                logger.error(f"❌ 매수 미체결: {symbol} 주문 {qty}주 접수됐으나 체결 0주")
                return {"success": False, "error": f"주문 접수됐으나 미체결(체결수량 0)"}
            if filled_qty < qty:
                self.invalidate_balance_cache()
                logger.warning(f"⚠️ 매수 부분체결: {symbol} {filled_qty}/{qty}주만 체결")
                return {"success": True, "order_no": order_no, "filled_qty": filled_qty, "partial": True}
            self.invalidate_balance_cache()
            logger.info(f"✅ 매수 체결 확인: {symbol} {price:,}원 × {filled_qty}주")
            return {"success": True, "order_no": order_no, "filled_qty": filled_qty}
        return {"success": False, "error": "매수 실패"}

    # ── 매도 주문 ───────────────────────────────────────
    async def sell(self, symbol: str, price: int, qty: int) -> dict:
        """시장가 매도 (토큰 만료 시 자동 재시도)
        지정가(00)는 가격이 안 맞으면 미체결/거부될 수 있어 매도는 시장가(01)로 확실히 체결
        주문 접수(rt_cd=0)는 '접수'만 의미하고 실제 체결을 보장하지 않으므로,
        접수 후 실제 체결 수량을 재조회해 진짜 성공 여부를 판정한다."""
        url = f"{self.BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        tr_id = "VTTC0801U" if config.KIS_IS_PAPER else "TTTC0801U"
        payload = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "PDNO": symbol,
            "ORD_DVSN": "01",
            "ORD_QTY": str(qty),
            "ORD_UNPR": "0",
        }
        async with self._new_session() as sess:
          async with sess.post(
            url, headers=self._headers(tr_id), json=payload
          ) as resp:
            data = await resp.json()
            rt_cd = data.get("rt_cd")
            if rt_cd != "0":
                logger.error(f"❌ 매도 주문 접수 실패: {symbol} — {data.get('msg1')}")
                return {"success": False, "error": data.get("msg1")}

            order_no = data.get("output", {}).get("ODNO")
            logger.info(f"📝 매도 주문 접수: {symbol} × {qty}주 (주문번호 {order_no}) — 체결 확인 중")

        # 접수 성공 ≠ 체결 성공. 잠시 대기 후 실제 체결 수량을 재조회해 확정한다.
        import asyncio as _aio
        await _aio.sleep(1.5)
        filled_qty, avg_fill_price = await self._get_filled_qty(order_no, symbol, with_price=True)
        if filled_qty is None:
            # 체결 조회 자체가 실패하면 판정 불가 — 접수는 됐으니 보수적으로 성공 처리하되 표시
            self.invalidate_balance_cache()
            logger.warning(f"⚠️ 체결 확인 API 실패 [{symbol}] — 접수 결과만으로 판정")
            return {"success": True, "order_no": order_no, "fill_unconfirmed": True}
        if filled_qty <= 0:
            logger.error(f"❌ 매도 미체결: {symbol} 주문 {qty}주 접수됐으나 체결 0주")
            return {"success": False, "error": f"주문 접수됐으나 미체결(체결수량 0)"}
        await self._check_sell_slippage(symbol, price, avg_fill_price)
        if filled_qty < qty:
            self.invalidate_balance_cache()
            logger.warning(f"⚠️ 매도 부분체결: {symbol} {filled_qty}/{qty}주만 체결")
            return {"success": True, "order_no": order_no, "filled_qty": filled_qty, "partial": True}
        self.invalidate_balance_cache()
        logger.info(f"✅ 매도 체결 확인: {symbol} {price:,}원 × {filled_qty}주")
        return {"success": True, "order_no": order_no, "filled_qty": filled_qty}

    async def _check_sell_slippage(
        self, symbol: str, signal_price: int, avg_fill_price: Optional[float], threshold_pct: float = 3.0
    ):
        """매도 체결가가 신호 발생 시점 가격(signal_price)보다 threshold_pct%(기본 3%) 이상
        낮으면 경고 로그와 텔레그램 알림을 남긴다. 시장가(01) 매도는 이미 체결된 뒤 확인만
        가능하므로 주문 자체를 막지 않고 감지·기록·알림까지만 수행한다."""
        if not avg_fill_price or signal_price <= 0:
            return
        slippage_pct = (avg_fill_price - signal_price) / signal_price * 100
        if slippage_pct > -threshold_pct:
            return
        logger.warning(
            f"⚠️ 매도 슬리피지 경고 [{symbol}] 신호가 {signal_price:,}원 → 체결가 {avg_fill_price:,.0f}원 "
            f"({slippage_pct:+.1f}%)"
        )
        try:
            from common.telegram import send_stock
            await send_stock(
                f"⚠️ <b>{symbol} 매도 슬리피지 경고</b>\n"
                f"신호가 {signal_price:,}원 → 체결가 {avg_fill_price:,.0f}원 ({slippage_pct:+.1f}%)\n"
                f"시장가 매도로 급락 갭 발생 — 참고용 알림입니다."
            )
        except Exception as e:
            logger.warning(f"슬리피지 텔레그램 알림 실패 [{symbol}]: {e}")

    async def _get_filled_qty(self, order_no: str, symbol: str, side: str = "01", with_price: bool = False):
        """당일 주문체결내역조회로 특정 주문번호의 실제 체결수량 확인. 실패 시 None.
        with_price=True면 (체결수량, 평균체결단가) 튜플을 반환한다(조회 실패 시 (None, None)).
        side: '01' 매도, '02' 매수"""
        if not order_no:
            return (None, None) if with_price else None
        try:
            today = datetime.now().strftime("%Y%m%d")
            tr_id = "VTTC0081R" if config.KIS_IS_PAPER else "TTTC0081R"
            params = {
                "CANO": self._cano, "ACNT_PRDT_CD": self._acnt_prdt_cd,
                "INQR_STRT_DT": today, "INQR_END_DT": today,
                "SLL_BUY_DVSN_CD": side, "INQR_DVSN": "00",
                "PDNO": symbol, "CCLD_DVSN": "00",
                "ORD_GNO_BRNO": "", "ODNO": order_no,
                "INQR_DVSN_3": "00", "INQR_DVSN_1": "",
                "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
            }
            async with self._new_session() as sess:
              async with sess.get(
                f"{self.BASE_URL}/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
                headers=self._headers(tr_id), params=params
              ) as resp:
                data = await resp.json()
            rows = data.get("output1", []) or []
            for row in rows:
                if row.get("odno") == order_no:
                    fq = int(row.get("tot_ccld_qty", 0) or 0)
                    if with_price:
                        return fq, float(row.get("avg_prvs", 0) or 0)
                    return fq
            return (0, 0.0) if with_price else 0
        except Exception as e:
            logger.warning(f"체결 조회 오류 [{symbol}]: {e}")
            return (None, None) if with_price else None

    # ── 일봉 데이터 조회 ────────────────────────────────
    async def get_daily_ohlcv(self, symbol: str, start: str, end: str) -> list:
        """
        KIS 일봉 차트 조회 (FHKST03010100)
        start/end: YYYYMMDD
        Returns: [{"date","open","high","low","close","volume","change_rate"}, ...]
        """
        url = f"{self.BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
            "FID_INPUT_DATE_1": start,
            "FID_INPUT_DATE_2": end,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
        }
        tr_id = "FHKST03010100"
        async with self._new_session() as sess:
          async with sess.get(
            url, headers=self._headers(tr_id), params=params
          ) as resp:
            data = await resp.json()

        candles = []
        for row in data.get("output2", []):
            date_str = row.get("stck_bsop_date", "")
            close    = int(row.get("stck_clpr", 0) or 0)
            if not date_str or close <= 0:
                continue
            candles.append({
                "date":        date_str,
                "open":        int(row.get("stck_oprc", 0) or 0),
                "high":        int(row.get("stck_hgpr", 0) or 0),
                "low":         int(row.get("stck_lwpr", 0) or 0),
                "close":       close,
                "volume":      int(row.get("acml_vol", 0) or 0),
                "change_rate": float(row.get("prdy_ctrt", 0) or 0),
            })
        return candles
