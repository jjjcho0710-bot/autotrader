"""
KIS API — 주문 실행 모듈
매수 / 매도 / 잔고조회 / 보유종목 조회
"""
import logging
from datetime import datetime, timedelta

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

    async def start(self):
        await self._get_token()
        logger.info("✅ KISTrader 시작")

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
        # Redis에서 토큰 재사용
        try:
            cached = await cache.client.get("kis:access_token")
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
        async with self._new_session() as sess:
          async with sess.post(url, json=payload) as resp:
            data = await resp.json()
            token = data.get("access_token", "")
            if token:
                self.access_token = token
                try:
                    await cache.client.setex("kis:access_token", 82800, token)
                except Exception:
                    pass
                logger.info("✅ KIS 토큰 발급 완료 (23시간 유효)")

    @property
    def _cano(self) -> str:
        """계좌번호 앞자리"""
        parts = config.KIS_ACCOUNT_NO.split("-")
        return parts[0] if parts else config.KIS_ACCOUNT_NO

    @property
    def _acnt_prdt_cd(self) -> str:
        """계좌번호 뒷자리 (상품코드)"""
        parts = config.KIS_ACCOUNT_NO.split("-")
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
        """예수금 + 총평가금액 조회"""
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
        async with self._new_session() as sess:
          async with sess.get(
            url, headers=self._headers(tr_id), params=params
          ) as resp:
            data = await resp.json()
            output = data.get("output", {})
            cash = (int(output.get("ord_psbl_cash", 0)) or
                    int(output.get("dnca_tot_amt", 0)) or
                    int(output.get("nass_amt", 0)) or
                    self._last_cash)  # get_positions에서 읽은 잔고 fallback
            if cash > 0:
                self._last_cash = cash
            return {
                "cash":  cash,
                "total": int(output.get("tot_evlu_amt", 0)),
            }

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
                    positions.append({
                        "symbol":    row.get("pdno"),
                        "name":      row.get("prdt_name"),
                        "qty":       qty,
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
                await cache.client.delete("kis:access_token")
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
        """지정가 매수 (토큰 만료 시 자동 재시도)"""
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
                if rt_cd == "0":
                    logger.info(f"✅ 매수 체결: {symbol} {price:,}원 × {qty}주")
                    return {"success": True, "order_no": data.get("output", {}).get("ODNO")}
                # 토큰 만료 → 재발급 후 재시도
                if attempt == 0 and await self._refresh_token_if_expired(data):
                    continue
                logger.error(f"❌ 매수 실패: {symbol} — {data.get('msg1')}")
                return {"success": False, "error": data.get("msg1")}
        return {"success": False, "error": "매수 실패"}

    # ── 매도 주문 ───────────────────────────────────────
    async def sell(self, symbol: str, price: int, qty: int) -> dict:
        """지정가 매도 (토큰 만료 시 자동 재시도)"""
        url = f"{self.BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        tr_id = "VTTC0801U" if config.KIS_IS_PAPER else "TTTC0801U"
        payload = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "PDNO": symbol,
            "ORD_DVSN": "00",
            "ORD_QTY": str(qty),
            "ORD_UNPR": str(price),
        }
        async with self._new_session() as sess:
          async with sess.post(
            url, headers=self._headers(tr_id), json=payload
          ) as resp:
            data = await resp.json()
            rt_cd = data.get("rt_cd")
            if rt_cd == "0":
                logger.info(f"✅ 매도 체결: {symbol} {price:,}원 × {qty}주")
                return {"success": True, "order_no": data.get("output", {}).get("ODNO")}
            else:
                logger.error(f"❌ 매도 실패: {symbol} — {data.get('msg1')}")
                return {"success": False, "error": data.get("msg1")}

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
