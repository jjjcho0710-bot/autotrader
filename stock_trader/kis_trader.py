"""
KIS API — 주문 실행 모듈
매수 / 매도 / 잔고조회 / 보유종목 조회
"""
import logging
from datetime import datetime

import aiohttp

from common.config import config

logger = logging.getLogger(__name__)


class KISTrader:
    """한국투자증권 주문 실행"""

    BASE_URL = config.kis_base_url

    def __init__(self):
        self.access_token: str = ""
        self.session: aiohttp.ClientSession = None

    async def start(self):
        self.session = aiohttp.ClientSession()
        await self._get_token()
        logger.info("✅ KISTrader 시작")

    async def stop(self):
        if self.session:
            await self.session.close()

    async def _get_token(self):
        url = f"{self.BASE_URL}/oauth2/tokenP"
        payload = {
            "grant_type": "client_credentials",
            "appkey": config.KIS_APP_KEY,
            "appsecret": config.KIS_APP_SECRET,
        }
        async with self.session.post(url, json=payload) as resp:
            data = await resp.json()
            self.access_token = data.get("access_token", "")
            logger.info("✅ KIS 토큰 발급")

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
            "appkey": config.KIS_APP_KEY,
            "appsecret": config.KIS_APP_SECRET,
            "tr_id": tr_id,
            "custtype": "P",
        }

    # ── 현재가 조회 ─────────────────────────────────────
    async def get_current_price(self, symbol: str) -> int:
        url = f"{self.BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}
        async with self.session.get(
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
        async with self.session.get(
            url, headers=self._headers(tr_id), params=params
        ) as resp:
            data = await resp.json()
            output = data.get("output", {})
            return {
                "cash":  int(output.get("ord_psbl_cash", 0)),
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
        async with self.session.get(
            url, headers=self._headers(tr_id), params=params
        ) as resp:
            data = await resp.json()
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
            return positions

    # ── 매수 주문 ───────────────────────────────────────
    async def buy(self, symbol: str, price: int, qty: int) -> dict:
        """지정가 매수"""
        url = f"{self.BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        tr_id = "VTTC0802U" if config.KIS_IS_PAPER else "TTTC0802U"
        payload = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "PDNO": symbol,
            "ORD_DVSN": "00",         # 지정가
            "ORD_QTY": str(qty),
            "ORD_UNPR": str(price),
        }
        async with self.session.post(
            url, headers=self._headers(tr_id), json=payload
        ) as resp:
            data = await resp.json()
            rt_cd = data.get("rt_cd")
            if rt_cd == "0":
                logger.info(f"✅ 매수 체결: {symbol} {price:,}원 × {qty}주")
                return {"success": True, "order_no": data.get("output", {}).get("ODNO")}
            else:
                logger.error(f"❌ 매수 실패: {symbol} — {data.get('msg1')}")
                return {"success": False, "error": data.get("msg1")}

    # ── 매도 주문 ───────────────────────────────────────
    async def sell(self, symbol: str, price: int, qty: int) -> dict:
        """지정가 매도"""
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
        async with self.session.post(
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
