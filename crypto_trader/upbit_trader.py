"""
업비트 API — 주문 실행 모듈
매수 / 매도 / 잔고 / 보유 코인 조회
"""
import hashlib
import logging
import uuid
from urllib.parse import urlencode

import aiohttp
import jwt

from common.config import config

logger = logging.getLogger(__name__)

UPBIT_API = "https://api.upbit.com/v1"


class UpbitTrader:
    """업비트 주문 실행"""

    def __init__(self):
        self.session: aiohttp.ClientSession = None

    async def start(self):
        self.session = aiohttp.ClientSession()
        logger.info("✅ UpbitTrader 시작")

    async def stop(self):
        if self.session:
            await self.session.close()

    def _auth_header(self, query: dict = None) -> dict:
        """JWT 인증 헤더 생성"""
        payload = {
            "access_key": config.UPBIT_ACCESS_KEY,
            "nonce": str(uuid.uuid4()),
        }
        if query:
            query_str = urlencode(query)
            m = hashlib.sha512()
            m.update(query_str.encode())
            payload["query_hash"] = m.hexdigest()
            payload["query_hash_alg"] = "SHA512"

        token = jwt.encode(payload, config.UPBIT_SECRET_KEY, algorithm="HS256")
        return {"Authorization": f"Bearer {token}"}

    # ── 잔고 조회 ───────────────────────────────────────
    async def get_balance(self, currency: str = "KRW") -> float:
        async with self.session.get(
            f"{UPBIT_API}/accounts",
            headers=self._auth_header(),
        ) as resp:
            data = await resp.json()
            for item in data:
                if item.get("currency") == currency:
                    return float(item.get("balance", 0))
            return 0.0

    async def get_all_balances(self) -> list:
        async with self.session.get(
            f"{UPBIT_API}/accounts",
            headers=self._auth_header(),
        ) as resp:
            return await resp.json()

    # ── 현재가 조회 ─────────────────────────────────────
    async def get_current_price(self, pair: str) -> float:
        async with self.session.get(
            f"{UPBIT_API}/ticker",
            params={"markets": pair},
        ) as resp:
            data = await resp.json()
            return float(data[0].get("trade_price", 0)) if data else 0.0

    # ── 보유 코인 조회 ──────────────────────────────────
    async def get_positions(self) -> list:
        balances = await self.get_all_balances()
        positions = []
        for b in balances:
            if b["currency"] == "KRW":
                continue
            qty = float(b.get("balance", 0))
            if qty < 0.00001:
                continue
            avg = float(b.get("avg_buy_price", 0))
            pair = f"KRW-{b['currency']}"
            cur = await self.get_current_price(pair)
            positions.append({
                "pair":      pair,
                "currency":  b["currency"],
                "qty":       qty,
                "avg_price": avg,
                "cur_price": cur,
                "pnl":       (cur - avg) * qty,
                "pnl_rate":  (cur - avg) / avg * 100 if avg > 0 else 0,
            })
        return positions

    # ── 매수 (시장가) ───────────────────────────────────
    async def buy_market(self, pair: str, amount_krw: float) -> dict:
        """시장가 매수 — amount_krw: 원화 금액"""
        query = {
            "market": pair,
            "side": "bid",
            "price": str(amount_krw),
            "ord_type": "price",   # 시장가 매수
        }
        async with self.session.post(
            f"{UPBIT_API}/orders",
            headers=self._auth_header(query),
            json=query,
        ) as resp:
            data = await resp.json()
            if "uuid" in data:
                logger.info(f"✅ 매수 주문 [{pair}] {amount_krw:,.0f}원")
                return {"success": True, "uuid": data["uuid"], "data": data}
            else:
                logger.error(f"❌ 매수 실패 [{pair}]: {data}")
                return {"success": False, "error": str(data)}

    # ── 매도 (시장가) ───────────────────────────────────
    async def sell_market(self, pair: str, volume: float) -> dict:
        """시장가 매도 — volume: 코인 수량"""
        query = {
            "market": pair,
            "side": "ask",
            "volume": str(volume),
            "ord_type": "market",  # 시장가 매도
        }
        async with self.session.post(
            f"{UPBIT_API}/orders",
            headers=self._auth_header(query),
            json=query,
        ) as resp:
            data = await resp.json()
            if "uuid" in data:
                logger.info(f"✅ 매도 주문 [{pair}] {volume} 코인")
                return {"success": True, "uuid": data["uuid"], "data": data}
            else:
                logger.error(f"❌ 매도 실패 [{pair}]: {data}")
                return {"success": False, "error": str(data)}
