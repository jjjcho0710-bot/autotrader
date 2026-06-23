"""
crypto-trader — AutoTrader
업비트 코인 자동매매 메인 프로세스
Jarvis AI 최종 판단 → 자동 매수/매도
"""
import asyncio
import json
import logging
import signal
import time as _time
from datetime import datetime, timezone, timedelta

from common.config import config
from common.database import db, cache
from upbit_trader import UpbitTrader
from strategy.macd import MACDStrategy, MACDConfig

# KST 로그 포맷
class KSTFormatter(logging.Formatter):
    def converter(self, timestamp):
        return _time.gmtime(timestamp + 9 * 3600)

_fmt = KSTFormatter(
    fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logging.basicConfig(level=logging.INFO)
logging.root.handlers[0].setFormatter(_fmt)
logger = logging.getLogger("crypto-trader")

KST = timezone(timedelta(hours=9))


class CryptoTrader:
    def __init__(self):
        self.running    = False
        self.trader     = UpbitTrader()
        self.positions  = {}
        self.strategies = {}

    async def start(self):
        self.running = True
        await db.connect()
        await cache.connect()
        await self.trader.start()
        await self.load_strategies()
        logger.info("=" * 50)
        logger.info("🚀 AutoTrader crypto-trader 시작")
        logger.info("=" * 50)

        asyncio.create_task(self._subscribe_strategy_changes())
        await self._loop()

    async def load_strategies(self):
        try:
            async with db.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT name, is_active, params FROM strategy_config WHERE bot='crypto_trader'"
                )
            self.strategies = {}
            for r in rows:
                params = r["params"]
                if isinstance(params, str):
                    params = json.loads(params)
                self.strategies[r["name"]] = {
                    "is_active": r["is_active"],
                    "params":    params or {},
                }
            active = [n for n, s in self.strategies.items() if s["is_active"]]
            logger.info(f"📋 전략 로드 완료: {active}")
        except Exception as e:
            logger.error(f"전략 로드 실패: {e}")

    def get_active_strategy(self):
        for name, s in self.strategies.items():
            if s["is_active"]:
                return name, s["params"]
        return None, {}

    def build_strategy(self, name, params):
        if name == "MACD":
            cfg = MACDConfig(
                fast=int(params.get("fast", 12)),
                slow=int(params.get("slow", 26)),
                signal=int(params.get("signal", 9)),
                buy_amount_krw=float(params.get("buy_amount", 10000)),
            )
            return MACDStrategy(cfg)
        return None

    async def _subscribe_strategy_changes(self):
        try:
            pubsub = cache.client.pubsub()
            await pubsub.subscribe("strategy_changes")
            async for message in pubsub.listen():
                if message["type"] == "message":
                    logger.info(f"🔄 전략 변경 감지 — 재로드")
                    await self.load_strategies()
        except Exception as e:
            logger.warning(f"전략 구독 오류: {e}")

    async def _loop(self):
        logger.info("📡 전략 변경 구독 시작")
        # 시세 업데이트 태스크 별도 실행
        asyncio.create_task(self._price_loop())
        while self.running:
            now = datetime.now(KST)
            logger.info(f"🔄 코인 매매 사이클 [{now.strftime('%H:%M:%S')}]")
            try:
                await self._run_cycle()
            except Exception as e:
                logger.error(f"매매 사이클 오류: {e}")
                await self._notify_error(str(e))
            await asyncio.sleep(60)  # 매매는 60초

    async def _price_loop(self):
        """시세만 3초마다 Redis 업데이트"""
        import json
        while self.running:
            try:
                prices_data = {}
                for pair in config.CRYPTO_PAIRS:
                    try:
                        cur = await self.trader.get_current_price(pair)
                        if cur > 0:
                            # 변화율 계산 (이전 가격 대비)
                            prev_key = f"crypto:prev:{pair}"
                            prev = await cache.client.get(prev_key)
                            prev_price = float(prev) if prev else cur
                            change_rate = ((cur - prev_price) / prev_price * 100) if prev_price > 0 else 0
                            await cache.client.setex(prev_key, 86400, str(cur))
                            prices_data[pair] = {
                                "pair": pair,
                                "price": cur,
                                "change_rate": round(change_rate, 2),
                            }
                    except:
                        pass
                if prices_data:
                    await cache.client.setex("crypto:prices", 10, json.dumps(prices_data))
            except Exception as e:
                logger.debug(f"시세 업데이트 오류: {e}")
            await asyncio.sleep(3)  # 3초마다 시세 갱신

    async def _run_cycle(self):
        strat_name, params = self.get_active_strategy()
        if not strat_name:
            logger.info("⏸️ 활성화된 전략 없음 — 대기")
            return

        strategy = self.build_strategy(strat_name, params)
        if not strategy:
            return

        buy_amount = float(params.get("buy_amount", 10000))

        # ① 포지션 조회
        positions = await self.trader.get_positions()
        self.positions = {p["pair"]: p for p in positions}
        logger.info(f"📊 보유 코인: {list(self.positions.keys()) or '없음'}")

        # ② 손절/익절 체크
        for pair, pos in self.positions.items():
            avg = pos["avg_price"]
            cur = pos["cur_price"]
            qty = pos["qty"]

            if strategy.check_stop_loss(avg, cur):
                # 최소 매도 금액 체크 (5,000원 이상)
                if cur * qty < 5000:
                    logger.info(f"⏭️ [{pair}] 보유금액 {cur*qty:,.0f}원 < 5,000원 → 매도 스킵")
                    continue
                result = await self.trader.sell_market(pair, qty)
                if result["success"]:
                    pnl = (cur - avg) * qty
                    await db.insert_trade(
                        bot="crypto_trader", asset_type="crypto",
                        symbol=pair, side="SELL",
                        price=cur, quantity=qty,
                        amount=cur * qty,
                        strategy=f"{strat_name}_손절", pnl=pnl,
                    )
                    await self._notify(f"🛑 손절 [{pair}] {cur:,.0f}원 PnL: {pnl:+,.0f}원")
                continue

            if strategy.check_take_profit(avg, cur):
                # 최소 매도 금액 체크 (5,000원 이상)
                if cur * qty < 5000:
                    logger.info(f"⏭️ [{pair}] 보유금액 {cur*qty:,.0f}원 < 5,000원 → 매도 스킵")
                    continue
                result = await self.trader.sell_market(pair, qty)
                if result["success"]:
                    pnl = (cur - avg) * qty
                    await db.insert_trade(
                        bot="crypto_trader", asset_type="crypto",
                        symbol=pair, side="SELL",
                        price=cur, quantity=qty,
                        amount=cur * qty,
                        strategy=f"{strat_name}_익절", pnl=pnl,
                    )
                    await self._notify(f"🎯 익절 [{pair}] {cur:,.0f}원 PnL: {pnl:+,.0f}원")
                continue

        # ③ 신규 진입
        krw_balance = await self.trader.get_balance("KRW")

        for pair in config.CRYPTO_PAIRS:
            if pair in self.positions:
                continue
            if krw_balance < buy_amount:
                logger.info(f"💸 KRW 잔고 부족 ({krw_balance:,.0f}원)")
                break

            rows = await db.get_recent_ohlcv(pair, limit=50, asset="crypto")
            if len(rows) < 40:
                logger.info(f"⏳ [{pair}] 데이터 부족 ({len(rows)}개)")
                continue

            # DB는 ASC 정렬 → 오래된 것부터 → 그대로 사용
            prices = [float(r["close"]) for r in rows]
            signal_type = strategy.generate_signal(pair, prices)

            if signal_type == "BUY":
                cur_price = await self.trader.get_current_price(pair)
                if cur_price <= 0:
                    continue

                qty_would_buy = buy_amount / cur_price
                logger.info(f"📈 [{pair}] 매수 신호 발생 → Jarvis 판단 요청")

                # Jarvis 최종 판단
                await self._signal_jarvis(
                    action="BUY",
                    pair=pair,
                    price=cur_price,
                    qty=qty_would_buy,
                    amount=buy_amount,
                    strategy=strat_name,
                    reason=f"MACD 골든크로스 신호",
                )
                krw_balance -= buy_amount

        await cache.set_bot_status("crypto_trader", {
            "status":      "running",
            "last_cycle":  datetime.now(KST).isoformat(),
            "positions":   len(self.positions),
            "strategy":    strat_name,
            "krw_balance": krw_balance,
        })

        # 포지션 + 시세 Redis 캐시 저장 (dashboard에서 조회)
        import json
        positions_data = [
            {
                "pair": pair,
                "currency": pos.get("currency", pair.replace("KRW-", "")),
                "qty": pos.get("qty", 0),
                "avg_price": pos.get("avg_price", 0),
                "cur_price": pos.get("cur_price", 0),
                "pnl": pos.get("pnl", 0),
                "pnl_rate": pos.get("pnl_rate", 0),
                "name": pos.get("currency", pair.replace("KRW-", "")),
            }
            for pair, pos in self.positions.items()
        ]
        await cache.client.setex("crypto:positions", 30, json.dumps(positions_data))

        # 모니터링 코인 시세도 Redis 저장
        prices_data = {}
        for pair in config.CRYPTO_PAIRS:
            try:
                cur = await self.trader.get_current_price(pair)
                if cur > 0:
                    prices_data[pair] = {
                        "pair": pair,
                        "price": cur,
                        "change_rate": 0,
                    }
            except:
                pass
        if prices_data:
            await cache.client.setex("crypto:prices", 30, json.dumps(prices_data))

    async def _signal_jarvis(self, action: str, pair: str, price: float,
                              qty: float, amount: float, strategy: str, reason: str = ""):
        """매매 신호를 Jarvis에게 전달 → Jarvis가 판단 후 자동 실행"""
        import aiohttp as http
        import os
        dashboard_url = os.getenv("DASHBOARD_URL", "https://dashboard-production-65e3.up.railway.app")
        try:
            async with http.ClientSession() as session:
                await session.post(
                    f"{dashboard_url}/api/jarvis/signal",
                    json={
                        "bot": "crypto_trader",
                        "action": action,
                        "symbol": pair,
                        "name": pair.replace("KRW-", ""),
                        "price": price,
                        "qty": qty,
                        "amount": amount,
                        "strategy": strategy,
                        "reason": reason,
                    },
                    timeout=http.ClientTimeout(total=60),
                )
            logger.info(f"📡 Jarvis에게 신호 전달: {action} {pair}")
        except Exception as e:
            logger.error(f"Jarvis 신호 전달 실패: {e}")
            # Jarvis 실패 시 직접 매수
            if action == "BUY":
                result = await self.trader.buy_market(pair, amount)
                if result["success"]:
                    await db.insert_trade(
                        bot="crypto_trader", asset_type="crypto",
                        symbol=pair, side="BUY",
                        price=price, quantity=qty,
                        amount=amount, strategy=strategy,
                    )
                    await self._notify(f"📈 매수 [{pair}] {amount:,.0f}원 ({strategy})")

    async def _notify(self, msg: str):
        logger.info(f"📣 {msg}")
        try:
            from common.telegram import send_crypto
            await send_crypto(f"[crypto-trader]\n{msg}")
        except Exception as e:
            logger.warning(f"텔레그램 전송 실패: {e}")

    async def _notify_error(self, error: str):
        import os
        if os.getenv("CRYPTO_NOTIFY_ERRORS", "false").lower() != "true":
            return
        try:
            from common.telegram import notify_error
            await notify_error("crypto_trader", error)
        except:
            pass

    async def stop(self):
        logger.info("🛑 crypto-trader 종료 중...")
        self.running = False
        await self.trader.stop()
        await db.disconnect()
        await cache.disconnect()
        logger.info("✅ 종료 완료")


async def main():
    trader = CryptoTrader()
    loop = asyncio.get_event_loop()

    def shutdown():
        loop.create_task(trader.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown)

    await trader.start()


if __name__ == "__main__":
    asyncio.run(main())
