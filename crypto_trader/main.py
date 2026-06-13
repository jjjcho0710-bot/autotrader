"""
crypto-trader — AutoTrader
비트코인 자동매매 메인 프로세스
업비트 API + MACD 전략 (24시간)
"""
import asyncio
import logging
import os
import signal
from datetime import datetime

from common.config import config
from common.database import db, cache
from upbit_trader import UpbitTrader
from strategy.macd import MACDStrategy, MACDConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("crypto-trader")

STRATEGY_CONFIG = MACDConfig(
    fast=int(os.getenv("MACD_FAST", "12")),
    slow=int(os.getenv("MACD_SLOW", "26")),
    signal=int(os.getenv("MACD_SIGNAL", "9")),
    stop_loss=float(os.getenv("STOP_LOSS", "-0.03")),
    take_profit=float(os.getenv("TAKE_PROFIT", "0.07")),
    buy_amount_krw=float(os.getenv("BUY_AMOUNT", "500000")),
)


class CryptoTrader:
    def __init__(self):
        self.running = False
        self.trader = UpbitTrader()
        self.strategy = MACDStrategy(STRATEGY_CONFIG)
        self.positions: dict = {}

    async def start(self):
        logger.info("=" * 50)
        logger.info("🚀 AutoTrader crypto-trader 시작")
        logger.info(f"   전략: MACD ({STRATEGY_CONFIG.fast}/{STRATEGY_CONFIG.slow}/{STRATEGY_CONFIG.signal})")
        logger.info(f"   손절: {STRATEGY_CONFIG.stop_loss:.1%} / 익절: {STRATEGY_CONFIG.take_profit:.1%}")
        logger.info(f"   1회 매수: {STRATEGY_CONFIG.buy_amount_krw:,.0f}원")
        logger.info("=" * 50)

        await db.connect()
        await cache.connect()
        await self.trader.start()

        positions = await self.trader.get_positions()
        self.positions = {p["pair"]: p for p in positions}
        logger.info(f"₿ 보유 코인: {list(self.positions.keys())}")

        await cache.set_bot_status("crypto_trader", {
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "strategy": "MACD",
        })

        self.running = True
        await self._loop()

    async def _loop(self):
        while self.running:
            logger.info(f"🔄 코인 매매 사이클 [{datetime.now().strftime('%H:%M:%S')}]")
            try:
                await self._run_cycle()
            except Exception as e:
                logger.error(f"❌ 사이클 오류: {e}")
                await cache.set_bot_status("crypto_trader", {
                    "status": "error", "error": str(e),
                    "ts": datetime.now().isoformat(),
                })
            await asyncio.sleep(config.COLLECT_INTERVAL_SEC)

    async def _run_cycle(self):
        # ① 보유 코인 손절/익절 체크
        positions = await self.trader.get_positions()
        self.positions = {p["pair"]: p for p in positions}

        for pair, pos in self.positions.items():
            avg = pos["avg_price"]
            cur = pos["cur_price"]
            qty = pos["qty"]

            if self.strategy.check_stop_loss(avg, cur):
                result = await self.trader.sell_market(pair, qty)
                if result["success"]:
                    pnl = (cur - avg) * qty
                    await db.insert_trade(
                        bot="crypto_trader", asset_type="crypto",
                        symbol=pair, side="SELL",
                        price=cur, quantity=qty,
                        amount=cur * qty,
                        strategy="MACD_손절", pnl=pnl,
                    )
                    await self._notify(f"🛑 손절 [{pair}] PnL: {pnl:+,.0f}원")
                continue

            if self.strategy.check_take_profit(avg, cur):
                result = await self.trader.sell_market(pair, qty)
                if result["success"]:
                    pnl = (cur - avg) * qty
                    await db.insert_trade(
                        bot="crypto_trader", asset_type="crypto",
                        symbol=pair, side="SELL",
                        price=cur, quantity=qty,
                        amount=cur * qty,
                        strategy="MACD_익절", pnl=pnl,
                    )
                    await self._notify(f"🎯 익절 [{pair}] PnL: {pnl:+,.0f}원")
                continue

        # ② 신규 진입 신호 체크
        krw_balance = await self.trader.get_balance("KRW")

        for pair in config.CRYPTO_PAIRS:
            if pair in self.positions:
                continue
            if krw_balance < STRATEGY_CONFIG.buy_amount_krw:
                logger.info(f"💸 KRW 잔고 부족 ({krw_balance:,.0f}원)")
                break

            # DB에서 최근 종가
            rows = await db.get_recent_ohlcv(pair, limit=50, asset="crypto")
            if len(rows) < 40:
                continue

            prices = [float(r["close"]) for r in reversed(rows)]
            signal_type = self.strategy.generate_signal(pair, prices)

            if signal_type == "BUY":
                cur_price = await self.trader.get_current_price(pair)
                result = await self.trader.buy_market(pair, STRATEGY_CONFIG.buy_amount_krw)
                if result["success"]:
                    qty_bought = STRATEGY_CONFIG.buy_amount_krw / cur_price
                    await db.insert_trade(
                        bot="crypto_trader", asset_type="crypto",
                        symbol=pair, side="BUY",
                        price=cur_price, quantity=qty_bought,
                        amount=STRATEGY_CONFIG.buy_amount_krw,
                        strategy="MACD",
                    )
                    await self._notify(f"📈 매수 [{pair}] {STRATEGY_CONFIG.buy_amount_krw:,.0f}원 (MACD 골든크로스)")
                    krw_balance -= STRATEGY_CONFIG.buy_amount_krw

        await cache.set_bot_status("crypto_trader", {
            "status": "running",
            "last_cycle": datetime.now().isoformat(),
            "positions": len(self.positions),
            "krw_balance": krw_balance,
        })

    async def _notify(self, msg: str):
        logger.info(f"📣 {msg}")
        try:
            import aiohttp
            url = f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/sendMessage"
            async with aiohttp.ClientSession() as s:
                await s.post(url, json={
                    "chat_id": config.TELEGRAM_CHAT_ID,
                    "text": f"[crypto-trader]\n{msg}",
                })
        except Exception as e:
            logger.warning(f"텔레그램 전송 실패: {e}")

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
