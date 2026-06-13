"""
stock-trader — AutoTrader
한국주식 자동매매 메인 프로세스
KIS API + MA크로스 전략
"""
import asyncio
import logging
import os
import signal
from datetime import datetime, time

from common.config import config
from common.database import db, cache
from kis_trader import KISTrader
from strategy.ma_cross import MACrossStrategy, MACrossConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("stock-trader")

# 장 운영시간
MARKET_OPEN  = time(9, 0)
MARKET_CLOSE = time(15, 30)

# 전략 설정 (환경변수 override 가능)
STRATEGY_CONFIG = MACrossConfig(
    short_period=int(os.getenv("MA_SHORT", "5")),
    long_period=int(os.getenv("MA_LONG", "20")),
    stop_loss=float(os.getenv("STOP_LOSS", "-0.02")),
    take_profit=float(os.getenv("TAKE_PROFIT", "0.05")),
    buy_amount=int(os.getenv("BUY_AMOUNT", "500000")),
    max_positions=int(os.getenv("MAX_POSITIONS", "5")),
)


class StockTrader:
    def __init__(self):
        self.running = False
        self.trader = KISTrader()
        self.strategy = MACrossStrategy(STRATEGY_CONFIG)
        self.positions: dict = {}   # symbol → position info

    async def start(self):
        logger.info("=" * 50)
        logger.info("🚀 AutoTrader stock-trader 시작")
        logger.info(f"   전략: MA크로스 (MA{STRATEGY_CONFIG.short_period}/MA{STRATEGY_CONFIG.long_period})")
        logger.info(f"   손절: {STRATEGY_CONFIG.stop_loss:.1%} / 익절: {STRATEGY_CONFIG.take_profit:.1%}")
        logger.info(f"   1회 매수: {STRATEGY_CONFIG.buy_amount:,}원 / 최대 {STRATEGY_CONFIG.max_positions}종목")
        logger.info("=" * 50)

        await db.connect()
        await cache.connect()
        await self.trader.start()

        # 현재 보유 포지션 로드
        self.positions = {
            p["symbol"]: p for p in await self.trader.get_positions()
        }
        logger.info(f"📊 보유 종목: {list(self.positions.keys())}")

        await cache.set_bot_status("stock_trader", {
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "strategy": "MA크로스",
        })

        self.running = True
        await self._loop()

    async def _loop(self):
        while self.running:
            now = datetime.now()
            cur_time = now.time()

            # 장 시간 체크
            if not (MARKET_OPEN <= cur_time <= MARKET_CLOSE):
                logger.info(f"🕐 장외 시간 [{cur_time.strftime('%H:%M')}] — 대기")
                await asyncio.sleep(60)
                continue

            logger.info(f"🔄 매매 사이클 [{now.strftime('%H:%M:%S')}]")

            try:
                await self._run_cycle()
            except Exception as e:
                logger.error(f"❌ 사이클 오류: {e}")
                await cache.set_bot_status("stock_trader", {
                    "status": "error", "error": str(e),
                    "ts": datetime.now().isoformat(),
                })

            await asyncio.sleep(config.COLLECT_INTERVAL_SEC)

    async def _run_cycle(self):
        """한 사이클 실행 — 신호 체크 + 손절익절 + 주문"""

        # ① 보유 포지션 손절/익절 체크
        positions = await self.trader.get_positions()
        self.positions = {p["symbol"]: p for p in positions}

        for symbol, pos in self.positions.items():
            cur_price = pos["cur_price"]
            avg_price = pos["avg_price"]

            # 손절
            if self.strategy.check_stop_loss(avg_price, cur_price):
                result = await self.trader.sell(symbol, cur_price, pos["qty"])
                if result["success"]:
                    pnl = (cur_price - avg_price) * pos["qty"]
                    await db.insert_trade(
                        bot="stock_trader", asset_type="stock",
                        symbol=symbol, side="SELL",
                        price=cur_price, quantity=pos["qty"],
                        amount=cur_price * pos["qty"],
                        strategy="MA크로스_손절", pnl=pnl,
                    )
                    await self._notify(f"🛑 손절 [{symbol}] {cur_price:,}원 × {pos['qty']}주 / PnL: {pnl:+,}원")
                continue

            # 익절
            if self.strategy.check_take_profit(avg_price, cur_price):
                result = await self.trader.sell(symbol, cur_price, pos["qty"])
                if result["success"]:
                    pnl = (cur_price - avg_price) * pos["qty"]
                    await db.insert_trade(
                        bot="stock_trader", asset_type="stock",
                        symbol=symbol, side="SELL",
                        price=cur_price, quantity=pos["qty"],
                        amount=cur_price * pos["qty"],
                        strategy="MA크로스_익절", pnl=pnl,
                    )
                    await self._notify(f"🎯 익절 [{symbol}] {cur_price:,}원 × {pos['qty']}주 / PnL: {pnl:+,}원")
                continue

        # ② 신규 진입 신호 체크
        if len(self.positions) >= STRATEGY_CONFIG.max_positions:
            logger.info(f"⚠️ 최대 보유 종목수 도달 ({len(self.positions)}/{STRATEGY_CONFIG.max_positions})")
            return

        for symbol in config.STOCK_SYMBOLS:
            if symbol in self.positions:
                continue  # 이미 보유중

            # DB에서 최근 종가 조회
            rows = await db.get_recent_ohlcv(symbol, limit=25, asset="stock")
            if len(rows) < 21:
                continue

            prices = [r["close"] for r in reversed(rows)]
            signal_type = self.strategy.generate_signal(symbol, prices)

            if signal_type == "BUY":
                cur_price = await self.trader.get_current_price(symbol)
                if cur_price <= 0:
                    continue
                qty = self.strategy.calc_buy_qty(cur_price)
                result = await self.trader.buy(symbol, cur_price, qty)
                if result["success"]:
                    await db.insert_trade(
                        bot="stock_trader", asset_type="stock",
                        symbol=symbol, side="BUY",
                        price=cur_price, quantity=qty,
                        amount=cur_price * qty,
                        strategy="MA크로스",
                    )
                    await self._notify(f"📈 매수 [{symbol}] {cur_price:,}원 × {qty}주 (MA크로스)")

        # 상태 업데이트
        await cache.set_bot_status("stock_trader", {
            "status": "running",
            "last_cycle": datetime.now().isoformat(),
            "positions": len(self.positions),
        })

    async def _notify(self, msg: str):
        """텔레그램 알림"""
        logger.info(f"📣 {msg}")
        try:
            import aiohttp
            url = f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/sendMessage"
            async with aiohttp.ClientSession() as s:
                await s.post(url, json={
                    "chat_id": config.TELEGRAM_CHAT_ID,
                    "text": f"[stock-trader]\n{msg}",
                })
        except Exception as e:
            logger.warning(f"텔레그램 전송 실패: {e}")

    async def stop(self):
        logger.info("🛑 stock-trader 종료 중...")
        self.running = False
        await self.trader.stop()
        await db.disconnect()
        await cache.disconnect()
        logger.info("✅ 종료 완료")


async def main():
    trader = StockTrader()
    loop = asyncio.get_event_loop()

    def shutdown():
        loop.create_task(trader.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown)

    await trader.start()


if __name__ == "__main__":
    asyncio.run(main())
