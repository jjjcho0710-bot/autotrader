"""
crypto-trader — AutoTrader
비트코인 자동매매 메인 프로세스
DB에서 전략 설정 읽기 + Redis 실시간 전략 변경 구독
"""
import asyncio
import json
import logging
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


class CryptoTrader:
    def __init__(self):
        self.running    = False
        self.trader     = UpbitTrader()
        self.positions  = {}
        self.strategies = {}

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
            return MACDStrategy(MACDConfig(
                fast           = int(params.get("fast", 12)),
                slow           = int(params.get("slow", 26)),
                signal         = int(params.get("signal", 9)),
                stop_loss      = float(params.get("stop_loss", -0.03)),
                take_profit    = float(params.get("take_profit", 0.07)),
                buy_amount_krw = float(params.get("buy_amount", 500000)),
            ))
        return None

    async def subscribe_strategy_updates(self):
        try:
            pubsub = cache.client.pubsub()
            await pubsub.subscribe("strategy:update")
            logger.info("📡 전략 변경 구독 시작")
            async for msg in pubsub.listen():
                if msg["type"] == "message":
                    data = json.loads(msg["data"])
                    if data.get("bot") == "crypto_trader":
                        logger.info(f"🔄 전략 변경 감지: {data['name']} → {'ON' if data['is_active'] else 'OFF'}")
                        await self.load_strategies()
        except Exception as e:
            logger.error(f"전략 구독 오류: {e}")

    async def start(self):
        logger.info("=" * 50)
        logger.info("🚀 AutoTrader crypto-trader 시작")
        logger.info("=" * 50)

        await db.connect()
        await cache.connect()
        await self.trader.start()
        await self.load_strategies()

        positions = await self.trader.get_positions()
        self.positions = {p["pair"]: p for p in positions}
        logger.info(f"₿ 보유 코인: {list(self.positions.keys())}")

        await cache.set_bot_status("crypto_trader", {
            "status": "running",
            "started_at": datetime.now().isoformat(),
        })

        self.running = True

        await asyncio.gather(
            self._loop(),
            self.subscribe_strategy_updates(),
        )

    async def _loop(self):
        while self.running:
            logger.info(f"🔄 코인 매매 사이클 [{datetime.now().strftime('%H:%M:%S')}]")
            try:
                await self._run_cycle()
            except Exception as e:
                logger.error(f"❌ 사이클 오류: {e}")
                await self._notify_error(str(e))
            await asyncio.sleep(config.COLLECT_INTERVAL_SEC)

    async def _run_cycle(self):
        strat_name, params = self.get_active_strategy()
        if not strat_name:
            logger.info("⏸️ 활성화된 전략 없음 — 대기")
            return

        strategy = self.build_strategy(strat_name, params)
        if not strategy:
            return

        buy_amount = float(params.get("buy_amount", 500000))

        # ① 손절/익절 체크
        positions = await self.trader.get_positions()
        self.positions = {p["pair"]: p for p in positions}

        for pair, pos in self.positions.items():
            avg = pos["avg_price"]
            cur = pos["cur_price"]
            qty = pos["qty"]

            if strategy.check_stop_loss(avg, cur):
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
                    await self._notify(f"🛑 손절 [{pair}] PnL: {pnl:+,.0f}원")
                continue

            if strategy.check_take_profit(avg, cur):
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
                    await self._notify(f"🎯 익절 [{pair}] PnL: {pnl:+,.0f}원")
                continue

        # ② 신규 진입
        krw_balance = await self.trader.get_balance("KRW")

        for pair in config.CRYPTO_PAIRS:
            if pair in self.positions:
                continue
            if krw_balance < buy_amount:
                logger.info(f"💸 KRW 잔고 부족 ({krw_balance:,.0f}원)")
                break

            rows = await db.get_recent_ohlcv(pair, limit=50, asset="crypto")
            if len(rows) < 40:
                continue

            prices = [float(r["close"]) for r in reversed(rows)]
            signal_type = strategy.generate_signal(pair, prices)

            if signal_type == "BUY":
                cur_price = await self.trader.get_current_price(pair)
                result = await self.trader.buy_market(pair, buy_amount)
                if result["success"]:
                    qty_bought = buy_amount / cur_price
                    await db.insert_trade(
                        bot="crypto_trader", asset_type="crypto",
                        symbol=pair, side="BUY",
                        price=cur_price, quantity=qty_bought,
                        amount=buy_amount, strategy=strat_name,
                    )
                    await self._notify(f"📈 매수 [{pair}] {buy_amount:,.0f}원 ({strat_name})")
                    krw_balance -= buy_amount

        await cache.set_bot_status("crypto_trader", {
            "status":      "running",
            "last_cycle":  datetime.now().isoformat(),
            "positions":   len(self.positions),
            "strategy":    strat_name,
            "krw_balance": krw_balance,
        })

    async def _notify(self, msg: str):
        logger.info(f"📣 {msg}")
        try:
            from common.telegram import send_crypto
            await send_crypto(f"[crypto-trader]\n{msg}")
        except Exception as e:
            logger.warning(f"텔레그램 전송 실패: {e}")

    async def _notify_error(self, error: str):
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
