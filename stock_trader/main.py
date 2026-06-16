"""
stock-trader — AutoTrader
한국주식 자동매매 메인 프로세스
DB에서 전략 설정 읽기 + Redis 실시간 전략 변경 구독
"""
import asyncio
import json
import logging
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

MARKET_OPEN  = time(9, 0)
MARKET_CLOSE = time(15, 30)


class StockTrader:
    def __init__(self):
        self.running   = False
        self.trader    = KISTrader()
        self.positions = {}
        self.strategies = {}   # name → {is_active, config}

    # ── DB에서 전략 설정 로드 ────────────────────────────
    async def load_strategies(self):
        try:
            async with db.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT name, is_active, params FROM strategy_config WHERE bot='stock_trader'"
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
        """활성화된 첫 번째 전략 반환"""
        for name, s in self.strategies.items():
            if s["is_active"]:
                return name, s["params"]
        return None, {}

    def build_strategy(self, name, params):
        """전략 객체 생성"""
        if name == "MA크로스":
            return MACrossStrategy(MACrossConfig(
                short_period  = int(params.get("short", 5)),
                long_period   = int(params.get("long", 20)),
                stop_loss     = float(params.get("stop_loss", -0.02)),
                take_profit   = float(params.get("take_profit", 0.05)),
                buy_amount    = int(params.get("buy_amount", 500000)),
                max_positions = int(params.get("max_positions", 5)),
            ))
        # 추후 RSI, 볼린저 등 추가
        return None

    # ── Redis 전략 변경 구독 ─────────────────────────────
    async def subscribe_strategy_updates(self):
        try:
            pubsub = cache.client.pubsub()
            await pubsub.subscribe("strategy:update")
            logger.info("📡 전략 변경 구독 시작")
            async for msg in pubsub.listen():
                if msg["type"] == "message":
                    data = json.loads(msg["data"])
                    if data.get("bot") == "stock_trader":
                        logger.info(f"🔄 전략 변경 감지: {data['name']} → {'ON' if data['is_active'] else 'OFF'}")
                        await self.load_strategies()
        except Exception as e:
            logger.error(f"전략 구독 오류: {e}")

    # ── 시작 ─────────────────────────────────────────────
    async def start(self):
        logger.info("=" * 50)
        logger.info("🚀 AutoTrader stock-trader 시작")
        logger.info("=" * 50)

        await db.connect()
        await cache.connect()
        await self.trader.start()
        await self.load_strategies()

        # 보유 포지션 로드
        try:
            positions = await self.trader.get_positions()
            self.positions = {p["symbol"]: p for p in positions if isinstance(p, dict) and p.get("symbol")}
            logger.info(f"📊 보유 종목: {list(self.positions.keys())}")
        except Exception as e:
            logger.warning(f"보유 포지션 로드 실패 (무시): {e}")
            self.positions = {}

        await cache.set_bot_status("stock_trader", {
            "status": "running",
            "started_at": datetime.now().isoformat(),
        })

        self.running = True

        # 매매 루프 + 전략 구독 동시 실행
        await asyncio.gather(
            self._loop(),
            self.subscribe_strategy_updates(),
        )

    # ── 메인 루프 ─────────────────────────────────────────
    async def _loop(self):
        while self.running:
            now = datetime.now()
            cur_time = now.time()

            if not (MARKET_OPEN <= cur_time <= MARKET_CLOSE):
                logger.info(f"🕐 장외 시간 [{cur_time.strftime('%H:%M')}] — 대기")
                await asyncio.sleep(60)
                continue

            logger.info(f"🔄 매매 사이클 [{now.strftime('%H:%M:%S')}]")

            try:
                await self._run_cycle()
            except Exception as e:
                logger.error(f"❌ 사이클 오류: {e}")
                await self._notify_error(str(e))

            await asyncio.sleep(config.COLLECT_INTERVAL_SEC)

    # ── 매매 사이클 ───────────────────────────────────────
    async def _run_cycle(self):
        # 활성 전략 확인
        strat_name, params = self.get_active_strategy()
        if not strat_name:
            logger.info("⏸️ 활성화된 전략 없음 — 대기")
            return

        strategy = self.build_strategy(strat_name, params)
        if not strategy:
            logger.warning(f"⚠️ 전략 객체 생성 실패: {strat_name}")
            return

        max_positions = int(params.get("max_positions", 5))

        # ① 보유 포지션 손절/익절 체크
        try:
            positions = await self.trader.get_positions()
            self.positions = {p["symbol"]: p for p in positions if isinstance(p, dict) and p.get("symbol")}
        except Exception as e:
            logger.warning(f"포지션 조회 실패: {e}")
            self.positions = {}

        for symbol, pos in self.positions.items():
            cur_price = pos["cur_price"]
            avg_price = pos["avg_price"]

            if strategy.check_stop_loss(avg_price, cur_price):
                result = await self.trader.sell(symbol, cur_price, pos["qty"])
                if result["success"]:
                    pnl = (cur_price - avg_price) * pos["qty"]
                    await db.insert_trade(
                        bot="stock_trader", asset_type="stock",
                        symbol=symbol, side="SELL",
                        price=cur_price, quantity=pos["qty"],
                        amount=cur_price * pos["qty"],
                        strategy=f"{strat_name}_손절", pnl=pnl,
                    )
                    await self._notify(f"🛑 손절 [{symbol}] {cur_price:,}원 × {pos['qty']}주 / PnL: {pnl:+,}원")
                continue

            if strategy.check_take_profit(avg_price, cur_price):
                result = await self.trader.sell(symbol, cur_price, pos["qty"])
                if result["success"]:
                    pnl = (cur_price - avg_price) * pos["qty"]
                    await db.insert_trade(
                        bot="stock_trader", asset_type="stock",
                        symbol=symbol, side="SELL",
                        price=cur_price, quantity=pos["qty"],
                        amount=cur_price * pos["qty"],
                        strategy=f"{strat_name}_익절", pnl=pnl,
                    )
                    await self._notify(f"🎯 익절 [{symbol}] {cur_price:,}원 × {pos['qty']}주 / PnL: {pnl:+,}원")
                continue

        # ② 신규 진입 신호 체크
        if len(self.positions) >= max_positions:
            logger.info(f"⚠️ 최대 보유 종목수 ({len(self.positions)}/{max_positions})")
            return

        for symbol in config.STOCK_SYMBOLS:
            if symbol in self.positions:
                continue

            rows = await db.get_recent_ohlcv(symbol, limit=25, asset="stock")
            if len(rows) < 21:
                continue

            prices = [r["close"] for r in reversed(rows)]
            signal_type = strategy.generate_signal(symbol, prices)

            if signal_type == "BUY":
                cur_price = await self.trader.get_current_price(symbol)
                if cur_price <= 0:
                    continue
                qty = strategy.calc_buy_qty(cur_price)
                result = await self.trader.buy(symbol, cur_price, qty)
                if result["success"]:
                    await db.insert_trade(
                        bot="stock_trader", asset_type="stock",
                        symbol=symbol, side="BUY",
                        price=cur_price, quantity=qty,
                        amount=cur_price * qty,
                        strategy=strat_name,
                    )
                    await self._notify(f"📈 매수 [{symbol}] {cur_price:,}원 × {qty}주 ({strat_name})")

        # 상태 업데이트
        await cache.set_bot_status("stock_trader", {
            "status":     "running",
            "last_cycle": datetime.now().isoformat(),
            "positions":  len(self.positions),
            "strategy":   strat_name,
        })

    # ── 알림 ──────────────────────────────────────────────
    async def _notify(self, msg: str):
        logger.info(f"📣 {msg}")
        try:
            from common.telegram import send_stock
            await send_stock(f"[stock-trader]\n{msg}")
        except Exception as e:
            logger.warning(f"텔레그램 전송 실패: {e}")

    async def _notify_error(self, error: str):
        try:
            from common.telegram import notify_error
            await notify_error("stock_trader", error)
        except:
            pass

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
