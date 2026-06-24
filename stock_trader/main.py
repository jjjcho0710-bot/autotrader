"""
stock-trader — AutoTrader
한국주식 자동매매 메인 프로세스
DB에서 전략 설정 읽기 + Redis 실시간 전략 변경 구독
"""
import asyncio
import json
import logging
import signal
from datetime import datetime, time, timezone, timedelta

from common.config import config
from common.database import db, cache
from kis_trader import KISTrader
from strategy.ma_cross import MACrossStrategy, MACrossConfig
from strategy.rsi import RSIStrategy, RSIConfig
from strategy.bollinger import BollingerStrategy, BollingerConfig

import time as _time

class KSTFormatter(logging.Formatter):
    def converter(self, timestamp):
        return _time.gmtime(timestamp + 9 * 3600)

_fmt = KSTFormatter(
    fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logging.basicConfig(level=logging.INFO)
logging.root.handlers[0].setFormatter(_fmt)
logger = logging.getLogger("stock-trader")

MARKET_OPEN   = time(9, 0)
MARKET_CLOSE  = time(15, 30)
ML_TRAIN_TIME = time(15, 40)

KST = timezone(timedelta(hours=9))


class StockTrader:
    def __init__(self):
        self.running    = False
        self.trader     = KISTrader()
        self.positions  = {}
        self.strategies = {}   # name → {is_active, config}
        self.ml_trained_date = None   # 오늘 ML 학습 완료 여부 추적

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
        """활성화된 첫 번째 전략 반환 (하위호환)"""
        for name, s in self.strategies.items():
            if s["is_active"]:
                return name, s["params"]
        return None, {}

    def get_all_active_strategies(self):
        """활성화된 모든 전략 반환"""
        return [(name, s["params"]) for name, s in self.strategies.items() if s["is_active"]]

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
        if name == "RSI반등":
            return RSIStrategy(RSIConfig(
                period        = int(params.get("period", 14)),
                entry         = float(params.get("entry", 30)),
                exit          = float(params.get("exit", 60)),
                stop_loss     = float(params.get("stop_loss", -0.03)),
                buy_amount    = int(params.get("buy_amount", 500000)),
                max_positions = int(params.get("max_positions", 5)),
            ))
        if name == "볼린저밴드":
            return BollingerStrategy(BollingerConfig(
                period        = int(params.get("period", 20)),
                std_dev       = float(params.get("std", 2.0)),
                stop_loss     = float(params.get("stop_loss", -0.03)),
                buy_amount    = int(params.get("buy_amount", 500000)),
                max_positions = int(params.get("max_positions", 5)),
            ))
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
            now = datetime.now(KST)
            cur_time = now.time().replace(tzinfo=None)
            today = now.date()

            # ── 장 마감 후 ML 자동 학습 (15:40, 하루 1회) ──
            if (cur_time >= ML_TRAIN_TIME
                    and self.ml_trained_date != today
                    and now.weekday() < 5):   # 평일만
                logger.info("🎓 장 마감 후 ML 자동 학습 시작...")
                await self._run_ml_training()
                self.ml_trained_date = today

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

    # ── ML 자동 학습 ──────────────────────────────────────
    async def _run_ml_training(self):
        """장 마감 후 감시 종목 전체 자동 학습"""
        try:
            from ml.model import MLModelManager
            ml = MLModelManager(db_pool=db.pool)

            results = []
            # watchlist 전체 종목 학습 (고정 심볼 아님)
            try:
                watchlist_symbols = await db.get_watchlist_symbols()
            except Exception:
                watchlist_symbols = []
            symbols_to_train = watchlist_symbols if watchlist_symbols else config.STOCK_SYMBOLS
            logger.info(f"🎓 ML 학습 대상: {len(symbols_to_train)}종목 (watchlist)")

            for symbol in symbols_to_train:
                try:
                    ohlcv = await db.get_recent_ohlcv(symbol, limit=1500, asset="stock", daily=True)
                    if len(ohlcv) < 60:
                        logger.warning(f"[{symbol}] OHLCV 데이터 부족 ({len(ohlcv)}개) → 스킵")
                        continue

                    result = await ml.train(symbol, ohlcv)
                    if result["success"]:
                        logger.info(
                            f"✅ [{symbol}] 학습 완료 — "
                            f"샘플: {result['samples']}개, 정확도: {result['accuracy']}%"
                        )
                        results.append(f"✅ {symbol}: 정확도 {result['accuracy']}%")
                    else:
                        logger.warning(f"⚠️ [{symbol}] 학습 실패: {result['error']}")
                        results.append(f"⚠️ {symbol}: {result['error']}")

                except Exception as e:
                    logger.error(f"❌ [{symbol}] 학습 오류: {e}")

            # 학습 완료 텔레그램 알림
            if results:
                msg = "🎓 ML 자동 학습 완료\n" + "\n".join(results)
                await self._notify(msg)
                logger.info("📣 ML 학습 결과 텔레그램 전송 완료")

                # Jarvis 메모리에 학습 결과 저장 (전략 회의용)
                await self._save_ml_memory(results, symbols_to_train)

        except Exception as e:
            logger.error(f"❌ ML 자동 학습 전체 오류: {e}")
            await self._notify_error(f"ML 자동 학습 실패: {e}")

    async def _save_ml_memory(self, results: list, symbols: list):
        """ML 학습 결과를 Jarvis 메모리에 저장"""
        try:
            import aiohttp
            import os
            from datetime import datetime, timezone, timedelta
            KST = timezone(timedelta(hours=9))
            now = datetime.now(KST).strftime("%Y-%m-%d")

            # 정확도 요약
            success = [r for r in results if r.startswith("✅")]
            avg_acc = 0
            accs = []
            for r in success:
                try:
                    acc = float(r.split("정확도 ")[1].replace("%",""))
                    accs.append(acc)
                except:
                    pass
            avg_acc = sum(accs) / len(accs) if accs else 0

            memory = (
                f"[ML학습기록 {now}] "
                f"총 {len(symbols)}종목 학습, "
                f"성공 {len(success)}종목, "
                f"평균정확도 {avg_acc:.1f}%. "
                f"상위종목: {', '.join(success[:5])}"
            )

            dashboard_url = os.getenv("DASHBOARD_URL", "https://dashboard-production-65e3.up.railway.app")
            async with aiohttp.ClientSession() as session:
                await session.post(
                    f"{dashboard_url}/api/jarvis/memory",
                    json={"content": memory, "type": "ml_training"},
                    timeout=aiohttp.ClientTimeout(total=10)
                )
            logger.info(f"🧠 ML 학습 결과 Jarvis 메모리 저장 완료")
        except Exception as e:
            logger.warning(f"Jarvis 메모리 저장 실패: {e}")

    # ── 매매 사이클 ───────────────────────────────────────
    async def _run_cycle(self):
        # 활성화된 모든 전략 실행
        active_strategies = self.get_all_active_strategies()
        if not active_strategies:
            logger.info("⏸️ 활성화된 전략 없음 — 대기")
            return

        logger.info(f"📋 활성 전략: {[s[0] for s in active_strategies]}")

        # 첫 번째 전략 기준으로 max_positions 설정
        strat_name, params = active_strategies[0]
        max_positions = int(params.get("max_positions", 5))

        # ① 보유 포지션 손절/익절 체크
        try:
            positions = await self.trader.get_positions()
            self.positions = {p["symbol"]: p for p in positions if isinstance(p, dict) and p.get("symbol")}
        except Exception as e:
            logger.warning(f"포지션 조회 실패: {e}")
            self.positions = {}

        # 손절/익절용 전략 객체 (첫 번째 활성 전략 사용)
        default_strategy = self.build_strategy(strat_name, params)

        for symbol, pos in self.positions.items():
            cur_price = pos["cur_price"]
            avg_price = pos["avg_price"]

            if not default_strategy or cur_price <= 0 or avg_price <= 0:
                continue

            if default_strategy.check_stop_loss(avg_price, cur_price):
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

            if default_strategy.check_take_profit(avg_price, cur_price):
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

        # DB watchlist에서 감시 종목 읽기 (없으면 환경변수 fallback)
        try:
            symbols = await db.get_watchlist_symbols()
            if not symbols:
                symbols = config.STOCK_SYMBOLS
                logger.info("⚠️ watchlist 비어있음 → 환경변수 STOCK_SYMBOLS 사용")
        except Exception:
            symbols = config.STOCK_SYMBOLS

        for symbol in symbols:
            if symbol in self.positions:
                continue

            rows = await db.get_recent_ohlcv(symbol, limit=100, asset="stock", daily=True)
            if len(rows) < 21:
                continue

            prices = [float(r["close"]) for r in rows]  # 이미 ASC 정렬

            # 모든 활성 전략에서 신호 체크
            signal_type = "HOLD"
            triggered_strategy = ""
            for s_name, s_params in active_strategies:
                s_obj = self.build_strategy(s_name, s_params)
                if not s_obj:
                    logger.debug(f"[{symbol}] {s_name} 전략 객체 생성 실패")
                    continue
                sig = s_obj.generate_signal(symbol, prices)
                if sig == "BUY":
                    signal_type = "BUY"
                    triggered_strategy = s_name
                    break
                elif sig == "SELL" and symbol in self.positions:
                    signal_type = "SELL"
                    triggered_strategy = s_name
                    break

            if signal_type == "BUY":
                strat_name = triggered_strategy
                logger.info(f"📈 [{symbol}] {triggered_strategy} 매수 신호 → ML 필터 검사")
                cur_price = await self.trader.get_current_price(symbol)
                if cur_price <= 0:
                    continue

                # ── ML 예측 필터 ──────────────────────────────
                ml_ok, ml_reason = await self._check_ml_signal(symbol, rows)
                if not ml_ok:
                    logger.info(f"⛔ [{symbol}] ML 필터 차단: {ml_reason}")
                    continue

                # ── 수급 필터 (DB 직접 조회) ──────────────────
                supply_reason = "수급 데이터 없음"
                try:
                    async with db.pool.acquire() as conn:
                        sup = await conn.fetchrow("""
                            SELECT foreign_net, institute_net
                            FROM stock_supply
                            WHERE symbol=$1
                            ORDER BY date DESC LIMIT 1
                        """, symbol)
                    if sup:
                        fn = int(sup["foreign_net"] or 0)
                        inst = int(sup["institute_net"] or 0)
                        supply_score = (1 if fn > 0 else -1 if fn < 0 else 0) +                                        (1 if inst > 0 else -1 if inst < 0 else 0)
                        supply_reason = f"외국인{fn:+,} 기관{inst:+,}"
                        if supply_score <= -2:
                            logger.info(f"⛔ [{symbol}] 수급 필터 차단: {supply_reason}")
                            continue
                except Exception as e:
                    logger.debug(f"수급 조회 실패: {e}")

                # ── 뉴스 감성 필터 (DB 직접 조회) ────────────
                news_reason = "뉴스 데이터 없음"
                try:
                    async with db.pool.acquire() as conn:
                        news = await conn.fetchrow("""
                            SELECT sentiment_score, signal, summary
                            FROM stock_news_sentiment
                            WHERE symbol=$1
                            ORDER BY date DESC LIMIT 1
                        """, symbol)
                    if news:
                        news_score = int(news["sentiment_score"] or 0)
                        news_reason = news["summary"] or news["signal"]
                        if news_score <= -2:
                            logger.info(f"⛔ [{symbol}] 뉴스 감성 차단: {news_reason}")
                            continue
                except Exception as e:
                    logger.debug(f"뉴스 조회 실패: {e}")
                # ─────────────────────────────────────────────

                # Jarvis가 ML 확률 기반으로 매수 금액 자율 결정
                ml_result = await self._get_ml_result(symbol, rows)
                buy_prob = ml_result.get("buy_prob", 0.65)
                available_cash = await self.trader.get_balance()
                cash = available_cash.get("cash", 0)

                # 잔고 0이면 Redis에서 직접 조회
                if cash <= 0:
                    try:
                        import json
                        cached = await cache.client.get("stock:balance")
                        if cached:
                            bal = json.loads(cached)
                            cash = int(bal.get("cash", 0))
                    except:
                        pass

                # 그래도 0이면 DB에서 최근 잔고 사용 (기본 1000만원)
                if cash <= 0:
                    cash = 10000000
                    logger.info(f"⚠️ 잔고 조회 실패 → 기본값 {cash:,}원 사용")

                if cash < 100000:
                    logger.info(f"💸 잔고 부족 ({cash:,}원) → 매수 스킵")
                    continue

                # ML 확률 기반 금액 결정
                if buy_prob >= 0.90:
                    ratio, strength = 0.30, "강함"
                elif buy_prob >= 0.80:
                    ratio, strength = 0.20, "보통"
                elif buy_prob >= 0.70:
                    ratio, strength = 0.15, "약함"
                else:
                    ratio, strength = 0.10, "최소"

                buy_amount = min(int(cash * ratio), cash)
                buy_amount = max(buy_amount, 100000)  # 최소 10만원
                qty = max(1, buy_amount // cur_price)
                actual_amount = qty * cur_price

                logger.info(f"💡 [{symbol}] ML 자동매수: {actual_amount:,}원 "
                            f"(ML:{buy_prob:.0%} 신호강도:{strength} 잔고:{cash:,}원)")

                # ML 판단으로 직접 매수 (Jarvis API 호출 없음)
                result = await self.trader.buy(symbol, cur_price, qty)
                if result.get("success"):
                    await db.insert_trade(
                        bot="stock_trader", asset_type="stock",
                        symbol=symbol, side="BUY",
                        price=cur_price, quantity=qty,
                        amount=actual_amount,
                        strategy=triggered_strategy,
                    )
                    logger.info(f"✅ 매수 완료 [{symbol}] {cur_price:,}원 × {qty}주 = {actual_amount:,}원")
                else:
                    logger.error(f"❌ 매수 실패 [{symbol}]: {result.get('error')}")

        # 상태 업데이트
        await cache.set_bot_status("stock_trader", {
            "status":     "running",
            "last_cycle": datetime.now().isoformat(),
            "positions":  len(self.positions),
            "strategy":   strat_name,
        })

    # ── ML 예측 필터 ──────────────────────────────────────
    async def _get_ml_result(self, symbol: str, ohlcv_rows: list) -> dict:
        """ML 예측 결과 반환 (확률 포함)"""
        try:
            from ml.model import MLModelManager
            ml = MLModelManager(db_pool=db.pool)
            ohlcv = [{"date": str(r.get("ts",""))[:10].replace("-",""),
                      "open": float(r.get("open",0)), "high": float(r.get("high",0)),
                      "low": float(r.get("low",0)), "close": float(r.get("close",0)),
                      "volume": float(r.get("volume",0))} for r in ohlcv_rows]
            result = await ml.predict(symbol, ohlcv)
            return result if result.get("success") else {"buy_prob": 0.65}
        except:
            return {"buy_prob": 0.65}

    async def _check_ml_signal(self, symbol: str, ohlcv_rows: list) -> tuple[bool, str]:
        """
        ML 예측으로 매수 신호 검증
        Returns: (통과여부, 이유)
        """
        try:
            from ml.model import MLModelManager
            ml = MLModelManager(db_pool=db.pool)

            # ohlcv_rows → ML 입력 포맷 변환
            ohlcv = []
            for r in ohlcv_rows:   # 이미 ASC 정렬 (오래된 것부터)
                ohlcv.append({
                    "date":   str(r.get("ts", ""))[:10].replace("-", ""),
                    "open":   float(r.get("open", 0)),
                    "high":   float(r.get("high", 0)),
                    "low":    float(r.get("low", 0)),
                    "close":  float(r.get("close", 0)),
                    "volume": float(r.get("volume", 0)),
                })

            result = await ml.predict(symbol, ohlcv)

            if not result.get("success"):
                logger.info(f"[{symbol}] ML 모델 없음 → MA크로스 단독 신호 허용")
                return True, "ML 모델 미학습 (MA크로스 단독)"

            signal    = result.get("signal", "HOLD")
            buy_prob  = result.get("buy_prob", 0.5)
            confidence = result.get("confidence", 0)

            logger.info(f"[{symbol}] ML 결과: signal={signal} buy_prob={buy_prob:.0%}")
            if signal == "BUY" and buy_prob >= 0.60:
                return True, f"ML 매수확률 {buy_prob:.0%} (신뢰도 {confidence:.0f}%)"
            elif signal == "HOLD":
                return False, f"ML 관망 신호 (매수확률 {buy_prob:.0%})"
            else:
                return False, f"ML 매도 신호 (매수확률 {buy_prob:.0%})"

        except Exception as e:
            import traceback
            logger.error(f"[{symbol}] ML 필터 오류: {e}\n{traceback.format_exc()}")
            return False, f"ML 오류: {e}"

    # ── 알림 ──────────────────────────────────────────────
    async def _signal_jarvis(self, action: str, symbol: str, name: str, price: int, qty: int, strategy: str, reason: str = ""):
        """매매 신호를 Jarvis에게 전달 → Jarvis가 판단 후 자동 실행"""
        import aiohttp as http
        import os
        dashboard_url = os.getenv("DASHBOARD_URL", "https://dashboard-production-65e3.up.railway.app")
        try:
            async with http.ClientSession() as session:
                await session.post(
                    f"{dashboard_url}/api/jarvis/signal",
                    json={
                        "bot": "stock_trader",
                        "action": action,
                        "symbol": symbol,
                        "name": name,
                        "price": price,
                        "qty": qty,
                        "strategy": strategy,
                        "reason": reason,
                    },
                    timeout=http.ClientTimeout(total=60),
                )
            logger.info(f"📡 Jarvis에게 신호 전달: {action} {symbol}")
        except Exception as e:
            logger.error(f"Jarvis 신호 전달 실패: {e}")

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
