"""
stock-trader — AutoTrader
한국주식 자동매매 메인 프로세스
DB에서 전략 설정 읽기 + Redis 실시간 전략 변경 구독
"""
import asyncio
import json
import logging
import signal
import os
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
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "https://dashboard-production-65e3.up.railway.app")


class StockTrader:
    def __init__(self):
        self.running    = False
        self.trader     = KISTrader()
        self.positions  = {}
        self.strategies = {}
        self.ml_trained_date = None

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

    def get_all_active_strategies(self):
        return [(name, s["params"]) for name, s in self.strategies.items() if s["is_active"]]

    def build_strategy(self, name, params):
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

        try:
            positions = await self.trader.get_positions() or []
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

        await asyncio.gather(
            self._loop(),
            self.subscribe_strategy_updates(),
            self._price_monitor(),
            self._six_hour_report(),
        )

    # ── 메인 루프 ─────────────────────────────────────────
    async def _loop(self):
        while self.running:
            now = datetime.now(KST)
            cur_time = now.time().replace(tzinfo=None)
            today = now.date()

            # 장 마감 후 ML 자동 학습 (15:40, 하루 1회, 평일만)
            if (cur_time >= ML_TRAIN_TIME
                    and self.ml_trained_date != today
                    and now.weekday() < 5):
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
                err_str = str(e)
                logger.error(f"❌ 사이클 오류: {e}")
                if "Server disconnected" not in err_str and "ServerDisconnected" not in err_str:
                    await self._notify_error(err_str)

            await asyncio.sleep(config.COLLECT_INTERVAL_SEC)

    # ── ML 자동 학습 ──────────────────────────────────────
    async def _run_ml_training(self):
        """장 마감 후 감시 종목 전체 자동 학습"""
        try:
            from ml.model import MLModelManager
            ml = MLModelManager(db_pool=db.pool)

            try:
                watchlist_symbols = await db.get_watchlist_symbols()
            except Exception:
                watchlist_symbols = []
            symbols_to_train = watchlist_symbols if watchlist_symbols else config.STOCK_SYMBOLS
            logger.info(f"🎓 ML 학습 대상: {len(symbols_to_train)}종목")

            results = []
            for symbol in symbols_to_train:
                try:
                    ohlcv = await db.get_recent_ohlcv(symbol, limit=1500, asset="stock", daily=True)
                    if len(ohlcv) < 60:
                        logger.warning(f"[{symbol}] OHLCV 부족 ({len(ohlcv)}개) → 스킵")
                        continue
                    result = await ml.train(symbol, ohlcv)
                    if result["success"]:
                        logger.info(f"✅ [{symbol}] 학습 완료 — 정확도: {result['accuracy']}%")
                        results.append(f"✅ {symbol}: {result['accuracy']}%")
                    else:
                        results.append(f"⚠️ {symbol}: {result['error']}")
                except Exception as e:
                    logger.error(f"❌ [{symbol}] 학습 오류: {e}")

            if results:
                now_kst = datetime.now(KST).strftime("%m/%d %H:%M")
                msg = f"🎓 ML 자동 학습 완료 ({now_kst})\n" + "\n".join(results[:15])
                from common.telegram import send_stock
                await send_stock(msg)
                await self._save_ml_memory(results, symbols_to_train)

        except Exception as e:
            logger.error(f"❌ ML 학습 오류: {e}")
            await self._notify_error(f"ML 자동 학습 실패: {e}")

    async def _save_ml_memory(self, results: list, symbols: list):
        try:
            import aiohttp
            now = datetime.now(KST).strftime("%Y-%m-%d")
            success = [r for r in results if r.startswith("✅")]
            accs = []
            for r in success:
                try:
                    accs.append(float(r.split(": ")[1].replace("%", "")))
                except:
                    pass
            avg_acc = sum(accs) / len(accs) if accs else 0
            memory = (
                f"[ML학습기록 {now}] "
                f"총 {len(symbols)}종목, 성공 {len(success)}종목, "
                f"평균정확도 {avg_acc:.1f}%"
            )
            async with aiohttp.ClientSession() as session:
                await session.post(
                    f"{DASHBOARD_URL}/api/jarvis/memory",
                    json={"content": memory, "type": "ml_training"},
                    timeout=aiohttp.ClientTimeout(total=10)
                )
        except Exception as e:
            logger.warning(f"Jarvis 메모리 저장 실패: {e}")

    # ── 실시간 가격 모니터 (3초) ──────────────────────────
    async def _price_monitor(self):
        """3초마다 보유 포지션 급락/급등 감지 → Jarvis 즉시 판단"""
        alert_cooldown = {}

        while self.running:
            try:
                if self.positions:
                    for symbol, pos in list(self.positions.items()):
                        avg_price = pos.get("avg_price", 0)
                        if avg_price <= 0:
                            continue
                        try:
                            cached = await cache.client.get(f"stock:price:{symbol}")
                            if not cached:
                                continue
                            price_data = json.loads(cached)
                            cur_price = int(price_data.get("price", 0))
                        except:
                            continue

                        if cur_price <= 0:
                            continue

                        pnl_rate = (cur_price - avg_price) / avg_price * 100
                        now_ts = datetime.now().timestamp()
                        last_alert = alert_cooldown.get(symbol, 0)
                        if now_ts - last_alert < 300:  # 5분 쿨다운
                            continue

                        if pnl_rate <= -3.0 or pnl_rate >= 7.0:
                            alert_cooldown[symbol] = now_ts
                            direction = "급락" if pnl_rate < 0 else "급등"
                            logger.info(f"⚡ [{symbol}] {direction} 감지: {pnl_rate:+.1f}% → Jarvis 판단")
                            await self._jarvis_exit_check(
                                symbol=symbol,
                                cur_price=cur_price,
                                avg_price=avg_price,
                                pnl_rate=pnl_rate,
                                qty=pos.get("qty", 0),
                            )
            except Exception as e:
                logger.debug(f"가격 모니터 오류: {e}")

            await asyncio.sleep(3)

    async def _jarvis_exit_check(self, symbol: str, cur_price: int,
                                  avg_price: int, pnl_rate: float, qty: int):
        """급락/급등 시 Jarvis에게 매도 여부 판단 요청"""
        try:
            import aiohttp as http
            direction = "급락" if pnl_rate < 0 else "급등"
            async with http.ClientSession() as session:
                resp = await session.post(
                    f"{DASHBOARD_URL}/api/jarvis/signal",
                    json={
                        "bot": "stock_trader",
                        "action": "sell",
                        "symbol": symbol,
                        "name": symbol,
                        "price": cur_price,
                        "qty": qty,
                        "strategy": "실시간모니터",
                        "reason": f"{direction} {pnl_rate:+.1f}% (평균단가: {avg_price:,}원)",
                    },
                    timeout=http.ClientTimeout(total=30)
                )
                result = await resp.json() or {}
                if result.get("executed"):
                    logger.info(f"✅ Jarvis 매도 결정 [{symbol}] {pnl_rate:+.1f}%")
                    self.positions.pop(symbol, None)
                else:
                    logger.info(f"⏸️ Jarvis HOLD [{symbol}] {pnl_rate:+.1f}%")
        except Exception as e:
            logger.error(f"Jarvis 매도 판단 실패 [{symbol}]: {e}")

    # ── 매매 사이클 ───────────────────────────────────────
    async def _run_cycle(self):
        active_strategies = self.get_all_active_strategies()
        if not active_strategies:
            logger.info("⏸️ 활성화된 전략 없음 — 대기")
            return

        logger.info(f"📋 활성 전략: {[s[0] for s in active_strategies]}")

        strat_name, params = active_strategies[0]
        max_positions = int(params.get("max_positions", 5))

        # ① 보유 포지션 조회 & 손절/익절 체크
        try:
            positions = await self.trader.get_positions() or []
            self.positions = {p["symbol"]: p for p in positions if isinstance(p, dict) and p.get("symbol")}
        except Exception as e:
            logger.warning(f"포지션 조회 실패: {e}")
            self.positions = {}

        default_strategy = self.build_strategy(strat_name, params)

        for symbol, pos in list(self.positions.items()):
            cur_price = pos["cur_price"]
            avg_price = pos["avg_price"]
            qty = pos.get("qty", 0)

            if not default_strategy or cur_price <= 0 or avg_price <= 0:
                continue

            pnl = (cur_price - avg_price) * qty
            pnl_rate = (cur_price - avg_price) / avg_price * 100

            # 손절 체크
            if default_strategy.check_stop_loss(avg_price, cur_price):
                result = await self.trader.sell(symbol, cur_price, qty)
                if result["success"]:
                    await db.insert_trade(
                        bot="stock_trader", asset_type="stock",
                        symbol=symbol, side="SELL",
                        price=cur_price, quantity=qty,
                        amount=cur_price * qty,
                        strategy=f"{strat_name}_손절", pnl=pnl,
                    )
                    await self._notify_trade(
                        action="매도", symbol=symbol, name=pos.get("name", symbol),
                        price=cur_price, qty=qty,
                        pnl=pnl, pnl_rate=pnl_rate, strategy=f"{strat_name}_손절"
                    )
                    self.positions.pop(symbol, None)
                continue

            # 익절 체크
            if default_strategy.check_take_profit(avg_price, cur_price):
                result = await self.trader.sell(symbol, cur_price, qty)
                if result["success"]:
                    await db.insert_trade(
                        bot="stock_trader", asset_type="stock",
                        symbol=symbol, side="SELL",
                        price=cur_price, quantity=qty,
                        amount=cur_price * qty,
                        strategy=f"{strat_name}_익절", pnl=pnl,
                    )
                    await self._notify_trade(
                        action="매도", symbol=symbol, name=pos.get("name", symbol),
                        price=cur_price, qty=qty,
                        pnl=pnl, pnl_rate=pnl_rate, strategy=f"{strat_name}_익절"
                    )
                    self.positions.pop(symbol, None)
                continue

        # ② 신규 진입 신호 체크
        if len(self.positions) >= max_positions:
            logger.info(f"⚠️ 최대 보유 종목수 ({len(self.positions)}/{max_positions})")
            return

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

            prices = [float(r["close"]) for r in rows]

            # 전략 신호 체크
            signal_type = "HOLD"
            triggered_strategy = ""
            for s_name, s_params in active_strategies:
                s_obj = self.build_strategy(s_name, s_params)
                if not s_obj:
                    continue
                sig = s_obj.generate_signal(symbol, prices)
                if sig == "BUY":
                    signal_type = "BUY"
                    triggered_strategy = s_name
                    break

            if signal_type != "BUY":
                continue

            logger.info(f"📈 [{symbol}] {triggered_strategy} 매수 신호 감지")

            cur_price = await self.trader.get_current_price(symbol)
            if cur_price <= 0:
                continue

            # 잔고 조회
            available_cash = await self.trader.get_balance()
            cash = available_cash.get("cash", 0)
            if cash <= 0:
                try:
                    cached = await cache.client.get("stock:balance")
                    if cached:
                        bal = json.loads(cached)
                        cash = int(bal.get("cash", 0))
                except:
                    pass
            if cash <= 0:
                cash = 10000000  # fallback

            if cash < 100000:
                logger.info(f"💸 잔고 부족 ({cash:,}원) → 매수 스킵")
                continue

            # ML 예측 결과 (Jarvis에게 참고 정보로 전달)
            ml_result = await self._get_ml_result(symbol, rows)
            buy_prob = ml_result.get("buy_prob", 0.65)

            # ML 확률 기반 매수 금액 산정
            if buy_prob >= 0.90:
                ratio, strength = 0.30, "강함"
            elif buy_prob >= 0.80:
                ratio, strength = 0.20, "보통"
            elif buy_prob >= 0.70:
                ratio, strength = 0.15, "약함"
            else:
                ratio, strength = 0.10, "최소"

            buy_amount = min(int(cash * ratio), cash)
            buy_amount = max(buy_amount, 100000)
            qty = max(1, buy_amount // cur_price)

            # 수급 정보 수집
            supply_reason = "수급 데이터 없음"
            supply_ok = True
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
                    supply_reason = f"외국인{fn:+,} 기관{inst:+,}"
                    supply_score = (1 if fn > 0 else -1 if fn < 0 else 0) + \
                                   (1 if inst > 0 else -1 if inst < 0 else 0)
                    if supply_score <= -2:
                        logger.info(f"⛔ [{symbol}] 수급 모두 매도 → 스킵")
                        supply_ok = False
            except Exception as e:
                logger.debug(f"수급 조회 실패: {e}")

            if not supply_ok:
                continue

            # 뉴스 감성 정보
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
                    news_reason = (news["summary"] or news["signal"] or "감성 없음")[:50]
                    if news_score <= -2:
                        logger.info(f"⛔ [{symbol}] 뉴스 부정 → 스킵: {news_reason}")
                        continue
            except Exception as e:
                logger.debug(f"뉴스 조회 실패: {e}")

            # ── Jarvis 최종 판단 (매수/매도 결정 + 실행 + 텔레그램 알림 모두 dashboard가 처리) ──
            reason = (
                f"전략:{triggered_strategy} | ML매수확률:{buy_prob:.0%}({strength}) "
                f"| 수급:{supply_reason} | 뉴스:{news_reason}"
            )
            logger.info(f"🤖 [{symbol}] Jarvis 최종 판단 요청 — {reason}")

            import aiohttp as http
            try:
                async with http.ClientSession() as session:
                    resp = await session.post(
                        f"{DASHBOARD_URL}/api/jarvis/signal",
                        json={
                            "bot": "stock_trader",
                            "action": "buy",
                            "symbol": symbol,
                            "name": symbol,
                            "price": cur_price,
                            "qty": qty,
                            "strategy": triggered_strategy,
                            "reason": reason,
                        },
                        timeout=http.ClientTimeout(total=60)
                    )
                    result = await resp.json() or {}

                if result.get("executed"):
                    logger.info(f"✅ Jarvis 매수 완료 [{symbol}] {cur_price:,}원 × {qty}주")
                    # positions에 즉시 등록 (중복 방지)
                    self.positions[symbol] = {
                        "symbol": symbol, "name": symbol,
                        "cur_price": cur_price, "avg_price": cur_price,
                        "qty": qty
                    }
                    # 최대 포지션 체크
                    if len(self.positions) >= max_positions:
                        break
                else:
                    jarvis_say = result.get("jarvis_reply", "SKIP")[:60]
                    logger.info(f"⏭️ Jarvis 스킵 [{symbol}]: {jarvis_say}")

            except Exception as e:
                logger.error(f"Jarvis 신호 전달 실패 [{symbol}]: {e}")

        # 상태 업데이트
        await cache.set_bot_status("stock_trader", {
            "status":     "running",
            "last_cycle": datetime.now().isoformat(),
            "positions":  len(self.positions),
            "strategy":   strat_name,
        })

    # ── ML 예측 (Jarvis 참고용) ──────────────────────────
    async def _get_ml_result(self, symbol: str, ohlcv_rows: list) -> dict:
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

    # ── 즉시 텔레그램 알림 (손절/익절용) ─────────────────
    async def _notify_trade(self, action: str, symbol: str, name: str,
                             price: int, qty: int, pnl: float = 0,
                             pnl_rate: float = 0, strategy: str = ""):
        """손절/익절 즉시 텔레그램 전송 (Jarvis 경유하지 않고 직접 체결된 경우)"""
        try:
            from common.telegram import send_stock
            now_kst = datetime.now(KST).strftime("%m/%d %H:%M")
            emoji = "🎯" if pnl >= 0 else "🛑"
            msg = (
                f"{emoji} <b>{name}({symbol}) {action} 체결</b>\n"
                f"가격: {price:,}원 × {qty}주\n"
                f"손익: {pnl:+,.0f}원 ({pnl_rate:+.1f}%)\n"
                f"전략: {strategy}\n"
                f"시각: {now_kst}"
            )
            await send_stock(msg)
            logger.info(f"📣 텔레그램: {action} {symbol} {pnl:+,.0f}원")
        except Exception as e:
            logger.warning(f"텔레그램 알림 실패: {e}")

    # ── 6시간 통합 리포트 ────────────────────────────────
    async def _six_hour_report(self):
        while self.running:
            now = datetime.now(KST)
            next_hour = ((now.hour // 6) + 1) * 6
            if next_hour >= 24:
                next_run = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            else:
                next_run = now.replace(hour=next_hour, minute=0, second=0, microsecond=0)
            await asyncio.sleep((next_run - now).total_seconds())

            try:
                from common.telegram import send_stock
                async with db.pool.acquire() as conn:
                    trades = await conn.fetch("""
                        SELECT side, symbol, amount, pnl, strategy, created_at
                        FROM trade_history
                        WHERE bot='stock_trader'
                        AND created_at >= NOW() - INTERVAL '6 hours'
                        ORDER BY created_at DESC
                    """)

                buys = [t for t in trades if t['side'] == 'BUY']
                sells = [t for t in trades if t['side'] == 'SELL']
                total_pnl = sum(float(t['pnl'] or 0) for t in trades)

                pos_list = []
                for sym, pos in self.positions.items():
                    rate = float(pos.get('pnl_rate', 0))
                    pos_list.append(f"{pos.get('name', sym)} {rate:+.1f}%")

                acct = await self.trader.get_balance()
                cash = acct.get('cash', 0)

                report = (
                    f"📊 주식 6시간 리포트 ({now.strftime('%m/%d %H:%M')})\n\n"
                    f"매수 {len(buys)}건 / 매도 {len(sells)}건\n"
                    f"손익: {total_pnl:+,.0f}원\n\n"
                    f"보유: {', '.join(pos_list) if pos_list else '없음'}\n"
                    f"예수금: {cash:,.0f}원"
                )

                if trades:
                    report += "\n\n최근 매매:"
                    for t in list(trades)[:5]:
                        pnl = float(t['pnl'] or 0)
                        report += f"\n{'📈매수' if t['side']=='BUY' else '📉매도'} {t['symbol']} {float(t['amount']):,.0f}원"
                        if pnl:
                            report += f" ({pnl:+,.0f}원)"

                await send_stock(report)
                logger.info("📨 6시간 주식 리포트 전송")
            except Exception as e:
                logger.error(f"주식 리포트 실패: {e}")

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
