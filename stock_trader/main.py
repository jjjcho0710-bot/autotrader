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

async def _resolve_stock_name(symbol: str) -> str:
    """종목코드 → 종목명 (dashboard 조회, 실패 시 코드 그대로)"""
    try:
        import aiohttp as http
        async with http.ClientSession() as session:
            resp = await session.get(f"{DASHBOARD_URL}/api/stock/name",
                                      params={"code": symbol}, timeout=http.ClientTimeout(total=5))
            data = await resp.json()
            return data.get("name") or symbol
    except Exception:
        return symbol

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

            # 주말(토·일): 신호 감지 완전 중단, 하트비트만
            if now.weekday() >= 5:
                try:
                    await cache.set_bot_status("stock_trader", {
                        "status": "running", "last_cycle": datetime.now().isoformat(),
                        "positions": len(self.positions), "strategy": "주말휴장",
                    })
                except Exception:
                    pass
                await asyncio.sleep(300)  # 5분마다만 체크
                continue

            # 장 마감 후 ML 자동 학습 (15:40, 하루 1회, 평일만)
            if (cur_time >= ML_TRAIN_TIME
                    and self.ml_trained_date != today
                    and now.weekday() < 5):
                logger.info("🎓 장 마감 후 ML 자동 학습 시작...")
                await self._run_ml_training()
                self.ml_trained_date = today

            if not (MARKET_OPEN <= cur_time <= MARKET_CLOSE):
                logger.info(f"🕐 장외 시간 [{cur_time.strftime('%H:%M')}] — 대기")
                # 장외에도 상태 하트비트 (대시보드 '이상' 오표시 방지)
                try:
                    await cache.set_bot_status("stock_trader", {
                        "status": "running", "last_cycle": datetime.now().isoformat(),
                        "positions": len(self.positions), "strategy": "장외대기",
                    })
                except Exception:
                    pass
                await asyncio.sleep(60)
                continue

            # 개장 직후 5분은 KIS 잔고 동기화가 불안정해 매도 실패가 잦음 — 판단만 하고 매매는 스킵
            if cur_time < time(9, 5):
                logger.info("🕐 개장 직후 5분 — KIS 잔고 동기화 대기")
                await asyncio.sleep(30)
                continue

            logger.info(f"🔄 매매 사이클 [{now.strftime('%H:%M:%S')}]")

            try:
                await self._run_cycle()
                self._db_err_count = 0
            except Exception as e:
                err_str = str(e)
                logger.error(f"❌ 사이클 오류: {e}")

                # DB 연결 계열 오류 → 자동 재연결 시도
                _lower = err_str.lower()
                if "pool is closed" in _lower or "connection" in _lower or "closed" in _lower:
                    self._db_err_count = getattr(self, "_db_err_count", 0) + 1
                    logger.warning(f"🔄 DB 재연결 시도 ({self._db_err_count}회차)...")
                    try:
                        try:
                            await db.disconnect()
                        except Exception:
                            pass
                        try:
                            await cache.disconnect()
                        except Exception:
                            pass
                        await db.connect()
                        await cache.connect()
                        logger.info("✅ DB/Redis 재연결 성공")
                    except Exception as re_err:
                        logger.error(f"❌ 재연결 실패: {re_err}")
                        # 3회 연속 실패 시에만 텔레그램 (스팸 방지)
                        if self._db_err_count >= 3:
                            await self._notify_error(f"DB 재연결 {self._db_err_count}회 실패: {re_err}")
                elif "Server disconnected" not in err_str and "ServerDisconnected" not in err_str:
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
                # 장중(평일 09:00~15:30)에만 감시 — 장외엔 캐시 가격으로 오알림 방지
                _now = datetime.now(KST)
                _ct = _now.time().replace(tzinfo=None)
                if _now.weekday() >= 5 or not (MARKET_OPEN <= _ct <= MARKET_CLOSE):
                    await asyncio.sleep(60)
                    continue
                if _ct < time(9, 5):
                    await asyncio.sleep(10)
                    continue
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
                            if pnl_rate <= -3.0:
                                # 손실 매도는 상의 원칙: 자동 매도 없이 알림만
                                try:
                                    from common.telegram import send_stock
                                    nm = pos.get("name", symbol)
                                    await send_stock(
                                        f"⚡ <b>{nm}({symbol}) 급락 {pnl_rate:+.1f}%</b>\n"
                                        f"자동 매도하지 않습니다. 매도 원하시면 "
                                        f"'{nm} 전량 매도' 지시해주세요."
                                    )
                                except Exception:
                                    pass
                                continue
                            if await self._recently_sold(symbol):
                                logger.info(f"⏭️ [{symbol}] 최근 10분 내 매도 기록 있음 — KIS 잔고 반영 지연으로 판단, 재시도 스킵")
                                self.positions.pop(symbol, None)
                                continue
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

    async def _recently_sold(self, symbol: str) -> bool:
        """최근 10분 내 이 종목 SELL 체결 기록이 있는지 확인
        (KIS 잔고 반영 지연으로 이미 판 종목이 self.positions에 잠깐 남아있는 경우
         중복 매도 시도를 막기 위함 — 매도 성공 직후 몇 분간 재시도 방지)"""
        try:
            async with db.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT 1 FROM trade_history
                    WHERE bot='stock_trader' AND symbol=$1 AND side='SELL'
                      AND created_at >= NOW() - INTERVAL '10 minutes'
                    LIMIT 1
                """, symbol)
            return row is not None
        except Exception as e:
            logger.debug(f"최근 매도 이력 확인 실패 [{symbol}]: {e}")
            return False

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
                        "name": await _resolve_stock_name(symbol),
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

    # ── 재매수 금지 (손절 쿨다운) ────────────────────────
    async def _check_rebuy_cooldown(self, symbol: str, cur_price: float) -> tuple:
        """
        손절 이력 기반 재매수 차단.
        반환: (허용 여부, 사유)
        - 최근 3거래일(달력 5일) 내 손절 1회 → 차단
        - 최근 10거래일(달력 14일) 내 손절 2회+ → 차단(7거래일 상당)
        - 예외: 마지막 손절가 대비 +5% 이상 위에서 신호 → 추세 전환으로 보고 허용
        """
        try:
            async with db.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT price, created_at
                    FROM trade_history
                    WHERE bot='stock_trader' AND symbol=$1 AND side='SELL'
                      AND strategy LIKE '%손절%'
                      AND created_at >= NOW() - INTERVAL '14 days'
                    ORDER BY created_at DESC
                """, symbol)
            if not rows:
                return True, ""
            last_stop_price = float(rows[0]["price"] or 0)
            # 예외: 손절가 +5% 위 신호면 허용
            if last_stop_price > 0 and cur_price >= last_stop_price * 1.05:
                return True, f"손절가({last_stop_price:,.0f}) +5% 상회 → 재진입 허용"
            # 2회 이상 손절 → 14일 차단
            if len(rows) >= 2:
                return False, f"최근 2회 손절 → 재매수 금지 (마지막 손절가 {last_stop_price:,.0f}원)"
            # 1회 손절 → 5일 차단
            from datetime import timezone as _tz
            age_days = (datetime.now(_tz.utc) - rows[0]["created_at"]).days
            if age_days < 5:
                return False, f"손절 후 {age_days}일 경과 (5일 미만) → 재매수 금지"
            return True, ""
        except Exception as e:
            logger.debug(f"쿨다운 조회 실패(허용): {e}")
            return True, ""

    async def _daily_stop_count(self) -> int:
        """오늘 손절 횟수 (2회 이상이면 당일 신규 매수 중단)"""
        try:
            async with db.pool.acquire() as conn:
                n = await conn.fetchval("""
                    SELECT COUNT(*) FROM trade_history
                    WHERE bot='stock_trader' AND side='SELL' AND strategy LIKE '%손절%'
                      AND DATE(created_at AT TIME ZONE 'Asia/Seoul') = (NOW() AT TIME ZONE 'Asia/Seoul')::date
                """)
            return int(n or 0)
        except Exception:
            return 0

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

            # 손절 -7% 도달 → 즉시 자동 매도 (주인 지시: 알아서 처리)
            if default_strategy.check_stop_loss(avg_price, cur_price):
                try:
                    nm = pos.get("name", symbol)
                    result = await self.trader.sell(symbol, cur_price, qty)
                    # 초당 거래건수 제한 등 일시 오류는 짧은 대기 후 1회 재시도
                    if not result.get("success") and any(
                        k in str(result.get("error", "")) for k in ("초당", "거래건수", "rate", "Rate")
                    ):
                        await asyncio.sleep(1.2)
                        result = await self.trader.sell(symbol, cur_price, qty)
                    if result.get("success"):
                        await db.insert_trade(
                            bot="stock_trader", asset_type="stock",
                            symbol=symbol, side="SELL", price=cur_price, quantity=qty,
                            amount=cur_price * qty, strategy=f"{strat_name}_손절", pnl=pnl,
                        )
                        try:
                            await cache.client.delete(f"half_tp:{symbol}")
                        except Exception:
                            pass
                        from common.telegram import send_stock
                        await send_stock(
                            f"🔴 <b>{nm}({symbol}) 손절 매도 체결 {pnl_rate:+.1f}%</b>\n"
                            f"{qty}주 @ {cur_price:,}원 (손실 {pnl:+,.0f}원)\n"
                            f"평단 {avg_price:,.0f} → 매도 {cur_price:,.0f}"
                        )
                        logger.info(f"🔴 손절 자동매도 체결 [{symbol}] {pnl_rate:+.1f}%")
                        self.positions.pop(symbol, None)
                    else:
                        from common.telegram import send_stock
                        await send_stock(
                            f"⚠️ <b>{nm}({symbol}) 손절 매도 실패</b> {pnl_rate:+.1f}%\n"
                            f"사유: {result.get('error', '알 수 없음')} — 수동 확인 필요"
                        )
                        logger.warning(f"손절 매도 실패 [{symbol}]: {result.get('error')}")
                except Exception as e:
                    logger.warning(f"손절 자동매도 오류 [{symbol}]: {e}")
                continue

            # 급등 절반익절: 1시간 내 +10%p 이상 급등 시 절반 자동 매도 (완만한 상승은 제외)
            try:
                surge_key = f"surge_ref:{symbol}"
                ref = await cache.client.get(surge_key)
                now_ts = datetime.now().timestamp()
                if ref:
                    ref_price, ref_ts = map(float, ref.split(":"))
                    if now_ts - ref_ts > 3600:
                        # 1시간 지난 기준가는 폐기하고 현재가로 갱신
                        await cache.client.setex(surge_key, 3600, f"{cur_price}:{now_ts}")
                        ref_price = cur_price
                else:
                    await cache.client.setex(surge_key, 3600, f"{cur_price}:{now_ts}")
                    ref_price = cur_price

                surge_rate = (cur_price - ref_price) / ref_price * 100 if ref_price > 0 else 0
                if surge_rate >= 10.0 and qty >= 2 and \
                   not await cache.client.get(f"surge_tp:{symbol}"):
                    half_qty = qty // 2
                    result = await self.trader.sell(symbol, cur_price, half_qty)
                    if result["success"]:
                        half_pnl = int((cur_price - avg_price) * half_qty)
                        await db.insert_trade(
                            bot="stock_trader", asset_type="stock",
                            symbol=symbol, side="SELL",
                            price=cur_price, quantity=half_qty,
                            amount=cur_price * half_qty,
                            strategy=f"{strat_name}_급등절반익절", pnl=half_pnl,
                        )
                        await cache.client.setex(f"surge_tp:{symbol}", 86400, "1")
                        try:
                            from common.telegram import send_stock
                            await send_stock(
                                f"🚀 <b>{pos.get('name', symbol)} 급등 절반 익절</b>\n"
                                f"1시간 내 {surge_rate:+.1f}% 급등 — {half_qty}주 매도 @ {cur_price:,}원 "
                                f"(전체 손익 {pnl_rate:+.1f}%, 실현 {half_pnl:+,}원)\n"
                                f"잔여 {qty - half_qty}주는 트레일링으로 계속 관리합니다.")
                        except Exception:
                            pass
                        pos["qty"] = qty - half_qty
                        self.positions[symbol] = pos
                        continue
            except Exception as e:
                logger.warning(f"급등 절반익절 처리 오류 [{symbol}]: {e}")

            # 익절 AI 판단: +3% 이상이면 자비스가 HOLD/HALF/ALL 판단 (30분 쿨다운)
            if pnl_rate >= 3.0 and qty >= 1:
                try:
                    if not await cache.client.get(f"exit_ai_cool:{symbol}"):
                        await cache.client.setex(f"exit_ai_cool:{symbol}", 1800, "1")
                        import aiohttp as http
                        decision, reason = "HOLD", ""
                        try:
                            async with http.ClientSession() as session:
                                resp = await session.post(
                                    f"{DASHBOARD_URL}/api/jarvis/exit_decision",
                                    json={"symbol": symbol, "name": pos.get("name", symbol),
                                          "qty": qty, "avg_price": avg_price,
                                          "cur_price": cur_price, "pnl_rate": pnl_rate},
                                    timeout=http.ClientTimeout(total=25))
                                dj = await resp.json()
                                decision = dj.get("decision", "HOLD")
                                reason = dj.get("reason", "")
                        except Exception as de:
                            logger.warning(f"익절 판단 요청 실패 [{symbol}]: {de}")

                        if decision in ("HALF", "ALL"):
                            sell_qty = max(1, qty // 2) if (decision == "HALF" and qty >= 2) else qty
                            result = await self.trader.sell(symbol, cur_price, sell_qty)
                            if result.get("success"):
                                s_pnl = int((cur_price - avg_price) * sell_qty)
                                await db.insert_trade(
                                    bot="stock_trader", asset_type="stock",
                                    symbol=symbol, side="SELL",
                                    price=cur_price, quantity=sell_qty,
                                    amount=cur_price * sell_qty,
                                    strategy=f"{strat_name}_AI익절{decision}", pnl=s_pnl,
                                )
                                from common.telegram import send_stock
                                remain = qty - sell_qty
                                await send_stock(
                                    f"🤖 <b>{pos.get('name', symbol)} AI 익절 판단: {decision}</b>\n"
                                    f"{sell_qty}주 매도 @ {cur_price:,}원 (손익 {pnl_rate:+.1f}%, 실현 {s_pnl:+,}원)\n"
                                    f"근거: {reason}\n"
                                    + (f"잔여 {remain}주는 계속 보유·관찰합니다." if remain > 0 else "전량 매도 완료.")
                                )
                                if remain > 0:
                                    pos["qty"] = remain
                                    self.positions[symbol] = pos
                                else:
                                    self.positions.pop(symbol, None)
                                    try:
                                        from datetime import datetime as _dt
                                        _now = _dt.now()
                                        _eod = _now.replace(hour=23, minute=59, second=0)
                                        await cache.client.setex(
                                            f"rebuy_block:{symbol}",
                                            max(60, int((_eod - _now).total_seconds())), "tp")
                                    except Exception:
                                        pass
                                continue
                except Exception as e:
                    logger.warning(f"AI 익절 판단 처리 오류 [{symbol}]: {e}")

        # ② 신규 진입 신호 체크
        if len(self.positions) >= max_positions:
            logger.info(f"⚠️ 최대 보유 종목수 ({len(self.positions)}/{max_positions})")
            return

        # 당일 2회 이상 손절 → 신규 매수 중단 (틸트 방지)
        stop_cnt = await self._daily_stop_count()
        if stop_cnt >= 2:
            logger.info(f"🛑 오늘 손절 {stop_cnt}회 → 당일 신규 매수 중단")
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

            # 재매수 금지 체크 (손절 쿨다운)
            allowed, cd_reason = await self._check_rebuy_cooldown(symbol, cur_price)
            if not allowed:
                logger.info(f"⛔ [{symbol}] {cd_reason}")
                continue
            elif cd_reason:
                logger.info(f"✅ [{symbol}] {cd_reason}")

            # 잔고 조회 (실패 시 매수 스킵 — 가짜 잔고로 판단 금지)
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
                logger.warning(f"⚠️ [{symbol}] 잔고 조회 실패 → 매수 스킵 (다음 사이클 재시도)")
                continue

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

            # SKIP 쿨다운: 자비스가 30분 내 SKIP한 종목은 재판단 요청 안 함
            try:
                skip_key = f"jarvis:skip:{symbol}"
                if await cache.client.get(skip_key):
                    logger.debug(f"⏸️ [{symbol}] SKIP 쿨다운 중 → 판단 생략")
                    continue
                # 익절 매도 후 당일 재매수 금지
                if await cache.client.get(f"rebuy_block:{symbol}"):
                    logger.debug(f"🚫 [{symbol}] 익절 후 당일 재매수 금지")
                    continue
            except Exception:
                pass

            # ── Jarvis 최종 판단 (매수/매도 결정 + 실행 + 텔레그램 알림 모두 dashboard가 처리) ──
            buy_amount_krw = cur_price * qty
            reason = (
                f"전략:{triggered_strategy} | ML매수확률:{buy_prob:.0%}({strength}) "
                f"| 수급:{supply_reason} | 뉴스:{news_reason} "
                f"| 예수금:{cash:,.0f}원 | 매수예정:{buy_amount_krw:,.0f}원"
            )
            logger.info(f"🤖 [{symbol}] Jarvis 판단 요청 — 예수금 {cash:,.0f}원 / 매수 {buy_amount_krw:,.0f}원 ({qty}주×{cur_price:,}원)")

            import aiohttp as http
            try:
                async with http.ClientSession() as session:
                    resp = await session.post(
                        f"{DASHBOARD_URL}/api/jarvis/signal",
                        json={
                            "bot": "stock_trader",
                            "action": "buy",
                            "symbol": symbol,
                            "name": await _resolve_stock_name(symbol),
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
                    # 30분 쿨다운 기록 (반복 판단·텔레그램 스팸 방지)
                    try:
                        await cache.client.setex(f"jarvis:skip:{symbol}", 1800, "1")
                    except Exception:
                        pass

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
# redeploy 1787544092
# weekend-fix 1787963538
