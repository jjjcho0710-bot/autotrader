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

from common.config import config, compute_total_pnl
from common.database import db, cache
from common import position_sizing
from kis_trader import KISTrader
from ml_report import build_ml_report_text
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
ML_TRAINED_KEY_TTL_SEC = 3 * 86400  # ml:trained:{KST날짜} 키 만료 (3일)

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
    # 차등 익절 정책 구간 (PM 승인, 2026-09-29): 손절 -7% 대비 익절 구간이 좁아
    # 손익비가 불리했던 문제 개선. +10% 이상은 2단계(PM 승인, 착수 승인 2026-09-29)로
    # 절반확정·트레일링 스탑을 구현.
    EXIT_BAND_HOLD            = "HOLD"             # <1%: 판단 근거 부족, 보유 유지
    EXIT_BAND_SIGNAL_CHECK    = "SIGNAL_CHECK"     # 1~5%: 진입 신호 유지 여부로 보유/전량매도 판단
    EXIT_BAND_AI_JUDGE        = "AI_JUDGE"         # 5~10%: 자비스 AI HOLD/HALF/ALL 판단
    EXIT_BAND_HALF_LOCK_TRAIL = "HALF_LOCK_TRAIL"  # 10%~: 절반 즉시 확정 + 잔여 트레일링 스탑

    # 트레일링 스탑 발동 기준: 잔여 보유분의 고점 대비 하락률(퍼센트 포인트)
    TRAILING_STOP_PCT = 3.0

    # ── 리스크 기반 포지션 사이징 (PM 승인, 2026-09-30) ──────────────
    # ML 정확도(42~64%)가 확신도 기반 사이징의 근거가 되기엔 약해 "종목당 최대 손실 고정"
    # 방식으로 재설계: 기본 매수금액 = 자산 × RISK_PER_TRADE_PCT ÷ |손절률|.
    # 손절률은 strategy_config.stop_loss 를 쓰고, 값이 없을 때만 이 참고값(현재 운영값 -7%)을 쓴다.
    # 실제 계산 로직은 common/position_sizing.py로 이관(order_handler.py와 공용, 중복 구현 금지).
    DEFAULT_STOP_LOSS_PCT = position_sizing.DEFAULT_STOP_LOSS_PCT
    VOLATILITY_BANDS = position_sizing.VOLATILITY_BANDS
    VOLATILITY_MULT_HIGH = position_sizing.VOLATILITY_MULT_HIGH
    VOLATILITY_MULT_FALLBACK = position_sizing.VOLATILITY_MULT_FALLBACK
    ATR_PERIOD = position_sizing.ATR_PERIOD

    def __init__(self):
        self.running    = False
        self.trader     = KISTrader()
        self.positions  = {}
        self.strategies = {}
        self.ml_trained_date = None
        self._selling   = set()  # 현재 매도 처리중인 심볼 (동시 매도 주문 충돌 방지)

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

    async def _warn_if_position_sizing_exceeds_equity(self):
        """리스크 기반 사이징(PM 승인, 2026-09-30) 정합성 점검: max_positions × 기본매수금액이
        총자산을 넘으면 시작 로그로 경고만 남긴다(자동 조정은 하지 않는다)."""
        try:
            balance = await self.trader.get_balance()
            equity = float(balance.get("total") or 0) or float(config.INITIAL_SEED_KRW)

            active = self.get_all_active_strategies()
            max_positions = 5
            stop_loss_pct = self.DEFAULT_STOP_LOSS_PCT
            if active:
                _, first_params = active[0]
                max_positions = int(first_params.get("max_positions", max_positions))
                stop_loss_pct = abs(float(first_params.get("stop_loss", stop_loss_pct)))

            base_amount = self._compute_base_amount(equity, stop_loss_pct, config.RISK_PER_TRADE_PCT)
            projected = max_positions * base_amount
            if projected > equity:
                logger.warning(
                    f"⚠️ 포지션 사이징 경고: max_positions({max_positions}) × 기본매수금액"
                    f"({base_amount:,.0f}원) = {projected:,.0f}원이 총자산({equity:,.0f}원)을 "
                    f"초과합니다. (RISK_PER_TRADE_PCT={config.RISK_PER_TRADE_PCT}%, "
                    f"손절률={stop_loss_pct}%)"
                )
        except Exception as e:
            logger.warning(f"포지션 사이징 정합성 점검 실패(무시): {e}")

    def build_strategy(self, name, params):
        # strategy_config.params의 stop_loss는 채팅 설정(router/handlers/setting_handler.py)·
        # DB 시드값(migrations/V001)과 동일하게 "퍼센트 숫자 그대로"(-7 = -7%) 저장 관례를
        # 따른다. 전략 클래스(MACrossConfig 등)의 stop_loss는 비율(-0.07)을 기대하므로
        # 여기서 /100 변환을 거치지 않으면 손절선이 사실상 도달 불가능한 값(-200%~-700%)이
        # 되어 강제 손절이 전혀 트리거되지 않는다 (일동제약 -10.9% 미손절 원인).
        if name == "MA크로스":
            return MACrossStrategy(MACrossConfig(
                short_period  = int(params.get("short", 5)),
                long_period   = int(params.get("long", 20)),
                stop_loss     = float(params.get("stop_loss", -2)) / 100,
                take_profit   = float(params.get("take_profit", 5)) / 100,
                buy_amount    = int(params.get("buy_amount", 500000)),
                max_positions = int(params.get("max_positions", 5)),
            ))
        if name == "RSI반등":
            return RSIStrategy(RSIConfig(
                period        = int(params.get("period", 14)),
                entry         = float(params.get("entry", 30)),
                exit          = float(params.get("exit", 60)),
                stop_loss     = float(params.get("stop_loss", -3)) / 100,
                buy_amount    = int(params.get("buy_amount", 500000)),
                max_positions = int(params.get("max_positions", 5)),
            ))
        if name == "볼린저밴드":
            return BollingerStrategy(BollingerConfig(
                period        = int(params.get("period", 20)),
                std_dev       = float(params.get("std", 2.0)),
                stop_loss     = float(params.get("stop_loss", -3)) / 100,
                buy_amount    = int(params.get("buy_amount", 500000)),
                max_positions = int(params.get("max_positions", 5)),
            ))
        return None

    @staticmethod
    def classify_exit_band(pnl_rate: float) -> str:
        """차등 익절 정책(PM 승인)의 손익률 구간 분류.
        pnl_rate는 퍼센트 숫자 그대로(+5 = +5%)를 받는다."""
        if pnl_rate < 1.0:
            return StockTrader.EXIT_BAND_HOLD
        if pnl_rate < 5.0:
            return StockTrader.EXIT_BAND_SIGNAL_CHECK
        if pnl_rate < 10.0:
            return StockTrader.EXIT_BAND_AI_JUDGE
        return StockTrader.EXIT_BAND_HALF_LOCK_TRAIL

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
        await self._warn_if_position_sizing_exceeds_equity()

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
                if await self._ml_already_trained(today):
                    logger.info("🎓 오늘 ML 학습 이미 완료 — 스킵")
                else:
                    logger.info("🎓 장 마감 후 ML 자동 학습 시작...")
                    if await self._run_ml_training():
                        await self._mark_ml_trained(today)
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
                await self._notify_kis_recovered_if_needed()
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
                elif self._is_kis_connect_error(e, err_str):
                    await self._handle_kis_connect_error(err_str)
                elif "Server disconnected" not in err_str and "ServerDisconnected" not in err_str:
                    await self._notify_error(err_str)

            await asyncio.sleep(config.COLLECT_INTERVAL_SEC)

    @staticmethod
    def _is_kis_connect_error(exc: Exception, err_str: str) -> bool:
        """KIS 등 외부 API로의 소켓 연결 자체가 실패한 경우를 판별.
        "Cannot connect to host ..."류 문자열엔 'connect'만 있고 'connection'은 없어
        위 DB 재연결 분기에 걸리지 않는다 — 이 경우 알림만 보내고 다음 사이클을 그냥 기다리게 된다."""
        try:
            import aiohttp
            if isinstance(exc, (aiohttp.ClientConnectorError, aiohttp.ClientConnectionError)):
                return True
        except Exception:
            pass
        _lower = err_str.lower()
        return "cannot connect to host" in _lower or "connect call failed" in _lower

    async def _handle_kis_connect_error(self, err_str: str):
        """KIS 연결 오류 연속 발생 시: 3회부터 토큰 강제 재발급 + 알림 빈도 축소(5분/회), 재시도는 계속 유지"""
        self._kis_conn_err_count = getattr(self, "_kis_conn_err_count", 0) + 1
        logger.warning(f"🔌 KIS 연결 오류 ({self._kis_conn_err_count}회 연속): {err_str}")

        if self._kis_conn_err_count < 3:
            await self._notify_error(err_str)
            return

        try:
            await self.trader.force_reissue_token()
            logger.info("🔄 KIS 토큰 강제 재발급 시도 완료")
        except Exception as tok_err:
            logger.error(f"❌ KIS 토큰 강제 재발급 실패: {tok_err}")

        now_ts = _time.time()
        last_notify = getattr(self, "_kis_conn_last_notify_ts", 0.0)
        if now_ts - last_notify >= 300:
            self._kis_conn_last_notify_ts = now_ts
            await self._notify_error(f"KIS 연결 오류 {self._kis_conn_err_count}회 연속 — 재시도 중")

    async def _notify_kis_recovered_if_needed(self):
        """직전까지 KIS 연결 오류가 있었다면 복구 알림 1회 발송 후 카운터 초기화"""
        if getattr(self, "_kis_conn_err_count", 0) > 0:
            logger.info("✅ KIS 연결 복구됨")
            try:
                from common.telegram import send_stock
                await send_stock("✅ KIS 연결 복구됨")
            except Exception:
                pass
        self._kis_conn_err_count = 0

    # ── ML 자동 학습 ──────────────────────────────────────
    async def _ml_already_trained(self, today) -> bool:
        """오늘 학습 여부 — 메모리 값과 Redis 키(재시작 후에도 유지) 둘 다 확인. Redis 실패 시 메모리 값만 사용"""
        if self.ml_trained_date == today:
            return True
        try:
            return bool(await cache.client.exists(f"ml:trained:{today.isoformat()}"))
        except Exception as e:
            logger.warning(f"ML 학습 여부 Redis 확인 실패 — 메모리 값만 사용: {type(e).__name__}")
            return False

    async def _mark_ml_trained(self, today):
        try:
            await cache.client.set(f"ml:trained:{today.isoformat()}", "1", ex=ML_TRAINED_KEY_TTL_SEC)
        except Exception as e:
            logger.warning(f"ML 학습 완료 Redis 기록 실패 — 재시작 시 재학습될 수 있음: {type(e).__name__}")

    async def _run_ml_training(self) -> bool:
        """장 마감 후 감시 종목 전체 자동 학습. 학습 절차가 예외 없이 끝나면 True"""
        try:
            from ml.model import MLModelManager
            ml = MLModelManager(db_pool=db.pool)

            try:
                watchlist_symbols = await db.get_watchlist_symbols()
            except Exception:
                watchlist_symbols = []
            symbols_to_train = watchlist_symbols if watchlist_symbols else config.STOCK_SYMBOLS
            logger.info(f"🎓 ML 학습 대상: {len(symbols_to_train)}종목")

            results = []          # _save_ml_memory 용 기존 형식 ("✅ 종목: 정확도%")
            trained = []          # 보고서용 성공 종목 {symbol, accuracy, samples}
            insufficient = other = 0
            for symbol in symbols_to_train:
                try:
                    ohlcv = await db.get_recent_ohlcv(symbol, limit=1500, asset="stock", daily=True)
                    if len(ohlcv) < 60:
                        logger.warning(f"[{symbol}] OHLCV 부족 ({len(ohlcv)}개) → 스킵")
                        insufficient += 1
                        continue
                    result = await ml.train(symbol, ohlcv)
                    if result["success"]:
                        logger.info(f"✅ [{symbol}] 학습 완료 — 정확도: {result['accuracy']}%")
                        results.append(f"✅ {symbol}: {result['accuracy']}%")
                        trained.append({"symbol": symbol, "accuracy": result["accuracy"],
                                        "samples": result.get("samples", 0)})
                    else:
                        results.append(f"⚠️ {symbol}: {result['error']}")
                        if "데이터 부족" in str(result.get("error", "")):
                            insufficient += 1
                        else:
                            other += 1
                except Exception as e:
                    logger.error(f"❌ [{symbol}] 학습 오류: {e}")
                    other += 1

            if symbols_to_train:
                sem = asyncio.Semaphore(10)

                async def _name(r):
                    async with sem:
                        r["name"] = await _resolve_stock_name(r["symbol"])

                await asyncio.gather(*(_name(r) for r in trained))
                meta = {"total": len(symbols_to_train), "insufficient": insufficient, "other": other}
                msg = build_ml_report_text(trained, meta, datetime.now(KST))
                from common.telegram import send_report
                await send_report(msg)
            if results:
                await self._save_ml_memory(results, symbols_to_train)
            return True

        except Exception as e:
            logger.error(f"❌ ML 학습 오류: {e}")
            await self._notify_error(f"ML 자동 학습 실패: {e}")
            return False

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

                        if pnl_rate <= -3.0:
                            # 손실 매도 알림: 상태 기반 스로틀링 (상태 변경 시 1회, 동일 상태 최소 1시간)
                            try:
                                nm = pos.get("name", symbol)
                                if pnl_rate <= -7.0:
                                    suppress_key = f"sell_fail_suppress:{symbol}"
                                    suppress_raw = None
                                    try:
                                        suppress_raw = await cache.client.get(suppress_key)
                                    except Exception:
                                        pass

                                    if suppress_raw:
                                        # 손절 재시도 대기: 텔레그램 발송하지 않고 로그만 기록 (알림 폭탄 방지)
                                        suppress_reason = "손절 매도 실패 재시도 억제 중"
                                        try:
                                            parsed = json.loads(suppress_raw)
                                            if isinstance(parsed, dict) and "reason" in parsed:
                                                suppress_reason = parsed["reason"]
                                        except Exception:
                                            if isinstance(suppress_raw, str) and ":" in suppress_raw:
                                                suppress_reason = suppress_raw.split(":", 1)[1]
                                        logger.info(
                                            f"⏳ [{symbol}] {nm} 손절 재시도 대기 중 ({pnl_rate:+.1f}%, 사유: {suppress_reason}) — 텔레그램 알림 생략"
                                        )
                                        continue
                                    else:
                                        new_state = "STOP_LOSS"
                                        msg = (
                                            f"⚠️ <b>{nm}({symbol}) 손절선(-7%) 도달 {pnl_rate:+.1f}%</b>\n"
                                            f"시스템이 즉시 자동 손절 매도 처리 중입니다."
                                        )
                                    from common.alert_throttle import should_send_symbol_alert
                                    should_send = await should_send_symbol_alert(
                                        symbol, "price_monitor", new_state,
                                        min_interval_sec=3600, redis_client=cache.client,
                                        now_ts=now_ts
                                    )
                                    if should_send:
                                        from common.telegram import send_stock
                                        await send_stock(msg)
                                else:
                                    new_state = "DROP"
                                    from common.alert_throttle import (
                                        should_send_symbol_alert,
                                        should_send_drop_alert,
                                    )
                                    # 일반 상태(STOP_LOSS 전이 감지용) 기록은 그대로 유지
                                    await should_send_symbol_alert(
                                        symbol, "price_monitor", new_state,
                                        min_interval_sec=3600, redis_client=cache.client,
                                        now_ts=now_ts
                                    )
                                    should_send, prev_pnl = await should_send_drop_alert(
                                        symbol, pnl_rate, redis_client=cache.client
                                    )
                                    if should_send:
                                        extra = ""
                                        if prev_pnl is not None:
                                            extra = f"\n(직전 알림 대비 {prev_pnl - pnl_rate:.1f}%p 추가 하락)"
                                        msg = (
                                            f"⚡ <b>{nm}({symbol}) 급락 {pnl_rate:+.1f}%</b>\n"
                                            f"-7% 도달 시 즉시 자동 손절 집행 예정(현재 손절선 근접 감시 중)\n"
                                            f"즉시 매도를 원하시면 '{nm} 전량 매도' 지시해주세요."
                                            f"{extra}"
                                        )
                                        from common.telegram import send_stock
                                        await send_stock(msg)
                            except Exception as se:
                                logger.warning(f"가격 모니터 알림 발송 실패 [{symbol}]: {se}")
                            continue
                        else:
                            # 정상 범위(>-3%) 복귀 시 상태 갱신 (다음 급락 시 즉시 1회 알림 보장)
                            try:
                                from common.alert_throttle import should_send_symbol_alert
                                await should_send_symbol_alert(
                                    symbol, "price_monitor", "NORMAL",
                                    min_interval_sec=3600, redis_client=cache.client,
                                    now_ts=now_ts
                                )
                            except Exception:
                                pass

                        if pnl_rate >= 7.0:
                            last_alert = alert_cooldown.get(symbol, 0)
                            if now_ts - last_alert < 300:  # 급등 판단 5분 쿨다운
                                continue
                            alert_cooldown[symbol] = now_ts
                            direction = "급등"
                            if await self._recently_sold(symbol):
                                logger.info(f"⏭️ [{symbol}] 최근 10분 내 매도 기록 있음 — KIS 잔고 반영 지연으로 판단, 재시도 스킵")
                                self.positions.pop(symbol, None)
                                continue
                            _sellable = pos.get("sellable_qty", pos.get("qty", 0))
                            if _sellable <= 0:
                                logger.info(f"⏸️ [{symbol}] 주문가능수량 0 — 급등 매도 판단 스킵")
                                continue
                            if symbol in self._selling:
                                logger.info(f"⏭️ [{symbol}] 다른 경로에서 매도 처리중 — 중복 시도 스킵")
                                continue
                            self._selling.add(symbol)
                            try:
                                logger.info(f"⚡ [{symbol}] {direction} 감지: {pnl_rate:+.1f}% → Jarvis 판단")
                                await self._jarvis_exit_check(
                                    symbol=symbol,
                                    cur_price=cur_price,
                                    avg_price=avg_price,
                                    pnl_rate=pnl_rate,
                                    qty=_sellable,
                                )
                            finally:
                                self._selling.discard(symbol)
            except Exception as e:
                logger.debug(f"가격 모니터 오류: {e}")

            await asyncio.sleep(3)

    async def _invalidate_position_cache(self):
        """체결 직후 대시보드 보유/계좌 캐시 즉시 삭제 → 화면에 옛 수량이 남지 않게
        (대시보드는 Redis cache:positions:stock 을 20초 캐시로 사용)"""
        try:
            for k in ("cache:positions:stock", "cache:account:stock"):
                await cache.client.delete(k)
        except Exception:
            pass

    _STOP_LOSS_TRANSIENT_MARKERS = ("체결 0주", "체결수량 0", "미체결")

    async def _compute_stop_loss_suppress_sec(self, symbol: str, err_msg: str):
        """손절 매도 실패 사유별 재시도 억제 시간 계산.
        - "체결 0주" 등 일시적 오류로 보이는 사유: 90초(60~120초 범위) 뒤 재시도
        - 같은 사유로 3회 이상 연속 실패: 5분 → 15분 → 30분으로 점진 확대(그 이상은 30분 고정)
        - 그 외 사유: 기존과 동일하게 30분 억제
        반환: (suppress_sec, 연속 실패 횟수)"""
        from common.alert_throttle import normalize_cause
        norm_reason = normalize_cause(err_msg)
        streak_key = f"sell_fail_streak:{symbol}"
        streak = None
        try:
            raw = await cache.client.get(streak_key)
            if raw:
                streak = json.loads(raw)
        except Exception:
            pass

        fail_count = 1
        if streak and streak.get("reason") == norm_reason:
            fail_count = int(streak.get("count", 0)) + 1

        try:
            await cache.client.setex(
                streak_key, 3600,
                json.dumps({"reason": norm_reason, "count": fail_count, "ts": datetime.now().timestamp()}),
            )
        except Exception:
            pass

        if fail_count >= 3:
            tier = min(fail_count - 2, 3)
            suppress_sec = {1: 300, 2: 900, 3: 1800}[tier]
        elif any(marker in err_msg for marker in self._STOP_LOSS_TRANSIENT_MARKERS):
            suppress_sec = 90
        else:
            suppress_sec = 1800

        return suppress_sec, fail_count

    @staticmethod
    def _format_suppress_sec(sec: int) -> str:
        if sec < 60:
            return f"{sec}초간"
        return f"{sec // 60}분간"

    async def _maybe_alert_stop_loss_worsening(self, symbol: str, name: str, pnl_rate: float):
        """손절 매도 재시도 억제 중에도 최소 5분 간격으로는 알림이 가도록 별도 스로틀 유지
        (원인별 시간당 1회 제한과 별개로, 사람이 계속 상황을 알 수 있게 하기 위함)"""
        alert_key = f"sell_fail_alert_5m:{symbol}"
        try:
            if await cache.client.get(alert_key):
                return
            await cache.client.setex(alert_key, 300, "1")
        except Exception:
            return
        try:
            from common.telegram import send_stock
            await send_stock(
                f"🔻 <b>{name}({symbol}) 손절 매도 대기 중 {pnl_rate:+.1f}%</b>\n"
                f"매도 재시도 억제 중입니다 — 상황을 계속 알려드립니다."
            )
        except Exception:
            pass

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
        """오늘 손절 횟수 (2회 이상이면 당일 신규 매수 중단)
        조회 자체가 실패하면 안전장치가 무력화된 것이므로, 실전에서는 통과(0)가 아니라
        차단 방향(큰 값)으로 fail-safe 처리한다."""
        try:
            async with db.pool.acquire() as conn:
                n = await conn.fetchval("""
                    SELECT COUNT(*) FROM trade_history
                    WHERE bot='stock_trader' AND side='SELL' AND strategy LIKE '%손절%'
                      AND DATE(ts AT TIME ZONE 'Asia/Seoul') = (NOW() AT TIME ZONE 'Asia/Seoul')::date
                """)
            return int(n or 0)
        except Exception as e:
            logger.error(f"⚠️ 당일 손절횟수 조회 실패 — 안전을 위해 신규매수 차단 처리: {e}")
            return 999

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
            # 매도 판단·실행은 '주문가능수량' 기준 (미체결/정산중 물량은 KIS가 매도 거부함)
            qty = pos.get("sellable_qty", pos.get("qty", 0))

            if not default_strategy or cur_price <= 0 or avg_price <= 0:
                continue
            if qty <= 0:
                logger.info(f"⏸️ [{symbol}] 주문가능수량 0 (미체결/정산중) — 매도 판단 스킵")
                continue

            pnl = (cur_price - avg_price) * qty
            pnl_rate = (cur_price - avg_price) / avg_price * 100

            # 손절 -7% 도달 → 즉시 자동 매도 (주인 지시: 알아서 처리)
            if default_strategy.check_stop_loss(avg_price, cur_price):
                nm = pos.get("name", symbol)
                suppress_key = f"sell_fail_suppress:{symbol}"
                try:
                    if await cache.client.get(suppress_key):
                        logger.debug(f"⏸️ [{symbol}] 손절 실패 쿨다운 중 — 매도 스킵")
                        await self._maybe_alert_stop_loss_worsening(symbol, nm, pnl_rate)
                        continue
                except Exception:
                    pass
                try:
                    # 속도제한(초당 거래건수 초과, EGW00201) 자동 재시도는
                    # KISTrader.sell() 내부에서 처리된다.
                    result = await self.trader.sell(symbol, cur_price, qty)
                    if result.get("success"):
                        _fq = result.get("filled_qty", qty)
                        _fpnl = int((cur_price - avg_price) * _fq)
                        remain = qty - _fq
                        await db.insert_trade(
                            bot="stock_trader", asset_type="stock",
                            symbol=symbol, side="SELL", price=cur_price, quantity=_fq,
                            amount=cur_price * _fq, strategy=f"{strat_name}_손절", pnl=_fpnl,
                        )
                        await self._invalidate_position_cache()
                        try:
                            await cache.client.delete(f"sell_fail_streak:{symbol}")
                            await cache.client.delete(f"sell_fail_alert_5m:{symbol}")
                        except Exception:
                            pass
                        from common.telegram import send_stock
                        await send_stock(
                            f"🔴 <b>{nm}({symbol}) 손절 매도 체결 {pnl_rate:+.1f}%</b>\n"
                            f"{_fq}주 @ {cur_price:,}원 (손실 {_fpnl:+,}원)\n"
                            f"평단 {avg_price:,.0f} → 매도 {cur_price:,.0f}"
                            + (f"\n잔여 {remain}주는 다음 사이클에 즉시 재시도합니다." if remain > 0 else "")
                        )
                        logger.info(f"🔴 손절 자동매도 체결 [{symbol}] {pnl_rate:+.1f}% ({_fq}주 체결, 잔여 {remain}주)")
                        if remain > 0:
                            # 부분체결: 포지션을 지우지 않고 잔량을 유지해 다음 사이클에 즉시 재시도
                            pos["qty"] = remain
                            pos["sellable_qty"] = remain
                            self.positions[symbol] = pos
                        else:
                            try:
                                await cache.client.delete(
                                    f"half_tp:{symbol}", f"half_lock_done:{symbol}",
                                    f"trailing_high:{symbol}")
                            except Exception:
                                pass
                            self.positions.pop(symbol, None)
                            try:
                                from common.alert_throttle import reset_symbol_alert
                                await reset_symbol_alert(symbol, redis_client=cache.client)
                            except Exception:
                                pass
                    else:
                        err_msg = str(result.get('error', '알 수 없음'))
                        suppress_sec, fail_count = await self._compute_stop_loss_suppress_sec(symbol, err_msg)
                        try:
                            suppress_val = json.dumps({
                                "reason": err_msg, "ts": datetime.now().timestamp(), "fail_count": fail_count,
                            })
                            await cache.client.setex(suppress_key, suppress_sec, suppress_val)
                        except Exception:
                            pass
                        try:
                            from common.alert_throttle import should_send_symbol_alert
                            should_send = await should_send_symbol_alert(
                                symbol, "sell_fail", err_msg,
                                min_interval_sec=3600, redis_client=cache.client
                            )
                            if should_send:
                                from common.telegram import send_stock
                                await send_stock(
                                    f"⚠️ <b>{nm}({symbol}) 손절 매도 실패</b> {pnl_rate:+.1f}%\n"
                                    f"사유: {err_msg} — {self._format_suppress_sec(suppress_sec)} 재시도 억제 "
                                    f"(연속 {fail_count}회)"
                                )
                        except Exception:
                            pass
                        logger.warning(
                            f"손절 매도 실패 [{symbol}]: {err_msg} (연속 {fail_count}회, {suppress_sec}초 억제)"
                        )
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
                        half_qty = result.get("filled_qty", half_qty)
                        half_pnl = int((cur_price - avg_price) * half_qty)
                        await db.insert_trade(
                            bot="stock_trader", asset_type="stock",
                            symbol=symbol, side="SELL",
                            price=cur_price, quantity=half_qty,
                            amount=cur_price * half_qty,
                            strategy=f"{strat_name}_급등절반익절", pnl=half_pnl,
                        )
                        await self._invalidate_position_cache()
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

            # 익절 구간 판단 +1~5%: 진입 신호(strategy.generate_signal)가 아직 살아있으면
            # (예: MA크로스 골든크로스 유지) 보유 지속, 신호가 꺾였으면(데드크로스 전환 등)
            # 당일 중 전량 매도로 확정 (차등 익절 정책, PM 승인)
            if self.classify_exit_band(pnl_rate) == self.EXIT_BAND_SIGNAL_CHECK \
                    and qty >= 1 and symbol not in self._selling:
                try:
                    rows = await db.get_recent_ohlcv(symbol, limit=100, asset="stock", daily=True)
                    entry_prices = [float(r["close"]) for r in rows] if rows else []
                    sig = default_strategy.generate_signal(symbol, entry_prices) if entry_prices else None
                    if sig == "SELL":
                        result = await self.trader.sell(symbol, cur_price, qty)
                        if result.get("success"):
                            sold_qty = result.get("filled_qty", qty)
                            s_pnl = int((cur_price - avg_price) * sold_qty)
                            remain = qty - sold_qty
                            await db.insert_trade(
                                bot="stock_trader", asset_type="stock",
                                symbol=symbol, side="SELL",
                                price=cur_price, quantity=sold_qty,
                                amount=cur_price * sold_qty,
                                strategy=f"{strat_name}_구간익절_신호소멸", pnl=s_pnl,
                            )
                            await self._invalidate_position_cache()
                            from common.telegram import send_stock
                            await send_stock(
                                f"🟡 <b>{pos.get('name', symbol)} 익절(+1~5% 신호소멸 전량매도)</b>\n"
                                f"{sold_qty}주 매도 @ {cur_price:,}원 (손익 {pnl_rate:+.1f}%, 실현 {s_pnl:+,}원)\n"
                                f"사유: {strat_name} 진입 신호 소멸(데드크로스 등)"
                                + (f"\n잔여 {remain}주는 다음 사이클에 즉시 재시도합니다." if remain > 0 else "")
                            )
                            if remain > 0:
                                pos["qty"] = remain
                                pos["sellable_qty"] = remain
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
                                try:
                                    from common.alert_throttle import reset_symbol_alert
                                    await reset_symbol_alert(symbol, redis_client=cache.client)
                                except Exception:
                                    pass
                            continue
                        else:
                            logger.warning(
                                f"구간익절(+1~5% 신호소멸) 매도 실패 [{symbol}]: {result.get('error')}")
                except Exception as e:
                    logger.warning(f"구간 익절(+1~5%) 판단 오류 [{symbol}]: {e}")

            # 익절 AI 판단: +5% 이상이면 자비스가 HOLD/HALF/ALL 판단 (30분 쿨다운)
            # 차등 익절 정책(PM 승인)으로 문턱을 3%→5%로 상향. +10% 이상 절반확정·
            # 트레일링 스탑은 다음 단계에서 별도 구현 예정 — 그 전까지는 이 AI 판단이
            # +5% 이상 전 구간에 적용된다.
            exit_band = self.classify_exit_band(pnl_rate)
            if exit_band == self.EXIT_BAND_AI_JUDGE and qty >= 1 and symbol not in self._selling:
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

                        if decision in ("HALF", "ALL") and symbol not in self._selling:
                            self._selling.add(symbol)
                            sell_qty = max(1, qty // 2) if (decision == "HALF" and qty >= 2) else qty
                            try:
                                result = await self.trader.sell(symbol, cur_price, sell_qty)
                            finally:
                                self._selling.discard(symbol)
                            if result.get("success"):
                                sell_qty = result.get("filled_qty", sell_qty)
                                s_pnl = int((cur_price - avg_price) * sell_qty)
                                await db.insert_trade(
                                    bot="stock_trader", asset_type="stock",
                                    symbol=symbol, side="SELL",
                                    price=cur_price, quantity=sell_qty,
                                    amount=cur_price * sell_qty,
                                    strategy=f"{strat_name}_AI익절{decision}", pnl=s_pnl,
                                )
                                await self._invalidate_position_cache()
                                from common.telegram import send_stock
                                remain = qty - sell_qty
                                await send_stock(
                                    f"🤖 <b>{pos.get('name', symbol)} 익절(+5%↑ AI판단 {decision})</b>\n"
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
                                        from common.alert_throttle import reset_symbol_alert
                                        await reset_symbol_alert(symbol, redis_client=cache.client)
                                    except Exception:
                                        pass
                                    try:
                                        from datetime import datetime as _dt
                                        _now = _dt.now()
                                        _eod = _now.replace(hour=23, minute=59, second=0)
                                        await cache.client.setex(
                                            f"rebuy_block:{symbol}",
                                            max(60, int((_eod - _now).total_seconds())), "tp")
                                    except Exception:
                                        pass
                            else:
                                # 매도 실패: 원인을 로그·매매일지에 남기고, 반복 재시도 방지 위해 쿨다운 대폭 연장
                                _err = result.get("error", "알 수 없음")
                                logger.warning(f"AI 익절 매도 실패 [{symbol}]: {_err}")
                                await cache.client.setex(f"exit_ai_cool:{symbol}", 6 * 3600, "1")
                                await db.insert_trade(
                                    bot="stock_trader", asset_type="stock",
                                    symbol=symbol, side="SELL",
                                    price=cur_price, quantity=0,
                                    amount=0,
                                    strategy=f"{strat_name}_AI익절{decision}_실패:{_err[:40]}", pnl=0,
                                )
                                try:
                                    from common.alert_throttle import should_send_symbol_alert
                                    should_send = await should_send_symbol_alert(
                                        symbol, "exit_ai_fail", _err,
                                        min_interval_sec=3600, redis_client=cache.client
                                    )
                                    if should_send:
                                        from common.telegram import send_stock
                                        await send_stock(
                                            f"⚠️ <b>{pos.get('name', symbol)} AI 익절 매도 실패</b>\n"
                                            f"판단: {decision} {sell_qty}주 시도\n사유: {_err}\n"
                                            f"6시간 동안 이 종목 재시도를 중단합니다.")
                                except Exception:
                                    pass
                                continue
                except Exception as e:
                    logger.warning(f"AI 익절 판단 처리 오류 [{symbol}]: {e}")

            # 익절 +10% 이상: 절반 즉시 확정 매도 + 잔여는 고점 대비 -TRAILING_STOP_PCT%
            # 하락 시 전량 매도하는 트레일링 스탑으로 관리 (차등 익절 정책 2단계, PM 승인)
            if exit_band == self.EXIT_BAND_HALF_LOCK_TRAIL and qty >= 1 and symbol not in self._selling:
                half_lock_key = f"half_lock_done:{symbol}"
                trailing_high_key = f"trailing_high:{symbol}"
                try:
                    if not await cache.client.get(half_lock_key):
                        half_qty = max(1, qty // 2)
                        self._selling.add(symbol)
                        try:
                            result = await self.trader.sell(symbol, cur_price, half_qty)
                        finally:
                            self._selling.discard(symbol)
                        if result.get("success"):
                            sold_qty = result.get("filled_qty", half_qty)
                            s_pnl = int((cur_price - avg_price) * sold_qty)
                            remain = qty - sold_qty
                            await db.insert_trade(
                                bot="stock_trader", asset_type="stock",
                                symbol=symbol, side="SELL",
                                price=cur_price, quantity=sold_qty,
                                amount=cur_price * sold_qty,
                                strategy=f"{strat_name}_절반확정", pnl=s_pnl,
                            )
                            await self._invalidate_position_cache()
                            await cache.client.setex(half_lock_key, 30 * 86400, "1")
                            await cache.client.setex(trailing_high_key, 30 * 86400, str(cur_price))
                            from common.telegram import send_stock
                            await send_stock(
                                f"🟢 <b>{pos.get('name', symbol)} 익절(+10%↑ 절반확정)</b>\n"
                                f"{sold_qty}주 매도 @ {cur_price:,}원 (손익 {pnl_rate:+.1f}%, 실현 {s_pnl:+,}원)\n"
                                f"잔여 {remain}주는 고점 대비 -{self.TRAILING_STOP_PCT:.0f}% 하락 시 "
                                f"전량 매도하는 트레일링 스탑으로 관리합니다."
                            )
                            if remain > 0:
                                pos["qty"] = remain
                                pos["sellable_qty"] = remain
                                self.positions[symbol] = pos
                            else:
                                self.positions.pop(symbol, None)
                                await cache.client.delete(half_lock_key, trailing_high_key)
                            continue
                        else:
                            logger.warning(f"절반확정 매도 실패 [{symbol}]: {result.get('error')}")
                    else:
                        high_raw = await cache.client.get(trailing_high_key)
                        high = float(high_raw) if high_raw else cur_price
                        if cur_price > high:
                            high = cur_price
                            await cache.client.setex(trailing_high_key, 30 * 86400, str(high))

                        drop_pct = (high - cur_price) / high * 100 if high > 0 else 0
                        if drop_pct >= self.TRAILING_STOP_PCT:
                            self._selling.add(symbol)
                            try:
                                result = await self.trader.sell(symbol, cur_price, qty)
                            finally:
                                self._selling.discard(symbol)
                            if result.get("success"):
                                sold_qty = result.get("filled_qty", qty)
                                s_pnl = int((cur_price - avg_price) * sold_qty)
                                remain = qty - sold_qty
                                await db.insert_trade(
                                    bot="stock_trader", asset_type="stock",
                                    symbol=symbol, side="SELL",
                                    price=cur_price, quantity=sold_qty,
                                    amount=cur_price * sold_qty,
                                    strategy=f"{strat_name}_트레일링스탑", pnl=s_pnl,
                                )
                                await self._invalidate_position_cache()
                                from common.telegram import send_stock
                                await send_stock(
                                    f"📉 <b>{pos.get('name', symbol)} 트레일링 스탑 발동"
                                    f"(고점 대비 -{self.TRAILING_STOP_PCT:.0f}%)</b>\n"
                                    f"{sold_qty}주 매도 @ {cur_price:,}원 (손익 {pnl_rate:+.1f}%, 실현 {s_pnl:+,}원)\n"
                                    f"고점 {high:,.0f}원 대비 {drop_pct:.1f}% 하락"
                                    + (f"\n잔여 {remain}주는 다음 사이클에 즉시 재시도합니다." if remain > 0 else "")
                                )
                                if remain > 0:
                                    pos["qty"] = remain
                                    pos["sellable_qty"] = remain
                                    self.positions[symbol] = pos
                                else:
                                    self.positions.pop(symbol, None)
                                    await cache.client.delete(half_lock_key, trailing_high_key)
                                    try:
                                        from common.alert_throttle import reset_symbol_alert
                                        await reset_symbol_alert(symbol, redis_client=cache.client)
                                    except Exception:
                                        pass
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
                            else:
                                logger.warning(f"트레일링 스탑 매도 실패 [{symbol}]: {result.get('error')}")
                except Exception as e:
                    logger.warning(f"절반확정/트레일링 처리 오류 [{symbol}]: {e}")

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

        # 잔고는 사이클 시작 시 1회만 조회해 재사용한다(종목마다 조회하면 KIS 호출이
        # 급증해 초당 거래건수 제한에 걸림). 매수 체결 때마다 체결 금액만큼 아래 cash를
        # 로컬에서 차감해 같은 사이클의 다음 종목 판단에 반영한다(실제 체결가/수량은
        # dashboard가 최종 사이징하므로 근사치지만, self.positions 등록 값과 동일 기준).
        available_cash = await self.trader.get_balance()
        cash = available_cash.get("cash", 0)
        cash_stale = bool(available_cash.get("stale"))
        if cash <= 0:
            try:
                cached = await cache.client.get("stock:balance")
                if cached:
                    bal = json.loads(cached)
                    cash = int(bal.get("cash", 0))
            except Exception:
                pass
        if cash <= 0:
            logger.warning("⚠️ 잔고 조회 실패 → 이번 사이클 신규 매수 스킵 (다음 사이클 재시도)")
            return
        equity = float(available_cash.get("total") or 0) or float(config.INITIAL_SEED_KRW)

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

            # 투자경고/VI 상태 체크 (신규·추가매수만 차단, 보유종목 강제매도는 하지 않음)
            warning = await self.trader.get_market_warning(symbol)
            if warning is None:
                logger.warning(f"⚠️ [{symbol}] 종목상태 확인 불가 → 안전을 위해 매수 스킵")
                continue
            warn_code = warning.get("mrkt_warn_cls_code", "")
            if warn_code != "00":
                logger.info(f"⚠️ [{symbol}] 투자경고 종목(mrkt_warn_cls_code={warn_code}) — 매수 스킵")
                continue

            # 재매수 금지 체크 (손절 쿨다운)
            allowed, cd_reason = await self._check_rebuy_cooldown(symbol, cur_price)
            if not allowed:
                logger.info(f"⛔ [{symbol}] {cd_reason}")
                continue
            elif cd_reason:
                logger.info(f"✅ [{symbol}] {cd_reason}")

            if cash < 100000:
                logger.info(f"💸 잔고 부족 ({cash:,}원) → 매수 스킵")
                continue

            # ML 예측 결과 (Jarvis에게 참고 정보로 전달)
            ml_result = await self._get_ml_result(symbol, rows)

            # ML 문구는 reason 생성에만 사용 — 매수 금액 산정에서는 제외한다(PM 승인, 2026-09-30).
            # ML 정확도가 42~64%(동전 수준)라 확신도 기반 사이징의 근거가 약해 "종목당 최대
            # 손실 고정" 방식으로 대체했다.
            _, ml_text = self._ml_buy_plan(ml_result, cash)

            # 리스크 기반 매수 금액 = 기본(자산×위험비율÷손절률) × 변동성배율.
            # 확신도 배율(EXECUTE=1.0/EXECUTE_SMALL=0.5)과 예수금 최종 캡은 AI 판단 이후
            # dashboard/main.py(jarvis_signal)에서 적용한다 — 판단 시점이 다르기 때문.
            stop_loss_pct = abs(float(s_params.get("stop_loss", self.DEFAULT_STOP_LOSS_PCT)))
            base_amount = self._compute_base_amount(equity, stop_loss_pct, config.RISK_PER_TRADE_PCT)
            atr_pct = self._compute_atr_pct(rows, cur_price)
            vol_mult = self._volatility_multiplier(atr_pct)
            planned_amount = base_amount * vol_mult

            if planned_amount < cur_price:
                logger.info(
                    f"💸 [{symbol}] 리스크 기반 매수금액 부족 (기본×변동성={planned_amount:,.0f}원, "
                    f"1주={cur_price:,}원) → 매수 스킵"
                )
                continue
            qty = max(1, int(planned_amount // cur_price))

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
            cash_note = " ⚠️(마지막 확인: 지연됨)" if cash_stale else ""
            reason = (
                f"전략:{triggered_strategy} | {ml_text} "
                f"| 수급:{supply_reason} | 뉴스:{news_reason} "
                f"| 예수금:{cash:,.0f}원{cash_note} | 매수예정:{buy_amount_krw:,.0f}원"
            )
            logger.info(f"🤖 [{symbol}] Jarvis 판단 요청 — 예수금 {cash:,.0f}원{cash_note} / 매수 {buy_amount_krw:,.0f}원 ({qty}주×{cur_price:,}원)")

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
                            # 리스크 기반 사이징(PM 승인, 2026-09-30): 기본×변동성 금액과 예수금을
                            # 그대로 실어 보내 dashboard가 확신도 배율(EXECUTE/EXECUTE_SMALL)을
                            # 적용한 뒤 예수금 기준 최종 캡을 걸 수 있게 한다.
                            "base_amount": planned_amount,
                            "cash": cash,
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
                    # 체결 금액만큼 로컬 잔고를 차감해 같은 사이클의 다음 종목 판단에 반영
                    cash -= buy_amount_krw
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
        """ML 예측 결과. 성공 시 predict() 결과(success=True, buy_prob 포함) 그대로,
        실패 시 확률을 만들어 내지 않고 {"success": False, "reason": 짧은 사유}만 반환한다."""
        try:
            from ml.model import MLModelManager
            ml = MLModelManager(db_pool=db.pool)
            ohlcv = [{"date": str(r.get("ts",""))[:10].replace("-",""),
                      "open": float(r.get("open",0)), "high": float(r.get("high",0)),
                      "low": float(r.get("low",0)), "close": float(r.get("close",0)),
                      "volume": float(r.get("volume",0))} for r in ohlcv_rows]
            result = await ml.predict(symbol, ohlcv)
        except Exception as e:  # CancelledError/KeyboardInterrupt 는 BaseException 이라 삼키지 않는다
            logger.warning(f"⚠️ [{symbol}] ML 예측 예외: {type(e).__name__}: {str(e)[:200]}")
            return {"success": False, "reason": f"예외:{type(e).__name__}"}

        if not isinstance(result, dict) or not result.get("success"):
            err = (result.get("error") if isinstance(result, dict) else None) or ""
            reason = "모델 없음" if "모델 없음" in err else (err[:40] or "예측 없음")
            logger.info(f"ℹ️ [{symbol}] ML 예측 없음: {reason}")
            return {"success": False, "reason": reason}
        if not isinstance(result.get("buy_prob"), (int, float)):
            logger.warning(f"⚠️ [{symbol}] ML 예측 결과에 buy_prob 없음")
            return {"success": False, "reason": "예측 없음"}
        return result

    @staticmethod
    def _ml_buy_plan(ml_result: dict, cash: float) -> tuple:
        """ML 결과 → (매수 금액, reason 에 넣을 ML 문구).
        성공: 확률 구간별 비율(강함 30%/보통 20%/약함 15%/최소 10%) + "ML매수확률:NN%(강도)".
        실패: 확률을 쓰지 않고 최소 비율(10%) 고정 + "ML예측 없음(사유)".
        (stark/decision_engine.py 의 AI 폴백은 "ML매수확률:NN%" 만 읽으므로 실패 문구는 확률 0 으로 처리된다.)

        주의(PM 승인, 2026-09-30): 이 함수가 반환하는 매수 금액은 더 이상 실제 매수 금액
        산정에 쓰이지 않는다(리스크 기반 사이징으로 대체, _compute_base_amount 참고). 호출부는
        ml_text 만 사용한다. 기존 회귀 테스트(tests/test_ml_no_prediction.py)를 유지하기 위해
        반환값 자체는 그대로 둔다."""
        if ml_result.get("success") and isinstance(ml_result.get("buy_prob"), (int, float)):
            buy_prob = ml_result["buy_prob"]
            if buy_prob >= 0.90:
                ratio, strength = 0.30, "강함"
            elif buy_prob >= 0.80:
                ratio, strength = 0.20, "보통"
            elif buy_prob >= 0.70:
                ratio, strength = 0.15, "약함"
            else:
                ratio, strength = 0.10, "최소"
            ml_text = f"ML매수확률:{buy_prob:.0%}({strength})"
        else:
            ratio = 0.10
            ml_text = f"ML예측 없음({ml_result.get('reason') or '사유 미상'})"

        buy_amount = min(int(cash * ratio), cash)
        buy_amount = max(buy_amount, 100000)
        return buy_amount, ml_text

    @staticmethod
    def _compute_base_amount(equity: float, stop_loss_pct: float, risk_per_trade_pct: float) -> float:
        """기본 매수금액 계산 (common/position_sizing.py로 이관, 여기선 얇은 위임만 유지)."""
        return position_sizing.compute_base_amount(equity, stop_loss_pct, risk_per_trade_pct)

    @staticmethod
    def _compute_atr_pct(rows: list, cur_price: float, period: int = None) -> float:
        """ATR% 계산 (common/position_sizing.py로 이관, 여기선 얇은 위임만 유지)."""
        return position_sizing.compute_atr_pct(rows, cur_price, period)

    @classmethod
    def _volatility_multiplier(cls, atr_pct: float) -> float:
        """변동성 배율 (common/position_sizing.py로 이관, 여기선 얇은 위임만 유지)."""
        return position_sizing.volatility_multiplier(atr_pct)

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
                from common.telegram import send_report, send_stock
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
                total_eval = acct.get('total', 0)
                cash_label = f"{cash:,.0f}원" + (" ⚠️(마지막 확인: 지연됨)" if acct.get('stale') else "")

                report = (
                    f"📊 주식 6시간 리포트 ({now.strftime('%m/%d %H:%M')})\n\n"
                    f"매수 {len(buys)}건 / 매도 {len(sells)}건\n"
                    f"손익: {total_pnl:+,.0f}원\n\n"
                    f"보유: {', '.join(pos_list) if pos_list else '없음'}\n"
                    f"예수금: {cash_label}"
                )
                if total_eval > 0:
                    cum_pnl, cum_pnl_rate = compute_total_pnl(total_eval)
                    report += f"\n누적손익(원금대비): {cum_pnl:+,.0f}원 ({cum_pnl_rate:+.2f}%)"

                if trades:
                    report += "\n\n최근 매매:"
                    for t in list(trades)[:5]:
                        pnl = float(t['pnl'] or 0)
                        report += f"\n{'📈매수' if t['side']=='BUY' else '📉매도'} {t['symbol']} {float(t['amount']):,.0f}원"
                        if pnl:
                            report += f" ({pnl:+,.0f}원)"

                await send_stock(report)
                await send_report(report)  # TELEGRAM_CHANNEL_ID 채널로도 전송 (개인방 전송은 유지)
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
