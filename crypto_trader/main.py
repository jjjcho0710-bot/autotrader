"""
crypto-trader — AutoTrader
업비트 코인 자동매매
낮 (04:00~22:00): RSI 35이하, 익절 +1%, 손절 -5%
야간 (22:00~04:00): RSI 20이하만, 익절 +3%, 손절 -5%
+3% 이상 수익 시 Jarvis 판단 (HOLD/SELL)
"""
import asyncio
import json
import logging
import os
import signal
import time as _time
from datetime import datetime, timezone, timedelta

from common.config import config
from common.database import db, cache
from upbit_trader import UpbitTrader
from strategy.macd import MACDStrategy, MACDConfig
from strategy.rsi import RSIStrategy, RSIConfig

class KSTFormatter(logging.Formatter):
    def converter(self, timestamp):
        return _time.gmtime(timestamp + 9 * 3600)

_fmt = KSTFormatter(fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logging.basicConfig(level=logging.INFO)
logging.root.handlers[0].setFormatter(_fmt)
logger = logging.getLogger("crypto-trader")

KST = timezone(timedelta(hours=9))
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "https://dashboard-production-65e3.up.railway.app")

MAJOR_PAIRS = [
    "KRW-BTC","KRW-ETH","KRW-XRP","KRW-SOL","KRW-ADA",
    "KRW-DOGE","KRW-AVAX","KRW-LINK","KRW-DOT","KRW-SUI",
    "KRW-TRX","KRW-NEAR","KRW-ARB","KRW-SHIB",
    "KRW-APT","KRW-SAND","KRW-ATOM","KRW-FIL","KRW-AXS","KRW-XLM",
]

COIN_NAMES = {
    "KRW-BTC":"비트코인","KRW-ETH":"이더리움","KRW-XRP":"리플",
    "KRW-SOL":"솔라나","KRW-ADA":"에이다","KRW-DOGE":"도지코인",
    "KRW-AVAX":"아발란체","KRW-LINK":"체인링크","KRW-DOT":"폴카닷",
    "KRW-SUI":"수이","KRW-TRX":"트론","KRW-NEAR":"니어프로토콜",
    "KRW-ARB":"아비트럼","KRW-SHIB":"시바이누","KRW-APT":"앱토스",
    "KRW-SAND":"샌드박스","KRW-ATOM":"코스모스","KRW-FIL":"파일코인",
    "KRW-AXS":"엑시인피니티","KRW-XLM":"스텔라루멘",
}

# 시간대별 전략 파라미터
DAY_PARAMS   = {"rsi_entry": 35, "take_profit": 0.01, "stop_loss": -0.05}
NIGHT_PARAMS = {"rsi_entry": 20, "take_profit": 0.03, "stop_loss": -0.05}

# 주말 보수 모드 (거래량 적어 하락 위험 큼)
WEEKEND_DAY_RSI   = 30    # 주말 낮: RSI 35→30 더 빡세게
WEEKEND_NIGHT_BUY = False # 주말 야간(금·토 22시~): 매수 중단
BUY_AMOUNT_KRW   = 500_000  # 1회 매수 고정금액 50만원
MAX_POSITIONS    = 4        # 최대 동시 보유 종목 수
MIN_NET_PROFIT   = 1_000    # 매도 시 최소 순수익 (수수료 제외 1천원)
UPBIT_FEE_RATE   = 0.0005  # 업비트 수수료 0.05% (왕복 0.1%)

# 코인 목록 자동 갱신 설정
MAX_EXTRA_PAIRS   = 10          # 메이저 외 거래량 상위 코인 최대 추가 개수
MIN_TRADE_VALUE   = 100_000_000_000  # 최소 24h 거래대금 1000억 (잡코인 차단 강화)
MIN_LISTING_DAYS  = 180         # 최소 상장 경과일 (6개월)
TOP_N_SCAN        = 40          # 거래량 상위 N개 안에서만 선택 (잡코인 차단)

# 블랙리스트 — 밈코인/부실코인/스테이블 제외
COIN_BLACKLIST = {
    "KRW-USDT","KRW-USDC","KRW-BUSD","KRW-DAI","KRW-TUSD",  # 스테이블
    "KRW-SLX","KRW-RE","KRW-BORA","KRW-MED","KRW-CRE",      # 부실 전력
}

# 하이브리드 트레일링 익절 설정
TRAIL_ACTIVATE    = 0.015       # +1.5% 넘으면 트레일링 발동
TRAIL_GAP         = 0.007       # 고점 대비 -0.7% 떨어지면 매도


def _is_night(now_kst: datetime) -> bool:
    """22:00~04:00 야간 여부"""
    h = now_kst.hour
    return h >= 22 or h < 4


def _is_weekend(now_kst: datetime) -> bool:
    """주말(토·일) 여부. 금요일 밤 22시 이후도 주말 취급"""
    wd = now_kst.weekday()  # 월0 ~ 일6
    # 토(5), 일(6)
    if wd in (5, 6):
        return True
    # 금(4) 22시 이후 → 주말 야간 시작
    if wd == 4 and now_kst.hour >= 22:
        return True
    return False


def _calc_rsi(prices: list, period: int = 14) -> float:
    if len(prices) < period + 1:
        return 50.0
    gains  = [max(prices[i] - prices[i-1], 0) for i in range(-period, 0)]
    losses = [max(prices[i-1] - prices[i], 0) for i in range(-period, 0)]
    ag = sum(gains) / period
    al = sum(losses) / period
    if al == 0:
        return 100.0
    return 100 - (100 / (1 + ag / al))


class CryptoTrader:
    def __init__(self):
        self.running          = False
        self.trader           = UpbitTrader()
        self.positions        = {}
        self.strategies       = {}
        self.report_sent_date = None
        self.active_pairs     = list(MAJOR_PAIRS)   # 메이저 20개 + 신규(스캔으로 추가)
        self.extra_pairs      = []                  # 스캔으로 추가된 신규 코인

    async def start(self):
        self.running = True
        await db.connect()
        await cache.connect()
        await self.trader.start()
        await self._init_default_strategies()
        await self.load_strategies()

        # 초기 코인 스캔 (메이저 20 + 신규 최대 5)
        await self._scan_coins()
        config.CRYPTO_PAIRS = self.active_pairs
        logger.info("✅ 활성 코인 %d개 (메이저 20 + 신규 %d)",
                    len(self.active_pairs), len(self.extra_pairs))

        asyncio.create_task(self._init_ohlcv())

        logger.info("=" * 50)
        logger.info("🚀 AutoTrader crypto-trader 시작")
        logger.info("=" * 50)

        asyncio.create_task(self._subscribe_strategy_changes())
        asyncio.create_task(self._scan_loop())
        asyncio.create_task(self._price_loop())
        asyncio.create_task(self._price_monitor())
        asyncio.create_task(self._ohlcv_update_loop())   # 1분봉 실시간 갱신
        asyncio.create_task(self._six_hour_report_loop())  # 6시간 요약
        asyncio.create_task(self._telegram_polling_loop())

        await self._loop()

    async def _init_default_strategies(self):
        defaults = [
            ("MACD",   True, {"fast":12,"slow":26,"signal":9,"stop_loss":-0.05,"take_profit":0.01,"buy_amount":10000}),
            ("RSI반등", True, {"period":14,"entry":35,"exit":65,"stop_loss":-0.05,"take_profit":0.01,"buy_amount":10000}),
        ]
        async with db.pool.acquire() as conn:
            for name, active, params in defaults:
                await conn.execute(
                    "INSERT INTO strategy_config (bot,name,is_active,params) VALUES ('crypto_trader',$1,$2,$3) ON CONFLICT (bot,name) DO NOTHING",
                    name, active, json.dumps(params)
                )
        logger.info("✅ 코인 기본 전략 확인 완료")

    async def load_strategies(self):
        try:
            async with db.pool.acquire() as conn:
                rows = await conn.fetch("SELECT name,is_active,params FROM strategy_config WHERE bot='crypto_trader'")
            self.strategies = {}
            for r in rows:
                params = r["params"]
                if isinstance(params, str):
                    params = json.loads(params)
                self.strategies[r["name"]] = {"is_active": r["is_active"], "params": params or {}}
            active = [n for n, s in self.strategies.items() if s["is_active"]]
            logger.info("📋 전략 로드: %s", active)
        except Exception as e:
            logger.error("전략 로드 실패: %s", e)

    async def _subscribe_strategy_changes(self):
        try:
            pubsub = cache.client.pubsub()
            await pubsub.subscribe("strategy_changes")
            async for msg in pubsub.listen():
                if msg["type"] == "message":
                    logger.info("🔄 전략 변경 → 재로드")
                    await self.load_strategies()
        except Exception as e:
            logger.warning("전략 구독 오류: %s", e)

    async def _loop(self):
        while self.running:
            now = datetime.now(KST)
            night_tag = "🌙야간" if _is_night(now) else "☀️낮"
            logger.info("🔄 코인 매매 사이클 [%s] %s", now.strftime('%H:%M:%S'), night_tag)
            try:
                await self._run_cycle()
            except Exception as e:
                logger.error("매매 사이클 오류: %s", e)
            await asyncio.sleep(60)

    async def _price_loop(self):
        """3초마다 전 코인 시세 갱신 → Redis"""
        while self.running:
            try:
                prices_data = {}
                for pair in self.active_pairs:
                    try:
                        cur = await self.trader.get_current_price(pair)
                        if cur > 0:
                            prev_key = "crypto:prev:" + pair
                            prev = await cache.client.get(prev_key)
                            prev_price = float(prev) if prev else cur
                            change_rate = ((cur - prev_price) / prev_price * 100) if prev_price > 0 else 0
                            await cache.client.setex(prev_key, 86400, str(cur))
                            prices_data[pair] = {"pair": pair, "price": cur, "change_rate": round(change_rate, 2)}
                    except:
                        pass
                if prices_data:
                    await cache.client.setex("crypto:prices", 10, json.dumps(prices_data))
            except Exception as e:
                logger.debug("시세 업데이트 오류: %s", e)
            await asyncio.sleep(3)

    async def _price_monitor(self):
        """3초마다 보유 포지션 손절/트레일링 익절 판단"""

        while self.running:
            try:
                # 매도 스위치 — 현재 익절만 허용 (손절은 CRYPTO_STOPLOSS_ENABLED로 별도 제어)
                sell_enabled = os.getenv("CRYPTO_SELL_ENABLED", "true").lower() != "false"
                if not sell_enabled:
                    await asyncio.sleep(5)
                    continue

                if not self.positions:
                    await asyncio.sleep(3)
                    continue

                now_kst = datetime.now(KST)
                night = _is_night(now_kst)
                tp = NIGHT_PARAMS["take_profit"] if night else DAY_PARAMS["take_profit"]
                sl = NIGHT_PARAMS["stop_loss"]   if night else DAY_PARAMS["stop_loss"]

                try:
                    cached = await cache.client.get("crypto:prices")
                    prices_data = json.loads(cached) if cached else {}
                except:
                    prices_data = {}

                for pair, pos in list(self.positions.items()):
                    avg_price = float(pos.get("avg_price", 0))
                    if avg_price <= 0:
                        continue

                    cur_price = float(prices_data.get(pair, {}).get("price", 0))
                    # 캐시에 없으면(비메이저/거래정지 코인) 직접 조회 → 손절 누락 방지
                    if cur_price <= 0:
                        try:
                            cur_price = await self.trader.get_current_price(pair)
                        except:
                            cur_price = 0
                    if cur_price <= 0:
                        # 시세 자체를 못 얻으면(완전 거래정지) 스킵 — 팔 수도 없음
                        continue

                    pnl_rate = (cur_price - avg_price) / avg_price
                    qty = float(pos.get("qty", 0))
                    if qty <= 0 or cur_price * qty < 5000:
                        continue

                    # ── 수수료 계산 ──────────────────────────────
                    buy_amount  = float(pos.get("amount", avg_price * qty))
                    fee_total   = buy_amount * UPBIT_FEE_RATE + cur_price * qty * UPBIT_FEE_RATE
                    pnl_krw     = (cur_price - avg_price) * qty
                    net_profit  = pnl_krw - fee_total  # 수수료 제외 순수익

                    # ── 손절 (-5%) — 항상 활성화 ────────────────────
                    if pnl_rate <= sl:
                        result = await self.trader.sell_market(pair, qty)
                        if result.get("success"):
                            await db.insert_trade(
                                bot="crypto_trader", asset_type="crypto", symbol=pair, side="SELL",
                                price=cur_price, quantity=qty, amount=cur_price * qty,
                                strategy="손절", pnl=pnl_krw,
                            )
                            logger.info("🛑 손절 [%s] %.1f%% | 손실 %s원 (수수료포함)", pair, pnl_rate*100, f"{net_profit:+,.0f}")
                            await cache.client.delete("crypto:peak:" + pair)
                            self.positions.pop(pair, None)
                        continue

                    # ── 하이브리드 트레일링 익절 ──────────────────
                    # tp% ~ +1.5% : 트레일링 발동 전, 도달 즉시 익절 (수수료 제외 순수익 1천원 이상)
                    # +1.5% 이상  : 트레일링 발동 → 고점 추적, 고점 -0.7% 시 매도
                    peak_key = "crypto:peak:" + pair

                    if pnl_rate >= TRAIL_ACTIVATE:
                        # 트레일링 구간: 고점 갱신
                        try:
                            prev_peak = await cache.client.get(peak_key)
                            peak_rate = float(prev_peak) if prev_peak else pnl_rate
                        except:
                            peak_rate = pnl_rate

                        if pnl_rate > peak_rate:
                            peak_rate = pnl_rate
                            await cache.client.setex(peak_key, 86400, str(peak_rate))

                        # 고점 대비 TRAIL_GAP 이상 하락 → 매도 (순수익 무조건 실행)
                        if pnl_rate <= peak_rate - TRAIL_GAP:
                            result = await self.trader.sell_market(pair, qty)
                            if result.get("success"):
                                await db.insert_trade(
                                    bot="crypto_trader", asset_type="crypto", symbol=pair, side="SELL",
                                    price=cur_price, quantity=qty, amount=cur_price * qty,
                                    strategy="트레일링익절", pnl=pnl_krw,
                                )
                                logger.info("🎯 트레일링익절 [%s] %.1f%% (고점%.1f%%) | 순수익 %s원",
                                            pair, pnl_rate*100, peak_rate*100, f"{net_profit:+,.0f}")
                                await cache.client.delete(peak_key)
                                self.positions.pop(pair, None)
                        else:
                            logger.info("📈 트레일링 추적중 [%s] 현재%.1f%% 고점%.1f%%",
                                        pair, pnl_rate*100, peak_rate*100)
                        continue

                    # tp ~ +1.5% : 일반 익절 — 수수료 제외 순수익 1천원 이상일 때만
                    if tp <= pnl_rate < TRAIL_ACTIVATE:
                        if net_profit < MIN_NET_PROFIT:
                            logger.debug("⏸ [%s] 익절 조건 충족 but 순수익 %s원 < %s원 → 대기",
                                        pair, f"{net_profit:,.0f}", f"{MIN_NET_PROFIT:,.0f}")
                            continue
                        result = await self.trader.sell_market(pair, qty)
                        if result.get("success"):
                            label = "야간익절" if night else "단타익절"
                            await db.insert_trade(
                                bot="crypto_trader", asset_type="crypto", symbol=pair, side="SELL",
                                price=cur_price, quantity=qty, amount=cur_price * qty,
                                strategy=label, pnl=pnl_krw,
                            )
                            logger.info("🎯 %s [%s] %.1f%% | 순수익 %s원", label, pair, pnl_rate*100, f"{net_profit:+,.0f}")
                            await cache.client.delete(peak_key)
                            self.positions.pop(pair, None)
                        continue

            except Exception as e:
                logger.debug("가격 모니터 오류: %s", e)
            await asyncio.sleep(3)

    async def _run_cycle(self):
        now_kst = datetime.now(KST)
        night = _is_night(now_kst)
        weekend = _is_weekend(now_kst)
        rsi_entry  = NIGHT_PARAMS["rsi_entry"]  if night else DAY_PARAMS["rsi_entry"]
        tp         = NIGHT_PARAMS["take_profit"] if night else DAY_PARAMS["take_profit"]
        sl         = NIGHT_PARAMS["stop_loss"]   if night else DAY_PARAMS["stop_loss"]
        mode_label = "🌙야간" if night else "☀️낮"

        # ── 주말 보수 모드 ────────────────────────────────
        weekend_block_buy = False
        if weekend:
            mode_label = "📉주말" + mode_label
            if night and not WEEKEND_NIGHT_BUY:
                # 주말 야간: 매수 완전 중단 (거래량 최저, 하락 위험 최대)
                weekend_block_buy = True
            elif not night:
                # 주말 낮: RSI 기준 더 빡세게 (35→30)
                rsi_entry = min(rsi_entry, WEEKEND_DAY_RSI)

        trade_mode = "scalping"
        try:
            mode = await cache.client.get("crypto:trade_mode")
            if mode:
                trade_mode = mode.decode() if isinstance(mode, bytes) else mode
        except:
            pass

        STABLE_COINS = ["USDT","BUSD","USDC","DAI","TUSD"]
        try:
            positions = await self.trader.get_positions()
            self.positions = {
                p["pair"]: p for p in positions
                if not any(s in p.get("pair","") for s in STABLE_COINS) and p.get("qty",0) > 0
            }
        except Exception as e:
            logger.warning("포지션 조회 실패: %s", e)

        logger.info("📊 보유 코인 (%s RSI≤%d TP+%.0f%% SL%.0f%%): %s",
                    mode_label, rsi_entry, tp*100, sl*100, list(self.positions.keys()) or "없음")

        krw_balance = await self.trader.get_balance("KRW")
        if krw_balance < 5_000:
            logger.info("💸 KRW 잔고 소진 (%s원) → 매수 불가", f"{krw_balance:,.0f}")
            await self._update_status(krw_balance)
            return

        # 전역 매수 스위치 — 현재 중단 (기본 false). 켜려면 CRYPTO_BUY_ENABLED=true
        buy_enabled = os.getenv("CRYPTO_BUY_ENABLED", "false").lower() == "true"
        if not buy_enabled:
            logger.info("🚫 신규 매수 중단 상태 — 보유 코인 손절/익절만 작동")
            await self._update_status(krw_balance)
            return

        # 주말 야간 매수 차단 (보유 포지션 손절/익절은 정상 작동)
        if weekend_block_buy:
            logger.info("📉 주말 야간 보수 모드 → 신규 매수 중단 (거래량 최저)")
            await self._update_status(krw_balance)
            return

        # ── BTC 급락 시 전체 매수 차단 ──────────────────
        if await self._is_btc_crashing():
            logger.info("🚨 BTC 급락 중 → 신규 매수 전면 차단")
            await self._update_status(krw_balance)
            return

        # 최대 종목 수 초과 시 매수 중단
        if len(self.positions) >= MAX_POSITIONS:
            logger.info("🚫 최대 보유 종목 %d개 도달 → 신규 매수 중단", MAX_POSITIONS)
            await self._update_status(krw_balance)
            return

        # 잔고 부족 시 매수 중단
        if krw_balance < BUY_AMOUNT_KRW:
            logger.info("💸 KRW 잔고 부족 (%s원 < %s원) → 매수 중단",
                        f"{krw_balance:,.0f}", f"{BUY_AMOUNT_KRW:,.0f}")
            await self._update_status(krw_balance)
            return

        for pair in self.active_pairs:
            # ── 중복 매수 완전 차단 ──
            if pair in self.positions:
                continue

            # ── 최대 종목 수 체크 (루프 중에도) ──
            if len(self.positions) >= MAX_POSITIONS:
                break

            # ── 잔고 체크 ──
            if krw_balance < BUY_AMOUNT_KRW:
                break

            rows = await db.get_recent_ohlcv(pair, limit=50, asset="crypto")
            if len(rows) < 40:
                logger.debug("⏳ [%s] 데이터 부족 (%d개/40)", pair, len(rows))
                continue

            prices = [float(r["close"]) for r in rows]
            rsi_now  = _calc_rsi(prices)
            rsi_prev = _calc_rsi(prices[:-1])

            # ── 1차: 1분봉 RSI 반등 확인 ──
            if not (rsi_prev <= rsi_entry and rsi_now > rsi_prev):
                continue

            # ── 2차: 5분봉 RSI 이중 확인 (가짜 신호 필터) ──
            try:
                rsi_5m = await self._get_5m_rsi(pair)
                threshold_5m = rsi_entry + 15  # 낮 35→50, 야간 20→35
                if rsi_5m > threshold_5m:
                    logger.debug("⛔ [%s] 5분봉 RSI %.1f > %.0f → 과매도 아님, 진입 보류",
                                 pair, rsi_5m, threshold_5m)
                    continue
                logger.info("✅ [%s] 1분봉 RSI %.1f + 5분봉 RSI %.1f → 이중 확인 통과",
                            pair, rsi_now, rsi_5m)
            except Exception as e:
                logger.debug("5분봉 RSI 조회 실패 [%s]: %s → 1분봉만으로 진행", pair, e)

            # MACD 데드크로스면 보류
            try:
                macd_strat = MACDStrategy(MACDConfig(fast=12,slow=26,signal=9,stop_loss=sl,take_profit=tp))
                if macd_strat.generate_signal(pair, prices) == "SELL":
                    logger.info("⛔ [%s] MACD 데드크로스 → 진입 보류", pair)
                    continue
            except:
                pass

            cur_price = await self.trader.get_current_price(pair)
            if cur_price <= 0:
                continue

            # ── 50만원 고정 매수 ──
            actual_amount = BUY_AMOUNT_KRW
            fee = actual_amount * UPBIT_FEE_RATE * 2  # 왕복 수수료
            name = COIN_NAMES.get(pair, pair.replace("KRW-",""))
            logger.info("📈 %s 신호 [%s] RSI:%.1f 금액:%s원 (수수료약 %s원)",
                        mode_label, name, rsi_now, f"{actual_amount:,.0f}", f"{fee:,.0f}")

            if trade_mode == "swing":
                execute = await self._ask_jarvis(pair=pair, rsi=rsi_now, cur_price=cur_price,
                                                  amount=actual_amount, krw_balance=krw_balance)
                if not execute:
                    continue

            result = await self.trader.buy_market(pair, actual_amount)
            if result.get("success"):
                qty = actual_amount / cur_price
                await db.insert_trade(
                    bot="crypto_trader", asset_type="crypto", symbol=pair, side="BUY",
                    price=cur_price, quantity=qty, amount=actual_amount,
                    strategy="RSI반등_" + mode_label,
                )
                self.positions[pair] = {"pair":pair,"avg_price":cur_price,"qty":qty,"amount":actual_amount}
                krw_balance -= actual_amount
                logger.info("✅ 매수 [%s] %s원 × %.6f = %s원", name, f"{cur_price:,.0f}", qty, f"{actual_amount:,.0f}")
            else:
                logger.error("❌ 매수 실패 [%s]: %s", pair, result.get("error","알 수 없음"))

        await self._update_status(krw_balance)

    async def _update_status(self, krw_balance: float):
        await cache.client.setex("crypto:krw_balance", 120, str(krw_balance))
        await cache.set_bot_status("crypto_trader", {
            "status": "running", "last_cycle": datetime.now(KST).isoformat(),
            "positions": len(self.positions), "krw_balance": krw_balance,
        })
        positions_data = [
            {"pair":pair,"currency":pair.replace("KRW-",""),"name":COIN_NAMES.get(pair,pair.replace("KRW-","")),
             "qty":pos.get("qty",0),"avg_price":pos.get("avg_price",0),"cur_price":pos.get("cur_price",0),
             "pnl":pos.get("pnl",0),"pnl_rate":pos.get("pnl_rate",0)}
            for pair, pos in self.positions.items()
        ]
        await cache.client.setex("crypto:positions", 120, json.dumps(positions_data))

    async def _ask_jarvis(self, pair: str, rsi: float, cur_price: float,
                           amount: float, krw_balance: float) -> bool:
        try:
            import aiohttp
            name = COIN_NAMES.get(pair, pair.replace("KRW-",""))
            btc_trend = "알 수 없음"
            try:
                btc_cached = await cache.client.get("crypto:prices")
                if btc_cached:
                    pm = json.loads(btc_cached)
                    btc_rate = float(pm.get("KRW-BTC",{}).get("change_rate",0))
                    btc_trend = ("상승" if btc_rate > 0 else "하락") + " %.2f%%" % btc_rate
            except:
                pass
            msg = (name + " RSI=" + str(round(rsi,1)) + " 반등 신호. BTC:" + btc_trend +
                   " 매수금액:" + str(round(amount)) + "원. BTC 급락(-3%이상) 아니면 EXECUTE, 아니면 SKIP. 한 단어.")
            async with aiohttp.ClientSession() as s:
                resp = await s.post(DASHBOARD_URL + "/api/jarvis/chat",
                                    json={"message": msg, "session_id": "crypto_signal"},
                                    timeout=aiohttp.ClientTimeout(total=20))
                if resp.status == 200:
                    reply = (await resp.json()).get("reply","SKIP")
                    execute = "EXECUTE" in reply.upper()
                    logger.info("🤖 Jarvis [%s]: %s", name, "✅ EXECUTE" if execute else "⏭️ SKIP")
                    return execute
        except Exception as e:
            logger.warning("Jarvis 실패 [%s]: %s → 허용", pair, e)
            return True
        return True

    async def _scan_coins(self):
        """
        업비트 전체 KRW 코인 스캔 → 필터 강화 → active_pairs 갱신
        - 거래량 상위 TOP_N_SCAN(40개) 안에서만 후보 선정
        - 유의종목/블랙리스트/스테이블/1000억 미만 제외
        - 상장 6개월 이상
        - 메이저 20개 항상 포함, 추가 최대 MAX_EXTRA_PAIRS(10개)
        """
        try:
            import aiohttp as _aio
            async with _aio.ClientSession() as s:
                # 1) 전체 KRW 마켓 목록 + 유의종목 필터
                r = await s.get("https://api.upbit.com/v1/market/all",
                                params={"isDetails": "true"},
                                timeout=_aio.ClientTimeout(total=10))
                markets = await r.json()
                if not isinstance(markets, list):
                    logger.warning("업비트 마켓 목록 응답 이상 → 기존 목록 유지")
                    return

                krw_markets = []
                for m in markets:
                    if not isinstance(m, dict):
                        continue
                    mk = m.get("market", "")
                    if not mk.startswith("KRW-"):
                        continue
                    if m.get("market_warning") == "CAUTION":
                        logger.debug("⚠️ 유의종목 제외: %s", mk)
                        continue
                    krw_markets.append(mk)

                # 2) 티커 (24h 거래대금) — 100개씩 나눠 조회
                tickers = []
                for i in range(0, len(krw_markets), 100):
                    chunk = krw_markets[i:i+100]
                    rt = await s.get("https://api.upbit.com/v1/ticker",
                                     params={"markets": ",".join(chunk)},
                                     timeout=_aio.ClientTimeout(total=10))
                    data = await rt.json()
                    if isinstance(data, list):
                        tickers.extend([t for t in data if isinstance(t, dict) and t.get("market")])
                    await asyncio.sleep(0.1)

            # 3) 거래대금 내림차순 정렬 → 상위 TOP_N_SCAN개만 후보
            tickers.sort(key=lambda t: t.get("acc_trade_price_24h", 0) or 0, reverse=True)
            top_pairs = {t["market"] for t in tickers[:TOP_N_SCAN]}
            logger.info("📊 거래량 상위 %d개 후보: %s", TOP_N_SCAN,
                        [t["market"].replace("KRW-","") for t in tickers[:TOP_N_SCAN]])

            new_extra = []
            for t in tickers:
                pair = t["market"]

                # 거래량 상위 TOP_N 밖이면 중단
                if pair not in top_pairs:
                    break

                # 메이저는 이미 포함
                if pair in set(MAJOR_PAIRS):
                    continue

                # 블랙리스트 + 스테이블 제외
                if pair in COIN_BLACKLIST:
                    continue

                # 거래대금 1000억 미만 제외
                if t.get("acc_trade_price_24h", 0) < MIN_TRADE_VALUE:
                    continue

                # 상장 6개월 미만 제외
                if not await self._check_listing_age(pair):
                    logger.info("🚫 상장 6개월 미만 제외: %s", pair)
                    continue

                new_extra.append(pair)
                logger.info("✅ 감시 추가: %s (거래대금 %s억)",
                            pair, f"{t.get('acc_trade_price_24h',0)/1e8:,.0f}")

                if len(new_extra) >= MAX_EXTRA_PAIRS:
                    break

            self.extra_pairs  = new_extra
            self.active_pairs = list(MAJOR_PAIRS) + new_extra
            await cache.client.setex("crypto:top_pairs", 86400, json.dumps(self.active_pairs))

            extra_names = [p.replace("KRW-", "") for p in new_extra]
            logger.info("🔍 코인 목록 갱신: 메이저 20개 + 신규 %d개 %s",
                        len(new_extra), extra_names or "없음")
        except Exception as e:
            logger.error("코인 스캔 실패: %s → 메이저 20개 유지", e)
            self.active_pairs = list(MAJOR_PAIRS)

    async def _check_listing_age(self, pair: str) -> bool:
        """일봉 200개 조회 → 상장 6개월(180일) 이상인지 확인"""
        try:
            import aiohttp as _aio
            async with _aio.ClientSession() as s:
                r = await s.get("https://api.upbit.com/v1/candles/days",
                                params={"market": pair, "count": 200},
                                timeout=_aio.ClientTimeout(total=10))
                candles = await r.json()
            if isinstance(candles, list) and len(candles) >= MIN_LISTING_DAYS:
                return True
            return False
        except:
            return False

    async def _scan_loop(self):
        """매일 새벽 04:05 코인 목록 자동 갱신 (거래량 기준 재편성)"""
        # 시작 즉시 1회 스캔
        await self._scan_coins()
        while self.running:
            now = datetime.now(KST)
            # 다음 04:05 계산
            next_run = now.replace(hour=4, minute=5, second=0, microsecond=0)
            if now >= next_run:
                next_run += timedelta(days=1)
            wait_sec = (next_run - now).total_seconds()
            logger.info("🕐 다음 코인 목록 갱신: %s (%.0f분 후)",
                        next_run.strftime("%m/%d %H:%M"), wait_sec / 60)
            await asyncio.sleep(wait_sec)
            try:
                await self._scan_coins()
                from common.telegram import send_crypto
                names = [p.replace("KRW-","") for p in self.active_pairs]
                now_str = datetime.now(KST).strftime("%m/%d %H:%M")
                await send_crypto(
                    f"🔄 <b>코인 감시 목록 갱신</b> ({now_str})\n"
                    f"총 {len(self.active_pairs)}개: {', '.join(names)}"
                )
            except Exception as e:
                logger.error("스캔 루프 오류: %s", e)

    async def _init_ohlcv(self):
        try:
            import aiohttp as _aio
            to_collect = []
            for pair in self.active_pairs:
                try:
                    async with db.pool.acquire() as conn:
                        cnt = await conn.fetchval("SELECT COUNT(*) FROM crypto_ohlcv WHERE pair=$1", pair)
                    if cnt < 40:
                        to_collect.append((pair, cnt))
                except:
                    to_collect.append((pair, 0))
            if not to_collect:
                logger.info("✅ 모든 코인 OHLCV 충분")
                return
            logger.info("📊 OHLCV 초기 수집: %d개 코인", len(to_collect))
            async with _aio.ClientSession() as s:
                for pair, existing in to_collect:
                    try:
                        r = await s.get("https://api.upbit.com/v1/candles/minutes/1",
                                        params={"market":pair,"count":200},
                                        timeout=_aio.ClientTimeout(total=10))
                        candles = await r.json()
                        if isinstance(candles, list) and candles:
                            rows = [(pair, datetime.fromisoformat(c["candle_date_time_kst"]),
                                     c["opening_price"],c["high_price"],c["low_price"],
                                     c["trade_price"],c["candle_acc_trade_volume"]) for c in candles]
                            async with db.pool.acquire() as conn:
                                await conn.executemany(
                                    "INSERT INTO crypto_ohlcv(pair,ts,open,high,low,close,volume) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(pair,ts) DO NOTHING",
                                    rows
                                )
                            logger.info("✅ %s: %d개 수집", pair, len(rows))
                        await asyncio.sleep(0.2)
                    except Exception as e:
                        logger.error("❌ %s 수집 실패: %s", pair, e)
            logger.info("🎉 OHLCV 초기 수집 완료")
        except Exception as e:
            logger.error("OHLCV 초기 수집 오류: %s", e)

    async def _is_btc_crashing(self) -> bool:
        """BTC 최근 5분봉 기준 -2% 이상 급락 중이면 True"""
        try:
            import aiohttp as _aio
            async with _aio.ClientSession() as s:
                r = await s.get(
                    "https://api.upbit.com/v1/candles/minutes/5",
                    params={"market": "KRW-BTC", "count": 6},
                    timeout=_aio.ClientTimeout(total=5),
                )
                candles = await r.json()
            if not isinstance(candles, list) or len(candles) < 2:
                return False
            # 가장 최근 캔들 기준 30분 전 대비 변화율
            latest = candles[0]["trade_price"]
            before = candles[-1]["opening_price"]
            change = (latest - before) / before * 100
            if change <= -2.0:
                logger.warning("⚠️ BTC 급락 감지 %.2f%% → 신규 매수 차단", change)
                return True
            return False
        except Exception as e:
            logger.debug("BTC 급락 체크 실패: %s", e)
            return False

    async def _get_5m_rsi(self, pair: str, period: int = 14) -> float:
        """업비트에서 5분봉 직접 조회 → RSI 계산"""
        import aiohttp as _aio
        async with _aio.ClientSession() as s:
            r = await s.get(
                "https://api.upbit.com/v1/candles/minutes/5",
                params={"market": pair, "count": period + 5},
                timeout=_aio.ClientTimeout(total=5),
            )
            candles = await r.json()
        if not isinstance(candles, list) or len(candles) < period + 1:
            return 50.0  # 데이터 부족 시 중립값
        prices = [c["trade_price"] for c in reversed(candles)]
        return _calc_rsi(prices)

    async def _ohlcv_update_loop(self):
        """1분마다 전 코인 최신 캔들 1개 DB 적재 — 데이터 상시 최신화"""
        import aiohttp as _aio
        logger.info("✅ OHLCV 실시간 갱신 루프 시작")
        while self.running:
            try:
                async with _aio.ClientSession() as s:
                    for pair in list(self.active_pairs):
                        try:
                            r = await s.get(
                                "https://api.upbit.com/v1/candles/minutes/1",
                                params={"market": pair, "count": 3},
                                timeout=_aio.ClientTimeout(total=5),
                            )
                            candles = await r.json()
                            if not isinstance(candles, list) or not candles:
                                continue
                            rows = [
                                (
                                    pair,
                                    datetime.fromisoformat(c["candle_date_time_kst"]),
                                    c["opening_price"], c["high_price"],
                                    c["low_price"],     c["trade_price"],
                                    c["candle_acc_trade_volume"],
                                )
                                for c in candles
                            ]
                            async with db.pool.acquire() as conn:
                                await conn.executemany(
                                    "INSERT INTO crypto_ohlcv(pair,ts,open,high,low,close,volume) "
                                    "VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(pair,ts) DO UPDATE "
                                    "SET close=$7, high=GREATEST(high,$5), low=LEAST(low,$6), volume=$8",
                                    [(r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[6]) for r in rows],
                                )
                            await asyncio.sleep(0.05)
                        except Exception as e:
                            logger.debug("OHLCV 갱신 오류 [%s]: %s", pair, e)
            except Exception as e:
                logger.error("OHLCV 갱신 루프 오류: %s", e)
            await asyncio.sleep(60)  # 1분마다 갱신

    async def _six_hour_report_loop(self):
        """코인 6시간 요약 리포트 — 00:00 / 06:00 / 12:00 / 18:00"""
        while self.running:
            now = datetime.now(KST)
            next_hour = ((now.hour // 6) + 1) * 6
            if next_hour >= 24:
                next_run = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            else:
                next_run = now.replace(hour=next_hour, minute=0, second=0, microsecond=0)
            await asyncio.sleep((next_run - now).total_seconds())
            try:
                from common.telegram import send_crypto
                now = datetime.now(KST)
                async with db.pool.acquire() as conn:
                    trades = await conn.fetch(
                        "SELECT side,symbol,amount,pnl,strategy,created_at FROM trade_history "
                        "WHERE bot='crypto_trader' AND created_at >= NOW() - INTERVAL '6 hours' "
                        "ORDER BY created_at DESC"
                    )
                buys   = [t for t in trades if t["side"] == "BUY"]
                sells  = [t for t in trades if t["side"] == "SELL"]
                wins   = [t for t in sells if float(t["pnl"] or 0) > 0]
                losses = [t for t in sells if float(t["pnl"] or 0) < 0]
                total_pnl = sum(float(t["pnl"] or 0) for t in sells)
                win_rate  = (len(wins) / len(sells) * 100) if sells else 0
                krw = await self.trader.get_balance("KRW")

                # 보유 포지션 현황
                pos_lines = []
                for pair, pos in list(self.positions.items())[:6]:
                    avg = float(pos.get("avg_price", 0))
                    try:
                        cached = await cache.client.get("crypto:prices")
                        import json as _json
                        pd = _json.loads(cached) if cached else {}
                        cur = float(pd.get(pair, {}).get("price", 0)) or avg
                    except:
                        cur = avg
                    rate = (cur - avg) / avg * 100 if avg > 0 else 0
                    emoji = "🟢" if rate >= 0 else "🔴"
                    pos_lines.append(f"{emoji} {pair.replace('KRW-','')} {rate:+.1f}%")

                emoji_total = "📈" if total_pnl >= 0 else "📉"
                lines = [
                    f"🕐 <b>코인 6시간 리포트</b> ({now.strftime('%m/%d %H:%M')})",
                    "",
                    f"매수 {len(buys)}건 / 매도 {len(sells)}건",
                ]
                if sells:
                    lines.append(f"익절 {len(wins)} / 손절 {len(losses)}  (승률 {win_rate:.0f}%)")
                lines.append(f"{emoji_total} 6h 손익: {total_pnl:+,.0f}원")
                lines.append("")
                lines.append(f"💰 KRW 잔고: {krw:,.0f}원")
                lines.append(f"📦 보유: {len(self.positions)}종목" +
                              (f"  {' | '.join(pos_lines)}" if pos_lines else ""))

                if trades:
                    lines.append("")
                    lines.append("최근 매매:")
                    for t in list(trades)[:5]:
                        side_e = "🟢" if t["side"] == "BUY" else "🔴"
                        pnl_str = f" ({float(t['pnl']):+,.0f}원)" if t["side"] == "SELL" and t["pnl"] else ""
                        t_time = t["created_at"].astimezone(KST).strftime("%H:%M")
                        lines.append(f"{side_e} {t['symbol'].replace('KRW-','')} "
                                     f"{float(t['amount']):,.0f}원{pnl_str} [{t_time}]")

                await send_crypto("\n".join(lines))
                logger.info("📨 코인 6시간 리포트 전송")
            except Exception as e:
                logger.error("코인 6시간 리포트 실패: %s", e)


    async def _daily_report_loop(self):
        while self.running:
            now = datetime.now(KST)
            today = now.date()
            if now.hour == 0 and now.minute == 0 and self.report_sent_date != today:
                self.report_sent_date = today
                await self._send_daily_report()
            await asyncio.sleep(60)

    async def _send_daily_report(self):
        try:
            from common.telegram import send_jarvis
            now = datetime.now(KST)
            yesterday = (now - timedelta(days=1)).strftime("%m/%d")
            async with db.pool.acquire() as conn:
                trades = await conn.fetch(
                    "SELECT symbol,side,amount,pnl,strategy,created_at FROM trade_history WHERE bot='crypto_trader' AND created_at >= NOW() - INTERVAL '24 hours' ORDER BY created_at"
                )
            krw = await self.trader.get_balance("KRW")
            buy_cnt  = sum(1 for t in trades if t["side"]=="BUY")
            sell_cnt = sum(1 for t in trades if t["side"]=="SELL")
            total_pnl = sum(float(t["pnl"] or 0) for t in trades if t["side"]=="SELL")
            emoji = "📈" if total_pnl >= 0 else "📉"
            lines = [
                "📊 코인 일일 결산 [" + yesterday + "]",
                "="*20,
                "매수 %d건 / 매도 %d건" % (buy_cnt, sell_cnt),
                f"{emoji} 실현손익: {total_pnl:+,.0f}원",
                f"KRW: {krw:,.0f}원 | 보유: {len(self.positions)}종목",
            ]
            if not trades:
                lines.append("거래 없음")
            else:
                lines.append("")
                lines.append("거래 내역:")
                for t in list(trades)[:8]:
                    side_e = "🟢" if t["side"]=="BUY" else "🔴"
                    lines.append(side_e + " " + t["symbol"].replace("KRW-","") + " " + f"{float(t['amount']):,.0f}원")
            await send_jarvis("\n".join(lines))
            logger.info("✅ 일일 결산 전송")
        except Exception as e:
            logger.error("일일 결산 실패: %s", e)

    # ── 텔레그램 명령어 처리 ──────────────────────────────
    async def _telegram_polling_loop(self):
        """텔레그램 명령어 폴링 루프"""
        import aiohttp
        token = config.CRYPTO_BOT_TOKEN
        if not token:
            logger.warning("CRYPTO_BOT_TOKEN 없음 — 텔레그램 폴링 스킵")
            return
        chat_id = config.CRYPTO_CHAT_ID or config.TELEGRAM_CHAT_ID
        offset = None
        logger.info("✅ 텔레그램 코인봇 폴링 시작")
        while self.running:
            try:
                params = {"timeout": 20, "allowed_updates": ["message"]}
                if offset:
                    params["offset"] = offset
                async with aiohttp.ClientSession() as s:
                    resp = await s.get(
                        f"https://api.telegram.org/bot{token}/getUpdates",
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=30)
                    )
                    data = await resp.json()
                if data.get("ok") and data.get("result"):
                    for update in data["result"]:
                        offset = update["update_id"] + 1
                        msg = update.get("message", {})
                        text = msg.get("text", "").strip()
                        if text:
                            await self._handle_telegram_command(text, chat_id, token)
            except Exception as e:
                logger.error(f"텔레그램 폴링 오류: {e}")
                await asyncio.sleep(5)

    async def _handle_telegram_command(self, text: str, chat_id: str, token: str):
        """텔레그램 명령어 처리"""
        import aiohttp
        cmd = text.lower().split()[0] if text else ""

        async def reply(msg: str):
            try:
                async with aiohttp.ClientSession() as s:
                    await s.post(
                        f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
                        timeout=aiohttp.ClientTimeout(total=10)
                    )
            except Exception as e:
                logger.error(f"텔레그램 응답 전송 실패: {e}")

        if cmd in ("/status", "상태"):
            await self._cmd_status(reply)
        elif cmd in ("/pnl", "수익"):
            await self._cmd_pnl(reply)
        elif cmd in ("/night", "야간", "밤"):
            await self._cmd_night(reply)
        elif cmd in ("/help", "도움말"):
            await reply(
                "🤖 <b>코인봇 명령어</b>\n\n"
                "/status — 현재 보유 포지션\n"
                "/pnl — 오늘 수익 현황\n"
                "/night — 야간 요약\n"
                "/help — 도움말"
            )

    async def _cmd_status(self, reply):
        """현재 보유 포지션 + 잔고"""
        try:
            krw = await self.trader.get_balance("KRW")
            lines = ["📊 <b>현재 상태</b>", ""]
            lines.append(f"💰 KRW 잔고: {krw:,.0f}원")
            lines.append(f"📦 보유 종목: {len(self.positions)}개")
            if self.positions:
                lines.append("")
                for pair, pos in list(self.positions.items())[:10]:
                    symbol = pair.replace("KRW-", "")
                    avg = pos.get("avg_price", 0)
                    qty = pos.get("quantity", 0)
                    cur_price = await cache.get_price(pair) or 0
                    if avg > 0 and cur_price > 0:
                        pnl_pct = (cur_price - avg) / avg * 100
                        emoji = "🟢" if pnl_pct >= 0 else "🔴"
                        lines.append(f"{emoji} {symbol}: {pnl_pct:+.2f}%")
                    else:
                        lines.append(f"⚪ {symbol}")
            now = datetime.now(KST).strftime("%H:%M")
            lines.append(f"\n🕐 {now} 기준")
            await reply("\n".join(lines))
        except Exception as e:
            await reply(f"⚠️ 상태 조회 실패: {e}")

    async def _cmd_pnl(self, reply):
        """오늘 수익 현황"""
        try:
            async with db.pool.acquire() as conn:
                trades = await conn.fetch(
                    "SELECT symbol, side, amount, pnl, created_at FROM trade_history "
                    "WHERE bot='crypto_trader' AND created_at >= NOW() - INTERVAL '24 hours' "
                    "ORDER BY created_at DESC"
                )
            buy_cnt = sum(1 for t in trades if t["side"] == "BUY")
            sell_cnt = sum(1 for t in trades if t["side"] == "SELL")
            total_pnl = sum(float(t["pnl"] or 0) for t in trades if t["side"] == "SELL")
            wins = sum(1 for t in trades if t["side"] == "SELL" and float(t["pnl"] or 0) > 0)
            losses = sum(1 for t in trades if t["side"] == "SELL" and float(t["pnl"] or 0) < 0)
            emoji = "📈" if total_pnl >= 0 else "📉"
            lines = [
                f"{emoji} <b>오늘 수익 현황</b>", "",
                f"매수: {buy_cnt}건 / 매도: {sell_cnt}건",
                f"익절: {wins}건 / 손절: {losses}건",
                f"실현손익: {total_pnl:+,.0f}원",
            ]
            if trades:
                lines.append("")
                lines.append("최근 매매:")
                for t in list(trades)[:5]:
                    side_e = "🟢" if t["side"] == "BUY" else "🔴"
                    pnl_str = f" ({float(t['pnl']):+,.0f}원)" if t["side"] == "SELL" and t["pnl"] else ""
                    lines.append(f"{side_e} {t['symbol'].replace('KRW-', '')} {float(t['amount']):,.0f}원{pnl_str}")
            await reply("\n".join(lines))
        except Exception as e:
            await reply(f"⚠️ 수익 조회 실패: {e}")

    async def _cmd_night(self, reply):
        """야간 요약 (22:00~현재)"""
        try:
            now = datetime.now(KST)
            # 오늘 22시 기준
            night_start = now.replace(hour=22, minute=0, second=0, microsecond=0)
            if now.hour < 22:
                night_start -= timedelta(days=1)
            async with db.pool.acquire() as conn:
                trades = await conn.fetch(
                    "SELECT symbol, side, amount, pnl, created_at FROM trade_history "
                    "WHERE bot='crypto_trader' AND created_at >= $1 ORDER BY created_at DESC",
                    night_start
                )
            krw = await self.trader.get_balance("KRW")
            buy_cnt = sum(1 for t in trades if t["side"] == "BUY")
            sell_cnt = sum(1 for t in trades if t["side"] == "SELL")
            total_pnl = sum(float(t["pnl"] or 0) for t in trades if t["side"] == "SELL")
            emoji = "🌙" if total_pnl >= 0 else "😰"
            lines = [
                f"{emoji} <b>야간 요약</b> ({night_start.strftime('%H:%M')}~{now.strftime('%H:%M')})", "",
                f"매수: {buy_cnt}건 / 매도: {sell_cnt}건",
                f"실현손익: {total_pnl:+,.0f}원",
                f"현재 KRW: {krw:,.0f}원",
                f"보유 종목: {len(self.positions)}개",
            ]
            if trades:
                lines.append("")
                lines.append("야간 거래:")
                for t in list(trades)[:8]:
                    side_e = "🟢" if t["side"] == "BUY" else "🔴"
                    pnl_str = f" ({float(t['pnl']):+,.0f}원)" if t["side"] == "SELL" and t["pnl"] else ""
                    t_time = t["created_at"].astimezone(KST).strftime("%H:%M")
                    lines.append(f"{side_e} {t['symbol'].replace('KRW-', '')} {t_time}{pnl_str}")
            else:
                lines.append("야간 거래 없음")
            await reply("\n".join(lines))
        except Exception as e:
            await reply(f"⚠️ 야간 요약 실패: {e}")

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
