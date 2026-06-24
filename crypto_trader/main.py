"""
crypto-trader — AutoTrader
업비트 코인 자동매매
ML 모델 판단 → 자동 매수/매도 (텔레그램 알림 없음)
하루 1번 자정 결산 보고
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
        self.daily_trades = []       # 오늘 매매 기록 (결산용)
        self.report_sent_date = None # 결산 보고 중복 방지

    # 업비트 코인 한글명 매핑
    COIN_NAMES = {
        "KRW-BTC": "비트코인", "KRW-ETH": "이더리움", "KRW-SOL": "솔라나",
        "KRW-XRP": "리플", "KRW-ADA": "에이다", "KRW-DOGE": "도지코인",
        "KRW-AVAX": "아발란체", "KRW-DOT": "폴카닷", "KRW-MATIC": "폴리곤",
        "KRW-LINK": "체인링크", "KRW-UNI": "유니스왑", "KRW-ATOM": "코스모스",
        "KRW-LTC": "라이트코인", "KRW-BCH": "비트코인캐시", "KRW-ETC": "이더리움클래식",
        "KRW-SAND": "샌드박스", "KRW-MANA": "디센트럴랜드", "KRW-SHIB": "시바이누",
        "KRW-APT": "앱토스", "KRW-ARB": "아비트럼", "KRW-OP": "옵티미즘",
        "KRW-SUI": "수이", "KRW-TRX": "트론", "KRW-NEAR": "니어프로토콜",
        "KRW-FIL": "파일코인", "KRW-AAVE": "에이브", "KRW-GRT": "그래프",
        "KRW-AXS": "엑시인피니티", "KRW-ALGO": "알고랜드", "KRW-VET": "비체인",
    }

    async def _update_top_pairs(self):
        """업비트 거래량 TOP 20 코인 자동 업데이트"""
        try:
            import aiohttp
            async with aiohttp.ClientSession() as s:
                # 전체 KRW 마켓 조회
                r = await s.get("https://api.upbit.com/v1/market/all?isDetails=false")
                markets = await r.json()
                krw_pairs = [m["market"] for m in markets if m["market"].startswith("KRW-")]

                # 현재가 + 거래량 조회 (100개씩)
                tickers = []
                for i in range(0, len(krw_pairs), 100):
                    chunk = krw_pairs[i:i+100]
                    r = await s.get(
                        "https://api.upbit.com/v1/ticker",
                        params={"markets": ",".join(chunk)}
                    )
                    tickers.extend(await r.json())
                    await asyncio.sleep(0.1)

            # USDT/스테이블코인 제외 + 거래대금 기준 TOP 20
            exclude = {"KRW-USDT", "KRW-USDC", "KRW-DAI", "KRW-BUSD"}
            sorted_tickers = sorted(
                [t for t in tickers if t["market"] not in exclude],
                key=lambda x: float(x.get("acc_trade_price_24h", 0)),
                reverse=True
            )[:20]

            top_pairs = [t["market"] for t in sorted_tickers]
            config.CRYPTO_PAIRS = top_pairs

            # 한글명 포함 로그
            names = [self.COIN_NAMES.get(p, p) for p in top_pairs]
            logger.info(f"📊 거래량 TOP 20 업데이트: {', '.join(names)}")

            # Redis에 저장
            import json
            await cache.client.setex("crypto:top_pairs", 86400, json.dumps(top_pairs))

        except Exception as e:
            logger.error(f"TOP 20 업데이트 실패: {e}")

    async def _daily_scan_loop(self):
        """매일 08:30 거래량 TOP 20 업데이트"""
        while self.running:
            now = datetime.now(KST)
            # 처음 실행 시 바로 한번
            await self._update_top_pairs()
            # 다음 08:30까지 대기
            next_run = now.replace(hour=8, minute=30, second=0, microsecond=0)
            if now >= next_run:
                next_run = next_run + timedelta(days=1)
            wait_sec = (next_run - now).total_seconds()
            await asyncio.sleep(wait_sec)

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
        asyncio.create_task(self._price_loop())
        asyncio.create_task(self._daily_report_loop())
        asyncio.create_task(self._price_monitor())  # 급락/급등 실시간 감지
        asyncio.create_task(self._daily_scan_loop())  # 거래량 TOP 20 자동 업데이트
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
            logger.info(f"📋 전략 로드: {active}")
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
                    logger.info("🔄 전략 변경 감지 → 재로드")
                    await self.load_strategies()
        except Exception as e:
            logger.warning(f"전략 구독 오류: {e}")

    async def _loop(self):
        while self.running:
            now = datetime.now(KST)
            logger.info(f"🔄 코인 매매 사이클 [{now.strftime('%H:%M:%S')}]")
            try:
                await self._run_cycle()
            except Exception as e:
                logger.error(f"매매 사이클 오류: {e}")
            await asyncio.sleep(60)

    async def _price_loop(self):
        while self.running:
            try:
                prices_data = {}
                for pair in config.CRYPTO_PAIRS:
                    try:
                        cur = await self.trader.get_current_price(pair)
                        if cur > 0:
                            prev_key = f"crypto:prev:{pair}"
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
                logger.debug(f"시세 업데이트 오류: {e}")
            await asyncio.sleep(3)

    async def _daily_report_loop(self):
        """자정(00:00 KST) 하루 1번 결산 보고"""
        while self.running:
            now = datetime.now(KST)
            today = now.date()

            if now.hour == 0 and now.minute == 0 and self.report_sent_date != today:
                self.report_sent_date = today
                await self._send_daily_report()

            await asyncio.sleep(60)

    async def _send_daily_report(self):
        """하루 결산 텔레그램 보고"""
        try:
            from common.telegram import send_jarvis
            now = datetime.now(KST)
            yesterday = (now - timedelta(days=1)).strftime("%m/%d")

            async with db.pool.acquire() as conn:
                trades = await conn.fetch("""
                    SELECT symbol, side, price, quantity, amount, pnl, strategy, created_at
                    FROM trades
                    WHERE DATE(created_at AT TIME ZONE 'Asia/Seoul') = CURRENT_DATE - 1
                      AND asset_type = 'crypto'
                    ORDER BY created_at
                """)
                krw = await self.trader.get_balance("KRW")

            buy_cnt  = sum(1 for t in trades if t["side"] == "BUY")
            sell_cnt = sum(1 for t in trades if t["side"] == "SELL")
            total_pnl = sum(float(t["pnl"] or 0) for t in trades if t["side"] == "SELL")
            pnl_emoji = "📈" if total_pnl >= 0 else "📉"

            msg = f"📊 코인 일일 결산 [{yesterday}]\n"
            msg += f"{'='*20}\n"
            msg += f"매수 {buy_cnt}건 / 매도 {sell_cnt}건\n"
            if sell_cnt > 0:
                msg += f"{pnl_emoji} 실현손익: {total_pnl:+,.0f}원\n"

            if trades:
                msg += "\n거래 내역:\n"
                for t in trades[:10]:
                    side_emoji = "🟢" if t["side"] == "BUY" else "🔴"
                    msg += f"  {side_emoji} {t['symbol']} {t['side']} {float(t['amount']):,.0f}원\n"

            msg += f"\nKRW 잔고: {krw:,.0f}원"
            msg += f"\n보유 코인: {len(self.positions)}종목"

            if not trades:
                msg += "\n오늘 거래 없음"

            await send_jarvis(msg)
            logger.info("✅ 일일 결산 보고 완료")
        except Exception as e:
            logger.error(f"일일 결산 보고 실패: {e}")

    async def _price_monitor(self):
        """3초마다 보유 코인 급락/급등 감지 → 즉시 대응"""
        alert_cooldown = {}

        while self.running:
            try:
                if self.positions:
                    # Redis에서 코인 시세 읽기
                    try:
                        import json as _json
                        cached = await cache.client.get("crypto:prices")
                        prices_data = _json.loads(cached) if cached else {}
                    except:
                        prices_data = {}

                    for pair, pos in list(self.positions.items()):
                        avg_price = float(pos.get("avg_price", 0))
                        if avg_price <= 0:
                            continue

                        price_info = prices_data.get(pair, {})
                        cur_price = float(price_info.get("price", 0))
                        if cur_price <= 0:
                            continue

                        pnl_rate = (cur_price - avg_price) / avg_price * 100

                        now_ts = datetime.now(KST).timestamp()
                        last_alert = alert_cooldown.get(pair, 0)
                        if now_ts - last_alert < 300:
                            continue

                        # 급락 -4% 또는 급등 +8% 감지
                        if pnl_rate <= -4.0 or pnl_rate >= 8.0:
                            alert_cooldown[pair] = now_ts
                            direction = "급락" if pnl_rate < 0 else "급등"
                            logger.info(f"⚡ [{pair}] {direction} 감지: {pnl_rate:+.1f}%")

                            qty = float(pos.get("qty", 0))
                            if qty <= 0:
                                continue

                            # 급락 시 즉시 매도, 급등 시 익절
                            if pnl_rate <= -4.0:
                                result = await self.trader.sell_market(pair, qty)
                                if result.get("success"):
                                    pnl = (cur_price - avg_price) * qty
                                    await db.insert_trade(
                                        bot="crypto_trader", asset_type="crypto",
                                        symbol=pair, side="SELL",
                                        price=cur_price, quantity=qty,
                                        amount=cur_price * qty,
                                        strategy="급락손절", pnl=pnl,
                                    )
                                    logger.info(f"🛑 급락 손절 [{pair}] {pnl_rate:+.1f}% PnL:{pnl:+,.0f}원")
                                    self.positions.pop(pair, None)
                            elif pnl_rate >= 8.0:
                                result = await self.trader.sell_market(pair, qty)
                                if result.get("success"):
                                    pnl = (cur_price - avg_price) * qty
                                    await db.insert_trade(
                                        bot="crypto_trader", asset_type="crypto",
                                        symbol=pair, side="SELL",
                                        price=cur_price, quantity=qty,
                                        amount=cur_price * qty,
                                        strategy="급등익절", pnl=pnl,
                                    )
                                    logger.info(f"🎯 급등 익절 [{pair}] {pnl_rate:+.1f}% PnL:{pnl:+,.0f}원")
                                    self.positions.pop(pair, None)

            except Exception as e:
                logger.debug(f"코인 가격 모니터 오류: {e}")

            await asyncio.sleep(3)


    async def _run_cycle(self):
        strat_name, params = self.get_active_strategy()
        if not strat_name:
            return

        strategy = self.build_strategy(strat_name, params)
        if not strategy:
            return

        buy_amount = float(params.get("buy_amount", 10000))

        # ① 포지션 조회
        STABLE_COINS = ['USDT', 'BUSD', 'USDC', 'DAI', 'TUSD']
        positions = await self.trader.get_positions()
        self.positions = {
            p["pair"]: p for p in positions
            if not any(s in p.get("pair", "") for s in STABLE_COINS)
            and p.get("qty", 0) > 0
        }
        logger.info(f"📊 보유 코인: {list(self.positions.keys()) or '없음'}")

        # ② 손절/익절 체크
        for pair, pos in list(self.positions.items()):
            avg = pos["avg_price"]
            cur = pos["cur_price"]
            qty = pos["qty"]

            if cur * qty < 5000:
                continue

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
                    logger.info(f"🛑 손절 [{pair}] PnL: {pnl:+,.0f}원")
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
                    logger.info(f"🎯 익절 [{pair}] PnL: {pnl:+,.0f}원")
                continue

        # ③ 신규 매수
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

            prices = [float(r["close"]) for r in rows]
            signal_type = strategy.generate_signal(pair, prices)

            if signal_type == "BUY":
                cur_price = await self.trader.get_current_price(pair)
                if cur_price <= 0:
                    continue

                # ML 판단
                ml_ok, ml_prob = await self._check_ml(pair, rows)
                if not ml_ok:
                    logger.info(f"⛔ [{pair}] ML 필터 차단")
                    continue

                # Jarvis가 잔고/신호강도 보고 매수 금액 결정
                actual_amount = await self._decide_amount(
                    pair=pair,
                    krw_balance=krw_balance,
                    ml_prob=ml_prob,
                    base_amount=buy_amount,
                )
                if actual_amount < 5000:
                    logger.info(f"⛔ [{pair}] Jarvis 결정 금액 부족 ({actual_amount:,.0f}원)")
                    continue

                # 자동 매수 실행
                result = await self.trader.buy_market(pair, actual_amount)
                if result["success"]:
                    qty = actual_amount / cur_price
                    await db.insert_trade(
                        bot="crypto_trader", asset_type="crypto",
                        symbol=pair, side="BUY",
                        price=cur_price, quantity=qty,
                        amount=actual_amount, strategy=strat_name,
                    )
                    krw_balance -= actual_amount
                    logger.info(f"✅ 매수 완료 [{pair}] {actual_amount:,.0f}원 (ML확률:{ml_prob:.0%})")

        # Redis 캐시 업데이트
        await cache.set_bot_status("crypto_trader", {
            "status":      "running",
            "last_cycle":  datetime.now(KST).isoformat(),
            "positions":   len(self.positions),
            "strategy":    strat_name,
            "krw_balance": krw_balance,
        })

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
        await cache.client.setex("crypto:positions", 120, json.dumps(positions_data))

    async def _check_ml(self, pair: str, rows: list) -> tuple:
        """ML 모델로 매수 신호 검증 → (통과여부, 확률)"""
        try:
            from ml.model import MLModelManager
            ml = MLModelManager(db_pool=db.pool)
            ohlcv = [
                {
                    "ts": str(r.get("ts", ""))[:10],
                    "open": float(r.get("open", 0)),
                    "high": float(r.get("high", 0)),
                    "low": float(r.get("low", 0)),
                    "close": float(r.get("close", 0)),
                    "volume": float(r.get("volume", 0)),
                }
                for r in rows
            ]
            result = await ml.predict(pair, ohlcv)
            if not result.get("success"):
                logger.info(f"[{pair}] ML 모델 없음 → 신호 허용")
                return True, 0.65  # 기본 확률
            signal = result.get("signal", "HOLD")
            prob   = result.get("buy_prob", 0.5)
            logger.info(f"[{pair}] ML 판단: {signal} ({prob:.0%})")
            return (signal == "BUY" and prob >= 0.60), prob
        except Exception as e:
            logger.warning(f"[{pair}] ML 오류 → 허용: {e}")
            return True, 0.65

    async def _decide_amount(self, pair: str, krw_balance: float,
                              ml_prob: float, base_amount: float) -> float:
        """Jarvis가 잔고와 ML 확률 보고 매수 금액 결정
        
        ML 확률에 따른 비중:
          90%+ → 잔고의 40% (강한 신호)
          80%+ → 잔고의 25%
          70%+ → 잔고의 15%
          60%+ → 잔고의 10% (최소)
        단, base_amount(전략 설정값) 이하로는 안 내려감
        최대 잔고의 50% 초과 금지
        """
        if krw_balance < 5000:
            return 0

        if ml_prob >= 0.90:
            ratio = 0.40
            strength = "강함"
        elif ml_prob >= 0.80:
            ratio = 0.25
            strength = "보통"
        elif ml_prob >= 0.70:
            ratio = 0.15
            strength = "약함"
        else:
            ratio = 0.10
            strength = "최소"

        amount = krw_balance * ratio
        # base_amount와 비교해서 더 큰 값 사용 (최소 보장)
        amount = max(amount, base_amount)
        # 잔고 50% 초과 금지
        amount = min(amount, krw_balance * 0.50)
        # 최소 5,000원
        amount = max(amount, 5000)
        # 잔고 초과 방지
        amount = min(amount, krw_balance)

        logger.info(f"💡 [{pair}] Jarvis 금액 결정: {amount:,.0f}원 "
                    f"(ML:{ml_prob:.0%} 신호강도:{strength} 잔고:{krw_balance:,.0f}원)")
        return round(amount)

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
