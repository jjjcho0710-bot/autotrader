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
from strategy.rsi import RSIStrategy, RSIConfig

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
            # 최소 거래대금 500억 이상 + 상위 20개
            filtered = [
                t for t in tickers
                if t["market"] not in exclude
                and float(t.get("acc_trade_price_24h", 0)) >= 50_000_000_000
            ]
            sorted_tickers = sorted(
                filtered,
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
            # 다음 08:30까지 대기 (처음엔 스캔 안 함)
            next_run = now.replace(hour=8, minute=30, second=0, microsecond=0)
            if now >= next_run:
                next_run = next_run + timedelta(days=1)
            wait_sec = (next_run - now).total_seconds()
            logger.info(f"📅 다음 TOP 20 업데이트: {next_run.strftime('%m/%d %H:%M')}")
            await asyncio.sleep(wait_sec)
            await self._update_top_pairs()

    async def start(self):
        self.running = True
        await db.connect()
        await cache.connect()
        await self.trader.start()
        await self._init_default_strategies()
        await self.load_strategies()

        # 메이저 코인 기본값 설정
        import json as _json
        existing = await cache.client.get("crypto:top_pairs")
        if not existing:
            await cache.client.setex("crypto:top_pairs", 86400*30, _json.dumps(self.MAJOR_PAIRS))
            config.CRYPTO_PAIRS = self.MAJOR_PAIRS
            logger.info(f"✅ 메이저 코인 {len(self.MAJOR_PAIRS)}개 설정")
        else:
            loaded = _json.loads(existing)
            config.CRYPTO_PAIRS = loaded
            logger.info(f"✅ 저장된 코인 {len(loaded)}개 로드")

        # 시작 시 OHLCV 데이터 자동 수집
        asyncio.create_task(self._init_ohlcv())
        logger.info("=" * 50)
        logger.info("🚀 AutoTrader crypto-trader 시작")
        logger.info("=" * 50)

        asyncio.create_task(self._subscribe_strategy_changes())
        asyncio.create_task(self._price_loop())
        asyncio.create_task(self._daily_report_loop())
        asyncio.create_task(self._price_monitor())  # 급락/급등 실시간 감지
        asyncio.create_task(self._daily_scan_loop())  # 거래량 TOP 20 자동 업데이트
        await self._loop()

    MAJOR_PAIRS = [
        "KRW-BTC","KRW-ETH","KRW-XRP","KRW-SOL","KRW-ADA",
        "KRW-DOGE","KRW-AVAX","KRW-LINK","KRW-DOT","KRW-SUI",
        "KRW-TRX","KRW-NEAR","KRW-MATIC","KRW-ARB","KRW-SHIB",
        "KRW-APT","KRW-SAND","KRW-ATOM","KRW-FIL","KRW-AXS"
    ]

    async def _init_default_strategies(self):
        """기본 전략이 없으면 자동 등록"""
        import json as _json
        defaults = [
            ("MACD", True, {"fast":12,"slow":26,"signal":9,"stop_loss":-0.02,"take_profit":0.005,"buy_amount":10000}),
            ("RSI반등", True, {"period":14,"entry":30,"exit":65,"stop_loss":-0.02,"take_profit":0.005,"buy_amount":10000}),
        ]
        async with db.pool.acquire() as conn:
            for name, active, params in defaults:
                await conn.execute("""
                    INSERT INTO strategy_config (bot, name, is_active, params)
                    VALUES ('crypto_trader', $1, $2, $3)
                    ON CONFLICT (bot, name) DO NOTHING
                """, name, active, _json.dumps(params))
        logger.info("✅ 코인 기본 전략 확인 완료")

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
                        if now_ts - last_alert < 60:  # 1분 쿨다운
                            continue

                        # 급락 -4% 또는 급등 +8% 감지
                        if pnl_rate <= -2.0 or pnl_rate >= 0.3:
                            alert_cooldown[pair] = now_ts
                            direction = "급락" if pnl_rate < 0 else "급등"
                            logger.info(f"⚡ [{pair}] {direction} 감지: {pnl_rate:+.1f}%")

                            qty = float(pos.get("qty", 0))
                            if qty <= 0:
                                continue

                            # 급락 시 즉시 매도, 급등 시 익절
                            if pnl_rate <= -2.0:
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
                            elif pnl_rate >= 0.3:
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
            if krw_balance < max(buy_amount * 0.5, 5000):
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

                # Jarvis 최종 판단
                jarvis_ok = await self._ask_jarvis(
                    pair=pair, signal=signal_type,
                    ml_prob=ml_prob, amount=actual_amount,
                    cur_price=cur_price, krw_balance=krw_balance,
                )
                if not jarvis_ok:
                    logger.info(f"⏭️ Jarvis SKIP [{pair}]")
                    continue

                # 매수 실행
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
        # KRW 잔고 Redis 저장 (dashboard에서 읽음)
        await cache.client.setex("crypto:krw_balance", 120, str(krw_balance))

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

    async def _init_ohlcv(self):
        """시작 시 OHLCV 데이터 없는 코인 자동 수집"""
        try:
            import aiohttp as _aio
            pairs_to_collect = []
            for pair in config.CRYPTO_PAIRS:
                async with db.pool.acquire() as conn:
                    cnt = await conn.fetchval(
                        "SELECT COUNT(*) FROM crypto_ohlcv WHERE pair=$1", pair
                    )
                if cnt < 40:
                    pairs_to_collect.append(pair)

            if not pairs_to_collect:
                logger.info("✅ 모든 코인 OHLCV 데이터 있음")
                return

            logger.info(f"📊 OHLCV 자동 수집: {len(pairs_to_collect)}개 코인")
            async with _aio.ClientSession() as s:
                for pair in pairs_to_collect:
                    try:
                        r = await s.get(
                            "https://api.upbit.com/v1/candles/minutes/1",
                            params={"market": pair, "count": 200},
                            timeout=_aio.ClientTimeout(total=10)
                        )
                        candles = await r.json()
                        if isinstance(candles, list) and candles:
                            from datetime import datetime as _dt
                            rows = [(pair,
                                     _dt.fromisoformat(c["candle_date_time_kst"]),
                                     c["opening_price"], c["high_price"],
                                     c["low_price"], c["trade_price"],
                                     c["candle_acc_trade_volume"]) for c in candles]
                            async with db.pool.acquire() as conn:
                                await conn.executemany("""
                                    INSERT INTO crypto_ohlcv(pair,ts,open,high,low,close,volume)
                                    VALUES($1,$2,$3,$4,$5,$6,$7)
                                    ON CONFLICT(pair,ts) DO NOTHING
                                """, rows)
                            logger.info(f"✅ {pair}: {len(rows)}개 수집")
                        await asyncio.sleep(0.2)
                    except Exception as e:
                        logger.error(f"❌ {pair} 수집 실패: {e}")
            logger.info("🎉 OHLCV 초기 수집 완료")
        except Exception as e:
            logger.error(f"OHLCV 초기 수집 오류: {e}")

    async def _ask_jarvis(self, pair: str, signal: str, ml_prob: float,
                           amount: float, cur_price: float, krw_balance: float) -> bool:
        """Jarvis에게 매수 판단 요청 + 결과 메모리 저장"""
        try:
            import aiohttp, os
            from common.database import cache as _cache
            import json as _json

            COIN_NAMES = {
                "KRW-BTC":"비트코인","KRW-ETH":"이더리움","KRW-XRP":"리플",
                "KRW-SOL":"솔라나","KRW-ADA":"에이다","KRW-DOGE":"도지코인",
                "KRW-AVAX":"아발란체","KRW-LINK":"체인링크","KRW-DOT":"폴카닷",
                "KRW-SUI":"수이","KRW-TRX":"트론","KRW-NEAR":"니어",
            }
            name = COIN_NAMES.get(pair, pair.replace("KRW-",""))

            # BTC 시장 추세 확인
            btc_trend = "알 수 없음"
            try:
                btc_cached = await _cache.client.get("crypto:prices")
                if btc_cached:
                    prices = _json.loads(btc_cached)
                    btc = prices.get("KRW-BTC", {})
                    btc_rate = float(btc.get("change_rate", 0))
                    btc_trend = f"{'상승' if btc_rate > 0 else '하락'} {btc_rate:+.2f}%"
            except: pass

            # 포지션 현황
            pos_count = len(self.positions)
            pos_list = list(self.positions.keys())

            dashboard_url = os.getenv("DASHBOARD_URL", "https://dashboard-production-65e3.up.railway.app")

            prompt = f"""코인 매수 신호 분석 요청

종목: {name} ({pair})
현재가: {cur_price:,.0f}원
신호: {signal} (ML확률 {ml_prob:.0%})
매수금액: {amount:,.0f}원
KRW 잔고: {krw_balance:,.0f}원
보유 코인: {pos_count}개 {pos_list}

시장 현황:
- BTC 추세: {btc_trend}

판단 기준:
1. BTC 급락 중이면 SKIP (시장 전체 하락)
2. ML 확률 70% 미만이면 SKIP
3. 잔고 대비 매수금액이 과하면 SKIP
4. 이미 같은 코인 보유 중이면 SKIP

반드시 EXECUTE 또는 SKIP 으로만 답해줘. 이유는 한 줄로."""

            async with aiohttp.ClientSession() as s:
                resp = await s.post(
                    f"{dashboard_url}/api/jarvis/chat",
                    json={"message": prompt, "session_id": "crypto_signal"},
                    timeout=aiohttp.ClientTimeout(total=20)
                )
                if resp.status == 200:
                    data = await resp.json()
                    reply = data.get("reply", "SKIP")
                    execute = reply.upper().startswith("EXECUTE") or "실행" in reply[:20]

                    # Jarvis 메모리에 판단 결과 저장 (학습용)
                    await s.post(
                        f"{dashboard_url}/api/jarvis/chat",
                        json={"message": f"[코인매매기록] {name} {signal} ML:{ml_prob:.0%} → {'EXECUTE' if execute else 'SKIP'} | {reply[:80]}",
                              "session_id": "crypto_memory"},
                        timeout=aiohttp.ClientTimeout(total=10)
                    )

                    logger.info(f"🤖 Jarvis [{pair}]: {'✅ EXECUTE' if execute else '⏭️ SKIP'} - {reply[:60]}")
                    return execute
                return True  # 응답 실패 시 허용

        except Exception as e:
            logger.warning(f"Jarvis 판단 실패 [{pair}]: {e} → 허용")
            return True  # 오류 시 허용

    async def _decide_amount(self, pair: str, krw_balance: float,
                              ml_prob: float, base_amount: float) -> float:
        """잔고 분산 매수 금액 결정

        잔고에 따라 최대 보유 종목수 자동 결정:
          100만원+ → 최대 5종목 (종목당 20%)
          50만원+  → 최대 4종목 (종목당 25%)
          20만원+  → 최대 3종목 (종목당 33%)
          10만원+  → 최대 2종목 (종목당 50%)
          5만원 미만 → 1종목 (전액)

        ML 확률로 비중 가감:
          90%+ → ×1.0 (강한 신호)
          80%+ → ×0.8
          70%+ → ×0.6
          60%+ → ×0.4 (약한 신호)
        """
        if krw_balance < 5000:
            return 0

        # 잔고에 따른 최대 종목수 & 기본 비중
        if krw_balance >= 1_000_000:
            max_pos, base_ratio = 5, 0.20
        elif krw_balance >= 500_000:
            max_pos, base_ratio = 4, 0.25
        elif krw_balance >= 200_000:
            max_pos, base_ratio = 3, 0.33
        elif krw_balance >= 100_000:
            max_pos, base_ratio = 2, 0.50
        else:
            max_pos, base_ratio = 1, 0.50  # 소액도 50%만 (나머지 예비금)

        # 현재 보유 종목수 확인
        current_pos = len(self.positions)
        if current_pos >= max_pos:
            logger.info(f"⚠️ [{pair}] 최대 보유종목 초과 ({current_pos}/{max_pos})")
            return 0

        # ML 확률로 비중 조정
        if ml_prob >= 0.90:
            ml_ratio, strength = 1.0, "강함"
        elif ml_prob >= 0.80:
            ml_ratio, strength = 0.8, "보통"
        elif ml_prob >= 0.70:
            ml_ratio, strength = 0.6, "약함"
        else:
            ml_ratio, strength = 0.4, "최소"

        amount = krw_balance * base_ratio * ml_ratio
        amount = max(amount, 5000)                      # 최소 5,000원
        amount = min(amount, krw_balance * base_ratio)  # 기본비중 초과 금지
        amount = min(amount, krw_balance * 0.95)        # 잔고 95% 초과 금지

        logger.info(f"💡 [{pair}] 분산매수: {amount:,.0f}원 "
                    f"(잔고:{krw_balance:,.0f}원 비중:{base_ratio:.0%} "
                    f"ML:{ml_prob:.0%} {strength} {current_pos+1}/{max_pos})")
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
