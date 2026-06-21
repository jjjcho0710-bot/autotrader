"""
외국인/기관 수급 데이터 수집기
pykrx를 이용해 매일 장 마감 후 수급 데이터 수집
"""
import logging
from datetime import datetime, timedelta

from common.database import db

logger = logging.getLogger(__name__)


class SupplyCollector:
    """외국인/기관 수급 데이터 수집"""

    async def collect(self, symbols: list[str]):
        """전체 종목 수급 데이터 수집"""
        try:
            from pykrx import stock as pykrx
            today = datetime.now().strftime("%Y%m%d")
            # 주말 고려해서 5일 전부터
            from_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")

            logger.info(f"📊 수급 데이터 수집 시작 — {len(symbols)}종목")
            success = 0

            for symbol in symbols:
                try:
                    # 외국인/기관 순매수 데이터
                    df = pykrx.get_market_trading_volume_by_date(
                        from_date, today, symbol
                    )
                    if df is None or len(df) == 0:
                        continue

                    latest = df.iloc[-1]
                    date = df.index[-1].date()

                    # 외국인 보유 비율
                    try:
                        frgn = pykrx.get_exhaustion_rates_of_foreign_investment(
                            today, today, symbol
                        )
                        frgn_ratio = float(frgn["보유비율"].iloc[-1]) if frgn is not None and len(frgn) > 0 else 0
                    except:
                        frgn_ratio = 0

                    async with db.pool.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO stock_supply
                            (symbol, date, foreign_net, institution_net, individual_net, foreign_hold_ratio)
                            VALUES ($1, $2, $3, $4, $5, $6)
                            ON CONFLICT (symbol, date) DO UPDATE
                            SET foreign_net=$3, institution_net=$4,
                                individual_net=$5, foreign_hold_ratio=$6
                        """,
                            symbol,
                            date,
                            int(latest.get("외국인", 0)),
                            int(latest.get("기관합계", 0)),
                            int(latest.get("개인", 0)),
                            frgn_ratio,
                        )
                    success += 1

                except Exception as e:
                    logger.debug(f"수급 수집 실패 [{symbol}]: {e}")
                    continue

            logger.info(f"✅ 수급 데이터 수집 완료 — {success}/{len(symbols)}종목")

        except Exception as e:
            logger.error(f"수급 수집 오류: {e}")

    async def get_supply_score(self, symbol: str) -> dict:
        """
        종목 수급 점수 계산
        Returns: {
            score: -3 ~ 3 (양수: 매수 우호, 음수: 매도 우호)
            foreign_net: 외국인 순매수
            institution_net: 기관 순매수
            signal: 'BUY' / 'SELL' / 'NEUTRAL'
        }
        """
        try:
            async with db.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT foreign_net, institution_net, individual_net, foreign_hold_ratio, date
                    FROM stock_supply
                    WHERE symbol=$1
                    ORDER BY date DESC LIMIT 5
                """, symbol)

            if not rows:
                return {"score": 0, "signal": "NEUTRAL", "reason": "수급 데이터 없음"}

            latest = rows[0]
            foreign_net = latest["foreign_net"]
            institution_net = latest["institution_net"]

            score = 0
            reasons = []

            # 외국인 순매수
            if foreign_net > 0:
                score += 2
                reasons.append(f"외국인 순매수 {foreign_net:+,}")
            elif foreign_net < 0:
                score -= 2
                reasons.append(f"외국인 순매도 {foreign_net:+,}")

            # 기관 순매수
            if institution_net > 0:
                score += 1
                reasons.append(f"기관 순매수 {institution_net:+,}")
            elif institution_net < 0:
                score -= 1
                reasons.append(f"기관 순매도 {institution_net:+,}")

            # 3일 연속 외국인 순매수
            if len(rows) >= 3:
                if all(r["foreign_net"] > 0 for r in rows[:3]):
                    score += 1
                    reasons.append("3일 연속 외국인 순매수")
                elif all(r["foreign_net"] < 0 for r in rows[:3]):
                    score -= 1
                    reasons.append("3일 연속 외국인 순매도")

            signal = "BUY" if score >= 2 else "SELL" if score <= -2 else "NEUTRAL"

            return {
                "score": score,
                "signal": signal,
                "foreign_net": foreign_net,
                "institution_net": institution_net,
                "reason": ", ".join(reasons) if reasons else "수급 중립",
            }

        except Exception as e:
            return {"score": 0, "signal": "NEUTRAL", "reason": f"수급 조회 실패: {e}"}
