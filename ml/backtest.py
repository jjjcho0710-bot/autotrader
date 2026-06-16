"""
백테스트 엔진
과거 OHLCV 데이터로 전략 성과 시뮬레이션
"""
from typing import List, Dict, Optional, Callable
from datetime import datetime
from ml.indicators import calculate_all, sma, rsi as calc_rsi, macd as calc_macd


class BacktestResult:
    """백테스트 결과"""
    def __init__(self):
        self.trades: List[Dict] = []
        self.equity_curve: List[Dict] = []
        self.initial_capital = 0
        self.final_capital = 0

    @property
    def total_return(self) -> float:
        if self.initial_capital == 0:
            return 0
        return (self.final_capital - self.initial_capital) / self.initial_capital * 100

    @property
    def win_rate(self) -> float:
        closed = [t for t in self.trades if t.get("pnl") is not None]
        if not closed:
            return 0
        wins = [t for t in closed if t["pnl"] > 0]
        return len(wins) / len(closed) * 100

    @property
    def avg_profit(self) -> float:
        wins = [t["pnl"] for t in self.trades if t.get("pnl", 0) > 0]
        return sum(wins) / len(wins) if wins else 0

    @property
    def avg_loss(self) -> float:
        losses = [t["pnl"] for t in self.trades if t.get("pnl", 0) < 0]
        return sum(losses) / len(losses) if losses else 0

    @property
    def profit_factor(self) -> float:
        total_profit = sum(t["pnl"] for t in self.trades if t.get("pnl", 0) > 0)
        total_loss = abs(sum(t["pnl"] for t in self.trades if t.get("pnl", 0) < 0))
        return total_profit / total_loss if total_loss > 0 else float('inf')

    @property
    def max_drawdown(self) -> float:
        if not self.equity_curve:
            return 0
        peak = self.equity_curve[0]["equity"]
        max_dd = 0
        for e in self.equity_curve:
            if e["equity"] > peak:
                peak = e["equity"]
            dd = (peak - e["equity"]) / peak * 100
            if dd > max_dd:
                max_dd = dd
        return max_dd

    @property
    def sharpe_ratio(self) -> float:
        """간단한 샤프 지수 (무위험 수익률 0% 가정)"""
        if len(self.equity_curve) < 2:
            return 0
        returns = []
        for i in range(1, len(self.equity_curve)):
            prev = self.equity_curve[i-1]["equity"]
            curr = self.equity_curve[i]["equity"]
            if prev > 0:
                returns.append((curr - prev) / prev)
        if not returns:
            return 0
        avg_r = sum(returns) / len(returns)
        variance = sum((r - avg_r) ** 2 for r in returns) / len(returns)
        std_r = variance ** 0.5
        return (avg_r / std_r * (252 ** 0.5)) if std_r > 0 else 0

    def to_dict(self) -> Dict:
        return {
            "total_return": round(self.total_return, 2),
            "win_rate": round(self.win_rate, 2),
            "avg_profit": round(self.avg_profit, 0),
            "avg_loss": round(self.avg_loss, 0),
            "profit_factor": round(self.profit_factor, 2),
            "max_drawdown": round(self.max_drawdown, 2),
            "sharpe_ratio": round(self.sharpe_ratio, 2),
            "total_trades": len(self.trades),
            "initial_capital": self.initial_capital,
            "final_capital": round(self.final_capital, 0),
            "trades": self.trades[-20:],  # 최근 20건
        }


class Backtest:
    """백테스트 실행 엔진"""

    def __init__(self, initial_capital: float = 10_000_000):
        self.initial_capital = initial_capital

    def run_ma_cross(self, ohlcv: List[Dict], short: int = 5, long: int = 20,
                     stop_loss: float = -0.02, take_profit: float = 0.05,
                     buy_amount: int = 500_000) -> BacktestResult:
        """MA 크로스 전략 백테스트"""
        result = BacktestResult()
        result.initial_capital = self.initial_capital
        capital = self.initial_capital

        closes = [float(c["close"]) for c in ohlcv]
        sma_short = sma(closes, short)
        sma_long = sma(closes, long)

        position = None  # {"price", "qty", "buy_ts"}

        for i in range(long, len(ohlcv)):
            price = closes[i]
            ts = ohlcv[i].get("ts", "")

            # 포지션 없을 때 매수 신호
            if position is None:
                if (sma_short[i] and sma_long[i] and
                    sma_short[i] > sma_long[i] and
                    sma_short[i-1] and sma_long[i-1] and
                    sma_short[i-1] <= sma_long[i-1]):
                    # 골든크로스 → 매수
                    qty = int(buy_amount / price)
                    if qty > 0 and capital >= buy_amount:
                        position = {"price": price, "qty": qty, "buy_ts": ts}
                        capital -= price * qty
                        result.trades.append({
                            "side": "BUY", "price": price, "qty": qty,
                            "ts": ts, "strategy": f"MA{short}/{long}_골든크로스"
                        })

            # 포지션 있을 때 매도 신호
            elif position:
                pnl_rate = (price - position["price"]) / position["price"]

                should_sell = False
                reason = ""

                if pnl_rate <= stop_loss:
                    should_sell = True
                    reason = f"손절 {pnl_rate*100:.1f}%"
                elif pnl_rate >= take_profit:
                    should_sell = True
                    reason = f"익절 {pnl_rate*100:.1f}%"
                elif (sma_short[i] and sma_long[i] and
                      sma_short[i] < sma_long[i] and
                      sma_short[i-1] and sma_long[i-1] and
                      sma_short[i-1] >= sma_long[i-1]):
                    should_sell = True
                    reason = "데드크로스"

                if should_sell:
                    pnl = (price - position["price"]) * position["qty"]
                    capital += price * position["qty"]
                    result.trades.append({
                        "side": "SELL", "price": price, "qty": position["qty"],
                        "ts": ts, "pnl": round(pnl, 0),
                        "strategy": f"MA{short}/{long}_{reason}"
                    })
                    position = None

            result.equity_curve.append({"ts": ts, "equity": capital + (price * position["qty"] if position else 0)})

        result.final_capital = capital + (closes[-1] * position["qty"] if position else 0)
        return result

    def run_rsi(self, ohlcv: List[Dict], period: int = 14,
                entry: float = 30, exit_: float = 70,
                stop_loss: float = -0.03, buy_amount: int = 500_000) -> BacktestResult:
        """RSI 전략 백테스트"""
        result = BacktestResult()
        result.initial_capital = self.initial_capital
        capital = self.initial_capital

        closes = [float(c["close"]) for c in ohlcv]
        rsi_vals = calc_rsi(closes, period)

        position = None

        for i in range(period + 1, len(ohlcv)):
            price = closes[i]
            ts = ohlcv[i].get("ts", "")
            rsi_val = rsi_vals[i]

            if rsi_val is None:
                continue

            if position is None:
                if rsi_val < entry:
                    qty = int(buy_amount / price)
                    if qty > 0 and capital >= buy_amount:
                        position = {"price": price, "qty": qty, "buy_ts": ts}
                        capital -= price * qty
                        result.trades.append({"side": "BUY", "price": price, "qty": qty, "ts": ts, "strategy": f"RSI{period}_과매도"})
            elif position:
                pnl_rate = (price - position["price"]) / position["price"]
                if rsi_val > exit_ or pnl_rate <= stop_loss:
                    pnl = (price - position["price"]) * position["qty"]
                    capital += price * position["qty"]
                    reason = "RSI과매수" if rsi_val > exit_ else f"손절{pnl_rate*100:.1f}%"
                    result.trades.append({"side": "SELL", "price": price, "qty": position["qty"], "ts": ts, "pnl": round(pnl, 0), "strategy": f"RSI_{reason}"})
                    position = None

            result.equity_curve.append({"ts": ts, "equity": capital + (price * position["qty"] if position else 0)})

        result.final_capital = capital + (closes[-1] * position["qty"] if position else 0)
        return result

    def optimize_ma_cross(self, ohlcv: List[Dict], buy_amount: int = 500_000) -> Dict:
        """MA 크로스 파라미터 최적화 (그리드 서치)"""
        best = {"score": -float('inf'), "params": {}}
        results = []

        short_range = [3, 5, 7, 10]
        long_range = [15, 20, 25, 30]

        for short in short_range:
            for long_ in long_range:
                if short >= long_:
                    continue
                try:
                    r = self.run_ma_cross(ohlcv, short, long_, buy_amount=buy_amount)
                    score = r.total_return - r.max_drawdown  # 수익률 - MDD
                    results.append({
                        "short": short, "long": long_,
                        "total_return": r.total_return,
                        "win_rate": r.win_rate,
                        "max_drawdown": r.max_drawdown,
                        "sharpe": r.sharpe_ratio,
                        "trades": len(r.trades),
                        "score": round(score, 2)
                    })
                    if score > best["score"]:
                        best = {"score": score, "params": {"short": short, "long": long_}}
                except:
                    pass

        results.sort(key=lambda x: x["score"], reverse=True)
        return {"best": best, "all": results[:10]}
