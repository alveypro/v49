from __future__ import annotations

import numpy as np
import pandas as pd


class PerformanceMetrics:
    """Compute all standard quant performance metrics."""

    def total_return(self, equity_curve: pd.Series | list) -> float:
        arr = np.array(equity_curve)
        if len(arr) < 2 or arr[0] <= 0:
            return 0.0
        return float(arr[-1] / arr[0] - 1)

    def annualised_return(self, equity_curve: pd.Series | list, trading_days: int = 252) -> float:
        tr = self.total_return(equity_curve)
        n = len(equity_curve)
        if n <= 1:
            return 0.0
        return float((1 + tr) ** (trading_days / n) - 1)

    def max_drawdown(self, equity_curve: pd.Series | list) -> float:
        arr = np.array(equity_curve, dtype=float)
        if len(arr) < 2:
            return 0.0
        peak = np.maximum.accumulate(arr)
        dd = (arr - peak) / np.where(peak > 0, peak, 1)
        return float(dd.min())

    def max_drawdown_duration(self, equity_curve: pd.Series | list) -> int:
        arr = np.array(equity_curve, dtype=float)
        peak = np.maximum.accumulate(arr)
        in_dd = arr < peak
        max_dur = 0
        current = 0
        for flag in in_dd:
            if flag:
                current += 1
                max_dur = max(max_dur, current)
            else:
                current = 0
        return max_dur

    def sharpe_ratio(self, equity_curve: pd.Series | list, rf: float = 0.0) -> float:
        returns = self._daily_returns(equity_curve)
        if len(returns) < 2 or returns.std() == 0:
            return 0.0
        excess = returns - rf / 252
        return float(excess.mean() / excess.std() * np.sqrt(252))

    def sortino_ratio(self, equity_curve: pd.Series | list, rf: float = 0.0) -> float:
        returns = self._daily_returns(equity_curve)
        if len(returns) < 2:
            return 0.0
        excess = returns - rf / 252
        downside = excess[excess < 0]
        if len(downside) == 0 or downside.std() == 0:
            return 0.0
        return float(excess.mean() / downside.std() * np.sqrt(252))

    def calmar_ratio(self, equity_curve: pd.Series | list) -> float:
        ann = self.annualised_return(equity_curve)
        mdd = self.max_drawdown(equity_curve)
        if mdd == 0:
            return 0.0
        return float(ann / abs(mdd))

    def volatility(self, equity_curve: pd.Series | list) -> float:
        returns = self._daily_returns(equity_curve)
        if len(returns) < 2:
            return 0.0
        return float(returns.std() * np.sqrt(252))

    def win_rate(self, trades: pd.DataFrame) -> float:
        if trades.empty or 'pnl' not in trades.columns:
            return 0.0
        sells = trades[trades['side'] == 'sell'] if 'side' in trades.columns else trades
        if sells.empty:
            return 0.0
        return float((sells['pnl'] > 0).mean())

    def profit_factor(self, trades: pd.DataFrame) -> float:
        if trades.empty or 'pnl' not in trades.columns:
            return 0.0
        gross_profit = trades[trades['pnl'] > 0]['pnl'].sum()
        gross_loss = abs(trades[trades['pnl'] < 0]['pnl'].sum())
        if gross_loss == 0:
            return float('inf') if gross_profit > 0 else 0.0
        return float(gross_profit / gross_loss)

    def avg_holding_period(self, trades: pd.DataFrame) -> float:
        if trades.empty or 'holding_days' not in trades.columns:
            return 0.0
        return float(trades['holding_days'].mean())

    def reward_risk_ratio(self, trades: pd.DataFrame) -> float:
        if trades.empty or 'pnl' not in trades.columns:
            return 0.0
        sells = trades[trades['side'] == 'sell'] if 'side' in trades.columns else trades
        if sells.empty:
            return 0.0
        wins = sells[sells['pnl'] > 0]['pnl']
        losses = sells[sells['pnl'] < 0]['pnl'].abs()
        if wins.empty or losses.empty:
            return 0.0
        avg_win = float(wins.mean())
        avg_loss = float(losses.mean())
        if avg_loss <= 0:
            return 0.0
        return avg_win / avg_loss

    def return_distribution(self, equity_curve: pd.Series | list) -> dict:
        returns = self._daily_returns(equity_curve)
        if returns.empty:
            return {
                'return_mean': 0.0,
                'return_median': 0.0,
                'return_p05': 0.0,
                'return_p95': 0.0,
                'positive_day_ratio': 0.0,
            }
        return {
            'return_mean': float(returns.mean()),
            'return_median': float(returns.median()),
            'return_p05': float(returns.quantile(0.05)),
            'return_p95': float(returns.quantile(0.95)),
            'positive_day_ratio': float((returns > 0).mean()),
        }

    def consistency_score(self, equity_curve: pd.Series | list) -> float:
        returns = self._daily_returns(equity_curve)
        if len(returns) < 5:
            return 0.0
        positive_ratio = float((returns > 0).mean())
        tail_penalty = min(abs(float(returns.quantile(0.05))), 0.08) / 0.08
        return float(max(0.0, min(1.0, positive_ratio * 0.75 + (1.0 - tail_penalty) * 0.25)))

    def robustness_score(self, equity_curve: pd.Series | list, trades: pd.DataFrame | None = None) -> float:
        total_return = self.total_return(equity_curve)
        sharpe = self.sharpe_ratio(equity_curve)
        calmar = self.calmar_ratio(equity_curve)
        max_drawdown = abs(self.max_drawdown(equity_curve))
        drawdown_duration = self.max_drawdown_duration(equity_curve)
        consistency = self.consistency_score(equity_curve)
        returns_dist = self.return_distribution(equity_curve)
        tail_return = abs(float(returns_dist.get('return_p05', 0.0)))
        win_rate = self.win_rate(trades) if trades is not None and not trades.empty else 0.5
        reward_risk = self.reward_risk_ratio(trades) if trades is not None and not trades.empty else 0.0

        score = 0.0
        score += max(min(sharpe, 3.0), -2.0) * 14.0
        score += max(min(calmar, 4.0), -2.0) * 8.0
        score += max(min(total_return, 0.60), -0.30) * 45.0
        score += consistency * 18.0
        score += max(min(win_rate, 0.8), 0.2) * 12.0
        score += max(min(reward_risk, 3.0), 0.0) * 3.0
        score -= min(max_drawdown, 0.40) * 60.0
        score -= min(drawdown_duration, 120) / 120.0 * 12.0
        score -= min(tail_return, 0.08) / 0.08 * 10.0
        return round(float(score), 4)

    def full_report(self, equity_curve: pd.Series | list, trades: pd.DataFrame | None = None) -> dict:
        report = {
            'total_return': round(self.total_return(equity_curve), 4),
            'annualised_return': round(self.annualised_return(equity_curve), 4),
            'max_drawdown': round(self.max_drawdown(equity_curve), 4),
            'max_drawdown_duration': self.max_drawdown_duration(equity_curve),
            'sharpe_ratio': round(self.sharpe_ratio(equity_curve), 4),
            'sortino_ratio': round(self.sortino_ratio(equity_curve), 4),
            'calmar_ratio': round(self.calmar_ratio(equity_curve), 4),
            'volatility': round(self.volatility(equity_curve), 4),
            'consistency_score': round(self.consistency_score(equity_curve), 4),
        }
        if trades is not None and not trades.empty:
            report['win_rate'] = round(self.win_rate(trades), 4)
            report['profit_factor'] = round(self.profit_factor(trades), 4)
            report['reward_risk_ratio'] = round(self.reward_risk_ratio(trades), 4)
            report['avg_holding_period'] = round(self.avg_holding_period(trades), 2)
            if 'holding_days' in trades.columns:
                report['median_holding_period'] = round(float(trades['holding_days'].median()), 2)
                report['max_holding_period'] = int(trades['holding_days'].max())
            report['total_trades'] = len(trades)
        report.update({k: round(v, 4) for k, v in self.return_distribution(equity_curve).items()})
        report['robustness_score'] = self.robustness_score(equity_curve, trades)
        return report

    def _daily_returns(self, equity_curve: pd.Series | list) -> pd.Series:
        arr = pd.Series(equity_curve, dtype=float)
        return arr.pct_change().dropna()
