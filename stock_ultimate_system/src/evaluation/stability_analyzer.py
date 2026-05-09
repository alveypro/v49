from __future__ import annotations

import numpy as np
import pandas as pd


class StabilityAnalyzer:
    """Analyze strategy stability across different time windows and market regimes."""

    def rolling_sharpe(self, equity_curve: pd.Series | list, window: int = 60) -> pd.Series:
        returns = pd.Series(equity_curve, dtype=float).pct_change().dropna()
        if len(returns) < window:
            return pd.Series(dtype=float)
        rolling_mean = returns.rolling(window).mean()
        rolling_std = returns.rolling(window).std()
        return (rolling_mean / rolling_std.replace(0, np.nan) * np.sqrt(252)).dropna()

    def rolling_return(self, equity_curve: pd.Series | list, window: int = 60) -> pd.Series:
        arr = pd.Series(equity_curve, dtype=float)
        return arr.pct_change(window).dropna()

    def monthly_returns(self, equity_curve: pd.DataFrame) -> pd.Series:
        """Expects DataFrame with 'date' and 'equity' columns."""
        df = equity_curve.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        monthly = df['equity'].resample('ME').last()
        return monthly.pct_change().dropna()

    def yearly_returns(self, equity_curve: pd.DataFrame) -> pd.Series:
        df = equity_curve.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        yearly = df['equity'].resample('YE').last()
        return yearly.pct_change().dropna()

    def regime_performance(self, equity_curve: pd.DataFrame, regime_series: pd.Series) -> dict[str, dict]:
        """Compute return/risk metrics per market regime."""
        df = equity_curve.copy()
        df['daily_return'] = df['equity'].pct_change()
        df['regime'] = regime_series.values[:len(df)] if len(regime_series) >= len(df) else 'unknown'
        result = {}
        for regime, group in df.groupby('regime'):
            rets = group['daily_return'].dropna()
            result[str(regime)] = {
                'mean_return': float(rets.mean()),
                'volatility': float(rets.std() * np.sqrt(252)),
                'sharpe': float(rets.mean() / rets.std() * np.sqrt(252)) if rets.std() > 0 else 0,
                'days': len(rets),
            }
        return result

    def stability_score(self, equity_curve: pd.Series | list) -> float:
        """0–1 score: 1 = perfectly stable upward curve."""
        arr = np.array(equity_curve, dtype=float)
        if len(arr) < 10:
            return 0.0
        log_equity = np.log(np.maximum(arr, 1e-9))
        x = np.arange(len(log_equity))
        slope, intercept = np.polyfit(x, log_equity, 1)
        predicted = slope * x + intercept
        residuals = log_equity - predicted
        r_squared = 1 - np.var(residuals) / np.var(log_equity) if np.var(log_equity) > 0 else 0
        return float(max(0, min(r_squared, 1)))

    def full_stability_report(self, equity_curve: pd.DataFrame) -> dict:
        ec = equity_curve['equity'].tolist()
        monthly_returns = {}
        if len(ec) > 30:
            monthly = self.monthly_returns(equity_curve).round(4)
            monthly_returns = {str(idx): float(value) for idx, value in monthly.items()}
        return {
            'stability_score': round(self.stability_score(ec), 4),
            'rolling_sharpe_mean': round(float(self.rolling_sharpe(ec).mean()), 4) if len(ec) > 60 else None,
            'rolling_sharpe_std': round(float(self.rolling_sharpe(ec).std()), 4) if len(ec) > 60 else None,
            'monthly_returns': monthly_returns,
        }
