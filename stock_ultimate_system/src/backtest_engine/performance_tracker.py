from __future__ import annotations

import numpy as np
import pandas as pd


class PerformanceTracker:
    """Track and compute performance metrics from backtest results."""

    def __init__(self) -> None:
        self._equity_records: list[dict] = []
        self._trade_records: list[dict] = []

    def record_equity(self, date: str, equity: float, cash: float) -> None:
        self._equity_records.append({'date': date, 'equity': equity, 'cash': cash})

    def record_trade(self, trade: dict) -> None:
        self._trade_records.append(trade)

    @property
    def equity_curve(self) -> pd.DataFrame:
        return pd.DataFrame(self._equity_records)

    @property
    def trades(self) -> pd.DataFrame:
        return pd.DataFrame(self._trade_records)

    def total_return(self) -> float:
        if not self._equity_records:
            return 0.0
        return self._equity_records[-1]['equity'] / self._equity_records[0]['equity'] - 1

    def annualised_return(self) -> float:
        tr = self.total_return()
        n = len(self._equity_records)
        if n <= 1:
            return 0.0
        return (1 + tr) ** (252 / n) - 1

    def max_drawdown(self) -> float:
        curve = self.equity_curve
        if curve.empty:
            return 0.0
        peak = curve['equity'].cummax()
        dd = (curve['equity'] - peak) / peak
        return float(dd.min())

    def sharpe_ratio(self, rf: float = 0.0) -> float:
        curve = self.equity_curve
        if len(curve) < 2:
            return 0.0
        daily_ret = curve['equity'].pct_change().dropna()
        excess = daily_ret - rf / 252
        if excess.std() == 0:
            return 0.0
        return float(excess.mean() / excess.std() * np.sqrt(252))

    def sortino_ratio(self, rf: float = 0.0) -> float:
        curve = self.equity_curve
        if len(curve) < 2:
            return 0.0
        daily_ret = curve['equity'].pct_change().dropna()
        excess = daily_ret - rf / 252
        downside = excess[excess < 0]
        if downside.std() == 0:
            return 0.0
        return float(excess.mean() / downside.std() * np.sqrt(252))

    def calmar_ratio(self) -> float:
        ann = self.annualised_return()
        mdd = self.max_drawdown()
        if mdd == 0:
            return 0.0
        return ann / abs(mdd)

    def win_rate(self) -> float:
        trades = self.trades
        if trades.empty:
            return 0.0
        sells = trades[trades.get('side', pd.Series()) == 'sell']
        if sells.empty:
            return 0.0
        wins = sells[sells.get('pnl', pd.Series(dtype=float)) > 0]
        return len(wins) / len(sells)

    def profit_factor(self) -> float:
        trades = self.trades
        if trades.empty:
            return 0.0
        sells = trades[trades.get('side', pd.Series()) == 'sell']
        if sells.empty or 'pnl' not in sells.columns:
            return 0.0
        gross_profit = sells[sells['pnl'] > 0]['pnl'].sum()
        gross_loss = abs(sells[sells['pnl'] < 0]['pnl'].sum())
        if gross_loss == 0:
            return float('inf') if gross_profit > 0 else 0.0
        return float(gross_profit / gross_loss)

    def summary(self) -> dict:
        return {
            'total_return': round(self.total_return(), 4),
            'annualised_return': round(self.annualised_return(), 4),
            'max_drawdown': round(self.max_drawdown(), 4),
            'sharpe_ratio': round(self.sharpe_ratio(), 4),
            'sortino_ratio': round(self.sortino_ratio(), 4),
            'calmar_ratio': round(self.calmar_ratio(), 4),
            'win_rate': round(self.win_rate(), 4),
            'profit_factor': round(self.profit_factor(), 4),
            'total_trades': len(self._trade_records),
            'trading_days': len(self._equity_records),
        }
