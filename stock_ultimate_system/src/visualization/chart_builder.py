from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class ChartBuilder:
    """Generate charts for backtest analysis using matplotlib."""

    def __init__(self, output_dir: str = 'data/reports/charts') -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def plot_equity_curve(self, equity_curve: pd.DataFrame, title: str = 'Equity Curve',
                          save_path: str | None = None) -> str:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        fig, ax = plt.subplots(figsize=(14, 6))
        dates = pd.to_datetime(equity_curve['date'])
        equity = equity_curve['equity']

        ax.plot(dates, equity, linewidth=1.5, color='#2196F3')
        ax.fill_between(dates, equity, alpha=0.1, color='#2196F3')

        peak = equity.cummax()
        dd = (equity.values - peak.values) / peak.values
        ax2 = ax.twinx()
        ax2.fill_between(dates, dd, 0, alpha=0.3, color='#F44336', label='Drawdown')
        ax2.set_ylabel('Drawdown', color='#F44336')

        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_xlabel('Date')
        ax.set_ylabel('Equity')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        fig.autofmt_xdate()
        ax.grid(True, alpha=0.3)
        plt.tight_layout()

        path = save_path or str(self.output_dir / 'equity_curve.png')
        fig.savefig(path, dpi=150)
        plt.close(fig)
        return path

    def plot_monthly_returns(self, equity_curve: pd.DataFrame, save_path: str | None = None) -> str:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        df = equity_curve.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        monthly = df['equity'].resample('ME').last().pct_change().dropna()

        fig, ax = plt.subplots(figsize=(14, 5))
        colors = ['#4CAF50' if v >= 0 else '#F44336' for v in monthly.values]
        ax.bar(monthly.index, monthly.values, width=20, color=colors, alpha=0.8)
        ax.set_title('Monthly Returns', fontsize=14, fontweight='bold')
        ax.set_ylabel('Return')
        ax.axhline(0, color='black', linewidth=0.5)
        ax.grid(True, alpha=0.3, axis='y')
        fig.autofmt_xdate()
        plt.tight_layout()

        path = save_path or str(self.output_dir / 'monthly_returns.png')
        fig.savefig(path, dpi=150)
        plt.close(fig)
        return path

    def plot_trade_distribution(self, trades: pd.DataFrame, save_path: str | None = None) -> str:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        fig, axes = plt.subplots(1, 2, figsize=(14, 5))

        if 'pnl' in trades.columns:
            pnl = trades['pnl']
            colors = ['#4CAF50' if v >= 0 else '#F44336' for v in pnl.values]
            axes[0].bar(range(len(pnl)), pnl, color=colors, alpha=0.7)
            axes[0].set_title('Trade PnL')
            axes[0].axhline(0, color='black', linewidth=0.5)

            axes[1].hist(pnl, bins=30, color='#2196F3', alpha=0.7, edgecolor='white')
            axes[1].set_title('PnL Distribution')
            axes[1].axvline(0, color='black', linewidth=0.5)

        plt.tight_layout()
        path = save_path or str(self.output_dir / 'trade_distribution.png')
        fig.savefig(path, dpi=150)
        plt.close(fig)
        return path

    def plot_rolling_metrics(self, equity_curve: pd.DataFrame, window: int = 60,
                             save_path: str | None = None) -> str:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        df = equity_curve.copy()
        dates = pd.to_datetime(df['date'])
        returns = df['equity'].pct_change().dropna()

        fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

        rolling_ret = returns.rolling(window).mean() * 252
        axes[0].plot(dates[1:], rolling_ret, color='#2196F3', linewidth=1)
        axes[0].axhline(0, color='black', linewidth=0.5)
        axes[0].set_title(f'Rolling {window}-day Annualised Return')
        axes[0].grid(True, alpha=0.3)

        rolling_vol = returns.rolling(window).std() * np.sqrt(252)
        axes[1].plot(dates[1:], rolling_vol, color='#FF9800', linewidth=1)
        axes[1].set_title(f'Rolling {window}-day Volatility')
        axes[1].grid(True, alpha=0.3)

        fig.autofmt_xdate()
        plt.tight_layout()
        path = save_path or str(self.output_dir / 'rolling_metrics.png')
        fig.savefig(path, dpi=150)
        plt.close(fig)
        return path
