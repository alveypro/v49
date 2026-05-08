from __future__ import annotations

import numpy as np
import pandas as pd


class StopLossEngine:
    """Multiple stop-loss strategies."""

    def fixed_stop_loss(self, entry_price: float, stop_pct: float) -> float:
        return entry_price * (1 - stop_pct)

    def atr_stop_loss(self, entry_price: float, atr: float, multiplier: float = 2.0) -> float:
        return entry_price - atr * multiplier

    def trailing_stop_loss(self, peak_price: float, trail_pct: float) -> float:
        return peak_price * (1 - trail_pct)

    def chandelier_stop(self, df: pd.DataFrame, period: int = 22, multiplier: float = 3.0) -> float:
        if len(df) < period:
            return 0.0
        high_max = df['high'].tail(period).max()
        atr = self._calc_atr(df, period)
        return high_max - multiplier * atr

    def _calc_atr(self, df: pd.DataFrame, period: int) -> float:
        tr = pd.concat([
            df['high'] - df['low'],
            (df['high'] - df['close'].shift(1)).abs(),
            (df['low'] - df['close'].shift(1)).abs(),
        ], axis=1).max(axis=1)
        return float(tr.tail(period).mean())
