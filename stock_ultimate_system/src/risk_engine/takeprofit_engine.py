from __future__ import annotations

import pandas as pd


class TakeProfitEngine:
    """Multiple take-profit strategies."""

    def fixed_take_profit(self, entry_price: float, take_pct: float) -> float:
        return entry_price * (1 + take_pct)

    def atr_take_profit(self, entry_price: float, atr: float, multiplier: float = 3.0) -> float:
        return entry_price + atr * multiplier

    def staged_take_profit(self, entry_price: float, stages: list[tuple[float, float]] | None = None) -> list[dict]:
        """Return staged take-profit levels.

        Each stage is (pct_gain, proportion_to_sell).
        """
        if stages is None:
            stages = [(0.05, 0.3), (0.10, 0.3), (0.15, 0.4)]
        return [
            {'price': entry_price * (1 + pct), 'sell_ratio': ratio}
            for pct, ratio in stages
        ]

    def dynamic_take_profit(self, entry_price: float, df: pd.DataFrame, confidence: float) -> float:
        """Higher confidence → wider take-profit target."""
        base_pct = 0.05
        bonus = confidence * 0.15
        return entry_price * (1 + base_pct + bonus)
