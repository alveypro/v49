from __future__ import annotations

import pandas as pd


class SecurityFilter:
    """Filter out ST, suspended, new-listing, and low-liquidity stocks."""

    def __init__(self, market_rules: dict) -> None:
        self.filter_st = market_rules.get('filter_st', True)
        self.filter_new_stock_days = market_rules.get('filter_new_stock_days', 60)
        self.min_turnover = market_rules.get(
            'execution_liquidity_min_turnover',
            market_rules.get('extreme_low_liquidity_turnover', 300_000),
        )

    def passes(self, row: pd.Series) -> bool:
        if row.get('is_suspend', 0):
            return False
        if self.filter_st and row.get('is_st', 0):
            return False
        if row.get('amount', 0) < self.min_turnover:
            return False
        return True

    def filter_pool(self, df: pd.DataFrame) -> pd.DataFrame:
        mask = df.apply(self.passes, axis=1)
        return df[mask].copy()
