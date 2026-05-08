from __future__ import annotations

import pandas as pd


class RiskFilter:
    """Pre-trade risk filters."""

    def __init__(self, risk_rules: dict | None = None) -> None:
        rules = risk_rules or {}
        self.vol_threshold = rules.get('volatility_filter_threshold', 0.04)
        self.min_turnover = rules.get(
            'execution_liquidity_min_turnover',
            rules.get('extreme_low_liquidity_turnover', 300_000),
        )
        self.max_drawdown = rules.get('max_drawdown_protection', 0.10)

    def filter_by_volatility(self, df: pd.DataFrame) -> bool:
        vol = df.iloc[-1].get('hist_vol_20', 0) if 'hist_vol_20' in df.columns else 0
        return float(vol) < self.vol_threshold * 2

    def filter_by_liquidity(self, df: pd.DataFrame) -> bool:
        return float(df.iloc[-1].get('amount', 0)) > self.min_turnover

    def filter_by_suspension(self, df: pd.DataFrame) -> bool:
        return int(df.iloc[-1].get('is_suspend', 0)) == 0

    def filter_by_st(self, df: pd.DataFrame) -> bool:
        return int(df.iloc[-1].get('is_st', 0)) == 0

    def all_pass(self, df: pd.DataFrame) -> bool:
        return (
            self.filter_by_volatility(df)
            and self.filter_by_liquidity(df)
            and self.filter_by_suspension(df)
            and self.filter_by_st(df)
        )
