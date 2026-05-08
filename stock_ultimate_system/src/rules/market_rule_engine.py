from __future__ import annotations

import pandas as pd

from .price_limit_rules import PriceLimitRules
from .security_filter import SecurityFilter


class MarketRuleEngine:
    """Enforce A-share market rules on trade decisions."""

    def __init__(self, config: dict) -> None:
        market_rules = config.get('market_rules', {})
        self.price_limits = PriceLimitRules(market_rules)
        self.security_filter = SecurityFilter(market_rules)
        self.t_plus_one = market_rules.get('t_plus_one', True)

    def is_tradeable(self, row: pd.Series, direction: str = 'buy') -> bool:
        ok, _ = self.check_tradeable(row, direction)
        return ok

    def check_tradeable(self, row: pd.Series, direction: str = 'buy') -> tuple[bool, str]:
        if not self.security_filter.passes(row):
            if row.get('is_suspend', 0):
                return False, 'suspended'
            if row.get('is_st', 0):
                return False, 'st_filtered'
            if row.get('amount', 0) < self.security_filter.min_turnover:
                return False, 'low_liquidity'
            return False, 'security_filter_reject'

        if direction == 'buy' and self.price_limits.is_limit_up(row):
            return False, 'limit_up'
        if direction == 'sell' and self.price_limits.is_limit_down(row):
            return False, 'limit_down'
        return True, 'ok'

    def filter_tradeable_stocks(self, df: pd.DataFrame, direction: str = 'buy') -> pd.DataFrame:
        mask = df.apply(lambda r: self.is_tradeable(r, direction), axis=1)
        return df[mask].copy()

    def check_t_plus_one(self, buy_date: str, sell_date: str) -> bool:
        if not self.t_plus_one:
            return True
        return pd.Timestamp(sell_date) > pd.Timestamp(buy_date)
