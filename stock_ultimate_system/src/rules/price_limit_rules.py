from __future__ import annotations

import pandas as pd


class PriceLimitRules:
    """A-share price limit rules (涨跌停判断)."""

    def __init__(self, market_rules: dict) -> None:
        self.main_board_limit = market_rules.get('main_board_limit', 0.10)
        self.st_limit = market_rules.get('st_limit', 0.05)
        self.chinext_limit = market_rules.get('chinext_limit', 0.20)
        self.star_limit = market_rules.get('star_limit', 0.20)

    def _get_limit(self, row: pd.Series) -> float:
        ts_code = str(row.get('ts_code', ''))
        if row.get('is_st', 0):
            return self.st_limit
        if ts_code.startswith('3'):
            return self.chinext_limit
        if ts_code.startswith('68'):
            return self.star_limit
        return self.main_board_limit

    def is_limit_up(self, row: pd.Series) -> bool:
        limit = self._get_limit(row)
        pre_close = row.get('pre_close', 0)
        if pre_close <= 0:
            return False
        pct = (row.get('close', 0) - pre_close) / pre_close
        return pct >= limit - 1e-6

    def is_limit_down(self, row: pd.Series) -> bool:
        limit = self._get_limit(row)
        pre_close = row.get('pre_close', 0)
        if pre_close <= 0:
            return False
        pct = (row.get('close', 0) - pre_close) / pre_close
        return pct <= -limit + 1e-6
