from __future__ import annotations

from typing import Any

import pandas as pd

from src.constants.market_constants import (
    DEFAULT_COMMISSION_RATE,
    DEFAULT_SLIPPAGE_RATE,
    DEFAULT_STAMP_TAX_RATE,
)


class TradeSimulator:
    """Simulate order matching with realistic cost model."""

    def __init__(
        self,
        commission_rate: float = DEFAULT_COMMISSION_RATE,
        slippage_rate: float = DEFAULT_SLIPPAGE_RATE,
        stamp_tax_rate: float = DEFAULT_STAMP_TAX_RATE,
    ) -> None:
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate
        self.stamp_tax_rate = stamp_tax_rate

    def match_order(self, order_info: dict, market_row: pd.Series | dict) -> dict[str, Any]:
        side = order_info.get('side', 'buy')
        price = order_info.get('price', 0)
        qty = order_info.get('qty', 0)

        if side == 'buy':
            fill_price = price * (1 + self.slippage_rate)
        else:
            fill_price = price * (1 - self.slippage_rate)

        gross = fill_price * qty
        commission = max(gross * self.commission_rate, 5.0)
        stamp_tax = gross * self.stamp_tax_rate if side == 'sell' else 0.0
        slippage_cost = abs(fill_price - price) * qty

        if side == 'buy':
            net_amount = gross + commission
        else:
            net_amount = gross - commission - stamp_tax

        return {
            'ts_code': order_info.get('ts_code', ''),
            'side': side,
            'fill_price': round(fill_price, 4),
            'qty': qty,
            'gross_amount': round(gross, 2),
            'commission': round(commission, 2),
            'stamp_tax': round(stamp_tax, 2),
            'slippage_cost': round(slippage_cost, 2),
            'net_amount': round(net_amount, 2),
        }

    def can_fill(self, order_info: dict, market_row: pd.Series | dict) -> bool:
        if market_row.get('is_suspend', 0):
            return False
        side = order_info.get('side', 'buy')
        pre_close = market_row.get('pre_close', 0)
        close_price = market_row.get('close', 0)
        if pre_close > 0:
            pct = (close_price - pre_close) / pre_close
            if side == 'buy' and pct >= 0.095:
                return False
            if side == 'sell' and pct <= -0.095:
                return False
        return True
