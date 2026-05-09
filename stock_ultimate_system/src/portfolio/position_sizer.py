from __future__ import annotations

import math


class PositionSizer:
    """Compute position size with multiple strategies."""

    def fixed_fractional(self, capital: float, ratio: float) -> float:
        return capital * ratio

    def kelly_criterion(self, win_rate: float, win_loss_ratio: float) -> float:
        """Kelly fraction = W - (1-W)/R."""
        if win_loss_ratio <= 0:
            return 0.0
        kelly = win_rate - (1 - win_rate) / win_loss_ratio
        return max(0.0, min(kelly, 0.25))

    def volatility_adjusted(self, capital: float, target_vol: float, asset_vol: float) -> float:
        if asset_vol <= 0:
            return 0.0
        ratio = target_vol / asset_vol
        return capital * min(ratio, 0.3)

    def risk_parity(self, capital: float, risk_per_trade: float, stop_distance: float) -> float:
        if stop_distance <= 0:
            return 0.0
        return capital * risk_per_trade / stop_distance

    def round_to_lot(self, amount: float, price: float, lot_size: int = 100) -> int:
        if price <= 0:
            return 0
        shares = int(amount / price)
        return (shares // lot_size) * lot_size
