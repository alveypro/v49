from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass
class HoldingInfo:
    ts_code: str
    qty: int
    avg_cost: float
    buy_date: str
    current_price: float = 0.0

    @property
    def market_value(self) -> float:
        return self.current_price * self.qty

    @property
    def pnl(self) -> float:
        return (self.current_price - self.avg_cost) * self.qty

    @property
    def pnl_pct(self) -> float:
        if self.avg_cost <= 0:
            return 0.0
        return (self.current_price - self.avg_cost) / self.avg_cost


class PortfolioManager:
    """Track portfolio positions, cash, and exposure."""

    def __init__(self, initial_cash: float) -> None:
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.positions: dict[str, HoldingInfo] = {}
        self._history: list[dict] = []

    @property
    def equity(self) -> float:
        return self.cash + sum(h.market_value for h in self.positions.values())

    @property
    def total_position_value(self) -> float:
        return sum(h.market_value for h in self.positions.values())

    @property
    def position_ratio(self) -> float:
        eq = self.equity
        if eq <= 0:
            return 0.0
        return self.total_position_value / eq

    def update_prices(self, price_map: dict[str, float]) -> None:
        for code, holding in self.positions.items():
            if code in price_map:
                holding.current_price = price_map[code]

    def buy(self, ts_code: str, price: float, qty: int, date: str, cost: float = 0.0) -> bool:
        total = price * qty + cost
        if total > self.cash:
            return False
        self.cash -= total
        if ts_code in self.positions:
            h = self.positions[ts_code]
            new_qty = h.qty + qty
            h.avg_cost = (h.avg_cost * h.qty + price * qty) / new_qty
            h.qty = new_qty
            h.current_price = price
        else:
            self.positions[ts_code] = HoldingInfo(
                ts_code=ts_code, qty=qty, avg_cost=price,
                buy_date=date, current_price=price,
            )
        return True

    def sell(self, ts_code: str, price: float, qty: int | None = None, cost: float = 0.0) -> float:
        if ts_code not in self.positions:
            return 0.0
        h = self.positions[ts_code]
        sell_qty = qty if qty and qty <= h.qty else h.qty
        proceeds = price * sell_qty - cost
        self.cash += proceeds
        h.qty -= sell_qty
        if h.qty <= 0:
            del self.positions[ts_code]
        return proceeds

    def snapshot(self, date: str) -> dict[str, Any]:
        snap = {
            'date': date,
            'cash': self.cash,
            'equity': self.equity,
            'position_ratio': self.position_ratio,
            'n_holdings': len(self.positions),
            'holdings': {c: {'qty': h.qty, 'avg_cost': h.avg_cost, 'pnl_pct': h.pnl_pct}
                         for c, h in self.positions.items()},
        }
        self._history.append(snap)
        return snap

    def get_concentration(self) -> dict[str, float]:
        eq = self.equity
        if eq <= 0:
            return {}
        return {code: h.market_value / eq for code, h in self.positions.items()}

    def max_single_position_pct(self) -> float:
        conc = self.get_concentration()
        return max(conc.values()) if conc else 0.0
