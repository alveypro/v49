from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class Order:
    order_id: str
    ts_code: str
    side: str
    price: float
    qty: int
    order_type: str = 'limit'
    status: str = 'pending'
    created_at: str = ''
    filled_at: str = ''
    filled_price: float = 0.0
    filled_qty: int = 0
    commission: float = 0.0
    reason: str = ''


class OrderManager:
    """Create, track, and manage orders."""

    def __init__(self) -> None:
        self._orders: dict[str, Order] = {}
        self._history: list[Order] = []

    def create_order(self, ts_code: str, side: str, price: float, qty: int,
                     order_type: str = 'limit', reason: str = '') -> Order:
        order = Order(
            order_id=str(uuid.uuid4())[:8],
            ts_code=ts_code,
            side=side,
            price=price,
            qty=qty,
            order_type=order_type,
            status='pending',
            created_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            reason=reason,
        )
        self._orders[order.order_id] = order
        return order

    def create_buy_order(self, ts_code: str, price: float, qty: int, reason: str = '') -> Order:
        return self.create_order(ts_code, 'buy', price, qty, reason=reason)

    def create_sell_order(self, ts_code: str, price: float, qty: int, reason: str = '') -> Order:
        return self.create_order(ts_code, 'sell', price, qty, reason=reason)

    def fill_order(self, order_id: str, filled_price: float, filled_qty: int, commission: float = 0.0) -> Order | None:
        order = self._orders.get(order_id)
        if order is None:
            return None
        order.filled_price = filled_price
        order.filled_qty = filled_qty
        order.commission = commission
        order.filled_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        order.status = 'filled'
        self._history.append(order)
        del self._orders[order_id]
        return order

    def cancel_order(self, order_id: str) -> bool:
        order = self._orders.pop(order_id, None)
        if order:
            order.status = 'cancelled'
            self._history.append(order)
            return True
        return False

    @property
    def pending_orders(self) -> list[Order]:
        return list(self._orders.values())

    @property
    def filled_orders(self) -> list[Order]:
        return [o for o in self._history if o.status == 'filled']
