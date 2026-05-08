from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from src.execution.order_manager import OrderManager
from src.execution.trade_simulator import TradeSimulator
from src.portfolio.portfolio_manager import PortfolioManager
from src.rules.market_rule_engine import MarketRuleEngine

logger = logging.getLogger(__name__)


class ExecutionAgent:
    """Coordinate order creation, simulation, and portfolio updates."""

    def __init__(self, config: dict) -> None:
        self.config = config
        bt_cfg = config.get('settings', config).get('backtest', {})
        self.orders = OrderManager()
        self.sim = TradeSimulator(
            commission_rate=bt_cfg.get('commission_rate', 0.0003),
            slippage_rate=bt_cfg.get('slippage_rate', 0.0005),
            stamp_tax_rate=bt_cfg.get('stamp_tax_rate', 0.001),
        )
        self.market_rules = MarketRuleEngine(config)

    def execute_buy(self, ts_code: str, price: float, qty: int, market_row: pd.Series | dict,
                    portfolio: PortfolioManager, date: str) -> dict[str, Any] | None:
        row = market_row if isinstance(market_row, pd.Series) else pd.Series(market_row)
        ok, _ = self.market_rules.check_tradeable(row, direction='buy')
        if not ok:
            return None
        order = self.orders.create_buy_order(ts_code, price, qty, reason='signal_buy')
        if not self.sim.can_fill(vars(order) if hasattr(order, '__dict__') else {}, market_row):
            self.orders.cancel_order(order.order_id)
            return None

        result = self.sim.match_order({
            'ts_code': ts_code, 'side': 'buy', 'price': price, 'qty': qty,
        }, market_row)

        if result['net_amount'] > portfolio.cash:
            self.orders.cancel_order(order.order_id)
            return None

        portfolio.buy(ts_code, result['fill_price'], qty, date, result['commission'])
        self.orders.fill_order(order.order_id, result['fill_price'], qty, result['commission'])
        logger.info('BUY %s qty=%d price=%.2f cost=%.2f', ts_code, qty, result['fill_price'], result['net_amount'])
        return result

    def execute_sell(self, ts_code: str, price: float, qty: int, market_row: pd.Series | dict,
                     portfolio: PortfolioManager, date: str) -> dict[str, Any] | None:
        row = market_row if isinstance(market_row, pd.Series) else pd.Series(market_row)
        ok, _ = self.market_rules.check_tradeable(row, direction='sell')
        if not ok:
            return None
        order = self.orders.create_sell_order(ts_code, price, qty, reason='signal_sell')
        if not self.sim.can_fill(vars(order) if hasattr(order, '__dict__') else {}, market_row):
            self.orders.cancel_order(order.order_id)
            return None

        result = self.sim.match_order({
            'ts_code': ts_code, 'side': 'sell', 'price': price, 'qty': qty,
        }, market_row)

        portfolio.sell(ts_code, result['fill_price'], qty, result['commission'] + result['stamp_tax'])
        self.orders.fill_order(order.order_id, result['fill_price'], qty, result['commission'])
        logger.info('SELL %s qty=%d price=%.2f net=%.2f', ts_code, qty, result['fill_price'], result['net_amount'])
        return result
