from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from src.constants.market_constants import (
    DEFAULT_COMMISSION_RATE,
    DEFAULT_SLIPPAGE_RATE,
    DEFAULT_STAMP_TAX_RATE,
)
from src.rules.market_rule_engine import MarketRuleEngine

logger = logging.getLogger(__name__)


@dataclass
class Trade:
    ts_code: str
    date: str
    side: str
    price: float
    qty: int
    commission: float = 0.0
    stamp_tax: float = 0.0
    slippage_cost: float = 0.0
    net_amount: float = 0.0
    cost_basis: float = 0.0
    pnl: float = 0.0
    pnl_pct: float = 0.0
    holding_days: int = 0


@dataclass
class Position:
    ts_code: str
    qty: int = 0
    avg_cost: float = 0.0
    buy_date: str = ''


@dataclass
class Account:
    cash: float
    positions: dict[str, Position] = field(default_factory=dict)
    trade_log: list[Trade] = field(default_factory=list)
    equity_curve: list[dict[str, Any]] = field(default_factory=list)
    signal_log: list[dict[str, Any]] = field(default_factory=list)


class EventDrivenBacktester:
    """Event-driven backtester that respects A-share rules."""

    def __init__(self, config: dict) -> None:
        settings = config.get('settings', config)
        bt_cfg = settings.get('backtest', {})
        self.initial_cash = bt_cfg.get('initial_cash', 1_000_000)
        self.commission_rate = bt_cfg.get('commission_rate', DEFAULT_COMMISSION_RATE)
        self.slippage_rate = bt_cfg.get('slippage_rate', DEFAULT_SLIPPAGE_RATE)
        self.stamp_tax_rate = bt_cfg.get('stamp_tax_rate', DEFAULT_STAMP_TAX_RATE)
        self.t_plus_one = bt_cfg.get('t_plus_one', True)
        self.price_limit_check = bt_cfg.get('price_limit_check', True)
        self.market_rules = MarketRuleEngine(config)

    def run(
        self,
        stock_pool: list[str],
        start_date: str,
        end_date: str,
        data_dict: dict[str, pd.DataFrame] | None = None,
        signal_func=None,
    ) -> dict[str, Any]:
        account = Account(cash=self.initial_cash)
        all_dates = self._collect_trade_dates(data_dict, start_date, end_date)
        rule_block_stats: dict[str, int] = {}
        signal_stats = {'total': 0, 'buy': 0, 'sell': 0}

        total_days = len(all_dates)
        for idx, date in enumerate(all_dates, start=1):
            daily_market: dict[str, pd.Series] = {}
            for code in stock_pool:
                df = data_dict.get(code) if data_dict else None
                if df is None:
                    continue
                row = df[df['date'] == date]
                if row.empty:
                    continue
                daily_market[code] = row.iloc[0]

            if signal_func:
                signals = signal_func(date, daily_market, account)
            else:
                signals = self._default_signal(date, daily_market, account)

            signal_stats['total'] += len(signals)
            for sig in signals:
                side = sig.get('side', '')
                if side == 'buy':
                    signal_stats['buy'] += 1
                elif side == 'sell':
                    signal_stats['sell'] += 1

            self._execute_signals(account, signals, daily_market, date, rule_block_stats)

            equity = self._calc_equity(account, daily_market)
            account.equity_curve.append({'date': date, 'equity': equity, 'cash': account.cash})
            if idx % 250 == 0:
                logger.info('Backtest progress: %d/%d days', idx, total_days)

        return self._build_result(account, stock_pool, start_date, end_date, rule_block_stats, signal_stats)

    def _collect_trade_dates(self, data_dict: dict | None, start: str, end: str) -> list[str]:
        if not data_dict:
            return pd.bdate_range(start, end).strftime('%Y-%m-%d').tolist()
        all_dates = set()
        for df in data_dict.values():
            dates = df[(df['date'] >= start) & (df['date'] <= end)]['date'].tolist()
            all_dates.update(dates)
        return sorted(all_dates)

    def _default_signal(self, date: str, market: dict, account: Account) -> list[dict]:
        return []

    def _execute_signals(
        self,
        account: Account,
        signals: list[dict],
        market: dict,
        date: str,
        rule_block_stats: dict[str, int],
    ) -> None:
        for sig in signals:
            code = sig.get('ts_code', '')
            side = sig.get('side', '')
            target_pct = sig.get('target_pct', 0.0)
            sig_reason = sig.get('reason', '')
            sig_regime = sig.get('regime', '')
            sig_environment_score = sig.get('environment_score')
            row = market.get(code)
            if row is None:
                account.signal_log.append({
                    'date': date, 'ts_code': code, 'side': side, 'target_pct': target_pct,
                    'signal_reason': sig_reason, 'regime': sig_regime,
                    'environment_score': sig_environment_score, 'status': 'no_market_data',
                })
                continue

            tradeable, reason = self.market_rules.check_tradeable(row, direction=side or 'buy')
            if not tradeable:
                rule_block_stats[reason] = rule_block_stats.get(reason, 0) + 1
                logger.debug('Rule blocked %s %s on %s: %s', side, code, date, reason)
                account.signal_log.append({
                    'date': date, 'ts_code': code, 'side': side, 'target_pct': target_pct,
                    'signal_reason': sig_reason, 'regime': sig_regime,
                    'environment_score': sig_environment_score, 'status': 'blocked', 'block_reason': reason,
                })
                continue

            price = float(row.get('open', row.get('close', 0)))
            if price <= 0:
                account.signal_log.append({
                    'date': date, 'ts_code': code, 'side': side, 'target_pct': target_pct,
                    'signal_reason': sig_reason, 'regime': sig_regime,
                    'environment_score': sig_environment_score, 'status': 'invalid_price',
                })
                continue

            if side == 'buy':
                status = self._execute_buy(account, code, price, target_pct, date)
                account.signal_log.append({
                    'date': date, 'ts_code': code, 'side': side, 'target_pct': target_pct,
                    'signal_reason': sig_reason, 'regime': sig_regime,
                    'environment_score': sig_environment_score, 'status': status,
                })
            elif side == 'sell':
                status = self._execute_sell(account, code, price, date, rule_block_stats)
                account.signal_log.append({
                    'date': date, 'ts_code': code, 'side': side, 'target_pct': target_pct,
                    'signal_reason': sig_reason, 'regime': sig_regime,
                    'environment_score': sig_environment_score, 'status': status,
                })
            else:
                account.signal_log.append({
                    'date': date, 'ts_code': code, 'side': side, 'target_pct': target_pct,
                    'signal_reason': sig_reason, 'regime': sig_regime,
                    'environment_score': sig_environment_score, 'status': 'unknown_side',
                })

    def _execute_buy(self, account: Account, code: str, price: float, target_pct: float, date: str) -> str:
        fill_price = price * (1 + self.slippage_rate)
        alloc = account.cash * target_pct
        qty = int(alloc / (fill_price * 100)) * 100
        if qty <= 0:
            return 'skip_qty_zero'
        gross = fill_price * qty
        commission = max(gross * self.commission_rate, 5.0)
        total_cost = gross + commission
        if total_cost > account.cash:
            return 'skip_insufficient_cash'

        account.cash -= total_cost
        pos = account.positions.get(code)
        if pos:
            new_qty = pos.qty + qty
            pos.avg_cost = (pos.avg_cost * pos.qty + fill_price * qty) / new_qty
            pos.qty = new_qty
        else:
            account.positions[code] = Position(ts_code=code, qty=qty, avg_cost=fill_price, buy_date=date)

        account.trade_log.append(Trade(
            ts_code=code, date=date, side='buy', price=fill_price, qty=qty,
            commission=commission, slippage_cost=fill_price * qty * self.slippage_rate,
            net_amount=total_cost,
        ))
        return 'filled'

    def _execute_sell(self, account: Account, code: str, price: float, date: str,
                      rule_block_stats: dict[str, int]) -> str:
        pos = account.positions.get(code)
        if not pos or pos.qty <= 0:
            return 'skip_no_position'
        if not self.market_rules.check_t_plus_one(pos.buy_date, date):
            rule_block_stats['t_plus_one'] = rule_block_stats.get('t_plus_one', 0) + 1
            return 'blocked_t_plus_one'

        fill_price = price * (1 - self.slippage_rate)
        cost_basis = pos.avg_cost * pos.qty
        gross = fill_price * pos.qty
        commission = max(gross * self.commission_rate, 5.0)
        stamp_tax = gross * self.stamp_tax_rate
        net = gross - commission - stamp_tax
        pnl = net - cost_basis
        pnl_pct = pnl / cost_basis if cost_basis > 0 else 0.0
        holding_days = max((pd.Timestamp(date) - pd.Timestamp(pos.buy_date)).days, 0)

        account.cash += net
        qty_sold = pos.qty
        del account.positions[code]

        account.trade_log.append(Trade(
            ts_code=code, date=date, side='sell', price=fill_price, qty=qty_sold,
            commission=commission, stamp_tax=stamp_tax,
            slippage_cost=fill_price * qty_sold * self.slippage_rate,
            net_amount=net,
            cost_basis=cost_basis,
            pnl=pnl,
            pnl_pct=pnl_pct,
            holding_days=holding_days,
        ))
        return 'filled'

    def _calc_equity(self, account: Account, market: dict) -> float:
        equity = account.cash
        for code, pos in account.positions.items():
            row = market.get(code)
            if row is not None:
                equity += float(row.get('close', pos.avg_cost)) * pos.qty
            else:
                equity += pos.avg_cost * pos.qty
        return equity

    def _build_result(self, account: Account, stock_pool, start_date, end_date,
                      rule_block_stats: dict[str, int], signal_stats: dict[str, int]) -> dict[str, Any]:
        curve = pd.DataFrame(account.equity_curve)
        trades_df = pd.DataFrame([vars(t) for t in account.trade_log]) if account.trade_log else pd.DataFrame()
        signal_logs_df = pd.DataFrame(account.signal_log) if account.signal_log else pd.DataFrame()

        if curve.empty:
            return {'stock_pool': stock_pool, 'start_date': start_date, 'end_date': end_date,
                    'status': 'no_data', 'metrics': {}}

        total_return = (curve['equity'].iloc[-1] / self.initial_cash) - 1
        max_equity = curve['equity'].cummax()
        drawdowns = (curve['equity'] - max_equity) / max_equity
        max_drawdown = float(drawdowns.min())

        n_days = len(curve)
        ann_factor = 252 / max(n_days, 1)
        ann_return = (1 + total_return) ** ann_factor - 1
        daily_returns = curve['equity'].pct_change().dropna()
        sharpe = float(daily_returns.mean() / daily_returns.std() * np.sqrt(252)) if daily_returns.std() > 0 else 0.0
        calmar = float(ann_return / abs(max_drawdown)) if max_drawdown < 0 else 0.0

        sell_trades = [t for t in account.trade_log if t.side == 'sell']
        wins = [t for t in sell_trades if t.pnl > 0]
        win_rate = len(wins) / max(len(sell_trades), 1)
        gross_profit = float(sum(t.pnl for t in sell_trades if t.pnl > 0))
        gross_loss = abs(float(sum(t.pnl for t in sell_trades if t.pnl < 0)))
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float('inf') if gross_profit > 0 else 0.0)
        avg_win = (gross_profit / len(wins)) if wins else 0.0
        losses = [t for t in sell_trades if t.pnl < 0]
        avg_loss = (abs(sum(t.pnl for t in losses)) / len(losses)) if losses else 0.0
        reward_risk_ratio = (avg_win / avg_loss) if avg_loss > 0 else 0.0
        holding_days = [t.holding_days for t in sell_trades]
        avg_holding_days = float(np.mean(holding_days)) if holding_days else 0.0
        median_holding_days = float(np.median(holding_days)) if holding_days else 0.0

        commission_total = float(sum(t.commission for t in account.trade_log))
        stamp_tax_total = float(sum(t.stamp_tax for t in account.trade_log))
        slippage_total = float(sum(t.slippage_cost for t in account.trade_log))
        total_cost = commission_total + stamp_tax_total + slippage_total
        cost_sensitivity = self._build_cost_sensitivity(curve['equity'].iloc[-1], total_cost)
        returns_dist = self._build_return_distribution(daily_returns)

        return {
            'stock_pool': stock_pool,
            'start_date': start_date,
            'end_date': end_date,
            'status': 'ok',
            'metrics': {
                'total_return': round(total_return, 4),
                'annualised_return': round(ann_return, 4),
                'max_drawdown': round(max_drawdown, 4),
                'sharpe_ratio': round(sharpe, 4),
                'calmar_ratio': round(calmar, 4),
                'total_trades': len(account.trade_log),
                'win_rate': round(win_rate, 4),
                'profit_factor': round(profit_factor, 4),
                'reward_risk_ratio': round(reward_risk_ratio, 4),
                'avg_holding_days': round(avg_holding_days, 2),
                'median_holding_days': round(median_holding_days, 2),
                'return_mean': round(returns_dist['return_mean'], 4),
                'return_median': round(returns_dist['return_median'], 4),
                'return_p05': round(returns_dist['return_p05'], 4),
                'return_p95': round(returns_dist['return_p95'], 4),
                'positive_day_ratio': round(returns_dist['positive_day_ratio'], 4),
                'total_commission': round(commission_total, 2),
                'total_stamp_tax': round(stamp_tax_total, 2),
                'total_slippage_cost': round(slippage_total, 2),
                'total_transaction_cost': round(total_cost, 2),
                'final_equity': round(curve['equity'].iloc[-1], 2),
            },
            'cost_sensitivity': cost_sensitivity,
            'signal_stats': signal_stats,
            'rule_block_stats': rule_block_stats,
            'rule_block_total': int(sum(rule_block_stats.values())),
            'signal_logs': signal_logs_df,
            'equity_curve': curve,
            'trades': trades_df,
        }

    def _build_cost_sensitivity(self, final_equity: float, base_cost: float) -> list[dict[str, float]]:
        multipliers = [0.5, 1.0, 1.5, 2.0]
        rows: list[dict[str, float]] = []
        for m in multipliers:
            adjusted_equity = final_equity - base_cost * (m - 1.0)
            adjusted_return = adjusted_equity / self.initial_cash - 1 if self.initial_cash > 0 else 0.0
            rows.append({
                'cost_multiplier': m,
                'adjusted_equity': round(float(adjusted_equity), 2),
                'adjusted_total_return': round(float(adjusted_return), 4),
            })
        return rows

    @staticmethod
    def _build_return_distribution(daily_returns: pd.Series) -> dict[str, float]:
        if daily_returns.empty:
            return {
                'return_mean': 0.0,
                'return_median': 0.0,
                'return_p05': 0.0,
                'return_p95': 0.0,
                'positive_day_ratio': 0.0,
            }
        return {
            'return_mean': float(daily_returns.mean()),
            'return_median': float(daily_returns.median()),
            'return_p05': float(daily_returns.quantile(0.05)),
            'return_p95': float(daily_returns.quantile(0.95)),
            'positive_day_ratio': float((daily_returns > 0).mean()),
        }
