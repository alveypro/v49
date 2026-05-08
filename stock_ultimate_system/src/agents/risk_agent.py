from __future__ import annotations

from typing import Any

import pandas as pd

from src.risk_engine.stoploss_engine import StopLossEngine
from src.risk_engine.takeprofit_engine import TakeProfitEngine
from src.risk_engine.risk_filter import RiskFilter
from src.risk_engine.drawdown_controller import DrawdownController
from src.rules.market_rule_engine import MarketRuleEngine


class RiskAgent:
    """Aggregate risk evaluation across all risk sub-engines."""

    def __init__(self, config: dict) -> None:
        self.config = config
        settings = config.get('settings', config)
        risk_cfg = settings.get('risk', {})
        risk_rules = config.get('risk_rules', {})

        self.stoploss = StopLossEngine()
        self.takeprofit = TakeProfitEngine()
        self.risk_filter = RiskFilter(risk_rules)
        self.drawdown_ctrl = DrawdownController(
            max_drawdown=risk_cfg.get('max_drawdown_protection', 0.10),
            protection_scale=risk_rules.get('drawdown_protection_position_scale', 0.30),
            recovery_ratio=risk_rules.get('drawdown_recovery_ratio', 0.50),
        )
        self.market_rules = MarketRuleEngine(config)
        self.range_confidence_min = float(risk_rules.get('range_confidence_min', 0.05))
        self.atr_stop_loss_multiplier = float(risk_rules.get('atr_stop_loss_multiplier', 2.0))
        self.atr_take_profit_multiplier = float(risk_rules.get('atr_take_profit_multiplier', 3.0))
        self.env_weak_score_threshold = float(risk_rules.get('environment_weak_score_threshold', 0.45))
        self.env_weak_position_scale = float(risk_rules.get('environment_weak_position_scale', 0.60))
        self.env_risk_off_position_scale = float(risk_rules.get('environment_risk_off_position_scale', 0.35))
        self.env_risk_off_regimes = set(risk_rules.get('environment_risk_off_regimes', ['extreme', 'range_volatile']))

    def evaluate_trade_risk(self, df: pd.DataFrame, forecast_result: dict, regime_info: dict) -> dict[str, Any]:
        latest_price = float(df.iloc[-1]['close'])
        settings = self.config.get('settings', self.config)
        risk_cfg = settings.get('risk', {})

        allow_trade = self.risk_filter.all_pass(df)
        latest_row = df.iloc[-1]
        market_ok, market_reason = self.market_rules.check_tradeable(latest_row, direction='buy')
        if not market_ok:
            allow_trade = False

        if regime_info.get('regime') == 'range' and forecast_result.get('confidence', 0) < self.range_confidence_min:
            allow_trade = False

        if self.drawdown_ctrl.in_protection:
            allow_trade = False

        env_score = float(regime_info.get('environment_score', 0.5))
        regime = str(regime_info.get('regime', ''))
        market_trend = str(regime_info.get('market_trend', ''))
        is_weak_env = env_score < self.env_weak_score_threshold
        is_risk_off = regime in self.env_risk_off_regimes or market_trend == 'bearish'

        env_scale = 1.0
        if is_weak_env:
            env_scale = min(env_scale, self.env_weak_position_scale)
        if is_risk_off:
            env_scale = min(env_scale, self.env_risk_off_position_scale)

        risk_level = 'low'
        if not allow_trade:
            risk_level = 'high'
        elif forecast_result.get('confidence', 0) < 0.5:
            risk_level = 'medium'

        stop_loss = self.stoploss.fixed_stop_loss(latest_price, risk_cfg.get('stop_loss_pct', 0.05))
        take_profit = self.takeprofit.fixed_take_profit(latest_price, risk_cfg.get('take_profit_pct', 0.10))

        if 'atr' in df.columns:
            atr_val = float(df.iloc[-1].get('atr', 0))
            if atr_val > 0:
                stop_loss = max(
                    stop_loss,
                    self.stoploss.atr_stop_loss(latest_price, atr_val, multiplier=self.atr_stop_loss_multiplier),
                )
                take_profit = min(
                    take_profit,
                    self.takeprofit.atr_take_profit(latest_price, atr_val, multiplier=self.atr_take_profit_multiplier),
                )

        position_scale = self.drawdown_ctrl.get_position_scale() * env_scale

        return {
            'allow_trade': allow_trade,
            'market_rule_ok': market_ok,
            'market_rule_reason': market_reason,
            'risk_level': risk_level,
            'stop_loss': round(stop_loss, 4),
            'take_profit': round(take_profit, 4),
            'position_scale': round(position_scale, 4),
            'drawdown_protection_active': self.drawdown_ctrl.in_protection,
            'environment_de_risk_active': bool(env_scale < 1.0),
            'atr_stop_loss_multiplier': self.atr_stop_loss_multiplier,
            'atr_take_profit_multiplier': self.atr_take_profit_multiplier,
            'max_position_pct': risk_cfg.get('max_position_pct', 0.20) * position_scale,
        }

    def update_equity(self, equity: float) -> None:
        self.drawdown_ctrl.update(equity)
