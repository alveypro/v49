from __future__ import annotations

from typing import Any

import pandas as pd

from src.signal_engine.factor_scorer import FactorScorer
from src.signal_engine.signal_fusion import SignalFusionEngine
from src.signal_engine.entry_exit_planner import EntryExitPlanner
from src.rules.market_rule_engine import MarketRuleEngine


class SignalAgent:
    """Generate trading signals by fusing forecast, factors, regime, and risk."""

    def __init__(self, config: dict) -> None:
        self.config = config
        signal_rules = config.get('signal_rules', {})
        self.factor_scorer = FactorScorer()
        self.fusion = SignalFusionEngine(signal_rules)
        self.planner = EntryExitPlanner()
        self.market_rules = MarketRuleEngine(config)

    def generate_signal(self, df: pd.DataFrame, forecast_result: dict,
                        regime_info: dict, risk_info: dict) -> dict[str, Any]:
        market_ok, market_reason = self.market_rules.check_tradeable(df.iloc[-1], direction='buy')
        if not market_ok:
            return {
                'signal': 'watch',
                'score': 0.0,
                'reason': f'market_rule_blocked({market_reason})',
                'research_signal': 'blocked_watch',
                'execution_state': 'blocked',
                'forecast_confidence': float(forecast_result.get('confidence', 0.0)),
                'market_rule_ok': False,
                'market_rule_reason': market_reason,
                'entry_plan': {},
                'factor_detail': {},
            }

        factor_scores = {}
        factor_scores.update(self.factor_scorer.score_trend_factors(df))
        factor_scores.update(self.factor_scorer.score_momentum_factors(df))
        factor_scores.update(self.factor_scorer.score_volume_factors(df))
        factor_scores.update(self.factor_scorer.score_research_factors(df))
        factor_result = self.factor_scorer.aggregate_factor_score(factor_scores)

        signal = self.fusion.fuse(forecast_result, factor_result, regime_info, risk_info)
        signal['forecast_confidence'] = float(forecast_result.get('confidence', 0.0))
        signal['market_rule_ok'] = market_ok
        signal['market_rule_reason'] = market_reason
        signal['regime'] = str(regime_info.get('regime', ''))
        signal['environment_score'] = float(regime_info.get('environment_score', 0.5) or 0.5)
        signal['research_signal'] = signal.get('signal', 'watch')
        signal['execution_state'] = 'tradeable' if market_ok else 'blocked'
        if signal.get('signal') in ('strong_buy', 'buy') and not market_ok:
            signal['signal'] = 'watch'
            old_reason = signal.get('reason', '')
            signal['reason'] = f'{old_reason}, market_rule_blocked({market_reason})' if old_reason else f'market_rule_blocked({market_reason})'
            signal['research_signal'] = 'blocked_watch'
            signal['execution_state'] = 'blocked'
        signal['entry_plan'] = self.planner.plan_entry(signal, float(df.iloc[-1]['close']), risk_info)
        signal['factor_detail'] = factor_result
        return signal
