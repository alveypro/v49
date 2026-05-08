from __future__ import annotations


class SignalFusionEngine:
    """Fuse forecast, factor, regime and risk into a unified signal."""

    def __init__(self, signal_rules: dict | None = None) -> None:
        rules = signal_rules or {}
        self.strong_buy_score = rules.get('strong_buy_score', 70)
        self.buy_score = rules.get('buy_score', 55)
        self.watch_score = rules.get('watch_score', 40)
        self.sell_score = rules.get('sell_score', 25)
        self.regime_overrides = rules.get('regime_overrides', {}) or {}

    def _resolve_regime_override(self, regime_name: str) -> dict:
        exact = self.regime_overrides.get(regime_name, {})
        if exact:
            return exact
        parts = str(regime_name).split('_')
        while len(parts) > 1:
            parts = parts[:-1]
            candidate = '_'.join(parts)
            if candidate in self.regime_overrides:
                return self.regime_overrides[candidate]
        return {}

    def fuse(self, forecast_result: dict, factor_result: dict,
             regime_info: dict, risk_info: dict) -> dict:
        prob = forecast_result.get('direction_prob', 0.5)
        confidence = forecast_result.get('confidence', 0)
        factor_total = factor_result.get('total', 0)
        env_score = regime_info.get('environment_score', 0.5)
        agreement = forecast_result.get('model_agreement', confidence)
        pred_return = forecast_result.get('pred_return', forecast_result.get('expected_return', 0.0))
        regime_name = str(regime_info.get('regime', 'range'))
        market_trend = str(regime_info.get('market_trend', 'bearish'))
        override = self._resolve_regime_override(regime_name)

        score = prob * 35 + factor_total + env_score * 20 + agreement * 10

        if confidence > 0.6:
            score += 5
        if pred_return > 0.03:
            score += 4
        elif pred_return < -0.02:
            score -= 4
        if not risk_info.get('allow_trade', True):
            score -= 20
        if risk_info.get('risk_level') == 'high':
            score -= 10
        if 'trend' in regime_name and 'volatile' not in regime_name:
            score += 4
        if market_trend == 'bullish':
            score += 2
        if 'range' in regime_name:
            score -= 2
        if 'extreme' in regime_name:
            score -= 6
        if 'volatile' in regime_name:
            score -= 5
        score += float(override.get('score_delta', 0.0) or 0.0)

        strong_buy_score = self.strong_buy_score
        buy_score = self.buy_score
        watch_score = self.watch_score
        sell_score = self.sell_score
        if 'volatile' in regime_name or 'extreme' in regime_name:
            strong_buy_score += 4
            buy_score += 3
        elif 'trend' in regime_name and market_trend == 'bullish':
            strong_buy_score -= 2
            buy_score -= 1
        strong_buy_score += float(override.get('strong_buy_score_delta', 0.0) or 0.0)
        buy_score += float(override.get('buy_score_delta', 0.0) or 0.0)
        watch_score += float(override.get('watch_score_delta', 0.0) or 0.0)
        sell_score += float(override.get('sell_score_delta', 0.0) or 0.0)

        if score >= strong_buy_score:
            signal = 'strong_buy'
        elif score >= buy_score:
            signal = 'buy'
        elif score >= watch_score:
            signal = 'watch'
        elif score >= sell_score:
            signal = 'reduce'
        else:
            signal = 'sell'

        reasons = []
        if prob > 0.6:
            reasons.append(f'model_bullish({prob:.0%})')
        if factor_total > 30:
            reasons.append(f'factor_strong({factor_total})')
        if env_score > 0.7:
            reasons.append('env_favorable')
        if agreement > 0.7:
            reasons.append(f'model_aligned({agreement:.0%})')
        if pred_return > 0.02:
            reasons.append(f'expected_move({pred_return:.1%})')
        if 'trend' in regime_name and 'volatile' not in regime_name:
            reasons.append('trend_regime')
        if 'volatile' in regime_name:
            reasons.append('volatile_regime')
        if not risk_info.get('allow_trade', True):
            reasons.append('risk_blocked')

        return {
            'signal': signal,
            'score': round(score, 2),
            'reason': ', '.join(reasons) if reasons else f'score={score:.1f}',
        }
