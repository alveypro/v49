from __future__ import annotations

from typing import Any

from src.portfolio.position_sizer import PositionSizer


class PositionAgent:
    """Compute position size using signal strength and risk parameters."""

    def __init__(self, config: dict) -> None:
        self.config = config
        settings = config.get('settings', config)
        self.risk_cfg = settings.get('risk', {})
        signal_rules = config.get('signal_rules', {})
        self.buy_position_multiplier = float(signal_rules.get('buy_position_multiplier', 0.45))
        self.strong_buy_position_multiplier = float(signal_rules.get('strong_buy_position_multiplier', 1.0))
        self.regime_overrides = signal_rules.get('regime_overrides', {}) or {}
        self.sizer = PositionSizer()

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

    def calculate_position_size(self, signal_result: dict, risk_info: dict,
                                account_info: dict) -> dict[str, Any]:
        signal = signal_result.get('signal', 'watch')
        score = signal_result.get('score', 0)
        capital = account_info.get('cash', 0)
        max_pct = risk_info.get('max_position_pct', self.risk_cfg.get('max_position_pct', 0.20))
        position_scale = risk_info.get('position_scale', 1.0)
        regime_name = str(signal_result.get('regime', '') or '')
        regime_override = self._resolve_regime_override(regime_name)
        buy_multiplier = float(regime_override.get('buy_position_multiplier', self.buy_position_multiplier))
        strong_buy_multiplier = float(
            regime_override.get('strong_buy_position_multiplier', self.strong_buy_position_multiplier)
        )
        max_pct *= float(regime_override.get('max_position_pct_scale', 1.0) or 1.0)

        if signal == 'strong_buy':
            base_ratio = max_pct * strong_buy_multiplier
        elif signal == 'buy':
            base_ratio = max_pct * buy_multiplier
        else:
            base_ratio = 0.0

        base_ratio *= position_scale

        confidence_adj = min(1.0, max(0.5, score / 80))
        final_ratio = base_ratio * confidence_adj

        final_ratio = max(0, min(final_ratio, max_pct))

        amount = self.sizer.fixed_fractional(capital, final_ratio)

        return {
            'position_pct': round(final_ratio, 4),
            'position_amount': round(amount, 2),
            'base_ratio': round(base_ratio, 4),
            'confidence_adj': round(confidence_adj, 4),
        }
