from __future__ import annotations

from typing import Any


class EntryExitPlanner:
    """Plan entry and exit strategies based on signal and risk."""

    def plan_entry(self, signal_result: dict, current_price: float,
                   risk_info: dict | None = None) -> dict[str, Any]:
        signal = signal_result.get('signal', 'watch')
        risk_info = risk_info or {}

        if signal in ('strong_buy', 'buy'):
            stop_loss = risk_info.get('stop_loss', current_price * 0.95)
            take_profit = risk_info.get('take_profit', current_price * 1.10)
            style = 'aggressive' if signal == 'strong_buy' else 'staggered'
            return {
                'action': 'buy',
                'style': style,
                'entry_price': current_price,
                'stop_loss': round(stop_loss, 4),
                'take_profit': round(take_profit, 4),
                'tranches': self._plan_tranches(current_price, style),
            }
        elif signal in ('reduce', 'sell'):
            return {
                'action': 'sell',
                'style': 'immediate' if signal == 'sell' else 'gradual',
                'exit_price': current_price,
            }
        return {'action': 'hold', 'style': 'none'}

    def _plan_tranches(self, price: float, style: str) -> list[dict]:
        if style == 'aggressive':
            return [{'ratio': 1.0, 'price': price}]
        return [
            {'ratio': 0.5, 'price': price},
            {'ratio': 0.3, 'price': price * 0.98},
            {'ratio': 0.2, 'price': price * 0.96},
        ]
