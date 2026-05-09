from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


class StrategyExplainer:
    """Explain strategy decisions and model contributions."""

    def explain_signal(self, signal_result: dict, forecast_result: dict,
                       factor_result: dict, regime_info: dict, risk_info: dict) -> str:
        lines = []
        signal = signal_result.get('signal', 'unknown')
        score = signal_result.get('score', 0)
        lines.append(f'Signal: {signal} (score={score:.1f})')

        regime = regime_info.get('regime', 'unknown')
        lines.append(f'Market regime: {regime}')

        prob = forecast_result.get('direction_prob', 0.5)
        conf = forecast_result.get('confidence', 0)
        lines.append(f'Direction probability: {prob:.2%}, confidence: {conf:.2%}')

        components = forecast_result.get('components', {})
        if components:
            lines.append('Model contributions:')
            for name, pred in components.items():
                p = pred.get('direction_prob', 0.5)
                lines.append(f'  - {name}: {p:.2%}')

        factor_total = factor_result.get('total', 0)
        lines.append(f'Factor score: {factor_total}')
        for k, v in factor_result.get('components', {}).items():
            lines.append(f'  - {k}: {v}')

        risk_level = risk_info.get('risk_level', 'unknown')
        lines.append(f'Risk level: {risk_level}')
        lines.append(f'Stop loss: {risk_info.get("stop_loss", 0):.2f}')
        lines.append(f'Take profit: {risk_info.get("take_profit", 0):.2f}')

        return '\n'.join(lines)

    def explain_trade(self, trade: dict) -> str:
        side = trade.get('side', '')
        code = trade.get('ts_code', '')
        price = trade.get('price', 0)
        qty = trade.get('qty', 0)
        reason = trade.get('reason', '')
        return f'{side.upper()} {code}: {qty} shares @ {price:.2f} | reason: {reason}'

    def feature_importance_summary(self, model, feature_names: list[str], top_n: int = 10) -> list[tuple[str, float]]:
        if hasattr(model, 'feature_importances_'):
            importances = model.feature_importances_
        elif hasattr(model, 'coef_'):
            importances = np.abs(model.coef_[0]) if len(model.coef_.shape) > 1 else np.abs(model.coef_)
        else:
            return []

        pairs = sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True)
        return pairs[:top_n]

    def shap_explanation(self, model: Any, X: pd.DataFrame, top_n: int = 10) -> dict[str, float] | None:
        try:
            import shap
            explainer = shap.TreeExplainer(model)
            shap_values = explainer.shap_values(X.tail(1))
            if isinstance(shap_values, list):
                shap_values = shap_values[1]
            importance = dict(zip(X.columns, np.abs(shap_values[0])))
            return dict(sorted(importance.items(), key=lambda x: x[1], reverse=True)[:top_n])
        except Exception:
            return None
