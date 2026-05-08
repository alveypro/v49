from __future__ import annotations

import numpy as np


class ModelEnsemble:
    """Ensemble predictions from multiple models."""

    @staticmethod
    def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
        cleaned = {name: max(float(weight), 0.0) for name, weight in weights.items()}
        total = sum(cleaned.values())
        if total <= 0:
            count = len(cleaned) or 1
            return {name: 1.0 / count for name in cleaned} if cleaned else {}
        return {name: weight / total for name, weight in cleaned.items()}

    def weighted_average(self, prediction_dict: dict, weights: dict) -> dict:
        s, w = 0.0, 0.0
        for name, pred in prediction_dict.items():
            weight = weights.get(name, 1.0)
            s += pred.get('direction_prob', 0.5) * weight
            w += weight
        return {'direction_prob': s / w if w else 0.5}

    def majority_vote(self, prediction_dict: dict, threshold: float = 0.5) -> dict:
        votes_up = sum(1 for p in prediction_dict.values() if p.get('direction_prob', 0.5) > threshold)
        total = len(prediction_dict)
        ratio = votes_up / total if total else 0.5
        return {'direction_prob': ratio, 'vote_ratio': ratio}

    def regime_weighted(self, prediction_dict: dict, regime: str, regime_weights: dict | None = None) -> dict:
        """Different models weighted differently per regime."""
        default_weights = {
            'trend': {'lightgbm': 1.5, 'xgboost': 1.2, 'lstm': 1.3, 'transformer': 1.0, 'logistic': 0.5, 'random_forest': 0.8},
            'range': {'lightgbm': 1.0, 'xgboost': 1.0, 'lstm': 0.8, 'transformer': 0.8, 'logistic': 1.2, 'random_forest': 1.0},
        }
        weights = (regime_weights or default_weights).get(regime, {})
        return self.weighted_average(prediction_dict, weights)

    def dynamic_blend(
        self,
        prediction_dict: dict[str, dict],
        performance_weights: dict[str, float] | None = None,
        regime: str = '',
        regime_weights: dict[str, dict[str, float]] | None = None,
    ) -> dict[str, float | dict[str, float]]:
        if not prediction_dict:
            return {
                'direction_prob': 0.5,
                'agreement': 0.0,
                'dispersion': 0.0,
                'weights': {},
            }

        default_regime_weights = {
            'trend': {'lightgbm': 1.30, 'xgboost': 1.20, 'random_forest': 1.05, 'logistic': 0.90},
            'range': {'logistic': 1.25, 'random_forest': 1.10, 'lightgbm': 0.95, 'xgboost': 0.95},
            'neutral': {'lightgbm': 1.10, 'xgboost': 1.10, 'random_forest': 1.00, 'logistic': 1.00},
        }
        regime_map = regime_weights or default_regime_weights
        regime_key = regime if regime in regime_map else 'neutral'
        normalized_perf = self._normalize_weights(performance_weights or {name: 1.0 for name in prediction_dict})

        blended_weights: dict[str, float] = {}
        probs: list[float] = []
        for name, pred in prediction_dict.items():
            prob = float(pred.get('direction_prob', 0.5))
            probs.append(prob)
            confidence_boost = 0.75 + abs(prob - 0.5) * 2.0
            regime_boost = float(regime_map.get(regime_key, {}).get(name, 1.0))
            blended_weights[name] = normalized_perf.get(name, 0.0) * confidence_boost * regime_boost

        normalized_weights = self._normalize_weights(blended_weights)
        weighted = self.weighted_average(prediction_dict, normalized_weights)
        dispersion = float(np.std(probs)) if probs else 0.0
        agreement = max(0.0, 1.0 - min(dispersion / 0.25, 1.0))
        return {
            'direction_prob': float(weighted.get('direction_prob', 0.5)),
            'agreement': agreement,
            'dispersion': dispersion,
            'weights': normalized_weights,
        }

    def stacking(self, predictions: dict[str, float], meta_weights: dict[str, float] | None = None) -> float:
        """Simple stacking with learned meta-weights."""
        if meta_weights is None:
            return float(np.mean(list(predictions.values())))
        s, w = 0.0, 0.0
        for name, prob in predictions.items():
            wt = meta_weights.get(name, 1.0)
            s += prob * wt
            w += wt
        return s / w if w else 0.5
