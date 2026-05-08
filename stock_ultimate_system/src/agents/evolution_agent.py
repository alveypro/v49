from __future__ import annotations

import logging
from typing import Any

from src.evolution.hyperparameter_tuner import HyperparameterTuner
from src.evolution.model_selector import ModelSelector
from src.evolution.factor_evolution import FactorEvolution

logger = logging.getLogger(__name__)


class EvolutionAgent:
    """Orchestrate model selection, hyperparameter tuning, and factor evolution."""

    def __init__(self, config: dict) -> None:
        self.config = config
        settings = config.get('settings', config)
        evo_cfg = settings.get('evolution', {})

        self.tuner = HyperparameterTuner(
            n_trials=evo_cfg.get('n_trials', 50),
            metric=evo_cfg.get('metric', 'trade_objective'),
        )
        self.selector = ModelSelector(
            top_k=evo_cfg.get('top_k_models', 3),
            metric=evo_cfg.get('metric', 'trade_objective'),
        )
        self.factor_evo = FactorEvolution(
            decay_threshold=evo_cfg.get('factor_decay_threshold', 0.3),
        )

    def evolve_models(
        self,
        models: dict,
        eval_results: dict,
        X_train,
        y_train,
        X_valid,
        y_valid,
        validation_returns=None,
    ) -> dict[str, Any]:
        selected = self.selector.select_top_models(eval_results)
        weights = self.selector.compute_model_weights(eval_results, selected)

        tuned_params = {}
        if 'lightgbm' in selected:
            tuned_params['lightgbm'] = self.tuner.tune_lightgbm(
                X_train, y_train, X_valid, y_valid, realized_returns=validation_returns
            )
        if 'xgboost' in selected:
            tuned_params['xgboost'] = self.tuner.tune_xgboost(
                X_train, y_train, X_valid, y_valid, realized_returns=validation_returns
            )

        return {
            'selected_models': selected,
            'model_weights': weights,
            'tuned_params': tuned_params,
        }

    def evolve_factors(self, df, factor_cols: list[str]) -> dict[str, Any]:
        ic_scores = self.factor_evo.evaluate_all_factors(df, factor_cols)
        active = self.factor_evo.get_active_factors(factor_cols)
        ranked = self.factor_evo.rank_factors()
        suggestions = self.factor_evo.suggest_new_factors(df)

        return {
            'ic_scores': ic_scores,
            'active_factors': active,
            'ranked_factors': ranked[:20],
            'suggested_new': suggestions,
            'removed_count': len(factor_cols) - len(active),
        }
