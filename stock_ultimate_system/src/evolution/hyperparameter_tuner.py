from __future__ import annotations

import logging
from typing import Any, Callable

import numpy as np
import pandas as pd

from src.evolution.trade_objective import trade_objective_from_predictions

logger = logging.getLogger(__name__)


class HyperparameterTuner:
    """Hyperparameter optimization using Optuna."""

    def __init__(self, n_trials: int = 50, metric: str = 'trade_objective') -> None:
        self.n_trials = n_trials
        self.metric = metric

    def _score_predictions(self, y_true, pred, prob_up=None, realized_returns=None) -> float:
        from sklearn.metrics import accuracy_score, roc_auc_score

        if self.metric == 'auc' and prob_up is not None:
            return float(roc_auc_score(y_true, prob_up))
        if self.metric == 'trade_objective' and prob_up is not None:
            return float(
                trade_objective_from_predictions(y_true, prob_up, realized_returns).get('trade_objective', 0.0)
            )
        return float(accuracy_score(y_true, pred))

    def tune_lightgbm(self, X_train, y_train, X_valid, y_valid, realized_returns=None) -> dict:
        try:
            import optuna
            optuna.logging.set_verbosity(optuna.logging.WARNING)
        except ImportError:
            logger.warning('optuna not installed, returning default params')
            return {'n_estimators': 100, 'learning_rate': 0.05, 'num_leaves': 31}

        from lightgbm import LGBMClassifier

        def objective(trial):
            params = {
                'n_estimators': trial.suggest_int('n_estimators', 50, 500),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
                'num_leaves': trial.suggest_int('num_leaves', 15, 127),
                'max_depth': trial.suggest_int('max_depth', 3, 12),
                'min_child_samples': trial.suggest_int('min_child_samples', 5, 100),
                'subsample': trial.suggest_float('subsample', 0.5, 1.0),
                'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
                'reg_alpha': trial.suggest_float('reg_alpha', 1e-8, 10.0, log=True),
                'reg_lambda': trial.suggest_float('reg_lambda', 1e-8, 10.0, log=True),
                'verbose': -1,
            }
            model = LGBMClassifier(**params)
            model.fit(X_train, y_train)
            pred = model.predict(X_valid)
            proba = model.predict_proba(X_valid)[:, 1]
            return self._score_predictions(y_valid, pred, proba, realized_returns)

        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=self.n_trials, show_progress_bar=False)
        logger.info('Best LightGBM params: %s (score=%.4f)', study.best_params, study.best_value)
        return study.best_params

    def tune_xgboost(self, X_train, y_train, X_valid, y_valid, realized_returns=None) -> dict:
        try:
            import optuna
            optuna.logging.set_verbosity(optuna.logging.WARNING)
        except ImportError:
            return {'n_estimators': 100, 'learning_rate': 0.05}

        from xgboost import XGBClassifier

        def objective(trial):
            params = {
                'n_estimators': trial.suggest_int('n_estimators', 50, 500),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
                'max_depth': trial.suggest_int('max_depth', 3, 10),
                'min_child_weight': trial.suggest_int('min_child_weight', 1, 20),
                'subsample': trial.suggest_float('subsample', 0.5, 1.0),
                'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
                'reg_alpha': trial.suggest_float('reg_alpha', 1e-8, 10.0, log=True),
                'reg_lambda': trial.suggest_float('reg_lambda', 1e-8, 10.0, log=True),
                'use_label_encoder': False,
                'eval_metric': 'logloss',
                'verbosity': 0,
            }
            model = XGBClassifier(**params)
            model.fit(X_train, y_train)
            pred = model.predict(X_valid)
            proba = model.predict_proba(X_valid)[:, 1]
            return self._score_predictions(y_valid, pred, proba, realized_returns)

        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=self.n_trials, show_progress_bar=False)
        logger.info('Best XGBoost params: %s (score=%.4f)', study.best_params, study.best_value)
        return study.best_params

    def tune_generic(self, objective_fn: Callable, param_space: dict, direction: str = 'maximize') -> dict:
        try:
            import optuna
            optuna.logging.set_verbosity(optuna.logging.WARNING)
        except ImportError:
            return {}

        study = optuna.create_study(direction=direction)
        study.optimize(objective_fn, n_trials=self.n_trials, show_progress_bar=False)
        return study.best_params
