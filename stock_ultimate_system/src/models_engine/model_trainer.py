from __future__ import annotations

import logging
import warnings
from importlib import import_module

import pandas as pd
from sklearn.preprocessing import StandardScaler

from .logistic_model import LogisticModel
from .random_forest_model import RandomForestModel
from .lightgbm_model import LightGBMModel
from .xgboost_model import XGBoostModel

logger = logging.getLogger(__name__)


_MODEL_CLASS_SPECS = {
    "lightgbm": (".lightgbm_model", "LightGBMModel"),
    "xgboost": (".xgboost_model", "XGBoostModel"),
    "lstm": (".lstm_model", "LSTMModel"),
    "transformer": (".transformer_model", "TransformerModel"),
}


def _load_optional_model_class(name: str):
    module_name, class_name = _MODEL_CLASS_SPECS[name]
    module = import_module(module_name, package=__package__)
    return getattr(module, class_name)


class ModelTrainer:
    """Train classical and deep learning models."""

    def __init__(self, params: dict | None = None) -> None:
        self.params = params or {}
        self.scaler = StandardScaler()
        self._is_scaled = False

    def split_train_valid_test(self, df: pd.DataFrame, feature_cols: list[str], target_col: str, train_ratio: float = 0.7, valid_ratio: float = 0.15):
        n = len(df)
        t1 = int(n * train_ratio)
        t2 = int(n * (train_ratio + valid_ratio))
        X = df[feature_cols].fillna(0)
        y = df[target_col].fillna(0)
        return X[:t1], X[t1:t2], X[t2:], y[:t1], y[t1:t2], y[t2:]

    def train_classical_models(self, X_train, y_train) -> dict:
        models = {}
        enabled = self.params.get('enabled_models', [])
        enabled_set = {
            str(name).strip()
            for name in enabled
            if str(name).strip()
        } if isinstance(enabled, (list, tuple, set)) else set()
        for name, cls_or_loader, params_key in [
            ('logistic', LogisticModel, 'logistic'),
            ('random_forest', RandomForestModel, 'random_forest'),
            ('lightgbm', LightGBMModel, 'lightgbm'),
            ('xgboost', XGBoostModel, 'xgboost'),
        ]:
            if enabled_set and name not in enabled_set:
                continue
            try:
                cls = cls_or_loader() if callable(cls_or_loader) and name in _MODEL_CLASS_SPECS else cls_or_loader
                m = cls(self.params.get(params_key, {}))
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        'ignore',
                        message=r'`sklearn\.utils\.parallel\.delayed` should be used with `sklearn\.utils\.parallel\.Parallel`.*',
                        category=UserWarning,
                    )
                    m.train(X_train, y_train)
                models[name] = m
                logger.info('Trained %s', name)
            except Exception as e:
                logger.warning('Failed to train %s: %s', name, e)
        return models

    def train_deep_models(self, X_train, y_train) -> dict:
        models = {}
        X_scaled = self.scaler.fit_transform(X_train)
        self._is_scaled = True

        for name, cls_loader, params_key in [
            ('lstm', lambda: _load_optional_model_class('lstm'), 'lstm'),
            ('transformer', lambda: _load_optional_model_class('transformer'), 'transformer'),
        ]:
            try:
                cls = cls_loader()
                m = cls(self.params.get(params_key, {}))
                m.train(X_scaled, y_train.values if hasattr(y_train, 'values') else y_train)
                models[name] = m
                logger.info('Trained %s', name)
            except Exception as e:
                logger.warning('Failed to train %s: %s', name, e)

        return models

    def train_all_models(self, X_train, y_train, enable_deep: bool = True) -> dict:
        models = self.train_classical_models(X_train, y_train)
        if enable_deep:
            deep = self.train_deep_models(X_train, y_train)
            models.update(deep)
        return models

    def evaluate_all(self, models: dict, X_test, y_test) -> dict[str, dict]:
        results = {}
        for name, model in models.items():
            try:
                if name in ('lstm', 'transformer') and self._is_scaled:
                    X_eval = self.scaler.transform(X_test)
                else:
                    X_eval = X_test
                results[name] = model.evaluate(X_eval, y_test)
            except Exception as e:
                results[name] = {'error': str(e)}
        return results
