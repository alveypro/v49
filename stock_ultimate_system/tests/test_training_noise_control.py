import warnings

import pandas as pd

from src.models_engine.lightgbm_model import LightGBMModel
from src.models_engine.model_trainer import ModelTrainer
from src.models_engine.xgboost_model import XGBoostModel


def test_tree_models_default_to_quiet_verbosity():
    lightgbm_model = LightGBMModel({})
    xgboost_model = XGBoostModel({})

    assert lightgbm_model.model.get_params()["verbosity"] == -1
    assert lightgbm_model.model.get_params()["verbose"] == -1
    assert xgboost_model.model.get_params()["verbosity"] == 0


def test_model_trainer_suppresses_parallel_warning(monkeypatch):
    trainer = ModelTrainer({})
    X_train = pd.DataFrame({"x": [0, 1, 2, 3]})
    y_train = pd.Series([0, 1, 0, 1])

    class WarnModel:
        def __init__(self, params):
            pass

        def train(self, X, y):
            warnings.warn(
                "`sklearn.utils.parallel.delayed` should be used with `sklearn.utils.parallel.Parallel` to make it possible to propagate the scikit-learn configuration of the current thread to the joblib workers.",
                UserWarning,
            )

    monkeypatch.setattr("src.models_engine.model_trainer.LogisticModel", WarnModel)
    monkeypatch.setattr("src.models_engine.model_trainer.RandomForestModel", WarnModel)
    monkeypatch.setattr("src.models_engine.model_trainer.LightGBMModel", WarnModel)
    monkeypatch.setattr("src.models_engine.model_trainer.XGBoostModel", WarnModel)

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        trainer.train_classical_models(X_train, y_train)

    assert not captured
