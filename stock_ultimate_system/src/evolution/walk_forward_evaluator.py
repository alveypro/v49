from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from src.evolution.trade_objective import summarize_trade_metrics, trade_objective_from_predictions
from src.models_engine.model_trainer import ModelTrainer


@dataclass
class WalkForwardPool:
    name: str
    frame: pd.DataFrame
    feature_cols: list[str]
    target_col: str


class WalkForwardEvaluator:
    def __init__(self, model_params: dict[str, Any] | None = None, *, enable_deep: bool = False) -> None:
        self.model_params = model_params or {}
        self.enable_deep = enable_deep

    @staticmethod
    def _return_col(frame: pd.DataFrame) -> str | None:
        for col in ("label_cross_sectional_excess_return_5", "label_excess_return_5", "label_return_5"):
            if col in frame.columns:
                return col
        return None

    @staticmethod
    def _split_indices(n_rows: int, folds: int) -> list[tuple[int, int, int]]:
        if n_rows < 60:
            return []
        fold_size = max(n_rows // max(folds + 2, 3), 20)
        splits: list[tuple[int, int, int]] = []
        train_end = fold_size * 2
        while train_end + fold_size <= n_rows and len(splits) < folds:
            valid_end = train_end + fold_size
            splits.append((0, train_end, valid_end))
            train_end += fold_size
        return splits

    def evaluate_pool(self, pool: WalkForwardPool, folds: int = 3) -> dict[str, Any]:
        frame = pool.frame.reset_index(drop=True)
        splits = self._split_indices(len(frame), folds)
        if not splits or not pool.feature_cols:
            return {
                "pool": pool.name,
                "folds": [],
                "summary": summarize_trade_metrics([]),
                "top_models": [],
            }

        fold_rows: list[dict[str, Any]] = []
        top_models: list[str] = []
        return_col = self._return_col(frame)

        for fold_id, (start_idx, train_end, valid_end) in enumerate(splits, start=1):
            train_slice = frame.iloc[start_idx:train_end]
            valid_slice = frame.iloc[train_end:valid_end]
            X_train = train_slice[pool.feature_cols].fillna(0.0)
            y_train = train_slice[pool.target_col].fillna(0.0)
            X_valid = valid_slice[pool.feature_cols].fillna(0.0)
            y_valid = valid_slice[pool.target_col].fillna(0.0)
            if X_train.empty or X_valid.empty:
                continue

            trainer = ModelTrainer(self.model_params)
            models = trainer.train_all_models(X_train, y_train, enable_deep=self.enable_deep)
            model_scores: list[tuple[str, float]] = []
            realized_returns = (
                valid_slice[return_col].fillna(0.0).astype(float).to_numpy() if return_col else None
            )
            for model_name, model in models.items():
                try:
                    X_eval = trainer.scaler.transform(X_valid) if model_name in ("lstm", "transformer") and trainer._is_scaled else X_valid
                    proba = model.predict_proba(X_eval)
                    arr = np.asarray(proba)
                    positive = arr[:, 1].astype(float) if arr.ndim > 1 else arr.astype(float)
                    metrics = trade_objective_from_predictions(y_valid.to_numpy(), positive, realized_returns)
                    metrics["pool"] = pool.name
                    metrics["fold"] = fold_id
                    metrics["model"] = model_name
                    fold_rows.append(metrics)
                    model_scores.append((model_name, float(metrics["trade_objective"])))
                except Exception:
                    continue
            if model_scores:
                model_scores.sort(key=lambda item: item[1], reverse=True)
                top_models.append(model_scores[0][0])

        summary = summarize_trade_metrics(fold_rows)
        summary["fold_count"] = float(len({int(row["fold"]) for row in fold_rows})) if fold_rows else 0.0
        summary["pool_count"] = 1.0
        return {
            "pool": pool.name,
            "folds": fold_rows,
            "summary": summary,
            "top_models": top_models,
        }

    def evaluate(self, pools: list[WalkForwardPool], folds: int = 3) -> dict[str, Any]:
        pool_results = [self.evaluate_pool(pool, folds=folds) for pool in pools]
        all_rows: list[dict[str, Any]] = []
        top_models: dict[str, int] = {}
        for pool_result in pool_results:
            all_rows.extend(pool_result.get("folds", []))
            for model_name in pool_result.get("top_models", []):
                top_models[model_name] = int(top_models.get(model_name, 0)) + 1

        summary = summarize_trade_metrics(all_rows)
        summary["pool_count"] = float(len(pool_results))
        summary["fold_count"] = float(len({(row.get("pool"), row.get("fold")) for row in all_rows})) if all_rows else 0.0
        stability_penalty = float(max(0.0, 0.25 - summary["trade_objective_std"]))
        summary["walk_forward_score"] = float(summary["trade_objective_mean"] * 0.7 + stability_penalty * 0.3)
        ordered_models = sorted(top_models.items(), key=lambda item: (-item[1], item[0]))
        return {
            "summary": summary,
            "pool_results": pool_results,
            "dominant_models": ordered_models,
        }
