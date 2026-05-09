from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from src.models_engine.model_trainer import ModelTrainer
from src.models_engine.model_ensemble import ModelEnsemble
from src.evolution.trade_objective import trade_objective_from_predictions

logger = logging.getLogger(__name__)


class ForecastAgent:
    """Train ensemble models and produce forecasts."""

    def __init__(self, config: dict) -> None:
        self.config = config
        settings = config.get('settings', config)
        self.trainer = ModelTrainer(config.get('model_params', {}))
        self.ensemble = ModelEnsemble()
        self.models: dict = {}
        self.feature_cols: list[str] = []
        self.eval_results: dict[str, dict[str, Any]] = {}
        self.model_weights: dict[str, float] = {}
        self.calibration_profile: list[dict[str, float]] = []
        self._enable_deep = settings.get('training', {}).get('enable_deep_models', False)

    @staticmethod
    def _derive_model_weights(eval_results: dict[str, dict[str, Any]]) -> dict[str, float]:
        weights: dict[str, float] = {}
        for name, metrics in eval_results.items():
            if not isinstance(metrics, dict):
                weights[name] = 0.02
                continue
            trade_objective = float(metrics.get('trade_objective', 0.0) or 0.0)
            accuracy = float(metrics.get('accuracy', 0.5) or 0.5)
            balanced = float(metrics.get('balanced_accuracy', accuracy) or accuracy)
            brier = float(metrics.get('brier_score', 0.25) or 0.25)
            edge = max(accuracy - 0.48, 0.01)
            balanced_edge = max(balanced - 0.48, 0.01)
            calibration_bonus = max(0.30 - brier, 0.02)
            trade_bonus = max(trade_objective + 0.1, 0.01)
            weights[name] = trade_bonus * 0.50 + edge * 0.25 + balanced_edge * 0.15 + calibration_bonus * 0.10
        return weights

    @staticmethod
    def _return_target_col(df: pd.DataFrame) -> str | None:
        for col in ('label_cross_sectional_excess_return_5', 'label_excess_return_5', 'label_return_5'):
            if col in df.columns:
                return col
        return None

    def _attach_trade_metrics(self, model_name: str, model, X_test, y_test, test_frame: pd.DataFrame) -> dict[str, float]:
        try:
            if model_name in ('lstm', 'transformer') and self.trainer._is_scaled:
                X_eval = self.trainer.scaler.transform(X_test)
            else:
                X_eval = X_test
            proba = model.predict_proba(X_eval)
            arr = np.asarray(proba)
            positive = arr[:, 1].astype(float) if arr.ndim > 1 else arr.astype(float)
        except Exception:
            return {}

        return_col = self._return_target_col(test_frame)
        realized_returns = test_frame[return_col].fillna(0.0).astype(float).to_numpy() if return_col else None
        return trade_objective_from_predictions(y_test, positive, realized_returns)

    @staticmethod
    def _infer_regime_name(regime_info: dict[str, Any]) -> str:
        regime = str(regime_info.get('regime', '')).strip().lower()
        if regime:
            return regime
        if float(regime_info.get('environment_score', 0.5) or 0.5) >= 0.6:
            return 'trend'
        return 'range'

    @staticmethod
    def _estimate_expected_move(df, regime_info: dict[str, Any], blend: dict[str, Any]) -> float:
        latest = df.iloc[-1]
        atr_pct = abs(float(latest.get('atr_pct', 0.0) or 0.0))
        hist_vol = abs(float(latest.get('hist_vol_20', 0.0) or 0.0))
        rel_strength = float(latest.get('rel_strength_index', 0.0) or 0.0)
        base_move = max(0.012, min(0.12, atr_pct * 1.6 + hist_vol * 0.8))
        regime_boost = 1.1 if float(regime_info.get('environment_score', 0.5) or 0.5) >= 0.6 else 0.9
        agreement = float(blend.get('agreement', 0.0) or 0.0)
        strength = abs(float(blend.get('direction_prob', 0.5)) - 0.5) * 2.0
        rel_boost = 1.0 + max(min(rel_strength, 0.08), -0.08)
        return base_move * regime_boost * (0.65 + 0.35 * agreement) * rel_boost * max(strength, 0.2)

    @staticmethod
    def _calibrate_direction_prob(direction_prob: float, agreement: float, dispersion: float) -> float:
        edge = direction_prob - 0.5
        shrink = max(0.55, min(0.95, 0.65 + agreement * 0.25 - min(dispersion, 0.25) * 0.6))
        calibrated = 0.5 + edge * shrink
        return max(0.01, min(0.99, calibrated))

    @staticmethod
    def _bucket_calibration_records(records: pd.DataFrame, max_buckets: int = 5) -> list[dict[str, float]]:
        if records.empty:
            return []

        bucket_count = min(max_buckets, max(2, len(records) // 40))
        try:
            bucket_ids = pd.qcut(records['direction_prob_up'], q=bucket_count, labels=False, duplicates='drop')
        except ValueError:
            return []

        profiled = records.copy()
        profiled['bucket_id'] = bucket_ids
        grouped = profiled.groupby('bucket_id', dropna=True)
        buckets: list[dict[str, float]] = []
        for _, bucket in grouped:
            if bucket.empty:
                continue
            min_prob = float(bucket['direction_prob_up'].min())
            max_prob = float(bucket['direction_prob_up'].max())
            midpoint = (min_prob + max_prob) / 2.0
            avg_return = float(bucket['realized_return'].mean())
            median_return = float(bucket['realized_return'].median())
            up_rate = float(bucket['realized_up'].mean())
            sample_size = int(len(bucket))
            buckets.append({
                'min_prob': min_prob,
                'max_prob': max_prob,
                'mid_prob': midpoint,
                'avg_return': avg_return,
                'median_return': median_return,
                'up_rate': up_rate,
                'sample_size': sample_size,
            })
        return sorted(buckets, key=lambda item: item['mid_prob'])

    @staticmethod
    def _lookup_calibration_bucket(direction_prob: float, profile: list[dict[str, float]]) -> dict[str, float]:
        if not profile:
            return {}
        for bucket in profile:
            if bucket['min_prob'] <= direction_prob <= bucket['max_prob']:
                return bucket
        return min(profile, key=lambda item: abs(item['mid_prob'] - direction_prob))

    def _predict_probabilities_for_frame(self, X_frame) -> dict[str, np.ndarray]:
        predictions: dict[str, np.ndarray] = {}
        for name, model in self.models.items():
            try:
                if name in ('lstm', 'transformer') and self.trainer._is_scaled:
                    X_eval = self.trainer.scaler.transform(X_frame)
                    proba = model.predict_proba(X_eval)
                    arr = np.asarray(proba)
                    if arr.ndim > 1:
                        predictions[name] = arr[:, 1].astype(float)
                    else:
                        predictions[name] = arr.astype(float)
                else:
                    proba = model.predict_proba(X_frame)
                    arr = np.asarray(proba)
                    if arr.ndim > 1:
                        predictions[name] = arr[:, 1].astype(float)
                    else:
                        predictions[name] = arr.astype(float)
            except Exception as e:
                logger.debug('Batch prediction failed for %s: %s', name, e)
        return predictions

    def _build_calibration_profile(self, df, X_test) -> list[dict[str, float]]:
        return_col = 'label_excess_return_5' if 'label_excess_return_5' in df.columns else 'label_return_5'
        if len(X_test) < 80 or return_col not in df.columns:
            return []

        probability_map = self._predict_probabilities_for_frame(X_test)
        if not probability_map:
            return []

        test_frame = df.tail(len(X_test)).reset_index(drop=True)
        records: list[dict[str, float]] = []
        for idx in range(len(test_frame)):
            pred_dict = {
                name: {'direction_prob': float(probabilities[idx])}
                for name, probabilities in probability_map.items()
                if idx < len(probabilities)
            }
            if not pred_dict:
                continue
            blend = self.ensemble.dynamic_blend(
                pred_dict,
                performance_weights=self.model_weights or {k: 1.0 for k in pred_dict},
                regime='neutral',
            )
            direction_prob_up = self._calibrate_direction_prob(
                float(blend.get('direction_prob', 0.5)),
                float(blend.get('agreement', 0.0) or 0.0),
                float(blend.get('dispersion', 0.0) or 0.0),
            )
            realized_return = test_frame.iloc[idx].get(return_col)
            if pd.isna(realized_return):
                continue
            records.append({
                'direction_prob_up': direction_prob_up,
                'realized_return': float(realized_return),
                'realized_up': float(float(realized_return) > 0.0),
            })

        if len(records) < 40:
            return []
        return self._bucket_calibration_records(pd.DataFrame(records))

    def train_models(self, df, feature_cols: list[str], target_col: str) -> dict[str, Any]:
        self.feature_cols = feature_cols
        X_train, X_valid, X_test, y_train, y_valid, y_test = self.trainer.split_train_valid_test(
            df, feature_cols, target_col
        )
        self.models = self.trainer.train_all_models(X_train, y_train, enable_deep=self._enable_deep)
        eval_results = self.trainer.evaluate_all(self.models, X_test, y_test)
        test_frame = df.tail(len(X_test)).reset_index(drop=True) if len(X_test) else df.tail(0).copy()
        for name, model in self.models.items():
            if name not in eval_results or not isinstance(eval_results[name], dict):
                continue
            eval_results[name].update(self._attach_trade_metrics(name, model, X_test, y_test, test_frame))
        self.eval_results = eval_results
        self.model_weights = self._derive_model_weights(eval_results)
        self.calibration_profile = self._build_calibration_profile(df, X_test)
        logger.info('Trained models: %s', list(self.models.keys()))
        for name, r in eval_results.items():
            logger.info('  %s: %s', name, r)
        return {
            'trained_models': list(self.models.keys()),
            'test_size': len(X_test),
            'eval_results': eval_results,
            'model_weights': self.model_weights,
            'calibration_profile': self.calibration_profile,
        }

    def apply_external_model_weights(self, external_weights: dict[str, Any], *, blend_ratio: float = 0.65) -> dict[str, float]:
        if not external_weights:
            return self.model_weights
        merged: dict[str, float] = {}
        names = set(self.model_weights.keys()) | set(str(name) for name in external_weights.keys())
        for name in names:
            current = float(self.model_weights.get(name, 0.0) or 0.0)
            external = float(external_weights.get(name, 0.0) or 0.0)
            merged[name] = current * (1.0 - blend_ratio) + external * blend_ratio
        total = sum(value for value in merged.values() if value > 0)
        if total > 0:
            self.model_weights = {name: value / total for name, value in merged.items() if value > 0}
        return self.model_weights

    def predict(
        self,
        df,
        feature_cols: list[str] | None = None,
        regime_info: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if df is None or len(df) == 0:
            return {
                'direction_prob_up': 0.5,
                'direction_prob_down': 0.5,
                'pred_return': 0.0,
                'pred_range_high': 0.0,
                'pred_range_low': 0.0,
                'model_votes': {},
                'confidence': 0.0,
                'direction_prob': 0.5,
                'expected_return': 0.0,
            }
        if not self.models:
            return {
                'direction_prob_up': 0.5,
                'direction_prob_down': 0.5,
                'pred_return': 0.0,
                'pred_range_high': 0.0,
                'pred_range_low': 0.0,
                'model_votes': {},
                'confidence': 0.0,
                # backward compatibility
                'direction_prob': 0.5,
                'expected_return': 0.0,
            }

        cols = feature_cols or self.feature_cols
        if not cols:
            return {
                'direction_prob_up': 0.5,
                'direction_prob_down': 0.5,
                'pred_return': 0.0,
                'pred_range_high': 0.0,
                'pred_range_low': 0.0,
                'model_votes': {},
                'confidence': 0.0,
                'direction_prob': 0.5,
                'expected_return': 0.0,
            }
        x = df[cols].fillna(0).tail(1)
        regime_info = regime_info or {}

        preds = {}
        for name, model in self.models.items():
            try:
                if name in ('lstm', 'transformer') and self.trainer._is_scaled:
                    x_input = df[cols].fillna(0)
                    x_scaled = self.trainer.scaler.transform(x_input)
                    proba = model.predict_proba(x_scaled)
                    p = float(proba[-1][1]) if len(proba.shape) > 1 else float(proba[-1])
                else:
                    proba = model.predict_proba(x)
                    p = float(proba[0][1]) if hasattr(proba[0], '__len__') else float(proba[0])
                preds[name] = {'direction_prob': p}
            except Exception as e:
                logger.debug('Prediction failed for %s: %s', name, e)

        if not preds:
            return {
                'direction_prob_up': 0.5,
                'direction_prob_down': 0.5,
                'pred_return': 0.0,
                'pred_range_high': 0.0,
                'pred_range_low': 0.0,
                'model_votes': {},
                'confidence': 0.0,
                'direction_prob': 0.5,
                'expected_return': 0.0,
            }

        blend = self.ensemble.dynamic_blend(
            preds,
            performance_weights=self.model_weights or {k: 1.0 for k in preds},
            regime=self._infer_regime_name(regime_info),
        )
        direction_prob_up = float(blend.get('direction_prob', 0.5))
        direction_prob_up = self._calibrate_direction_prob(
            direction_prob_up,
            float(blend.get('agreement', 0.0) or 0.0),
            float(blend.get('dispersion', 0.0) or 0.0),
        )
        direction_prob_down = 1.0 - direction_prob_up
        expected_move = self._estimate_expected_move(df, regime_info, blend)
        pred_return = (direction_prob_up - 0.5) * 2.0 * expected_move
        confidence = min(1.0, abs(direction_prob_up - 0.5) * 2 * (0.7 + 0.3 * float(blend.get('agreement', 0.0))))
        last_close = float(df.iloc[-1].get('close', 0.0))
        range_span = max(abs(pred_return) * 0.6, expected_move * 0.35)
        pred_range_high = last_close * (1 + max(pred_return, range_span))
        pred_range_low = last_close * (1 + min(pred_return, -range_span))

        model_votes = {
            name: ('up' if p.get('direction_prob', 0.5) >= 0.5 else 'down')
            for name, p in preds.items()
        }
        calibration = self._lookup_calibration_bucket(direction_prob_up, self.calibration_profile)

        return {
            'direction_prob_up': direction_prob_up,
            'direction_prob_down': direction_prob_down,
            'pred_return': pred_return,
            'pred_range_high': pred_range_high,
            'pred_range_low': pred_range_low,
            'model_votes': model_votes,
            'confidence': confidence,
            'components': preds,
            'ensemble_weights': blend.get('weights', {}),
            'model_agreement': blend.get('agreement', 0.0),
            'prediction_dispersion': blend.get('dispersion', 0.0),
            'calibrated_upside_win_rate': float(calibration.get('up_rate', direction_prob_up) or direction_prob_up),
            'calibrated_avg_return': float(calibration.get('avg_return', pred_return) or pred_return),
            'calibrated_return_median': float(calibration.get('median_return', pred_return) or pred_return),
            'calibration_sample_size': int(calibration.get('sample_size', 0) or 0),
            # backward compatibility
            'direction_prob': direction_prob_up,
            'expected_return': pred_return,
        }
