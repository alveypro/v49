from __future__ import annotations

import json
import logging
import time
from typing import Any, Callable

import pandas as pd

from src.config.config_loader import ConfigLoader
from src.agents.data_agent import DataAgent
from src.agents.feature_agent import FeatureAgent
from src.agents.regime_agent import RegimeAgent
from src.agents.forecast_agent import ForecastAgent
from src.agents.signal_agent import SignalAgent
from src.agents.risk_agent import RiskAgent
from src.agents.position_agent import PositionAgent
from src.agents.backtest_agent import BacktestAgent
from src.agents.evolution_agent import EvolutionAgent
from src.backtest_engine.strategy_runner import StrategyRunner
from src.evolution.version_manager import EvolutionVersionManager
from src.evolution.walk_forward_evaluator import WalkForwardEvaluator, WalkForwardPool
from src.evaluation.report_generator import ReportGenerator
from src.utils.experiment_tracker import ExperimentTracker
from src.utils.project_paths import resolve_project_path
from src.utils.research_pool import resolve_research_pool_with_meta

logger = logging.getLogger(__name__)


class PipelineManager:
    """Orchestrate the full pipeline: train → predict → backtest → evolve."""

    def __init__(self, config_dir: str) -> None:
        self.loader = ConfigLoader(config_dir)
        self.config = self.loader.load_all_configs()
        self.data_agent = DataAgent(self.config)
        self.feature_agent = FeatureAgent(self.config)
        self.regime_agent = RegimeAgent(self.config)
        self.forecast_agent = ForecastAgent(self.config)
        self.signal_agent = SignalAgent(self.config)
        self.risk_agent = RiskAgent(self.config)
        self.position_agent = PositionAgent(self.config)
        self.backtest_agent = BacktestAgent(self.config)
        self.evolution_agent = EvolutionAgent(self.config)
        self.version_manager = EvolutionVersionManager()
        self.reporter = ReportGenerator()
        self.tracker = ExperimentTracker()
        self._regime_profiles_cache: dict[str, Any] | None = None

    @staticmethod
    def _load_candidate_basket_feedback() -> dict[str, Any]:
        feedback_path = resolve_project_path("artifacts/primary_result_candidate_baskets/feedback_latest.json")
        if not feedback_path.exists():
            return {}
        try:
            payload = json.loads(feedback_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _load_candidate_basket_summary() -> dict[str, Any]:
        summary_path = resolve_project_path("data/experiments/candidates_basket_summary_latest.json")
        if not summary_path.exists():
            return {}
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _build_threshold_profile(self, walk_forward_summary: dict[str, Any]) -> dict[str, Any]:
        signal_rules = self.config.get('signal_rules', {}) or {}
        risk_rules = self.config.get('risk_rules', {}) or {}
        score = float(walk_forward_summary.get('walk_forward_score', 0.0) or 0.0)
        stability = float(walk_forward_summary.get('trade_objective_stability', 0.0) or 0.0)
        profile_name = 'neutral'
        score_shift = 0.0
        buy_scale = 1.0
        strong_buy_scale = 1.0
        weak_env_scale = 1.0
        risk_off_scale = 1.0
        range_conf_delta = 0.0

        if score >= 0.24 and stability >= 0.62:
            profile_name = 'offensive'
            score_shift = -2.0
            buy_scale = 1.06
            strong_buy_scale = 1.05
            weak_env_scale = 1.08
            risk_off_scale = 1.06
            range_conf_delta = -0.01
        elif score < 0.16 or stability < 0.55:
            profile_name = 'defensive'
            score_shift = 2.0
            buy_scale = 0.92
            strong_buy_scale = 0.95
            weak_env_scale = 0.92
            risk_off_scale = 0.90
            range_conf_delta = 0.01

        strong_buy = float(signal_rules.get('strong_buy_score', 70) or 70) + score_shift
        buy = float(signal_rules.get('buy_score', 55) or 55) + (score_shift * 0.75)
        watch = float(signal_rules.get('watch_score', 40) or 40) + (score_shift * 0.5)
        sell = float(signal_rules.get('sell_score', 25) or 25) + (score_shift * 0.25)
        if strong_buy <= buy:
            strong_buy = buy + 5.0
        if buy <= watch:
            buy = watch + 5.0
        if watch <= sell:
            watch = sell + 5.0

        weak_position_scale = min(
            1.0,
            max(0.2, float(risk_rules.get('environment_weak_position_scale', 0.60) or 0.60) * weak_env_scale),
        )
        risk_off_position_scale = min(
            weak_position_scale,
            max(0.15, float(risk_rules.get('environment_risk_off_position_scale', 0.35) or 0.35) * risk_off_scale),
        )

        return {
            'profile': profile_name,
            'signal_rules': {
                'strong_buy_score': round(strong_buy, 2),
                'buy_score': round(buy, 2),
                'watch_score': round(watch, 2),
                'sell_score': round(sell, 2),
                'buy_position_multiplier': round(
                    max(0.2, float(signal_rules.get('buy_position_multiplier', 0.45) or 0.45) * buy_scale),
                    4,
                ),
                'strong_buy_position_multiplier': round(
                    max(0.4, float(signal_rules.get('strong_buy_position_multiplier', 1.0) or 1.0) * strong_buy_scale),
                    4,
                ),
            },
            'risk_rules': {
                'range_confidence_min': round(
                    max(0.01, float(risk_rules.get('range_confidence_min', 0.05) or 0.05) + range_conf_delta),
                    4,
                ),
                'environment_weak_position_scale': round(weak_position_scale, 4),
                'environment_risk_off_position_scale': round(risk_off_position_scale, 4),
            },
            'walk_forward_score': round(score, 4),
            'trade_objective_stability': round(stability, 4),
        }

    def _apply_champion_profile(self) -> dict[str, Any]:
        if not hasattr(self, 'version_manager') or self.version_manager is None:
            return {
                'champion_version': '',
                'champion_weights_applied': False,
                'champion_params_applied': False,
                'champion_selected_models': [],
                'champion_selected_models_applied': False,
                'active_enabled_models': [],
                'champion_threshold_profile': {},
                'champion_thresholds_applied': False,
            }
        registry = self.version_manager.load_registry()
        champion_payload = registry.get('champion_payload', {}) or {}
        model_evolution = champion_payload.get('model_evolution', {}) or {}
        threshold_profile = champion_payload.get('threshold_profile', {}) or {}
        if not threshold_profile:
            threshold_profile = self._build_threshold_profile(registry.get('champion_summary', {}) or {})
        champion_weights = model_evolution.get('model_weights', {}) or {}
        champion_params = model_evolution.get('tuned_params', {}) or {}
        champion_selected_models = [
            str(name).strip()
            for name in (model_evolution.get('selected_models', []) or [])
            if str(name).strip()
        ]
        trainer = getattr(self.forecast_agent, 'trainer', None)
        trainer_params = getattr(trainer, 'params', {}) or {}
        for model_name, params in champion_params.items():
            if not isinstance(params, dict):
                continue
            current = dict(trainer_params.get(model_name, {}) or {})
            current.update(params)
            trainer_params[model_name] = current
        selected_models_applied = False
        available_models = set(getattr(self.forecast_agent, 'models', {}).keys())
        applicable_models = [
            name for name in champion_selected_models
            if not available_models or name in available_models
        ]
        if applicable_models:
            trainer_params['enabled_models'] = list(applicable_models)
            if available_models:
                self.forecast_agent.models = {
                    name: model for name, model in self.forecast_agent.models.items()
                    if name in applicable_models
                }
                eval_results = getattr(self.forecast_agent, 'eval_results', {}) or {}
                if eval_results:
                    self.forecast_agent.eval_results = {
                        name: metrics for name, metrics in eval_results.items()
                        if name in applicable_models
                    }
                model_weights = getattr(self.forecast_agent, 'model_weights', {}) or {}
                if model_weights:
                    filtered_weights = {
                        name: float(value or 0.0)
                        for name, value in model_weights.items()
                        if name in applicable_models and float(value or 0.0) > 0.0
                    }
                    total_weight = sum(filtered_weights.values())
                    self.forecast_agent.model_weights = (
                        {name: value / total_weight for name, value in filtered_weights.items()}
                        if total_weight > 0
                        else {}
                    )
            selected_models_applied = True
        elif champion_selected_models and 'enabled_models' not in trainer_params:
            trainer_params['enabled_models'] = list(champion_selected_models)
        if champion_weights and self.forecast_agent.model_weights:
            self.forecast_agent.apply_external_model_weights(champion_weights)
        thresholds_applied = False
        if isinstance(threshold_profile, dict) and threshold_profile:
            signal_rules = threshold_profile.get('signal_rules', {}) or {}
            risk_rules = threshold_profile.get('risk_rules', {}) or {}
            config_signal_rules = self.config.setdefault('signal_rules', {})
            config_risk_rules = self.config.setdefault('risk_rules', {})
            if isinstance(config_signal_rules, dict):
                config_signal_rules.update(signal_rules)
            if isinstance(config_risk_rules, dict):
                config_risk_rules.update(risk_rules)
            fusion = getattr(getattr(self, 'signal_agent', None), 'fusion', None)
            if fusion is not None:
                for field in ('strong_buy_score', 'buy_score', 'watch_score', 'sell_score'):
                    if field in signal_rules:
                        setattr(fusion, field, float(signal_rules[field]))
            position_agent = getattr(self, 'position_agent', None)
            if position_agent is not None:
                if 'buy_position_multiplier' in signal_rules:
                    position_agent.buy_position_multiplier = float(signal_rules['buy_position_multiplier'])
                if 'strong_buy_position_multiplier' in signal_rules:
                    position_agent.strong_buy_position_multiplier = float(signal_rules['strong_buy_position_multiplier'])
            risk_agent = getattr(self, 'risk_agent', None)
            if risk_agent is not None:
                if 'range_confidence_min' in risk_rules:
                    risk_agent.range_confidence_min = float(risk_rules['range_confidence_min'])
                if 'environment_weak_position_scale' in risk_rules:
                    risk_agent.env_weak_position_scale = float(risk_rules['environment_weak_position_scale'])
                if 'environment_risk_off_position_scale' in risk_rules:
                    risk_agent.env_risk_off_position_scale = float(risk_rules['environment_risk_off_position_scale'])
            thresholds_applied = bool(signal_rules or risk_rules)
        return {
            'champion_version': str(registry.get('champion_version', '') or ''),
            'champion_weights_applied': bool(champion_weights),
            'champion_params_applied': bool(champion_params),
            'champion_selected_models': list(champion_selected_models),
            'champion_selected_models_applied': selected_models_applied,
            'active_enabled_models': list(trainer_params.get('enabled_models', []) or []),
            'champion_threshold_profile': threshold_profile,
            'champion_thresholds_applied': thresholds_applied,
        }

    @staticmethod
    def _select_training_symbols(ts_codes: list[str], max_symbols: int) -> list[str]:
        cleaned = [str(code).strip() for code in ts_codes if str(code).strip()]
        if not cleaned:
            return []
        if max_symbols <= 0 or len(cleaned) <= max_symbols:
            return list(dict.fromkeys(cleaned))

        selected: list[str] = []
        last_index = len(cleaned) - 1
        for slot in range(max_symbols):
            index = round(slot * last_index / max(max_symbols - 1, 1))
            code = cleaned[index]
            if code not in selected:
                selected.append(code)
        for code in cleaned:
            if len(selected) >= max_symbols:
                break
            if code not in selected:
                selected.append(code)
        return selected

    def _build_pooled_training_frame(
        self,
        ts_codes: list[str],
        max_symbols: int | None = None,
    ) -> tuple[Any, list[str], str] | None:
        settings = self.config.get('settings', {})
        training_cfg = settings.get('training', {})
        sample_size = int(max_symbols or training_cfg.get('batch_training_symbols', 8) or 8)
        training_symbols = self._select_training_symbols(ts_codes, sample_size)
        pooled_frames = []
        shared_feature_cols: list[str] | None = None
        target_col = 'label_direction_5'

        for code in training_symbols:
            try:
                df = self.data_agent.prepare_dataset(code)
                df = self.feature_agent.build_features(df)
                frame, feature_cols, target_col = self.feature_agent.prepare_training_frame(df)
            except Exception as e:
                logger.warning('Failed to prepare pooled training sample for %s: %s', code, e)
                continue
            if frame.empty or not feature_cols:
                continue
            pooled_frames.append(frame.copy())
            if shared_feature_cols is None:
                shared_feature_cols = list(feature_cols)
            else:
                shared_feature_cols = [col for col in shared_feature_cols if col in feature_cols]

        if not pooled_frames or not shared_feature_cols:
            return None

        pooled = pd.concat(pooled_frames, ignore_index=True)
        pooled = self._add_cross_sectional_targets(pooled)
        pooled_target = str(
            training_cfg.get('pooled_target_col', 'label_cross_sectional_top_quantile_5')
            or 'label_cross_sectional_top_quantile_5'
        )
        target_col = self._resolve_pooled_target_col(
            pooled,
            preferred=pooled_target,
            fallback=target_col,
        )
        return pooled, shared_feature_cols, target_col

    @staticmethod
    def _add_cross_sectional_targets(df: pd.DataFrame, horizon: int = 5) -> pd.DataFrame:
        pooled = df.copy()
        return_col = f'label_excess_return_{horizon}'
        if pooled.empty or 'date' not in pooled.columns or return_col not in pooled.columns:
            return pooled
        date_series = pd.to_datetime(pooled['date'], errors='coerce')
        valid_mask = date_series.notna() & pooled[return_col].notna()
        if not valid_mask.any():
            return pooled
        baseline = pooled.loc[valid_mask].groupby(date_series[valid_mask])[return_col].transform('median')
        pooled.loc[valid_mask, f'label_cross_sectional_excess_return_{horizon}'] = pooled.loc[valid_mask, return_col] - baseline
        pooled.loc[valid_mask, f'label_cross_sectional_excess_direction_{horizon}'] = (
            pooled.loc[valid_mask, f'label_cross_sectional_excess_return_{horizon}'] > 0
        ).astype(int)
        rank_pct = pooled.loc[valid_mask].groupby(date_series[valid_mask])[return_col].rank(
            pct=True, method='average', ascending=True
        )
        pooled.loc[valid_mask, f'label_cross_sectional_rank_pct_{horizon}'] = rank_pct
        group_sizes = pooled.loc[valid_mask].groupby(date_series[valid_mask])[return_col].transform('size')
        strong_mask = valid_mask & (group_sizes >= 5)
        pooled.loc[strong_mask, f'label_cross_sectional_top_quantile_{horizon}'] = (
            pooled.loc[strong_mask, f'label_cross_sectional_rank_pct_{horizon}'] >= 0.7
        ).astype(int)
        return pooled

    @staticmethod
    def _resolve_pooled_target_col(df: pd.DataFrame, preferred: str, fallback: str) -> str:
        candidates = [preferred, 'label_cross_sectional_excess_direction_5', fallback, 'label_excess_direction_5', 'label_direction_5']
        for candidate in candidates:
            if candidate not in df.columns:
                continue
            series = df[candidate].dropna()
            if series.empty:
                continue
            unique = set(series.astype(int).unique().tolist())
            if unique == {0, 1} or len(unique) > 1:
                return candidate
        return fallback

    def _ensure_models_for_universe(self, ts_codes: list[str]) -> None:
        if self.forecast_agent.models:
            return
        pooled_training = self._build_pooled_training_frame(ts_codes)
        if pooled_training is None:
            return
        pooled_df, feature_cols, target_col = pooled_training
        if pooled_df.empty or not feature_cols:
            return
        self.forecast_agent.train_models(pooled_df, feature_cols, target_col)

    def _liquidity_prefilter_stock_pool(
        self,
        stock_pool: list[str],
        data_dict: dict[str, Any],
        lookback_days: int = 20,
    ) -> list[str]:
        market_rules = self.config.get('market_rules', {})
        risk_rules = self.config.get('risk_rules', {})
        min_turnover = float(
            market_rules.get(
                'liquidity_min_turnover',
                risk_rules.get('liquidity_min_turnover', 1_000_000),
            )
        )

        filtered: list[str] = []
        blocked: list[str] = []
        for code in stock_pool:
            df = data_dict.get(code)
            if df is None or df.empty or 'amount' not in df.columns:
                blocked.append(code)
                continue
            tail = df.tail(lookback_days)
            median_turnover = float(tail['amount'].median()) if not tail.empty else 0.0
            if median_turnover >= min_turnover:
                filtered.append(code)
            else:
                blocked.append(code)

        if blocked:
            logger.info(
                'Liquidity prefilter removed %d/%d symbols (min_turnover=%.0f): %s',
                len(blocked), len(stock_pool), min_turnover, blocked[:10],
            )
        return filtered or stock_pool

    def _load_regime_profiles(self) -> dict[str, Any]:
        cache = getattr(self, '_regime_profiles_cache', None)
        if cache is not None:
            return cache
        settings = self.config.get('settings', {})
        runtime_cfg = settings.get('runtime', {}) if isinstance(settings, dict) else {}
        path_text = str(
            runtime_cfg.get(
                'regime_profile_path',
                'data/experiments/grid_search/grid_backtest_regime_profiles_latest.json',
            )
        ).strip()
        path = resolve_project_path(path_text)
        if not path.exists():
            self._regime_profiles_cache = {}
            return self._regime_profiles_cache
        try:
            self._regime_profiles_cache = json.loads(path.read_text(encoding='utf-8'))
        except Exception as e:
            logger.warning('Failed to load regime profiles from %s: %s', path, e)
            self._regime_profiles_cache = {}
        return self._regime_profiles_cache

    @staticmethod
    def _match_regime_profile(profiles: dict[str, Any], regime_name: str) -> dict[str, Any]:
        if not profiles:
            return {}
        exact = profiles.get(regime_name, {})
        if exact:
            return exact
        parts = str(regime_name).split('_')
        while len(parts) > 1:
            parts = parts[:-1]
            candidate = '_'.join(parts)
            if candidate in profiles:
                return profiles[candidate]
        return {}

    def _build_runtime_agents(
        self,
        regime_info: dict[str, Any],
    ) -> tuple[RiskAgent, SignalAgent, PositionAgent, dict[str, Any]]:
        profiles = self._load_regime_profiles()
        regime_name = str(regime_info.get('regime', '') or '')
        profile = self._match_regime_profile(profiles, regime_name)
        if not profile:
            return self.risk_agent, self.signal_agent, self.position_agent, {}

        params = profile.get('params', {}) or {}
        runtime_config = {
            **self.config,
            'settings': {**self.config.get('settings', {})},
            'signal_rules': {**self.config.get('signal_rules', {})},
            'risk_rules': {**self.config.get('risk_rules', {})},
            'market_rules': {**self.config.get('market_rules', {})},
        }
        runtime_settings_risk = {**runtime_config['settings'].get('risk', {})}

        for key, value in params.items():
            if key in {
                'strong_buy_score',
                'buy_score',
                'watch_score',
                'sell_score',
                'buy_position_multiplier',
                'strong_buy_position_multiplier',
                'low_confidence_skip_threshold',
            }:
                runtime_config['signal_rules'][key] = value
            elif key in {'stop_loss_pct', 'take_profit_pct', 'max_position_pct'}:
                runtime_settings_risk[key] = value
            else:
                runtime_config['risk_rules'][key] = value

        runtime_config['settings']['risk'] = runtime_settings_risk
        return (
            RiskAgent(runtime_config),
            SignalAgent(runtime_config),
            PositionAgent(runtime_config),
            {
                'regime': regime_name,
                'run_id': profile.get('run_id', ''),
                'params': params,
            },
        )

    def run_system_demo(self) -> None:
        """Quick end-to-end demo."""
        logger.info('=== System Demo ===')
        train_result = self.run_training_pipeline()
        logger.info('Training: %s', train_result)
        pred_result = self.run_prediction_pipeline('000001.SZ')
        logger.info('Prediction for 000001.SZ: signal=%s, score=%.1f',
                     pred_result.get('signal_result', {}).get('signal'),
                     pred_result.get('signal_result', {}).get('score', 0))
        bt_result = self.run_backtest_pipeline(['000001.SZ'], source_type='demo')
        logger.info('Backtest: %s', bt_result.get('metrics', bt_result.get('detailed_metrics', {})))

    def run_training_pipeline(self, ts_code: str = '000001.SZ') -> dict[str, Any]:
        logger.info('--- Training Pipeline ---')
        df = self.data_agent.prepare_dataset(ts_code)
        df = self.feature_agent.build_features(df)
        df, feature_cols, target_col = self.feature_agent.prepare_training_frame(df)
        result = self.forecast_agent.train_models(df, feature_cols, target_col)
        tracker_path = self.tracker.log_training_run(
            self.config, ts_code, feature_cols, target_col, result
        )
        result['experiment_path'] = tracker_path
        return result

    def run_prediction_pipeline(self, ts_code: str) -> dict[str, Any]:
        logger.info('--- Prediction Pipeline: %s ---', ts_code)
        df = self.data_agent.prepare_dataset(ts_code)
        df = self.feature_agent.build_features(df)
        df, feature_cols, target_col = self.feature_agent.prepare_training_frame(df)
        if df.empty or not feature_cols:
            raise ValueError(f'Insufficient feature history for {ts_code}')

        if not self.forecast_agent.models:
            self.forecast_agent.train_models(df, feature_cols, target_col)

        regime_info = self.regime_agent.detect_market_regime(df)
        risk_agent, signal_agent, position_agent, runtime_profile = self._build_runtime_agents(regime_info)
        forecast_result = self.forecast_agent.predict(df, feature_cols, regime_info=regime_info)
        risk_info = risk_agent.evaluate_trade_risk(df, forecast_result, regime_info)
        signal_result = signal_agent.generate_signal(df, forecast_result, regime_info, risk_info)
        position_result = position_agent.calculate_position_size(
            signal_result, risk_info, {'cash': 1_000_000}
        )
        latest = df.iloc[-1]

        return {
            'ts_code': ts_code,
            'regime_info': regime_info,
            'forecast_result': forecast_result,
            'risk_info': risk_info,
            'signal_result': signal_result,
            'position_result': position_result,
            'style_snapshot': {
                'hist_vol_20': float(latest.get('hist_vol_20', 0.0) or 0.0),
                'rel_strength_index': float(latest.get('rel_strength_index', 0.0) or 0.0),
                'market_return_20': float(latest.get('market_return_20', 0.0) or 0.0),
            },
            'runtime_profile': runtime_profile,
        }

    def run_batch_prediction(
        self,
        ts_codes: list[str],
        progress_callback: Callable[[list[dict[str, Any]], list[dict[str, str]]], None] | None = None,
    ) -> dict[str, Any]:
        config = getattr(self, 'config', {}) or {}
        settings = config.get('settings', {}) if isinstance(config, dict) else {}
        runtime_cfg = settings.get('runtime', {}) if isinstance(settings, dict) else {}
        max_symbols = int(runtime_cfg.get('batch_prediction_max_symbols', 0) or 0)
        max_runtime_sec = float(runtime_cfg.get('batch_prediction_max_runtime_sec', 0) or 0)
        if max_symbols > 0 and len(ts_codes) > max_symbols:
            logger.info('Batch prediction symbol cap applied: %d -> %d', len(ts_codes), max_symbols)
            ts_codes = list(ts_codes[:max_symbols])
        batch_started_at = time.monotonic()
        build_started_at = time.monotonic()
        pooled_training = self._build_pooled_training_frame(ts_codes)
        build_elapsed = time.monotonic() - build_started_at
        forecast_agent = getattr(self, 'forecast_agent', None)
        trainer = getattr(forecast_agent, 'trainer', None)
        trainer_params = getattr(trainer, 'params', {}) or {}
        enabled_models = trainer_params.get('enabled_models', [])
        enabled_label = ','.join(str(name) for name in enabled_models) if isinstance(enabled_models, list) and enabled_models else 'default'
        logger.info(
            'Batch prediction setup: symbols=%d max_runtime=%.0fs models=%s pooled_build_sec=%.2f',
            len(ts_codes),
            max_runtime_sec,
            enabled_label,
            build_elapsed,
        )
        if pooled_training is not None:
            pooled_df, feature_cols, target_col = pooled_training
            training_symbols = self._select_training_symbols(
                ts_codes,
                int(settings.get('training', {}).get('batch_training_symbols', 8) or 8),
            )
            logger.info(
                'Training pooled candidate models with %d rows across %d symbols',
                len(pooled_df),
                len(training_symbols),
            )
            train_started_at = time.monotonic()
            forecast_agent.train_models(pooled_df, feature_cols, target_col)
            champion_info = self._apply_champion_profile()
            logger.info(
                'Batch prediction training complete: train_sec=%.2f rows=%d features=%d target=%s champion=%s applied=%s',
                time.monotonic() - train_started_at,
                len(pooled_df),
                len(feature_cols),
                target_col,
                champion_info.get('champion_version', ''),
                champion_info.get('champion_weights_applied', False),
            )

        results: list[dict[str, Any]] = []
        skipped: list[dict[str, str]] = []
        degraded = False
        degradation_reason = ''
        started_at = time.monotonic()
        for code in ts_codes:
            if max_runtime_sec > 0 and time.monotonic() - started_at >= max_runtime_sec:
                degraded = True
                degradation_reason = f'batch_prediction_timeout({max_runtime_sec:.0f}s)'
                logger.warning(
                    'Stopping batch prediction early after %.1fs with %d/%d results',
                    time.monotonic() - started_at,
                    len(results),
                    len(ts_codes),
                )
                skipped.append({'ts_code': str(code), 'reason': degradation_reason})
                break
            try:
                results.append(self.run_prediction_pipeline(code))
                if progress_callback is not None:
                    progress_callback(list(results), list(skipped))
            except Exception as e:
                logger.warning('Prediction failed for %s: %s', code, e)
                skipped.append({'ts_code': str(code), 'reason': str(e)})
                if progress_callback is not None:
                    progress_callback(list(results), list(skipped))
        if results:
            self.reporter.generate_signal_report(results)
        logger.info(
            'Batch prediction finished: total_sec=%.2f predict_sec=%.2f results=%d skipped=%d degraded=%s reason=%s',
            time.monotonic() - batch_started_at,
            time.monotonic() - started_at,
            len(results),
            len(skipped),
            degraded,
            degradation_reason or '-',
        )
        return {
            'results': results,
            'skipped': skipped,
            'degraded': degraded,
            'degradation_reason': degradation_reason,
            'champion_profile': self._apply_champion_profile() if getattr(forecast_agent, 'models', {}) else {},
        }

    def run_backtest_pipeline(
        self,
        stock_pool: list[str] | None = None,
        *,
        source_type: str = 'manual',
        generate_artifacts: bool = True,
        data_dict: dict[str, Any] | None = None,
        runtime_cache: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        logger.info('--- Backtest Pipeline ---')
        settings = self.config.get('settings', {})
        data_cfg = settings.get('data', {})
        start_date = data_cfg.get('start_date', '2020-01-01')
        end_date = data_cfg.get('end_date', '2026-12-31')

        if stock_pool is None:
            stock_pool = data_cfg.get('stock_pool', ['000001.SZ'])

        if data_dict is None:
            data_dict = self.data_agent.fetch_pool(stock_pool, start_date, end_date)

        filtered_pool = self._liquidity_prefilter_stock_pool(stock_pool, data_dict)
        stock_pool = [code for code in filtered_pool if code in data_dict]
        data_dict = {code: df for code, df in data_dict.items() if code in stock_pool}

        self._ensure_models_for_universe(stock_pool)

        strategy_runner = StrategyRunner(
            config=self.config,
            feature_agent=self.feature_agent,
            forecast_agent=self.forecast_agent,
            regime_agent=self.regime_agent,
            signal_agent=self.signal_agent,
            risk_agent=self.risk_agent,
            position_agent=self.position_agent,
            data_dict=data_dict,
            feature_cache=(runtime_cache or {}).get('feature_cache'),
            date_index_cache=(runtime_cache or {}).get('date_index_cache'),
            regime_cache=(runtime_cache or {}).get('regime_cache'),
            forecast_cache=(runtime_cache or {}).get('forecast_cache'),
        )

        result = self.backtest_agent.run_backtest(
            stock_pool, start_date, end_date,
            data_dict=data_dict,
            signal_func=strategy_runner.as_signal_func(),
            generate_artifacts=generate_artifacts,
        )
        champion_registry = self.version_manager.load_registry()
        tracker_path = self.tracker.log_backtest_run(
            self.config,
            stock_pool,
            start_date,
            end_date,
            result,
            source_type=source_type,
            metadata={
                'baseline_champion_version': str(champion_registry.get('production_champion_version', '') or champion_registry.get('champion_version', '') or ''),
                **(metadata or {}),
            },
        )
        result['experiment_path'] = tracker_path
        return result

    def run_evolution_pipeline(self, ts_code: str = '000001.SZ') -> dict[str, Any]:
        logger.info('--- Evolution Pipeline ---')
        df = self.data_agent.prepare_dataset(ts_code)
        df = self.feature_agent.build_features(df)
        df, feature_cols, target_col = self.feature_agent.prepare_training_frame(df)

        train_result = self.forecast_agent.train_models(df, feature_cols, target_col)
        eval_results = train_result.get('eval_results', {})

        trainer = self.forecast_agent.trainer
        X_train, X_valid, X_test, y_train, y_valid, y_test = trainer.split_train_valid_test(
            df, feature_cols, target_col
        )
        validation_returns = None
        for return_col in ('label_cross_sectional_excess_return_5', 'label_excess_return_5', 'label_return_5'):
            if return_col in df.columns and len(X_valid):
                validation_returns = df.tail(len(X_test) + len(X_valid)).head(len(X_valid))[return_col].fillna(0.0)
                break

        evo_result = self.evolution_agent.evolve_models(
            self.forecast_agent.models, eval_results,
            X_train, y_train, X_valid, y_valid,
            validation_returns=validation_returns,
        )

        factor_result = self.evolution_agent.evolve_factors(df, feature_cols)
        walk_forward = self._run_evolution_walk_forward(ts_code)
        version_id = pd.Timestamp.now().strftime('evo_%Y%m%d_%H%M%S')
        candidate_basket_feedback = self._load_candidate_basket_feedback()
        candidate_basket_summary = self._load_candidate_basket_summary()
        governance = self.version_manager.evaluate_candidate(
            version_id,
            walk_forward.get('summary', {}),
            candidate_payload={
                'model_evolution': evo_result,
                'factor_evolution': factor_result,
                'walk_forward_evaluation': walk_forward,
                'threshold_profile': self._build_threshold_profile(walk_forward.get('summary', {})),
                'candidate_basket_feedback': candidate_basket_feedback,
                'candidate_basket_feedback_path': "artifacts/primary_result_candidate_baskets/feedback_latest.json",
                'candidate_basket_summary': candidate_basket_summary,
                'candidate_basket_summary_path': "data/experiments/candidates_basket_summary_latest.json",
                'capacity_pressure': (candidate_basket_summary.get('capacity_pressure', {}) or {}),
            },
            min_improvement=float(self.config.get('settings', {}).get('evolution', {}).get('promotion_min_improvement', 0.02) or 0.02),
            min_walk_forward_score=float(self.config.get('settings', {}).get('evolution', {}).get('promotion_min_walk_forward_score', 0.12) or 0.12),
            min_stability=float(self.config.get('settings', {}).get('evolution', {}).get('promotion_min_stability', 0.55) or 0.55),
        )

        result = {
            'model_evolution': evo_result,
            'factor_evolution': factor_result,
            'walk_forward_evaluation': walk_forward,
            'version_governance': governance,
        }
        result['experiment_path'] = self.tracker.log_evolution_run(
            self.config,
            result,
            metadata={
                'candidate_version': version_id,
                'baseline_champion_version': str(governance.get('champion_version', '') or ''),
            },
        )
        return result

    def _resolve_evolution_symbol_pools(self, ts_code: str) -> list[tuple[str, list[str]]]:
        settings = self.config.get('settings', {})
        explicit = [
            str(code).strip()
            for code in ((settings.get('data', {}) or {}).get('stock_pool', []) or [])
            if str(code).strip()
        ]
        focus_pool = [ts_code]
        core_pool = list(dict.fromkeys(explicit + focus_pool))[:8] or focus_pool
        try:
            research_pool, _ = resolve_research_pool_with_meta(str(resolve_project_path('config')))
        except Exception:
            research_pool = []
        diversified_pool = list(dict.fromkeys((research_pool or [])[:12] + core_pool))
        pools = [
            ('focus', focus_pool),
            ('core', core_pool),
            ('research', diversified_pool or core_pool),
        ]
        return [(name, pool) for name, pool in pools if pool]

    def _run_evolution_walk_forward(self, ts_code: str) -> dict[str, Any]:
        pools: list[WalkForwardPool] = []
        for pool_name, symbols in self._resolve_evolution_symbol_pools(ts_code):
            pooled = self._build_pooled_training_frame(symbols, max_symbols=len(symbols))
            if pooled is None:
                continue
            frame, feature_cols, target_col = pooled
            if frame.empty or not feature_cols:
                continue
            pools.append(WalkForwardPool(pool_name, frame, feature_cols, target_col))
        evaluator = WalkForwardEvaluator(
            self.config.get('model_params', {}),
            enable_deep=bool(self.config.get('settings', {}).get('training', {}).get('enable_deep_models', False)),
        )
        return evaluator.evaluate(
            pools,
            folds=int(self.config.get('settings', {}).get('evolution', {}).get('walk_forward_folds', 3) or 3),
        )
