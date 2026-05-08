from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from itertools import product
import json
import logging
from pathlib import Path
import random
from tempfile import TemporaryDirectory
from typing import Any, Callable

import pandas as pd
import yaml

from src.pipeline.pipeline_manager import PipelineManager
from src.utils.project_paths import resolve_project_path

logger = logging.getLogger(__name__)


@dataclass
class GridRunResult:
    run_id: str
    params: dict[str, Any]
    metrics: dict[str, Any]
    report_path: str
    experiment_path: str


class GridBacktestRunner:
    """Run parameter-grid backtests and output ranked summaries."""

    def __init__(self, base_config_dir: str = 'config', output_dir: str = 'data/experiments/grid_search') -> None:
        self.base_config_dir = resolve_project_path(base_config_dir)
        self.output_dir = resolve_project_path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _load_yaml(path: Path) -> dict[str, Any]:
        with path.open('r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}

    @staticmethod
    def _save_yaml(path: Path, content: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('w', encoding='utf-8') as f:
            yaml.safe_dump(content, f, allow_unicode=False, sort_keys=False)

    @staticmethod
    def _cartesian_grid(grid: dict[str, list[Any]]) -> list[dict[str, Any]]:
        keys = list(grid.keys())
        values = [grid[k] for k in keys]
        combos = []
        for vals in product(*values):
            combos.append({k: v for k, v in zip(keys, vals)})
        return combos

    @staticmethod
    def _select_combos(
        combos: list[dict[str, Any]],
        max_runs: int | None,
        sampling_mode: str,
        random_seed: int | None,
    ) -> list[tuple[int, dict[str, Any]]]:
        indexed = list(enumerate(combos, start=1))
        if max_runs is None or max_runs >= len(indexed):
            return indexed
        if max_runs <= 0:
            return []
        if sampling_mode == 'stratified':
            rng = random.Random(random_seed)
            return GridBacktestRunner._stratified_sample(indexed, max_runs, rng)
        if sampling_mode == 'random':
            rng = random.Random(random_seed)
            return rng.sample(indexed, max_runs)
        return indexed[:max_runs]

    @staticmethod
    def _stratified_sample(
        indexed: list[tuple[int, dict[str, Any]]],
        max_runs: int,
        rng: random.Random,
    ) -> list[tuple[int, dict[str, Any]]]:
        if max_runs >= len(indexed):
            return list(indexed)

        selected: list[tuple[int, dict[str, Any]]] = []
        selected_ids: set[int] = set()

        keys = list(indexed[0][1].keys()) if indexed else []
        for key in keys:
            value_buckets: dict[str, list[tuple[int, dict[str, Any]]]] = {}
            for combo_id, combo in indexed:
                marker = repr(combo.get(key))
                value_buckets.setdefault(marker, []).append((combo_id, combo))
            for bucket in value_buckets.values():
                if len(selected) >= max_runs:
                    break
                candidate_id, candidate_combo = rng.choice(bucket)
                if candidate_id in selected_ids:
                    continue
                selected.append((candidate_id, candidate_combo))
                selected_ids.add(candidate_id)
            if len(selected) >= max_runs:
                break

        if len(selected) < max_runs:
            remaining = [(cid, cmb) for cid, cmb in indexed if cid not in selected_ids]
            need = max_runs - len(selected)
            selected.extend(rng.sample(remaining, min(need, len(remaining))))

        rng.shuffle(selected)
        return selected[:max_runs]

    @staticmethod
    def _apply_combo(
        settings: dict[str, Any],
        signal_rules: dict[str, Any],
        risk_rules: dict[str, Any],
        combo: dict[str, Any],
        start_date: str | None,
        end_date: str | None,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        s = dict(settings)
        sig = dict(signal_rules)
        risk = dict(risk_rules)
        data_cfg = dict(s.get('data', {}))
        backtest_cfg = dict(s.get('backtest', {}))
        settings_risk_cfg = dict(s.get('risk', {}))
        if start_date:
            data_cfg['start_date'] = start_date
        if end_date:
            data_cfg['end_date'] = end_date
        s['data'] = data_cfg

        for k, v in combo.items():
            if k in {
                'strong_buy_score',
                'buy_score',
                'watch_score',
                'sell_score',
                'buy_position_multiplier',
                'strong_buy_position_multiplier',
                'low_confidence_skip_threshold',
            }:
                sig[k] = v
            elif k in {'commission_rate', 'slippage_rate', 'stamp_tax_rate'}:
                backtest_cfg[k] = v
            elif k in {'stop_loss_pct', 'take_profit_pct', 'max_position_pct'}:
                settings_risk_cfg[k] = v
            else:
                risk[k] = v
        s['backtest'] = backtest_cfg
        s['risk'] = settings_risk_cfg
        return s, sig, risk

    def _write_ranked_outputs(self, rows: list[dict[str, Any]]) -> dict[str, str]:
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_path = self.output_dir / f'grid_backtest_{ts}.csv'
        md_path = self.output_dir / f'grid_backtest_{ts}.md'
        latest_csv = self.output_dir / 'grid_backtest_latest.csv'
        latest_md = self.output_dir / 'grid_backtest_latest.md'

        df = pd.DataFrame(rows)
        if df.empty:
            df.to_csv(csv_path, index=False)
            df.to_csv(latest_csv, index=False)
            md_path.write_text('# Grid Backtest\n\nNo runs executed.\n', encoding='utf-8')
            latest_md.write_text('# Grid Backtest\n\nNo runs executed.\n', encoding='utf-8')
            return {
                'csv_path': str(csv_path),
                'md_path': str(md_path),
                'latest_csv_path': str(latest_csv),
                'latest_md_path': str(latest_md),
            }

        for c in [
            'robustness_score',
            'stability_score',
            'calmar_ratio',
            'total_return',
            'sharpe_ratio',
            'max_drawdown',
            'total_trades',
            'win_rate',
        ]:
            if c not in df.columns:
                df[c] = 0.0
        df = self._rank_dataframe(df)
        df.to_csv(csv_path, index=False)
        df.to_csv(latest_csv, index=False)

        lines = [
            '# Grid Backtest Ranked Results',
            f'\nGenerated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
            f'\nTotal runs: {len(df)}',
            '\n| rank | run_id | robust | stable | sharpe | return | max_dd | trades | win_rate | params |',
            '|------|--------|--------|--------|--------|--------|--------|--------|----------|--------|',
        ]
        for i, row in enumerate(df.itertuples(index=False), start=1):
            lines.append(
                f'| {i} | {row.run_id} | {float(row.robustness_score):.4f} | {float(row.stability_score):.4f} | '
                f'{float(row.sharpe_ratio):.4f} | {float(row.total_return):.4f} | {float(row.max_drawdown):.4f} | '
                f'{int(row.total_trades)} | {float(row.win_rate):.4f} | {row.params} |'
            )
        md_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
        latest_md.write_text('\n'.join(lines) + '\n', encoding='utf-8')
        return {
            'csv_path': str(csv_path),
            'md_path': str(md_path),
            'latest_csv_path': str(latest_csv),
            'latest_md_path': str(latest_md),
        }

    @staticmethod
    def _score_tuple(row: dict[str, Any]) -> tuple[float, float, float, float, float, float]:
        return (
            float(row.get('robustness_score', 0.0)),
            float(row.get('stability_score', 0.0)),
            float(row.get('calmar_ratio', 0.0)),
            float(row.get('sharpe_ratio', 0.0)),
            float(row.get('total_return', 0.0)),
            GridBacktestRunner._drawdown_sort_value(row.get('max_drawdown', 0.0)),
        )

    @staticmethod
    def _drawdown_sort_value(raw_value: Any) -> float:
        try:
            value = float(raw_value)
        except Exception:
            return float('-inf')
        return value if value <= 0 else -value

    @staticmethod
    def _summarize_signal_regimes(signal_logs: pd.DataFrame | None) -> dict[str, Any]:
        if signal_logs is None or signal_logs.empty or 'regime' not in signal_logs.columns:
            return {'dominant_regime': 'unknown', 'regime_signal_counts': {}, 'avg_environment_score': 0.0}
        usable = signal_logs.copy()
        usable['regime'] = usable['regime'].fillna('').astype(str)
        usable = usable[usable['regime'] != '']
        if usable.empty:
            return {'dominant_regime': 'unknown', 'regime_signal_counts': {}, 'avg_environment_score': 0.0}
        counts = usable['regime'].value_counts().to_dict()
        dominant = next(iter(counts.keys())) if counts else 'unknown'
        avg_env = 0.0
        if 'environment_score' in usable.columns:
            avg_env = float(pd.to_numeric(usable['environment_score'], errors='coerce').dropna().mean() or 0.0)
        return {
            'dominant_regime': dominant,
            'regime_signal_counts': counts,
            'avg_environment_score': round(avg_env, 4),
        }

    @classmethod
    def _rank_rows(cls, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(rows, key=cls._score_tuple, reverse=True)

    @classmethod
    def _rank_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        ranked = df.copy()
        ranked['_drawdown_rank'] = ranked['max_drawdown'].apply(cls._drawdown_sort_value)
        ranked = ranked.sort_values(
            ['robustness_score', 'stability_score', 'calmar_ratio', 'sharpe_ratio', 'total_return', '_drawdown_rank'],
            ascending=[False, False, False, False, False, False],
        )
        return ranked.drop(columns=['_drawdown_rank'])

    @staticmethod
    def _should_early_stop(
        current_patience: int,
        patience_limit: int | None,
    ) -> bool:
        if patience_limit is None:
            return False
        return current_patience >= patience_limit

    def _run_single_combo(
        self,
        combo_idx: int,
        combo: dict[str, Any],
        stock_pool: list[str],
        settings: dict[str, Any],
        model_params: dict[str, Any],
        feature_params: dict[str, Any],
        signal_rules: dict[str, Any],
        risk_rules: dict[str, Any],
        market_rules: dict[str, Any],
        start_date: str | None,
        end_date: str | None,
        shared_context: dict[str, Any] | None = None,
        experiment_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with TemporaryDirectory(prefix='grid_cfg_') as tmp:
            cfg_dir = Path(tmp)
            s, sig, risk = self._apply_combo(settings, signal_rules, risk_rules, combo, start_date, end_date)
            self._save_yaml(cfg_dir / 'settings.yaml', s)
            self._save_yaml(cfg_dir / 'model_params.yaml', model_params)
            self._save_yaml(cfg_dir / 'feature_params.yaml', feature_params)
            self._save_yaml(cfg_dir / 'signal_rules.yaml', sig)
            self._save_yaml(cfg_dir / 'risk_rules.yaml', risk)
            self._save_yaml(cfg_dir / 'market_rules.yaml', market_rules)

            pm = PipelineManager(str(cfg_dir))
            if shared_context:
                pm.forecast_agent.models = shared_context.get('models', {})
                pm.forecast_agent.feature_cols = list(shared_context.get('feature_cols', []))
                trainer = shared_context.get('trainer')
                if trainer is not None:
                    pm.forecast_agent.trainer = trainer
            result = pm.run_backtest_pipeline(
                stock_pool,
                source_type='official_research',
                generate_artifacts=True,
                data_dict=shared_context.get('data_dict') if shared_context else None,
                runtime_cache=shared_context.get('runtime_cache') if shared_context else None,
                metadata=experiment_metadata,
            )
            metrics = result.get('detailed_metrics', result.get('metrics', {}))
            rule_block_stats = result.get('rule_block_stats', {}) or {}
            regime_summary = self._summarize_signal_regimes(result.get('signal_logs'))
            run_id = Path(result.get('experiment_path', '')).stem.replace('backtest_', '')
            return {
                'combo_index': combo_idx,
                'run_id': run_id,
                'params': combo,
                'total_return': float(metrics.get('total_return', 0.0)),
                'sharpe_ratio': float(metrics.get('sharpe_ratio', 0.0)),
                'max_drawdown': float(metrics.get('max_drawdown', 0.0)),
                'calmar_ratio': float(metrics.get('calmar_ratio', 0.0)),
                'robustness_score': float(metrics.get('robustness_score', 0.0)),
                'stability_score': float(metrics.get('stability_score', 0.0)),
                'total_trades': int(metrics.get('total_trades', 0) or 0),
                'win_rate': float(metrics.get('win_rate', 0.0)),
                'reward_risk_ratio': float(metrics.get('reward_risk_ratio', 0.0)),
                'avg_holding_days': float(metrics.get('avg_holding_days', 0.0)),
                'median_holding_days': float(metrics.get('median_holding_days', 0.0)),
                'return_mean': float(metrics.get('return_mean', 0.0)),
                'return_median': float(metrics.get('return_median', 0.0)),
                'return_p05': float(metrics.get('return_p05', 0.0)),
                'return_p95': float(metrics.get('return_p95', 0.0)),
                'positive_day_ratio': float(metrics.get('positive_day_ratio', 0.0)),
                'total_commission': float(metrics.get('total_commission', 0.0)),
                'total_slippage_cost': float(metrics.get('total_slippage_cost', 0.0)),
                'low_liquidity_blocks': int(rule_block_stats.get('low_liquidity', 0) or 0),
                'dominant_regime': regime_summary.get('dominant_regime', 'unknown'),
                'avg_environment_score': float(regime_summary.get('avg_environment_score', 0.0)),
                'regime_signal_counts': regime_summary.get('regime_signal_counts', {}),
                'report_path': result.get('report_path', ''),
                'experiment_path': result.get('experiment_path', ''),
            }

    def _write_regime_ranked_outputs(self, rows: list[dict[str, Any]]) -> dict[str, str]:
        if not rows:
            return {}
        df = pd.DataFrame(rows)
        if df.empty or 'dominant_regime' not in df.columns:
            return {}
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        md_path = self.output_dir / f'grid_backtest_by_regime_{ts}.md'
        latest_md = self.output_dir / 'grid_backtest_by_regime_latest.md'
        json_path = self.output_dir / f'grid_backtest_regime_profiles_{ts}.json'
        latest_json = self.output_dir / 'grid_backtest_regime_profiles_latest.json'
        lines = [
            '# Grid Backtest By Regime',
            f'\nGenerated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        ]
        profiles: dict[str, Any] = {}
        for regime, group in df.groupby('dominant_regime', dropna=False):
            ranked = self._rank_dataframe(group.copy()).head(5)
            lines.append(f'\n## Regime: {regime}')
            lines.append('\n| rank | run_id | robust | stable | sharpe | return | max_dd | avg_env | params |')
            lines.append('|------|--------|--------|--------|--------|--------|--------|---------|--------|')
            for i, row in enumerate(ranked.itertuples(index=False), start=1):
                lines.append(
                    f'| {i} | {row.run_id} | {float(row.robustness_score):.4f} | {float(row.stability_score):.4f} | '
                    f'{float(row.sharpe_ratio):.4f} | {float(row.total_return):.4f} | {float(row.max_drawdown):.4f} | '
                    f'{float(row.avg_environment_score):.4f} | {row.params} |'
                )
            if not ranked.empty:
                top_row = ranked.iloc[0].to_dict()
                profiles[str(regime)] = {
                    'run_id': top_row.get('run_id', ''),
                    'params': top_row.get('params', {}),
                    'robustness_score': float(top_row.get('robustness_score', 0.0) or 0.0),
                    'stability_score': float(top_row.get('stability_score', 0.0) or 0.0),
                    'avg_environment_score': float(top_row.get('avg_environment_score', 0.0) or 0.0),
                }
        content = '\n'.join(lines) + '\n'
        md_path.write_text(content, encoding='utf-8')
        latest_md.write_text(content, encoding='utf-8')
        json_path.write_text(json.dumps(profiles, ensure_ascii=False, indent=2), encoding='utf-8')
        latest_json.write_text(json.dumps(profiles, ensure_ascii=False, indent=2), encoding='utf-8')
        return {
            'regime_md_path': str(md_path),
            'latest_regime_md_path': str(latest_md),
            'regime_json_path': str(json_path),
            'latest_regime_json_path': str(latest_json),
        }

    @staticmethod
    def _parameter_sensitivity_score(rows: list[dict[str, Any]]) -> float:
        if not rows:
            return 1.0
        ranked = GridBacktestRunner._rank_rows(rows)[: min(10, len(rows))]
        robust_scores = [float(row.get('robustness_score', 0.0) or 0.0) for row in ranked]
        top_score = max(robust_scores) if robust_scores else 0.0
        bottom_score = min(robust_scores) if robust_scores else 0.0
        score_dispersion = 0.0 if top_score <= 0 else min(max((top_score - bottom_score) / max(abs(top_score), 1.0), 0.0), 1.0)
        param_keys = sorted({key for row in ranked for key in (row.get('params', {}) or {}).keys()})
        per_key_dispersion: list[float] = []
        for key in param_keys:
            values = {json.dumps((row.get('params', {}) or {}).get(key), ensure_ascii=False, sort_keys=True) for row in ranked}
            per_key_dispersion.append((len(values) - 1) / max(len(ranked) - 1, 1))
        value_dispersion = sum(per_key_dispersion) / len(per_key_dispersion) if per_key_dispersion else 0.0
        return round(min(score_dispersion * 0.45 + value_dispersion * 0.55, 1.0), 4)

    @staticmethod
    def _regime_coverage_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
        observed = sorted(
            {
                str(row.get('dominant_regime', '') or '').strip()
                for row in rows
                if str(row.get('dominant_regime', '') or '').strip() and str(row.get('dominant_regime', '') or '').strip() != 'unknown'
            }
        )
        score = round(min(len(observed) / 4.0, 1.0), 4) if observed else 0.0
        avg_env = round(
            sum(float(row.get('avg_environment_score', 0.0) or 0.0) for row in rows) / max(len(rows), 1),
            4,
        ) if rows else 0.0
        return {
            'observed_regimes': observed,
            'regime_coverage_score': score,
            'avg_environment_score': avg_env,
        }

    @staticmethod
    def _replay_validation_summary(replay_rows: list[dict[str, Any]]) -> dict[str, Any]:
        if not replay_rows:
            return {
                'run_count': 0,
                'avg_robustness_score': 0.0,
                'avg_stability_score': 0.0,
                'avg_total_return': 0.0,
                'positive_return_ratio': 0.0,
            }
        return {
            'run_count': len(replay_rows),
            'avg_robustness_score': round(sum(float(row.get('robustness_score', 0.0) or 0.0) for row in replay_rows) / len(replay_rows), 4),
            'avg_stability_score': round(sum(float(row.get('stability_score', 0.0) or 0.0) for row in replay_rows) / len(replay_rows), 4),
            'avg_total_return': round(sum(float(row.get('total_return', 0.0) or 0.0) for row in replay_rows) / len(replay_rows), 4),
            'positive_return_ratio': round(sum(1 for row in replay_rows if float(row.get('total_return', 0.0) or 0.0) > 0) / len(replay_rows), 4),
        }

    def _write_governance_outputs(
        self,
        *,
        rows: list[dict[str, Any]],
        replay_rows: list[dict[str, Any]],
        start_date: str | None,
        end_date: str | None,
        replay_start_date: str | None,
        replay_end_date: str | None,
        sampling_mode: str,
        random_seed: int | None,
        planned_runs: int,
        early_stopped: bool,
    ) -> dict[str, str]:
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_path = self.output_dir / f'grid_backtest_governance_{ts}.json'
        latest_json = self.output_dir / 'grid_backtest_governance_latest.json'
        regime = self._regime_coverage_summary(rows)
        replay = self._replay_validation_summary(replay_rows)
        payload = {
            'generated_at': datetime.now().isoformat(),
            'search_window': {
                'start_date': start_date,
                'end_date': end_date,
            },
            'validation_window': {
                'start_date': replay_start_date,
                'end_date': replay_end_date,
            } if replay_rows else {},
            'sampling_mode': sampling_mode,
            'random_seed': random_seed,
            'planned_runs': planned_runs,
            'executed_runs': len(rows),
            'replay_runs': len(replay_rows),
            'early_stopped': bool(early_stopped),
            'parameter_sensitivity_score': self._parameter_sensitivity_score(rows),
            'regime_coverage_score': regime['regime_coverage_score'],
            'observed_regimes': regime['observed_regimes'],
            'avg_environment_score': regime['avg_environment_score'],
            'replay_validation': replay,
            'top_result': self._rank_rows(rows)[0] if rows else {},
        }
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        latest_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return {
            'governance_json_path': str(json_path),
            'latest_governance_json_path': str(latest_json),
        }

    def _build_shared_context(
        self,
        stock_pool: list[str],
        settings: dict[str, Any],
        model_params: dict[str, Any],
        feature_params: dict[str, Any],
        signal_rules: dict[str, Any],
        risk_rules: dict[str, Any],
        market_rules: dict[str, Any],
        start_date: str | None,
        end_date: str | None,
    ) -> dict[str, Any]:
        with TemporaryDirectory(prefix='grid_train_cfg_') as tmp:
            cfg_dir = Path(tmp)
            s, sig, risk = self._apply_combo(settings, signal_rules, risk_rules, {}, start_date, end_date)
            self._save_yaml(cfg_dir / 'settings.yaml', s)
            self._save_yaml(cfg_dir / 'model_params.yaml', model_params)
            self._save_yaml(cfg_dir / 'feature_params.yaml', feature_params)
            self._save_yaml(cfg_dir / 'signal_rules.yaml', sig)
            self._save_yaml(cfg_dir / 'risk_rules.yaml', risk)
            self._save_yaml(cfg_dir / 'market_rules.yaml', market_rules)

            pm = PipelineManager(str(cfg_dir))
            data_cfg = s.get('data', {})
            resolved_start = data_cfg.get('start_date', '2020-01-01')
            resolved_end = data_cfg.get('end_date', '2026-12-31')
            data_dict = pm.data_agent.fetch_pool(stock_pool, resolved_start, resolved_end)
            feature_cache: dict[str, pd.DataFrame] = {}
            date_index_cache: dict[str, dict[str, int]] = {}
            for code, raw in data_dict.items():
                if raw is None or raw.empty:
                    continue
                full = pm.feature_agent.build_features(raw.copy()).reset_index(drop=True)
                feature_cache[str(code)] = full
                date_index_cache[str(code)] = {d: i for i, d in enumerate(full['date'].astype(str).tolist())}
            pooled_frames: list[pd.DataFrame] = []
            shared_feature_cols: list[str] | None = None
            target_col = 'label_direction_5'
            for code in stock_pool:
                featured = feature_cache.get(code)
                if featured is None or featured.empty:
                    continue
                frame, feature_cols, target_col = pm.feature_agent.prepare_training_frame(featured)
                if frame.empty or not feature_cols:
                    continue
                pooled_frames.append(frame.copy())
                if shared_feature_cols is None:
                    shared_feature_cols = list(feature_cols)
                else:
                    shared_feature_cols = [col for col in shared_feature_cols if col in feature_cols]
            if pooled_frames and shared_feature_cols and not pm.forecast_agent.models:
                pooled = pd.concat(pooled_frames, ignore_index=True)
                pm.forecast_agent.train_models(pooled, shared_feature_cols, target_col)
            return {
                'data_dict': data_dict,
                'models': pm.forecast_agent.models,
                'feature_cols': list(pm.forecast_agent.feature_cols),
                'trainer': pm.forecast_agent.trainer,
                'runtime_cache': {
                    'feature_cache': feature_cache,
                    'date_index_cache': date_index_cache,
                    'regime_cache': {},
                    'forecast_cache': {},
                },
            }

    def run(
        self,
        stock_pool: list[str],
        grid: dict[str, list[Any]],
        start_date: str | None = None,
        end_date: str | None = None,
        max_runs: int | None = None,
        sampling_mode: str = 'random',
        random_seed: int | None = 42,
        batch_size: int = 4,
        early_stop_patience: int | None = None,
        min_improve: float = 0.0,
        replay_top_k: int = 0,
        replay_start_date: str | None = None,
        replay_end_date: str | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        settings = self._load_yaml(self.base_config_dir / 'settings.yaml')
        model_params = self._load_yaml(self.base_config_dir / 'model_params.yaml')
        feature_params = self._load_yaml(self.base_config_dir / 'feature_params.yaml')
        signal_rules = self._load_yaml(self.base_config_dir / 'signal_rules.yaml')
        risk_rules = self._load_yaml(self.base_config_dir / 'risk_rules.yaml')
        market_rules = self._load_yaml(self.base_config_dir / 'market_rules.yaml')

        all_combos = self._cartesian_grid(grid)
        selected = self._select_combos(all_combos, max_runs, sampling_mode, random_seed)
        combos = [combo for _, combo in selected]
        combo_ids = [combo_id for combo_id, _ in selected]
        if batch_size <= 0:
            batch_size = 1

        rows: list[dict[str, Any]] = []
        best_robustness = float('-inf')
        patience_counter = 0
        shared_context = self._build_shared_context(
            stock_pool=stock_pool,
            settings=settings,
            model_params=model_params,
            feature_params=feature_params,
            signal_rules=signal_rules,
            risk_rules=risk_rules,
            market_rules=market_rules,
            start_date=start_date,
            end_date=end_date,
        )
        if progress_callback:
            progress_callback(
                {
                    'phase': 'grid_initialized',
                    'planned_runs': len(combos),
                    'total_combinations': len(all_combos),
                    'sampling_mode': sampling_mode,
                    'random_seed': random_seed,
                    'executed_runs': 0,
                    'early_stop_patience': early_stop_patience,
                    'batch_size': batch_size,
                }
            )

        for batch_start in range(0, len(combos), batch_size):
            batch = combos[batch_start: batch_start + batch_size]
            batch_no = batch_start // batch_size + 1
            logger.info('Grid batch %d: running %d combos', batch_no, len(batch))
            if progress_callback:
                progress_callback(
                    {
                        'phase': 'grid_batch_start',
                        'batch_no': batch_no,
                        'batch_size': len(batch),
                        'planned_runs': len(combos),
                        'total_combinations': len(all_combos),
                        'executed_runs': len(rows),
                    }
                )
            for offset, combo in enumerate(batch, start=1):
                idx = batch_start + offset
                source_combo_id = combo_ids[idx - 1]
                row = self._run_single_combo(
                    combo_idx=source_combo_id,
                    combo=combo,
                    stock_pool=stock_pool,
                    settings=settings,
                    model_params=model_params,
                    feature_params=feature_params,
                    signal_rules=signal_rules,
                    risk_rules=risk_rules,
                    market_rules=market_rules,
                    start_date=start_date,
                    end_date=end_date,
                    shared_context=shared_context,
                    experiment_metadata={
                        'sampling_mode': sampling_mode,
                        'random_seed': random_seed,
                        'combo_index': source_combo_id,
                    },
                )
                rows.append(row)

                robustness = float(row.get('robustness_score', 0.0))
                sharpe = float(row.get('sharpe_ratio', 0.0))
                if robustness > best_robustness + float(min_improve):
                    best_robustness = robustness
                    patience_counter = 0
                else:
                    patience_counter += 1

                logger.info(
                    'Grid run %d/%d (combo#%d) done: robust=%.4f sharpe=%.4f return=%.4f params=%s',
                    idx,
                    len(combos),
                    source_combo_id,
                    robustness,
                    sharpe,
                    float(row.get('total_return', 0.0)),
                    combo,
                )
                if progress_callback:
                    progress_callback(
                        {
                            'phase': 'grid_run_done',
                            'planned_runs': len(combos),
                            'total_combinations': len(all_combos),
                            'executed_runs': len(rows),
                            'current_run': idx,
                            'source_combo_id': source_combo_id,
                            'batch_no': batch_no,
                            'params': combo,
                            'robustness_score': robustness,
                            'sharpe_ratio': sharpe,
                            'total_return': float(row.get('total_return', 0.0)),
                            'best_robustness': best_robustness,
                            'patience_counter': patience_counter,
                        }
                    )

                if self._should_early_stop(patience_counter, early_stop_patience):
                    logger.info('Early stop triggered after %d non-improving runs', patience_counter)
                    if progress_callback:
                        progress_callback(
                            {
                                'phase': 'grid_early_stop',
                                'planned_runs': len(combos),
                                'executed_runs': len(rows),
                                'patience_counter': patience_counter,
                            }
                        )
                    break

            # checkpoint output for monitoring partial progress
            self._write_ranked_outputs(rows)
            if self._should_early_stop(patience_counter, early_stop_patience):
                break

        out_paths = self._write_ranked_outputs(rows)
        out_paths.update(self._write_regime_ranked_outputs(rows))
        top = self._rank_rows(rows)
        replay_rows: list[dict[str, Any]] = []
        if replay_top_k > 0 and top:
            k = min(replay_top_k, len(top))
            replay_start = replay_start_date or start_date
            replay_end = replay_end_date or end_date
            logger.info('Running replay validation for top %d configs', k)
            for i, item in enumerate(top[:k], start=1):
                combo = item['params']
                replay_row = self._run_single_combo(
                    combo_idx=i,
                    combo=combo,
                    stock_pool=stock_pool,
                    settings=settings,
                    model_params=model_params,
                    feature_params=feature_params,
                    signal_rules=signal_rules,
                    risk_rules=risk_rules,
                    market_rules=market_rules,
                    start_date=replay_start,
                    end_date=replay_end,
                    shared_context=(
                        shared_context
                        if replay_start == start_date and replay_end == end_date
                        else self._build_shared_context(
                            stock_pool=stock_pool,
                            settings=settings,
                            model_params=model_params,
                            feature_params=feature_params,
                            signal_rules=signal_rules,
                            risk_rules=risk_rules,
                            market_rules=market_rules,
                            start_date=replay_start,
                            end_date=replay_end,
                        )
                    ),
                    experiment_metadata={
                        'sampling_mode': sampling_mode,
                        'random_seed': random_seed,
                        'combo_index': i,
                        'replay_validation': True,
                        'validation_window': {
                            'start_date': replay_start,
                            'end_date': replay_end,
                        },
                    },
                )
                replay_rows.append(replay_row)

            if replay_rows:
                replay_df = self._rank_dataframe(pd.DataFrame(replay_rows))
                replay_csv = self.output_dir / 'grid_backtest_replay_latest.csv'
                replay_md = self.output_dir / 'grid_backtest_replay_latest.md'
                replay_df.to_csv(replay_csv, index=False)
                lines = [
                    '# Grid Backtest Replay Validation',
                    f'\nGenerated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
                    f'\nReplay period: {replay_start or "default"} ~ {replay_end or "default"}',
                    '\n| rank | run_id | robust | stable | sharpe | return | max_dd | trades | params |',
                    '|------|--------|--------|--------|--------|--------|--------|--------|--------|',
                ]
                for i, row in enumerate(replay_df.itertuples(index=False), start=1):
                    lines.append(
                        f'| {i} | {row.run_id} | {float(row.robustness_score):.4f} | {float(row.stability_score):.4f} | '
                        f'{float(row.sharpe_ratio):.4f} | {float(row.total_return):.4f} | '
                        f'{float(row.max_drawdown):.4f} | {int(row.total_trades)} | {row.params} |'
                    )
                replay_md.write_text('\n'.join(lines) + '\n', encoding='utf-8')

        governance_paths = self._write_governance_outputs(
            rows=rows,
            replay_rows=replay_rows,
            start_date=start_date,
            end_date=end_date,
            replay_start_date=replay_start_date or start_date,
            replay_end_date=replay_end_date or end_date,
            sampling_mode=sampling_mode,
            random_seed=random_seed,
            planned_runs=len(combos),
            early_stopped=self._should_early_stop(patience_counter, early_stop_patience),
        )
        result = {
            'total_runs': len(rows),
            'top_result': top[0] if top else {},
            'results': top,
            'early_stopped': self._should_early_stop(patience_counter, early_stop_patience),
            'executed_runs': len(rows),
            'planned_runs': len(combos),
            'total_combinations': len(all_combos),
            'sampling_mode': sampling_mode,
            'random_seed': random_seed,
            'replay_results': replay_rows,
            **out_paths,
            **governance_paths,
        }
        if replay_rows:
            result['replay_csv_path'] = str(self.output_dir / 'grid_backtest_replay_latest.csv')
            result['replay_md_path'] = str(self.output_dir / 'grid_backtest_replay_latest.md')
        if progress_callback:
            progress_callback(
                {
                    'phase': 'grid_completed',
                    'planned_runs': len(combos),
                    'total_combinations': len(all_combos),
                    'executed_runs': len(rows),
                    'replay_runs': len(replay_rows),
                    'early_stopped': result['early_stopped'],
                }
            )
        return result
