from __future__ import annotations

from datetime import datetime
import subprocess
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.project_paths import resolve_project_path
from src.utils.serialization import save_json


class ExperimentTracker:
    """Persist reproducible train/backtest run summaries."""

    def __init__(self, root_dir: str = 'data/experiments') -> None:
        self.root_dir = resolve_project_path(root_dir)
        self.train_dir = self.root_dir / 'train'
        self.backtest_dir = self.root_dir / 'backtest'
        self.stock_pool_dir = self.root_dir / 'stock_pools'
        self.signal_log_dir = self.root_dir / 'signal_logs'
        self.train_dir.mkdir(parents=True, exist_ok=True)
        self.backtest_dir.mkdir(parents=True, exist_ok=True)
        self.stock_pool_dir.mkdir(parents=True, exist_ok=True)
        self.signal_log_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _now_id() -> str:
        return datetime.now().strftime('%Y%m%d_%H%M%S_%f')

    @staticmethod
    def _snapshot_config(config: dict[str, Any]) -> dict[str, Any]:
        return {
            'settings': config.get('settings', {}),
            'model_params': config.get('model_params', {}),
            'feature_params': config.get('feature_params', {}),
            'signal_rules': config.get('signal_rules', {}),
            'risk_rules': config.get('risk_rules', {}),
            'market_rules': config.get('market_rules', {}),
        }

    @staticmethod
    def _safe_float(v: Any, default: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return default

    @staticmethod
    def _git_commit() -> str:
        try:
            completed = subprocess.run(
                ['git', 'rev-parse', 'HEAD'],
                cwd=str(resolve_project_path('.')),
                capture_output=True,
                text=True,
                check=True,
            )
            return completed.stdout.strip()
        except Exception:
            return ''

    def _write_stock_pool_snapshot(self, run_id: str, stock_pool: list[str]) -> str:
        path = self.stock_pool_dir / f'stock_pool_{run_id}.csv'
        pd.DataFrame({'ts_code': [str(code).strip() for code in stock_pool if str(code).strip()]}).to_csv(path, index=False)
        return str(path)

    def _base_metadata(self, config: dict[str, Any], extra: dict[str, Any] | None = None) -> dict[str, Any]:
        settings = config.get('settings', {}) or {}
        project_cfg = settings.get('project', {}) or {}
        data_cfg = settings.get('data', {}) or {}
        metadata = {
            'git_commit': self._git_commit(),
            'project_version': str(project_cfg.get('version', '') or ''),
            'data_provider': str(data_cfg.get('provider', '') or ''),
            'data_range': {
                'start_date': str(data_cfg.get('start_date', '') or ''),
                'end_date': str(data_cfg.get('end_date', '') or ''),
            },
        }
        if extra:
            metadata.update(extra)
        return metadata

    def _upsert_row(self, csv_path: Path, row: dict[str, Any], sort_by: str = 'run_id') -> None:
        new_df = pd.DataFrame([row])
        if csv_path.exists():
            old_df = pd.read_csv(csv_path)
            df = pd.concat([old_df, new_df], ignore_index=True)
            if 'run_id' in df.columns:
                df = df.drop_duplicates(subset=['run_id'], keep='last')
        else:
            df = new_df
        if sort_by in df.columns:
            df = df.sort_values(sort_by, ascending=False)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(csv_path, index=False)

    @classmethod
    def _rank_backtest_rows(cls, df: pd.DataFrame) -> pd.DataFrame:
        ranked = df.copy()
        for col in ['sharpe_ratio', 'total_return', 'max_drawdown']:
            if col not in ranked.columns:
                ranked[col] = 0.0
            ranked[col] = pd.to_numeric(ranked[col], errors='coerce').fillna(0.0)
        ranked['_drawdown_rank'] = ranked['max_drawdown'].apply(
            lambda value: value if value <= 0 else -value
        )
        ranked = ranked.sort_values(
            ['sharpe_ratio', 'total_return', '_drawdown_rank', 'created_at'],
            ascending=[False, False, False, False],
        )
        return ranked.drop(columns=['_drawdown_rank'])

    def _write_backtest_comparison_report(self, top_n: int = 10) -> str:
        csv_path = self.root_dir / 'backtest_leaderboard.csv'
        out_path = self.root_dir / 'backtest_comparison_latest.md'
        if not csv_path.exists():
            out_path.write_text('# Backtest Comparison\n\nNo backtest leaderboard data yet.\n', encoding='utf-8')
            return str(out_path)

        df = pd.read_csv(csv_path)
        if df.empty:
            out_path.write_text('# Backtest Comparison\n\nBacktest leaderboard is empty.\n', encoding='utf-8')
            return str(out_path)

        cols = [
            'run_id', 'created_at', 'source_type', 'stock_pool', 'start_date', 'end_date', 'total_return',
            'sharpe_ratio', 'max_drawdown', 'total_trades', 'win_rate',
            'rule_block_total', 'report_path',
        ]
        for c in cols:
            if c not in df.columns:
                df[c] = ''
        top = self._rank_backtest_rows(df)[cols].head(top_n).copy()
        lines = [
            '# Backtest Comparison (Latest)',
            f'\nGenerated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
            f'\nShowing latest {min(len(top), top_n)} runs',
            '\n| run_id | source_type | stock_pool | period | total_return | sharpe | max_dd | trades | win_rate | rule_blocks |',
            '|--------|-------------|------------|--------|--------------|--------|--------|--------|----------|-------------|',
        ]
        for _, row in top.iterrows():
            lines.append(
                f'| {row["run_id"]} | {row["source_type"]} | {row["stock_pool"]} | {row["start_date"]}~{row["end_date"]} | '
                f'{self._safe_float(row["total_return"]):.4f} | '
                f'{self._safe_float(row["sharpe_ratio"]):.4f} | {self._safe_float(row["max_drawdown"]):.4f} | '
                f'{int(row["total_trades"]) if str(row["total_trades"]).strip() else 0} | '
                f'{self._safe_float(row["win_rate"]):.4f} | {int(row["rule_block_total"]) if str(row["rule_block_total"]).strip() else 0} |'
            )

        if len(top) >= 2:
            cur = top.iloc[0]
            same_scope = top[
                (top['stock_pool'] == cur['stock_pool'])
                & (top['start_date'] == cur['start_date'])
                & (top['end_date'] == cur['end_date'])
            ]
            prev = same_scope.iloc[1] if len(same_scope) >= 2 else top.iloc[1]
            scope_label = 'same scope' if len(same_scope) >= 2 else 'latest different scope'
            lines.extend([
                f'\n## Delta vs Previous Run ({scope_label})',
                f'- total_return: {self._safe_float(cur["total_return"]) - self._safe_float(prev["total_return"]):+.4f}',
                f'- sharpe_ratio: {self._safe_float(cur["sharpe_ratio"]) - self._safe_float(prev["sharpe_ratio"]):+.4f}',
                f'- max_drawdown: {self._safe_float(cur["max_drawdown"]) - self._safe_float(prev["max_drawdown"]):+.4f}',
                f'- total_trades: {int(self._safe_float(cur["total_trades"]) - self._safe_float(prev["total_trades"])):+d}',
                f'- win_rate: {self._safe_float(cur["win_rate"]) - self._safe_float(prev["win_rate"]):+.4f}',
                f'- rule_block_total: {int(self._safe_float(cur["rule_block_total"]) - self._safe_float(prev["rule_block_total"])):+d}',
            ])

        out_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
        return str(out_path)

    def _write_weekly_brief(self, lookback_days: int = 7, top_n: int = 20) -> str:
        csv_path = self.root_dir / 'backtest_leaderboard.csv'
        out_path = self.root_dir / 'backtest_weekly_brief_latest.md'
        if not csv_path.exists():
            out_path.write_text('# Weekly Backtest Brief\n\nNo backtest data yet.\n', encoding='utf-8')
            return str(out_path)

        df = pd.read_csv(csv_path)
        if df.empty or 'created_at' not in df.columns:
            out_path.write_text('# Weekly Backtest Brief\n\nBacktest leaderboard is empty.\n', encoding='utf-8')
            return str(out_path)

        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        now = pd.Timestamp.now()
        cutoff = now - pd.Timedelta(days=lookback_days)
        recent = df[df['created_at'] >= cutoff].copy().head(top_n)

        lines = [
            '# Weekly Backtest Brief',
            f'\nGenerated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
            f'\nWindow: last {lookback_days} days',
            f'\nRuns included: {len(recent)}',
        ]
        if recent.empty:
            lines.append('\nNo runs found in this time window.')
            out_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
            return str(out_path)

        for col in ['total_return', 'sharpe_ratio', 'max_drawdown', 'total_trades', 'win_rate', 'rule_block_total']:
            if col not in recent.columns:
                recent[col] = 0

        lines.extend([
            '\n## Aggregate',
            f'- Avg total_return: {recent["total_return"].astype(float).mean():.4f}',
            f'- Avg sharpe_ratio: {recent["sharpe_ratio"].astype(float).mean():.4f}',
            f'- Avg max_drawdown: {recent["max_drawdown"].astype(float).mean():.4f}',
            f'- Avg total_trades: {recent["total_trades"].astype(float).mean():.2f}',
            f'- Avg win_rate: {recent["win_rate"].astype(float).mean():.4f}',
            f'- Total rule blocks: {int(recent["rule_block_total"].astype(float).sum())}',
            '\n## Best Run (by total_return)',
        ])
        best = recent.sort_values('total_return', ascending=False).iloc[0]
        lines.extend([
            f'- run_id: {best.get("run_id", "")}',
            f'- source_type: {best.get("source_type", "")}',
            f'- stock_pool: {best.get("stock_pool", "")}',
            f'- period: {best.get("start_date", "")}~{best.get("end_date", "")}',
            f'- total_return: {float(best.get("total_return", 0)):.4f}',
            f'- sharpe_ratio: {float(best.get("sharpe_ratio", 0)):.4f}',
            f'- max_drawdown: {float(best.get("max_drawdown", 0)):.4f}',
            f'- total_trades: {int(float(best.get("total_trades", 0)))}',
        ])

        out_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
        return str(out_path)

    def log_training_run(
        self,
        config: dict[str, Any],
        ts_code: str,
        feature_cols: list[str],
        target_col: str,
        train_result: dict[str, Any],
    ) -> str:
        run_id = self._now_id()
        path = self.train_dir / f'train_{run_id}.json'
        payload = {
            'run_id': run_id,
            'run_type': 'training',
            'created_at': datetime.now().isoformat(),
            'ts_code': ts_code,
            'feature_count': len(feature_cols),
            'feature_cols': feature_cols,
            'target_col': target_col,
            'result': train_result,
            'config_snapshot': self._snapshot_config(config),
        }
        save_json(payload, str(path))
        eval_results = train_result.get('eval_results', {})
        accuracies = [
            self._safe_float(m.get('accuracy'))
            for m in eval_results.values()
            if isinstance(m, dict) and 'accuracy' in m
        ]
        trade_scores = [
            self._safe_float(m.get('trade_objective'))
            for m in eval_results.values()
            if isinstance(m, dict) and 'trade_objective' in m
        ]
        self._upsert_row(
            self.root_dir / 'train_leaderboard.csv',
            {
                'run_id': run_id,
                'created_at': payload['created_at'],
                'ts_code': ts_code,
                'feature_count': len(feature_cols),
                'trained_models': '|'.join(train_result.get('trained_models', [])),
                'test_size': train_result.get('test_size', 0),
                'best_accuracy': max(accuracies) if accuracies else 0.0,
                'avg_accuracy': (sum(accuracies) / len(accuracies)) if accuracies else 0.0,
                'best_trade_objective': max(trade_scores) if trade_scores else 0.0,
                'avg_trade_objective': (sum(trade_scores) / len(trade_scores)) if trade_scores else 0.0,
                'experiment_path': str(path),
            },
        )
        return str(path)

    def log_backtest_run(
        self,
        config: dict[str, Any],
        stock_pool: list[str],
        start_date: str,
        end_date: str,
        result: dict[str, Any],
        *,
        source_type: str = 'manual',
        metadata: dict[str, Any] | None = None,
    ) -> str:
        run_id = self._now_id()
        path = self.backtest_dir / f'backtest_{run_id}.json'
        stock_pool_snapshot_path = self._write_stock_pool_snapshot(run_id, stock_pool)
        payload = {
            'run_id': run_id,
            'run_type': 'backtest',
            'created_at': datetime.now().isoformat(),
            'source_type': source_type,
            'stock_pool': stock_pool,
            'start_date': start_date,
            'end_date': end_date,
            'status': result.get('status'),
            'metrics': result.get('detailed_metrics', result.get('metrics', {})),
            'signal_stats': result.get('signal_stats', {}),
            'rule_block_stats': result.get('rule_block_stats', {}),
            'report_path': result.get('report_path'),
            'chart_paths': result.get('charts', {}),
            'config_snapshot': self._snapshot_config(config),
            'metadata': self._base_metadata(
                config,
                {
                    'stock_pool_snapshot_path': stock_pool_snapshot_path,
                    **(metadata or {}),
                },
            ),
        }
        signal_logs = result.get('signal_logs')
        signal_log_path = ''
        if isinstance(signal_logs, pd.DataFrame) and not signal_logs.empty:
            signal_log_path = str(self.signal_log_dir / f'signal_logs_{run_id}.csv')
            signal_logs.to_csv(signal_log_path, index=False)
        payload['signal_log_path'] = signal_log_path
        save_json(payload, str(path))
        metrics = payload['metrics'] if isinstance(payload.get('metrics'), dict) else {}
        self._upsert_row(
            self.root_dir / 'backtest_leaderboard.csv',
            {
                'run_id': run_id,
                'created_at': payload['created_at'],
                'source_type': source_type,
                'stock_pool': '|'.join(stock_pool),
                'start_date': start_date,
                'end_date': end_date,
                'status': payload['status'],
                'total_return': self._safe_float(metrics.get('total_return')),
                'sharpe_ratio': self._safe_float(metrics.get('sharpe_ratio')),
                'max_drawdown': self._safe_float(metrics.get('max_drawdown')),
                'total_trades': int(metrics.get('total_trades', 0) or 0),
                'win_rate': self._safe_float(metrics.get('win_rate')),
                'rule_block_total': int(sum(payload.get('rule_block_stats', {}).values())),
                'report_path': payload.get('report_path', ''),
                'signal_log_path': signal_log_path,
                'git_commit': str((payload.get('metadata', {}) or {}).get('git_commit', '')),
                'baseline_champion_version': str((payload.get('metadata', {}) or {}).get('baseline_champion_version', '')),
                'sampling_mode': str((payload.get('metadata', {}) or {}).get('sampling_mode', '')),
                'random_seed': str((payload.get('metadata', {}) or {}).get('random_seed', '')),
                'stock_pool_snapshot_path': stock_pool_snapshot_path,
                'experiment_path': str(path),
            },
        )
        comparison_path = self._write_backtest_comparison_report(top_n=10)
        weekly_brief_path = self._write_weekly_brief(lookback_days=7, top_n=20)
        payload['comparison_report_path'] = comparison_path
        payload['weekly_brief_path'] = weekly_brief_path
        save_json(payload, str(path))
        return str(path)

    def log_evolution_run(
        self,
        config: dict[str, Any],
        result: dict[str, Any],
        metadata: dict[str, Any] | None = None,
    ) -> str:
        run_id = self._now_id()
        path = self.root_dir / 'evolution' / f'evolution_{run_id}.json'
        payload = {
            'run_id': run_id,
            'run_type': 'evolution',
            'created_at': datetime.now().isoformat(),
            'result': result,
            'config_snapshot': self._snapshot_config(config),
            'metadata': self._base_metadata(config, metadata),
        }
        save_json(payload, str(path))
        walk_forward = result.get('walk_forward_evaluation', {}) or {}
        wf_summary = walk_forward.get('summary', {}) or {}
        governance = result.get('version_governance', {}) or {}
        model_evo = result.get('model_evolution', {}) or {}
        self._upsert_row(
            self.root_dir / 'evolution_leaderboard.csv',
            {
                'run_id': run_id,
                'created_at': payload['created_at'],
                'selected_models': '|'.join(model_evo.get('selected_models', []) or []),
                'walk_forward_score': self._safe_float(wf_summary.get('walk_forward_score')),
                'trade_objective_mean': self._safe_float(wf_summary.get('trade_objective_mean')),
                'trade_objective_stability': self._safe_float(wf_summary.get('trade_objective_stability')),
                'fold_count': int(self._safe_float(wf_summary.get('fold_count'))),
                'pool_count': int(self._safe_float(wf_summary.get('pool_count'))),
                'governance_action': str(governance.get('action', '')),
                'champion_version': str(governance.get('champion_version', '')),
                'git_commit': str((payload.get('metadata', {}) or {}).get('git_commit', '')),
                'baseline_champion_version': str((payload.get('metadata', {}) or {}).get('baseline_champion_version', '')),
                'candidate_version': str((payload.get('metadata', {}) or {}).get('candidate_version', '')),
                'experiment_path': str(path),
            },
        )
        return str(path)
