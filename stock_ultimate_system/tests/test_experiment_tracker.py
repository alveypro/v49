from pathlib import Path
import json

import pandas as pd

from src.utils.experiment_tracker import ExperimentTracker


def test_experiment_tracker_writes_json_and_leaderboards(tmp_path):
    tracker = ExperimentTracker(root_dir=str(tmp_path / 'exp'))
    config = {
        'settings': {'project': {'name': 'x'}},
        'model_params': {},
        'feature_params': {},
        'signal_rules': {},
        'risk_rules': {},
        'market_rules': {},
    }

    train_path = tracker.log_training_run(
        config=config,
        ts_code='000001.SZ',
        feature_cols=['f1', 'f2'],
        target_col='y',
        train_result={
            'trained_models': ['m1', 'm2'],
            'test_size': 10,
            'eval_results': {
                'm1': {'accuracy': 0.6, 'trade_objective': 0.12},
                'm2': {'accuracy': 0.8, 'trade_objective': 0.22},
            },
        },
    )
    backtest_path = tracker.log_backtest_run(
        config=config,
        stock_pool=['000001.SZ'],
        start_date='2026-01-01',
        end_date='2026-01-31',
        result={
            'status': 'ok',
            'metrics': {'total_return': 0.1, 'sharpe_ratio': 1.2, 'max_drawdown': -0.05, 'total_trades': 3, 'win_rate': 0.66},
            'rule_block_stats': {'limit_up': 2},
            'report_path': 'r.md',
            'charts': {'equity_curve': 'eq.png'},
            'signal_logs': pd.DataFrame([{'date': '2026-01-02', 'status': 'filled'}]),
        },
        source_type='official_research',
        metadata={
            'baseline_champion_version': 'prod_v1',
            'sampling_mode': 'stratified',
            'random_seed': 42,
        },
    )

    assert Path(train_path).exists()
    assert Path(backtest_path).exists()
    assert (tmp_path / 'exp' / 'train_leaderboard.csv').exists()
    assert (tmp_path / 'exp' / 'backtest_leaderboard.csv').exists()
    assert (tmp_path / 'exp' / 'backtest_comparison_latest.md').exists()
    assert (tmp_path / 'exp' / 'backtest_weekly_brief_latest.md').exists()
    assert any((tmp_path / 'exp' / 'signal_logs').glob('signal_logs_*.csv'))
    leaderboard = pd.read_csv(tmp_path / 'exp' / 'backtest_leaderboard.csv')
    assert leaderboard.iloc[0]['source_type'] == 'official_research'
    assert leaderboard.iloc[0]['baseline_champion_version'] == 'prod_v1'
    assert str(leaderboard.iloc[0]['random_seed']) == '42'
    assert Path(leaderboard.iloc[0]['stock_pool_snapshot_path']).exists()
    payload = json.loads(Path(backtest_path).read_text(encoding='utf-8'))
    assert payload['metadata']['sampling_mode'] == 'stratified'
    train_leaderboard = pd.read_csv(tmp_path / 'exp' / 'train_leaderboard.csv')
    assert float(train_leaderboard.iloc[0]['best_trade_objective']) == 0.22


def test_rank_backtest_rows_prefers_smaller_drawdown():
    df = pd.DataFrame([
        {'run_id': 'a', 'created_at': '2026-03-18T10:00:00', 'sharpe_ratio': 1.2, 'total_return': 0.1, 'max_drawdown': -0.20},
        {'run_id': 'b', 'created_at': '2026-03-18T10:01:00', 'sharpe_ratio': 1.2, 'total_return': 0.1, 'max_drawdown': -0.05},
    ])
    ranked = ExperimentTracker._rank_backtest_rows(df)
    assert ranked.iloc[0]['run_id'] == 'b'


def test_experiment_tracker_logs_evolution_runs(tmp_path):
    tracker = ExperimentTracker(root_dir=str(tmp_path / 'exp'))
    config = {
        'settings': {'project': {'name': 'x'}},
        'model_params': {},
        'feature_params': {},
        'signal_rules': {},
        'risk_rules': {},
        'market_rules': {},
    }
    path = tracker.log_evolution_run(
        config,
        {
            'model_evolution': {'selected_models': ['logistic', 'random_forest']},
            'walk_forward_evaluation': {
                'summary': {
                    'walk_forward_score': 0.23,
                    'trade_objective_mean': 0.18,
                    'trade_objective_stability': 0.71,
                    'fold_count': 3,
                    'pool_count': 2,
                }
            },
            'version_governance': {'action': 'promote_to_staging', 'champion_version': 'evo_1'},
        },
        metadata={'candidate_version': 'evo_candidate', 'baseline_champion_version': 'prod_v1'},
    )
    assert Path(path).exists()
    leaderboard = pd.read_csv(tmp_path / 'exp' / 'evolution_leaderboard.csv')
    assert leaderboard.iloc[0]['governance_action'] == 'promote_to_staging'
    assert float(leaderboard.iloc[0]['walk_forward_score']) == 0.23
    assert leaderboard.iloc[0]['candidate_version'] == 'evo_candidate'
    assert leaderboard.iloc[0]['baseline_champion_version'] == 'prod_v1'
