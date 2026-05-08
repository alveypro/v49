from types import SimpleNamespace

import pandas as pd
import pytest

from src.evolution.grid_backtest_runner import GridBacktestRunner
from run_grid_backtest import build_grid, resolve_experiment, resolve_profile_args, validate_replay_window


def test_cartesian_grid_builds_all_combinations():
    grid = {
        'buy_score': [34, 38],
        'strong_buy_score': [46, 50],
        'range_confidence_min': [0.0, 0.1],
    }
    combos = GridBacktestRunner._cartesian_grid(grid)
    assert len(combos) == 8
    assert {'buy_score': 34, 'strong_buy_score': 46, 'range_confidence_min': 0.0} in combos
    assert {'buy_score': 38, 'strong_buy_score': 50, 'range_confidence_min': 0.1} in combos


def test_early_stop_guard():
    assert not GridBacktestRunner._should_early_stop(0, None)
    assert not GridBacktestRunner._should_early_stop(1, 2)
    assert GridBacktestRunner._should_early_stop(2, 2)


def test_apply_combo_routes_cost_and_risk_settings():
    settings = {'data': {}, 'backtest': {}, 'risk': {}}
    signal_rules = {}
    risk_rules = {}
    combo = {
        'buy_score': 34,
        'commission_rate': 0.0005,
        'slippage_rate': 0.001,
        'stop_loss_pct': 0.04,
        'range_confidence_min': 0.0,
        'atr_stop_loss_multiplier': 1.5,
    }
    s, sig, risk = GridBacktestRunner._apply_combo(
        settings, signal_rules, risk_rules, combo, '2025-01-01', '2025-06-01'
    )
    assert sig['buy_score'] == 34
    assert s['backtest']['commission_rate'] == 0.0005
    assert s['backtest']['slippage_rate'] == 0.001
    assert s['risk']['stop_loss_pct'] == 0.04
    assert risk['range_confidence_min'] == 0.0
    assert risk['atr_stop_loss_multiplier'] == 1.5
    assert s['data']['start_date'] == '2025-01-01'
    assert s['data']['end_date'] == '2025-06-01'


def test_build_grid_presets():
    assert len(build_grid('small')) > 0
    assert len(build_grid('medium')) >= len(build_grid('small'))
    assert len(build_grid('large', search_mode='broad')) >= len(build_grid('medium'))
    assert 'atr_stop_loss_multiplier' in build_grid('large')


def test_resolve_experiment_preset():
    args = SimpleNamespace(experiment='direction_a_medium')
    preset = resolve_experiment(args)
    assert preset is not None
    assert preset['grid']['strong_buy_score'] == [48, 50, 52]
    assert preset['grid']['liquidity_min_turnover'] == [1_000_000, 1_200_000]


def test_profile_resolution_allows_override():
    args = SimpleNamespace(
        profile='short',
        grid_size=None,
        max_runs=None,
        batch_size=None,
        early_stop_patience=None,
        min_improve=None,
        replay_top_k=None,
        start_date=None,
        end_date=None,
        replay_start_date=None,
        replay_end_date=None,
    )
    cfg = resolve_profile_args(args)
    assert cfg['grid_size'] == 'small'
    assert cfg['max_runs'] == 8
    args.grid_size = 'large'
    args.max_runs = 5
    cfg2 = resolve_profile_args(args)
    assert cfg2['grid_size'] == 'large'
    assert cfg2['max_runs'] == 5


def test_profile_resolution_medium_requires_independent_validation_window():
    args = SimpleNamespace(
        profile='medium',
        grid_size=None,
        max_runs=None,
        batch_size=None,
        early_stop_patience=None,
        min_improve=None,
        replay_top_k=None,
        start_date=None,
        end_date=None,
        replay_start_date=None,
        replay_end_date=None,
        sampling_mode=None,
        random_seed=None,
    )
    cfg = resolve_profile_args(args)
    assert cfg['require_validation_window'] is True
    assert cfg['start_date'] == '2024-01-01'
    assert cfg['end_date'] == '2024-12-31'
    assert cfg['replay_start_date'] == '2025-01-01'
    assert cfg['replay_end_date'] == '2025-06-30'


def test_validate_replay_window_rejects_overlap():
    with pytest.raises(ValueError, match='overlaps search window'):
        validate_replay_window(
            {
                'profile': 'medium',
                'replay_top_k': 3,
                'require_validation_window': True,
                'start_date': '2024-01-01',
                'end_date': '2024-12-31',
                'replay_start_date': '2024-10-01',
                'replay_end_date': '2025-02-01',
            },
            profile='medium',
        )


def test_rank_rows_prefers_smaller_positive_drawdown():
    rows = [
        {'run_id': 'a', 'robustness_score': 10.0, 'stability_score': 0.6, 'calmar_ratio': 1.0, 'sharpe_ratio': 1.2, 'total_return': 0.1, 'max_drawdown': 0.20},
        {'run_id': 'b', 'robustness_score': 11.0, 'stability_score': 0.7, 'calmar_ratio': 1.1, 'sharpe_ratio': 1.2, 'total_return': 0.1, 'max_drawdown': 0.05},
    ]
    ranked = GridBacktestRunner._rank_rows(rows)
    assert ranked[0]['run_id'] == 'b'


def test_rank_dataframe_prefers_smaller_negative_drawdown():
    df = pd.DataFrame([
        {'run_id': 'a', 'robustness_score': 8.0, 'stability_score': 0.4, 'calmar_ratio': 0.8, 'sharpe_ratio': 1.2, 'total_return': 0.1, 'max_drawdown': -0.20},
        {'run_id': 'b', 'robustness_score': 9.5, 'stability_score': 0.5, 'calmar_ratio': 0.9, 'sharpe_ratio': 1.2, 'total_return': 0.1, 'max_drawdown': -0.05},
    ])
    ranked = GridBacktestRunner._rank_dataframe(df)
    assert ranked.iloc[0]['run_id'] == 'b'


def test_rank_dataframe_prefers_higher_robustness_over_raw_sharpe():
    df = pd.DataFrame([
        {'run_id': 'a', 'robustness_score': 18.0, 'stability_score': 0.72, 'calmar_ratio': 1.2, 'sharpe_ratio': 1.1, 'total_return': 0.12, 'max_drawdown': -0.06},
        {'run_id': 'b', 'robustness_score': 12.0, 'stability_score': 0.40, 'calmar_ratio': 0.7, 'sharpe_ratio': 1.4, 'total_return': 0.15, 'max_drawdown': -0.12},
    ])
    ranked = GridBacktestRunner._rank_dataframe(df)
    assert ranked.iloc[0]['run_id'] == 'a'


def test_summarize_signal_regimes_detects_dominant_regime():
    signal_logs = pd.DataFrame([
        {'regime': 'trend', 'environment_score': 0.8},
        {'regime': 'trend', 'environment_score': 0.7},
        {'regime': 'range', 'environment_score': 0.5},
    ])

    summary = GridBacktestRunner._summarize_signal_regimes(signal_logs)

    assert summary['dominant_regime'] == 'trend'
    assert summary['regime_signal_counts']['trend'] == 2
    assert summary['avg_environment_score'] > 0.6


def test_governance_helpers_summarize_regime_and_sensitivity():
    rows = [
        {'run_id': 'a', 'dominant_regime': 'trend', 'avg_environment_score': 0.8, 'robustness_score': 0.22, 'params': {'buy_score': 34, 'stop_loss_pct': 0.05}},
        {'run_id': 'b', 'dominant_regime': 'range', 'avg_environment_score': 0.5, 'robustness_score': 0.19, 'params': {'buy_score': 36, 'stop_loss_pct': 0.05}},
        {'run_id': 'c', 'dominant_regime': 'volatile', 'avg_environment_score': 0.4, 'robustness_score': 0.17, 'params': {'buy_score': 34, 'stop_loss_pct': 0.04}},
    ]

    regime = GridBacktestRunner._regime_coverage_summary(rows)
    sensitivity = GridBacktestRunner._parameter_sensitivity_score(rows)

    assert regime['observed_regimes'] == ['range', 'trend', 'volatile']
    assert regime['regime_coverage_score'] == 0.75
    assert 0.0 <= sensitivity <= 1.0


def test_select_combos_random_is_reproducible():
    combos = [{'x': i} for i in range(10)]
    sampled_a = GridBacktestRunner._select_combos(combos, max_runs=4, sampling_mode='random', random_seed=42)
    sampled_b = GridBacktestRunner._select_combos(combos, max_runs=4, sampling_mode='random', random_seed=42)
    sampled_c = GridBacktestRunner._select_combos(combos, max_runs=4, sampling_mode='random', random_seed=7)
    assert sampled_a == sampled_b
    assert sampled_a != sampled_c


def test_select_combos_stratified_covers_key_values():
    combos = [
        {'buy_score': 34, 'range_confidence_min': 0.05},
        {'buy_score': 34, 'range_confidence_min': 0.10},
        {'buy_score': 36, 'range_confidence_min': 0.05},
        {'buy_score': 36, 'range_confidence_min': 0.10},
    ]
    sampled = GridBacktestRunner._select_combos(
        combos, max_runs=3, sampling_mode='stratified', random_seed=42
    )
    picked = [combo for _, combo in sampled]
    buy_values = {item['buy_score'] for item in picked}
    range_values = {item['range_confidence_min'] for item in picked}
    assert 34 in buy_values and 36 in buy_values
    assert 0.05 in range_values and 0.10 in range_values
