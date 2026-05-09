import argparse
from copy import deepcopy
from datetime import date

from src.evolution.grid_backtest_runner import GridBacktestRunner
from src.utils.cli import setup_cli_logging
from src.utils.cli_output import print_header, print_json_kv, print_kv
from src.utils.project_paths import resolve_project_path
from src.utils.research_pool import resolve_research_pool


PROFILE_PRESETS = {
    # Fast smoke validation on short window.
    'short': {
        'grid_size': 'small',
        'max_runs': 8,
        'sampling_mode': 'sequential',
        'random_seed': 42,
        'batch_size': 2,
        'early_stop_patience': 3,
        'min_improve': 0.0001,
        'replay_top_k': 2,
        'start_date': '2025-01-01',
        'end_date': '2025-08-31',
        # Keep replay as independent OOS window to satisfy validation guardrail.
        'replay_start_date': '2025-09-01',
        'replay_end_date': '2025-12-31',
    },
    # Default research mode.
    'medium': {
        'grid_size': 'medium',
        'max_runs': 24,
        'sampling_mode': 'stratified',
        'random_seed': 42,
        'batch_size': 4,
        'early_stop_patience': 6,
        'min_improve': 0.0001,
        'replay_top_k': 3,
        'start_date': '2024-01-01',
        'end_date': '2024-12-31',
        'replay_start_date': '2025-01-01',
        'replay_end_date': '2025-06-30',
        'require_validation_window': True,
    },
    # More thorough search; usually run overnight.
    'long': {
        'grid_size': 'large',
        'max_runs': 48,
        'sampling_mode': 'stratified',
        'random_seed': 42,
        'batch_size': 6,
        'early_stop_patience': 10,
        'min_improve': 0.00005,
        'replay_top_k': 5,
        'start_date': '2023-01-01',
        'end_date': '2024-12-31',
        'replay_start_date': '2025-01-01',
        'replay_end_date': '2025-12-31',
        'require_validation_window': True,
    },
}

EXPERIMENT_PRESETS = {
    'direction_a_medium': {
        'grid': {
            'buy_score': [34, 36],
            'strong_buy_score': [48, 50, 52],
            'range_confidence_min': [0.05, 0.1],
            'liquidity_min_turnover': [1_000_000, 1_200_000],
            'low_confidence_skip_threshold': [0.55],
            'buy_position_multiplier': [0.45],
            'strong_buy_position_multiplier': [1.0],
            'commission_rate': [0.0003],
            'slippage_rate': [0.0005],
        },
        'sampling_mode': 'sequential',
        'max_runs': None,
    },
    'direction_b_medium': {
        'grid': {
            'buy_score': [38],
            'strong_buy_score': [50],
            'range_confidence_min': [0.05],
            'liquidity_min_turnover': [1_000_000],
            'atr_stop_loss_multiplier': [1.5, 2.0, 2.5],
            'atr_take_profit_multiplier': [2.5, 3.0, 3.5],
            'drawdown_protection_position_scale': [0.25, 0.30, 0.40],
            'environment_weak_position_scale': [0.50, 0.60],
            'environment_risk_off_position_scale': [0.25, 0.35],
            'commission_rate': [0.0003],
            'slippage_rate': [0.0005],
        },
        'sampling_mode': 'stratified',
        'max_runs': 24,
    },
    'direction_c_medium': {
        'grid': {
            'buy_score': [36, 38],
            'strong_buy_score': [50, 52],
            'range_confidence_min': [0.05, 0.1],
            'low_confidence_skip_threshold': [0.55, 0.60, 0.65],
            'buy_position_multiplier': [0.30, 0.40, 0.50],
            'strong_buy_position_multiplier': [0.90, 1.00, 1.10],
            'liquidity_min_turnover': [1_000_000, 1_200_000],
            'commission_rate': [0.0003],
            'slippage_rate': [0.0005],
        },
        'sampling_mode': 'stratified',
        'max_runs': 24,
    },
}


def build_grid(grid_size: str, search_mode: str = 'focused') -> dict:
    if grid_size == 'small':
        return {
            'buy_score': [34, 38],
            'strong_buy_score': [48, 50],
            'range_confidence_min': [0.05, 0.1],
            'low_confidence_skip_threshold': [0.50, 0.55],
            'liquidity_min_turnover': [1_000_000, 1_200_000],
            'slippage_rate': [0.0005],
            'commission_rate': [0.0003],
        }
    if grid_size == 'large':
        if search_mode != 'broad':
            return {
                'buy_score': [34, 36, 38],
                'strong_buy_score': [48, 50, 52],
                'buy_position_multiplier': [0.35, 0.45, 0.55],
                'strong_buy_position_multiplier': [0.95, 1.0, 1.05],
                'range_confidence_min': [0.05, 0.1],
                'low_confidence_skip_threshold': [0.55, 0.60],
                'liquidity_min_turnover': [1_000_000, 1_200_000, 1_500_000],
                'slippage_rate': [0.0005, 0.0010],
                'commission_rate': [0.0003, 0.0005],
                'atr_stop_loss_multiplier': [1.5, 2.0, 2.5],
                'atr_take_profit_multiplier': [2.5, 3.0, 3.5],
                'drawdown_protection_position_scale': [0.25, 0.30, 0.40],
                'environment_weak_position_scale': [0.50, 0.60],
                'environment_risk_off_position_scale': [0.25, 0.35],
            }
        return {
            'buy_score': [32, 36, 40],
            'strong_buy_score': [44, 48, 52],
            'range_confidence_min': [0.0, 0.05, 0.1],
            'liquidity_min_turnover': [500_000, 1_000_000, 2_000_000],
            'slippage_rate': [0.0003, 0.0005, 0.0010],
            'commission_rate': [0.0002, 0.0003, 0.0005],
            'stop_loss_pct': [0.04, 0.05, 0.06],
            'take_profit_pct': [0.08, 0.10, 0.12],
        }
    # medium default
    return {
        'buy_score': [34, 36],
        'strong_buy_score': [48, 50, 52],
        'buy_position_multiplier': [0.35, 0.45, 0.55],
        'range_confidence_min': [0.05, 0.1],
        'low_confidence_skip_threshold': [0.50, 0.55, 0.60],
        'liquidity_min_turnover': [1_000_000, 1_200_000],
        'slippage_rate': [0.0005, 0.0010],
        'commission_rate': [0.0003, 0.0005],
    }


def resolve_experiment(args) -> dict[str, object] | None:
    key = str(getattr(args, 'experiment', '') or '').strip()
    if not key:
        return None
    preset = EXPERIMENT_PRESETS.get(key)
    if not preset:
        raise ValueError(f'Unknown experiment preset: {key}')
    return dict(preset)


def resolve_profile_args(args) -> dict:
    profile_cfg = deepcopy(PROFILE_PRESETS.get(args.profile, {}))
    profile_cfg.update({
        'grid_size': args.grid_size if getattr(args, 'grid_size', None) is not None else profile_cfg.get('grid_size', 'medium'),
        'max_runs': args.max_runs if getattr(args, 'max_runs', None) is not None else profile_cfg.get('max_runs', 8),
        'sampling_mode': args.sampling_mode if getattr(args, 'sampling_mode', None) is not None else profile_cfg.get('sampling_mode', 'random'),
        'random_seed': args.random_seed if getattr(args, 'random_seed', None) is not None else profile_cfg.get('random_seed', 42),
        'batch_size': args.batch_size if getattr(args, 'batch_size', None) is not None else profile_cfg.get('batch_size', 4),
        'early_stop_patience': args.early_stop_patience if getattr(args, 'early_stop_patience', None) is not None else profile_cfg.get('early_stop_patience'),
        'min_improve': args.min_improve if getattr(args, 'min_improve', None) is not None else profile_cfg.get('min_improve', 0.0),
        'replay_top_k': args.replay_top_k if getattr(args, 'replay_top_k', None) is not None else profile_cfg.get('replay_top_k', 0),
        'start_date': args.start_date if getattr(args, 'start_date', None) is not None else profile_cfg.get('start_date'),
        'end_date': args.end_date if getattr(args, 'end_date', None) is not None else profile_cfg.get('end_date'),
        'replay_start_date': args.replay_start_date if getattr(args, 'replay_start_date', None) is not None else profile_cfg.get('replay_start_date'),
        'replay_end_date': args.replay_end_date if getattr(args, 'replay_end_date', None) is not None else profile_cfg.get('replay_end_date'),
        'require_validation_window': profile_cfg.get('require_validation_window', False),
    })
    return profile_cfg


def _parse_date(value: str | None) -> date | None:
    text = str(value or '').strip()
    if not text:
        return None
    return date.fromisoformat(text)


def validate_replay_window(resolved: dict, *, profile: str | None = None) -> None:
    replay_top_k = int(resolved.get('replay_top_k', 0) or 0)
    if replay_top_k <= 0:
        return

    require_validation_window = bool(resolved.get('require_validation_window', False))
    search_start = _parse_date(resolved.get('start_date'))
    search_end = _parse_date(resolved.get('end_date'))
    replay_start = _parse_date(resolved.get('replay_start_date'))
    replay_end = _parse_date(resolved.get('replay_end_date'))
    profile_label = str(profile or resolved.get('profile') or '-')

    if require_validation_window and (search_start is None or search_end is None):
        raise ValueError(
            f'Profile `{profile_label}` requires explicit search window before replay validation. '
            'Set --start-date and --end-date or provide preset defaults.'
        )
    if require_validation_window and (replay_start is None or replay_end is None):
        raise ValueError(
            f'Profile `{profile_label}` requires explicit validation window for replay. '
            'Set --replay-start-date and --replay-end-date.'
        )
    if search_start is None or search_end is None or replay_start is None or replay_end is None:
        return
    if search_start > search_end:
        raise ValueError(f'Invalid search window for profile `{profile_label}`: start_date > end_date.')
    if replay_start > replay_end:
        raise ValueError(f'Invalid validation window for profile `{profile_label}`: replay_start_date > replay_end_date.')

    overlaps = not (replay_end < search_start or replay_start > search_end)
    if overlaps:
        raise ValueError(
            f'Profile `{profile_label}` replay window overlaps search window: '
            f'search={search_start.isoformat()}~{search_end.isoformat()}, '
            f'validation={replay_start.isoformat()}~{replay_end.isoformat()}. '
            'Validation must be an independent out-of-sample window.'
        )


def main() -> None:
    setup_cli_logging()
    parser = argparse.ArgumentParser(description='Grid backtest for signal/risk params')
    parser.add_argument('--config-dir', default='config', help='Config directory, relative to project root unless absolute')
    parser.add_argument('--stocks', nargs='*', default=None, help='Stock pool, defaults to config settings')
    parser.add_argument('--profile', choices=['short', 'medium', 'long'], default='medium',
                        help='Recommended parameter template')
    parser.add_argument('--start-date', default=None, help='Override start_date')
    parser.add_argument('--end-date', default=None, help='Override end_date')
    parser.add_argument('--max-runs', type=int, default=None, help='Max grid combinations to run')
    parser.add_argument('--sampling-mode', choices=['sequential', 'random', 'stratified'], default=None,
                        help='How to select combos when max-runs is set')
    parser.add_argument('--random-seed', type=int, default=None, help='Random seed for random sampling mode')
    parser.add_argument('--grid-size', choices=['small', 'medium', 'large'], default=None,
                        help='Preset grid size')
    parser.add_argument('--batch-size', type=int, default=None, help='Batch size for grid execution')
    parser.add_argument('--early-stop-patience', type=int, default=None, help='Stop after N non-improving runs')
    parser.add_argument('--min-improve', type=float, default=None, help='Minimum sharpe improvement to reset patience')
    parser.add_argument('--replay-top-k', type=int, default=None, help='Replay top-k configs after initial ranking')
    parser.add_argument('--replay-start-date', default=None, help='Replay period start date')
    parser.add_argument('--replay-end-date', default=None, help='Replay period end date')
    parser.add_argument('--research-pool-size', type=int, default=None, help='Override auto-selected research pool size')
    parser.add_argument('--search-mode', choices=['focused', 'broad'], default='focused',
                        help='Focused search avoids returning to broad noisy sweeps')
    parser.add_argument('--experiment', choices=sorted(EXPERIMENT_PRESETS.keys()), default=None,
                        help='Run a formal experiment preset for Direction A/B/C')
    args = parser.parse_args()
    config_dir = resolve_project_path(args.config_dir)
    if args.stocks:
        stock_pool = args.stocks
    else:
        stock_pool = resolve_research_pool(config_dir, size_override=args.research_pool_size) or ['000001.SZ']

    resolved = resolve_profile_args(args)
    experiment = resolve_experiment(args)
    grid = build_grid(resolved['grid_size'], search_mode=args.search_mode)
    if experiment:
        grid = experiment['grid']
        if args.max_runs is None:
            resolved['max_runs'] = experiment.get('max_runs')
        if args.sampling_mode is None:
            resolved['sampling_mode'] = str(experiment.get('sampling_mode', resolved['sampling_mode']))
    validate_replay_window(resolved, profile=args.profile)

    runner = GridBacktestRunner(base_config_dir=str(config_dir))
    result = runner.run(
        stock_pool=stock_pool,
        grid=grid,
        start_date=resolved['start_date'],
        end_date=resolved['end_date'],
        max_runs=resolved['max_runs'],
        sampling_mode=resolved['sampling_mode'],
        random_seed=resolved['random_seed'],
        batch_size=resolved['batch_size'],
        early_stop_patience=resolved['early_stop_patience'],
        min_improve=resolved['min_improve'],
        replay_top_k=resolved['replay_top_k'],
        replay_start_date=resolved['replay_start_date'],
        replay_end_date=resolved['replay_end_date'],
    )

    print_header('Grid Backtest Complete')
    print_kv('Profile', args.profile)
    print_kv('Experiment', args.experiment or '-')
    print_kv('Search mode', args.search_mode)
    print_kv('Grid size preset', resolved['grid_size'])
    print_kv('Sampling', f"{resolved['sampling_mode']} (seed={resolved['random_seed']})")
    print_kv('Window', f"{resolved['start_date'] or 'config default'} ~ {resolved['end_date'] or 'config default'}")
    print_kv('Executed runs', f"{result.get('executed_runs', 0)} / Planned: {result.get('planned_runs', 0)}")
    print_kv('Early stopped', result.get('early_stopped', False))
    top = result.get('top_result', {})
    if top:
        print_kv('Top run', top.get('run_id', ''))
        print_json_kv('Top params', top.get('params', {}))
        print_kv(
            'Top metrics',
            f"return={top.get('total_return', 0):.4f}, sharpe={top.get('sharpe_ratio', 0):.4f}, trades={top.get('total_trades', 0)}",
        )
    print_kv('Ranked CSV', result.get('csv_path', ''))
    print_kv('Ranked Markdown', result.get('md_path', ''))
    print_kv('Latest CSV', result.get('latest_csv_path', ''))
    print_kv('Latest Markdown', result.get('latest_md_path', ''))
    if result.get('replay_md_path'):
        print_kv('Replay CSV', result.get('replay_csv_path', ''))
        print_kv('Replay Markdown', result.get('replay_md_path', ''))


if __name__ == '__main__':
    main()
