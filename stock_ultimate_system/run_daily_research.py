import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace
from typing import Any, Callable

from run_grid_backtest import build_grid, resolve_experiment, resolve_profile_args, validate_replay_window
from src.daily_research_models import AlertRecord, HealthSnapshot, ProfileRunRecord, ResearchPaths
from src.daily_research_support import (
    append_history,
    build_daily_summary_markdown,
    calculate_daily_health_score,
    classify_alert_level,
    classify_failure_reason,
    detect_consecutive_health_decline,
    load_last_profile_top_params,
    load_recent_health_entries,
    send_webhook_notification,
    summarize_failure_categories,
    write_health_trend_csv,
)
from src.evolution.grid_backtest_runner import GridBacktestRunner
from src.utils.cli import setup_cli_logging
from src.utils.project_paths import resolve_project_path
from src.utils.research_pool import resolve_research_pool, resolve_research_pool_with_meta

logger = logging.getLogger(__name__)


def _write_runtime_status(
    status_path: Path,
    *,
    state: str,
    stage: str,
    args: argparse.Namespace,
    run_summaries: list[ProfileRunRecord] | None = None,
    alerts: list[AlertRecord] | None = None,
    started_at: str | None = None,
    ended_at: str | None = None,
    health_score: HealthSnapshot | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    status_path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        'state': state,
        'stage': stage,
        'started_at': started_at,
        'ended_at': ended_at,
        'profiles': list(args.profiles),
        'stocks': list(args.stocks),
        'completed_profiles': [run.profile for run in (run_summaries or []) if run.status == 'ok'],
        'failed_profiles': [run.profile for run in (run_summaries or []) if run.status == 'failed'],
        'alert_count': len(alerts or []),
    }
    if run_summaries is not None:
        payload['runs'] = [run.to_dict() for run in run_summaries]
    if alerts is not None:
        payload['alerts'] = [alert.to_dict() for alert in alerts]
    if health_score is not None:
        payload['health_score'] = health_score.to_dict()
    if extra:
        payload.update(extra)
    status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Run daily multi-profile research workflow')
    parser.add_argument('--config-dir', default='config', help='Config directory, relative to project root unless absolute')
    parser.add_argument('--stocks', nargs='*', default=None, help='Stock pool, defaults to config settings')
    parser.add_argument('--profiles', nargs='*', default=['short', 'medium'],
                        choices=['short', 'medium', 'long'], help='Profile sequence to run')
    parser.add_argument('--max-runs', type=int, default=None, help='Override max_runs for all profiles')
    parser.add_argument('--batch-size', type=int, default=None, help='Override batch_size for all profiles')
    parser.add_argument('--retry', type=int, default=1, help='Retry times per profile on failure')
    parser.add_argument(
        '--degrade-on-failure',
        dest='degrade_on_failure',
        action='store_true',
        default=True,
        help='When a profile fails after retries, skip all remaining profiles',
    )
    parser.add_argument(
        '--no-degrade-on-failure',
        dest='degrade_on_failure',
        action='store_false',
        help='Continue remaining profiles even if one profile fails',
    )
    parser.add_argument('--health-threshold', type=float, default=70.0, help='Alert if daily health score below threshold')
    parser.add_argument('--health-decline-days', type=int, default=3, help='Trigger alert if health score declines consecutively for N days')
    parser.add_argument('--health-decline-min-drop', type=float, default=0.1, help='Minimum total score drop for decline alert')
    parser.add_argument('--webhook-url', default='', help='Optional webhook URL for alert notification')
    parser.add_argument(
        '--replay-failed-profiles',
        dest='replay_failed_profiles',
        action='store_true',
        default=True,
        help='Replay failed profiles once with conservative params',
    )
    parser.add_argument(
        '--no-replay-failed-profiles',
        dest='replay_failed_profiles',
        action='store_false',
        help='Disable replay recovery for failed profiles',
    )
    parser.add_argument('--replay-max-runs', type=int, default=1, help='Max runs for failed-profile replay')
    parser.add_argument('--out', default='data/experiments/daily_research_latest.md', help='Summary output path')
    parser.add_argument('--search-mode', choices=['focused', 'broad'], default='focused',
                        help='Focused search avoids broad noisy sweeps by default')
    parser.add_argument('--experiment', choices=['direction_a_medium', 'direction_b_medium', 'direction_c_medium'], default=None,
                        help='Optional formal experiment preset for profile runs')
    return parser


def _resolve_default_stocks(stocks: list[str] | None, config_dir: str = 'config') -> list[str]:
    if stocks:
        return stocks
    resolved = resolve_research_pool(config_dir)
    if resolved:
        return resolved
    return ['000001.SZ']


def _profile_namespace(profile: str, args: argparse.Namespace, *, replay_mode: bool = False) -> SimpleNamespace:
    if replay_mode:
        return SimpleNamespace(
            profile=profile,
            grid_size='small',
            max_runs=max(1, args.replay_max_runs),
            batch_size=1,
            early_stop_patience=1,
            min_improve=0.0,
            replay_top_k=0,
            start_date=None,
            end_date=None,
            replay_start_date=None,
            replay_end_date=None,
        )
    return SimpleNamespace(
        profile=profile,
        grid_size=None,
        max_runs=args.max_runs,
        batch_size=args.batch_size,
        early_stop_patience=None,
        min_improve=None,
        replay_top_k=None,
        start_date=None,
        end_date=None,
        replay_start_date=None,
        replay_end_date=None,
    )


def _grid_for_args(args: argparse.Namespace, grid_size: str) -> dict[str, Any]:
    experiment = resolve_experiment(args)
    if experiment and not getattr(args, '_replay_mode', False):
        return dict(experiment.get('grid', {}))
    return build_grid(grid_size, search_mode=args.search_mode)


def _run_with_resolved_profile(
    runner: GridBacktestRunner,
    args: argparse.Namespace,
    resolved: dict[str, Any],
) -> dict[str, Any]:
    validate_replay_window(resolved, profile=str(resolved.get('profile', '-')))
    return runner.run(
        stock_pool=args.stocks,
        grid=_grid_for_args(args, resolved['grid_size']),
        start_date=resolved['start_date'],
        end_date=resolved['end_date'],
        max_runs=resolved['max_runs'],
        batch_size=resolved['batch_size'],
        early_stop_patience=resolved['early_stop_patience'],
        min_improve=resolved['min_improve'],
        replay_top_k=resolved['replay_top_k'],
        replay_start_date=resolved['replay_start_date'],
        replay_end_date=resolved['replay_end_date'],
    )


def _run_profile_with_retry(
    runner: GridBacktestRunner,
    profile: str,
    args: argparse.Namespace,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> ProfileRunRecord:
    resolved = resolve_profile_args(_profile_namespace(profile, args))
    resolved['profile'] = profile
    try:
        validate_replay_window(resolved, profile=profile)
    except ValueError as exc:
        # For scheduled daily runs, never hard-stop the full chain on replay overlap.
        # Disable replay for this profile and keep research execution moving.
        if "overlaps search window" in str(exc):
            logger.warning(
                "Profile %s replay window overlap detected, disabling replay_top_k for this run: %s",
                profile,
                exc,
            )
            resolved['replay_top_k'] = 0
            resolved['replay_start_date'] = None
            resolved['replay_end_date'] = None
        else:
            raise
    max_attempts = max(1, args.retry + 1)
    attempt = 0
    profile_start = perf_counter()
    last_exc: Exception | None = None
    result: dict[str, Any] = {}
    while attempt < max_attempts:
        attempt += 1
        try:
            result = runner.run(
                stock_pool=args.stocks,
                grid=_grid_for_args(args, resolved['grid_size']),
                start_date=resolved['start_date'],
                end_date=resolved['end_date'],
                max_runs=resolved['max_runs'],
                batch_size=resolved['batch_size'],
                early_stop_patience=resolved['early_stop_patience'],
                min_improve=resolved['min_improve'],
                replay_top_k=resolved['replay_top_k'],
                replay_start_date=resolved['replay_start_date'],
                replay_end_date=resolved['replay_end_date'],
                progress_callback=progress_callback,
            )
            result['status'] = 'ok'
            break
        except Exception as e:
            last_exc = e
            logger.warning('Profile %s failed on attempt %d/%d: %s', profile, attempt, max_attempts, e)
            result = {
                'status': 'failed',
                'error': str(e),
                'failure_category': classify_failure_reason(str(e)),
                'results': [],
                'top_result': {},
            }
            if attempt >= max_attempts:
                break
    duration_sec = round(perf_counter() - profile_start, 3)
    if last_exc and result.get('status') != 'ok':
        result['error'] = str(last_exc)
        result['failure_category'] = classify_failure_reason(str(last_exc))
    return ProfileRunRecord.from_payload(
        profile=profile,
        status=str(result.get('status', 'failed')),
        attempts=attempt,
        duration_sec=duration_sec,
        payload=result,
    )


def _append_degrade_skips(
    run_summaries: list[ProfileRunRecord],
    failed_profile: str,
    remaining_profiles: list[str],
    result: ProfileRunRecord,
    alerts: list[AlertRecord],
) -> None:
    reason = result.error or 'unknown error'
    alert = AlertRecord(
        level=classify_alert_level(
            f'Profile `{failed_profile}` failed after {result.attempts} attempt(s): {reason}. '
            f'Degrade mode enabled, skipped remaining profiles: {remaining_profiles}'
        ),
        category=result.failure_category or 'other',
        message=(
        f'Profile `{failed_profile}` failed after {result.attempts} attempt(s): {reason}. '
        f'Degrade mode enabled, skipped remaining profiles: {remaining_profiles}'
        ),
    )
    logger.warning('[%s][%s] %s', alert.level.upper(), alert.category, alert.message)
    alerts.append(alert)
    for skipped in remaining_profiles:
        run_summaries.append(ProfileRunRecord.from_payload(
            profile=skipped,
            status='skipped_due_to_failure',
            attempts=0,
            duration_sec=0.0,
            payload={
                'executed_runs': 0,
                'planned_runs': 0,
                'early_stopped': False,
                'top_result': {},
                'latest_md_path': '',
                'replay_md_path': '',
                'skipped_by': failed_profile,
                'failure_category': 'unknown',
            },
        ))


def _run_profiles(
    runner: GridBacktestRunner,
    args: argparse.Namespace,
    progress_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> tuple[list[ProfileRunRecord], list[AlertRecord]]:
    run_summaries: list[ProfileRunRecord] = []
    alerts: list[AlertRecord] = []
    for idx, profile in enumerate(args.profiles):
        result = _run_profile_with_retry(
            runner,
            profile,
            args,
            progress_callback=(lambda payload, current_profile=profile: progress_callback(current_profile, payload))
            if progress_callback
            else None,
        )
        run_summaries.append(result)
        if result.status != 'ok' and args.degrade_on_failure:
            remaining = args.profiles[idx + 1:]
            if remaining:
                _append_degrade_skips(run_summaries, profile, remaining, result, alerts)
                break
    return run_summaries, alerts


def _evaluate_health(
    args: argparse.Namespace,
    history_path: Path,
    run_summaries: list[ProfileRunRecord],
    alerts: list[AlertRecord],
) -> tuple[HealthSnapshot, list[dict[str, Any]], dict[str, Any]]:
    run_dicts = [run.to_dict() for run in run_summaries]
    category_stats = summarize_failure_categories(run_dicts)
    health_score = HealthSnapshot.from_dict(calculate_daily_health_score(run_dicts, category_stats))
    if health_score.score < args.health_threshold:
        health_alert = AlertRecord(
            level='error',
            category='health',
            message=(
                f'Daily health score {health_score.score:.2f} below threshold {args.health_threshold:.2f}. '
                f'Check failure categories: {category_stats}'
            ),
        )
        alerts.append(health_alert)
        logger.warning('[%s][%s] %s', health_alert.level.upper(), health_alert.category, health_alert.message)
    previous_health_trend = load_recent_health_entries(history_path, limit=6)
    current_health_entry = health_score.trend_entry(alerts_count=len(alerts), generated_at=datetime.now().isoformat())
    health_trend = (previous_health_trend + [current_health_entry])[-7:]
    decline_info = detect_consecutive_health_decline(
        health_trend,
        days=max(2, args.health_decline_days),
        min_total_drop=max(0.0, args.health_decline_min_drop),
    )
    if decline_info.get('triggered'):
        trend_alert = AlertRecord(
            level='error',
            category='health_trend',
            message=(
                f'Health score declined consecutively for {decline_info["days"]} days. '
                f'Scores={decline_info["scores"]}, total_drop={decline_info["drop"]:.2f}'
            ),
        )
        alerts.append(trend_alert)
        logger.warning('[%s][%s] %s', trend_alert.level.upper(), trend_alert.category, trend_alert.message)
    return health_score, health_trend, decline_info


def _replay_failed_profiles(
    runner: GridBacktestRunner,
    args: argparse.Namespace,
    run_summaries: list[ProfileRunRecord],
    alerts: list[AlertRecord],
) -> list[dict[str, Any]]:
    recovery_runs: list[dict[str, Any]] = []
    if not args.replay_failed_profiles:
        return recovery_runs
    failed_profiles = [run.profile for run in run_summaries if run.status == 'failed']
    for profile in failed_profiles:
        resolved = resolve_profile_args(_profile_namespace(profile, args, replay_mode=True))
        rec_start = perf_counter()
        try:
            rec = _run_with_resolved_profile(runner, args, resolved)
            rec['profile'] = profile
            rec['status'] = 'recovered'
            rec['duration_sec'] = round(perf_counter() - rec_start, 3)
            recovery_runs.append(rec)
            alerts.append(AlertRecord(
                level='warning',
                category='recovery',
                message=f'Profile `{profile}` recovered in replay run {rec.get("top_result", {}).get("run_id", "")}',
            ))
        except Exception as e:
            recovery_runs.append({
                'profile': profile,
                'status': 'replay_failed',
                'duration_sec': round(perf_counter() - rec_start, 3),
                'error': str(e),
                'top_result': {},
            })
            alerts.append(AlertRecord(
                level='error',
                category='recovery',
                message=f'Profile `{profile}` replay recovery failed: {e}',
            ))
    return recovery_runs


def _maybe_send_webhook(
    args: argparse.Namespace,
    out_path: Path,
    health_score: HealthSnapshot,
    alerts: list[AlertRecord],
    recovery_runs: list[dict[str, Any]],
) -> None:
    if not args.webhook_url.strip() or not alerts:
        return
    payload = {
        'generated_at': datetime.now().isoformat(),
        'summary_path': str(out_path),
        'health_score': health_score.to_dict(),
        'alerts': [alert.to_dict() for alert in alerts],
        'recovery_runs': recovery_runs,
    }
    ok, detail = send_webhook_notification(args.webhook_url, payload)
    if ok:
        logger.info('Webhook sent successfully: %s', detail)
        return
    webhook_alert = AlertRecord(level='error', category='notification', message=f'Webhook notification failed: {detail}')
    alerts.append(webhook_alert)
    logger.warning('[%s][%s] %s', webhook_alert.level.upper(), webhook_alert.category, webhook_alert.message)


def _write_outputs(
    args: argparse.Namespace,
    out_path: Path,
    history_path: Path,
    previous_top_params: dict[str, dict[str, Any]],
    total_duration: float,
    run_summaries: list[ProfileRunRecord],
    alerts: list[AlertRecord],
    health_score: HealthSnapshot,
    health_trend: list[dict[str, Any]],
    decline_info: dict[str, Any],
    recovery_runs: list[dict[str, Any]],
) -> str:
    health_csv_path = out_path.with_name('daily_health_trend_latest.csv')
    out_path.parent.mkdir(parents=True, exist_ok=True)
    health_csv_written = write_health_trend_csv(health_csv_path, health_trend)
    run_dicts = [run.to_dict() for run in run_summaries]
    out_path.write_text(
        build_daily_summary_markdown(
            run_dicts,
            previous_top_params,
            total_duration,
            alerts=[alert.to_dict() for alert in alerts],
            health_score=health_score.to_dict(),
            health_trend=health_trend,
            health_trend_decline=decline_info,
            recovery_runs=recovery_runs,
        ),
        encoding='utf-8',
    )
    append_history(history_path, {
        'generated_at': datetime.now().isoformat(),
        'profiles': args.profiles,
        'stocks': args.stocks,
        'alerts': [alert.to_dict() for alert in alerts],
        'health_score': health_score.to_dict(),
        'health_trend_decline': decline_info,
        'health_trend_csv': health_csv_written,
        'recovery_runs': recovery_runs,
        'total_duration_sec': round(total_duration, 3),
        'runs': run_dicts,
        'summary_path': str(out_path),
    })
    return health_csv_written


def _print_summary(
    args: argparse.Namespace,
    out_path: Path,
    history_path: Path,
    health_csv_written: str,
    total_duration: float,
    health_score: HealthSnapshot,
    alerts: list[AlertRecord],
    run_summaries: list[ProfileRunRecord],
) -> None:
    print('\n=== Daily Research Complete ===')
    print(f'Profiles: {args.profiles}')
    print(f'Summary: {out_path}')
    print(f'History: {history_path}')
    print(f'Health trend CSV: {health_csv_written}')
    print(f'Total duration: {total_duration:.2f}s')
    print(
        f'Health score: {health_score.score:.2f}/100 '
        f'(success_rate={health_score.success_rate:.2%}, '
        f'failure_penalty={health_score.failure_penalty:.2f}, '
        f'category_penalty={health_score.category_penalty:.2f})'
    )
    if alerts:
        print('Alerts:')
        for alert in alerts:
            print(f'  - [{alert.level.upper()}][{alert.category}] {alert.message}')
    for result in run_summaries:
        top = result.top_result
        print(
            f'  - {result.profile}: run {top.get("run_id", "")} '
            f'sharpe={float(top.get("sharpe_ratio", 0.0)):.4f} '
            f'return={float(top.get("total_return", 0.0)):.4f} '
            f'status={result.status} attempts={result.attempts}'
        )


def main() -> None:
    setup_cli_logging()
    args = _build_parser().parse_args()
    if args.stocks:
        args.stocks = list(args.stocks)
        research_pool_meta: dict[str, Any] = {}
    else:
        args.stocks, research_pool_meta = resolve_research_pool_with_meta(args.config_dir)
        if not args.stocks:
            args.stocks = _resolve_default_stocks(args.stocks, args.config_dir)
    runner = GridBacktestRunner(base_config_dir=str(resolve_project_path(args.config_dir)))
    paths = ResearchPaths.from_summary_path(resolve_project_path(args.out))
    previous_top_params = load_last_profile_top_params(paths.history_path)
    total_start = perf_counter()
    started_at = datetime.now().isoformat()
    _write_runtime_status(
        paths.status_path,
        state='running',
        stage='running_profiles',
        args=args,
        started_at=started_at,
        extra={
            'search_mode': args.search_mode,
            'experiment': args.experiment,
            'research_pool_meta': research_pool_meta,
        },
    )

    try:
        runtime_ctx: dict[str, Any] = {
            'run_summaries': [],
            'alerts': [],
        }

        def _profile_progress(profile: str, payload: dict[str, Any]) -> None:
            _write_runtime_status(
                paths.status_path,
                state='running',
                stage='running_profiles',
                args=args,
                run_summaries=runtime_ctx.get('run_summaries'),
                alerts=runtime_ctx.get('alerts'),
                started_at=started_at,
                extra={
                    'active_profile': profile,
                    'active_progress': payload,
                    'search_mode': args.search_mode,
                    'experiment': args.experiment,
                    'research_pool_meta': research_pool_meta,
                },
            )

        run_summaries, alerts = _run_profiles(runner, args, progress_callback=_profile_progress)
        runtime_ctx['run_summaries'] = run_summaries
        runtime_ctx['alerts'] = alerts
        _write_runtime_status(
            paths.status_path,
            state='running',
            stage='evaluating_health',
            args=args,
            run_summaries=run_summaries,
            alerts=alerts,
            started_at=started_at,
            extra={
                'search_mode': args.search_mode,
                'experiment': args.experiment,
                'research_pool_meta': research_pool_meta,
            },
        )
        health_score, health_trend, decline_info = _evaluate_health(args, paths.history_path, run_summaries, alerts)
        _write_runtime_status(
            paths.status_path,
            state='running',
            stage='replaying_failed_profiles',
            args=args,
            run_summaries=run_summaries,
            alerts=alerts,
            started_at=started_at,
            health_score=health_score,
            extra={
                'search_mode': args.search_mode,
                'experiment': args.experiment,
                'research_pool_meta': research_pool_meta,
            },
        )
        recovery_runs = _replay_failed_profiles(runner, args, run_summaries, alerts)
        total_duration = perf_counter() - total_start
        _maybe_send_webhook(args, paths.summary_path, health_score, alerts, recovery_runs)
        _write_runtime_status(
            paths.status_path,
            state='running',
            stage='writing_outputs',
            args=args,
            run_summaries=run_summaries,
            alerts=alerts,
            started_at=started_at,
            health_score=health_score,
            extra={
                'search_mode': args.search_mode,
                'experiment': args.experiment,
                'research_pool_meta': research_pool_meta,
                'recovery_runs': recovery_runs,
                'total_duration_sec': round(total_duration, 3),
            },
        )
        health_csv_written = _write_outputs(
            args,
            paths.summary_path,
            paths.history_path,
            previous_top_params,
            total_duration,
            run_summaries,
            alerts,
            health_score,
            health_trend,
            decline_info,
            recovery_runs,
        )
        _write_runtime_status(
            paths.status_path,
            state='completed',
            stage='done',
            args=args,
            run_summaries=run_summaries,
            alerts=alerts,
            started_at=started_at,
            ended_at=datetime.now().isoformat(),
            health_score=health_score,
            extra={
                'search_mode': args.search_mode,
                'experiment': args.experiment,
                'research_pool_meta': research_pool_meta,
                'health_trend_csv': health_csv_written,
                'recovery_runs': recovery_runs,
                'total_duration_sec': round(total_duration, 3),
                'summary_path': str(paths.summary_path),
            },
        )
        _print_summary(
            args,
            paths.summary_path,
            paths.history_path,
            health_csv_written,
            total_duration,
            health_score,
            alerts,
            run_summaries,
        )
    except Exception as exc:
        _write_runtime_status(
            paths.status_path,
            state='failed',
            stage='failed',
            args=args,
            started_at=started_at,
            ended_at=datetime.now().isoformat(),
            extra={
                'error': str(exc),
                'search_mode': args.search_mode,
                'experiment': args.experiment,
                'research_pool_meta': research_pool_meta,
            },
        )
        raise


if __name__ == '__main__':
    main()
