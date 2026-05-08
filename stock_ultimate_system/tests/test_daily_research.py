from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from run_daily_research import _resolve_default_stocks, _run_with_resolved_profile, build_daily_summary_markdown
from src.daily_research_support import (
    calculate_daily_health_score,
    classify_alert_level,
    classify_failure_reason,
    detect_consecutive_health_decline,
    diff_params,
    load_recent_health_entries,
    send_webhook_notification,
    summarize_failure_categories,
    write_health_trend_csv,
)


def test_daily_summary_markdown_contains_profile_rows():
    runs = [
        {
            'profile': 'short',
            'status': 'ok',
            'attempts': 1,
            'duration_sec': 0.55,
            'executed_runs': 2,
            'planned_runs': 2,
            'early_stopped': False,
            'top_result': {'run_id': 'r1', 'sharpe_ratio': 0.1, 'total_return': 0.02, 'total_trades': 10, 'params': {'a': 1}},
            'latest_md_path': 'a.md',
            'replay_md_path': 'b.md',
        }
    ]
    health = calculate_daily_health_score(runs)
    content = build_daily_summary_markdown(
        runs,
        total_duration_sec=1.23,
        alerts=[{'level': 'warning', 'category': 'other', 'message': 'x failed'}],
        health_score=health,
        health_trend=[{
            'generated_at': '2026-03-16T10:00:00',
            'score': 80.0,
            'success_rate': 1.0,
            'failed_count': 0.0,
            'alerts_count': 0,
        }, {
            'generated_at': '2026-03-17T10:00:00',
            'score': 100.0,
            'success_rate': 1.0,
            'failed_count': 0.0,
            'alerts_count': 1,
        }],
    )
    assert 'Daily Research Summary' in content
    assert '| short | ok | 1 | 0.55 | 2/2 | False | r1 | 0.1000 | 0.0200 | 0.0000 | 0.0000 | 10 | 0 |' in content
    assert 'Total duration: 1.23s' in content
    assert '## Daily Health Score' in content
    assert 'score: 100.00/100' in content
    assert '## Health Trend (Recent)' in content
    assert 'score_delta_vs_previous: +20.00' in content
    assert '## Failure Category Stats' in content
    assert '## Alerts' in content
    assert '- [WARNING] [other] x failed' in content
    assert 'top_params' in content


def test_daily_summary_markdown_contains_param_diff():
    content = build_daily_summary_markdown(
        [
            {
                'profile': 'medium',
                'status': 'ok',
                'attempts': 1,
                'duration_sec': 0.25,
                'executed_runs': 1,
                'planned_runs': 1,
                'early_stopped': False,
                'top_result': {'run_id': 'r2', 'sharpe_ratio': 0.2, 'total_return': 0.03, 'total_trades': 3, 'params': {'a': 2}},
                'latest_md_path': 'c.md',
                'replay_md_path': '',
            }
        ],
        previous_top_params={'medium': {'a': 1}},
    )
    assert 'param_changes_vs_previous' in content
    assert '~ a: 1 -> 2' in content


def test_diff_params_add_remove_update():
    diffs = diff_params({'a': 2, 'b': 1}, {'a': 1, 'c': 9})
    assert '~ a: 1 -> 2' in diffs
    assert '+ b=1' in diffs
    assert '- c=9' in diffs


def test_daily_summary_markdown_contains_skipped_status():
    content = build_daily_summary_markdown([
        {
            'profile': 'medium',
            'status': 'failed',
            'attempts': 2,
            'duration_sec': 1.0,
            'executed_runs': 0,
            'planned_runs': 0,
            'early_stopped': False,
            'top_result': {},
            'latest_md_path': '',
            'replay_md_path': '',
        },
        {
            'profile': 'long',
            'status': 'skipped_due_to_failure',
            'attempts': 0,
            'duration_sec': 0.0,
            'executed_runs': 0,
            'planned_runs': 0,
            'early_stopped': False,
            'top_result': {},
            'latest_md_path': '',
            'replay_md_path': '',
        }
    ], alerts=[{'level': 'warning', 'category': 'model', 'message': 'skip long'}])
    assert '| long | skipped_due_to_failure | 0 | 0.00 | 0/0 | False |  | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0 | 0 |' in content
    assert '[WARNING] [model] skip long' in content


def test_classify_failure_reason_and_alert_level():
    assert classify_failure_reason('Tushare token invalid') == 'data'
    assert classify_failure_reason('LightGBM train failed') == 'model'
    assert classify_failure_reason('backtest order rejected') == 'backtest'
    assert classify_failure_reason('something weird') == 'other'
    assert classify_alert_level('fatal traceback happened') == 'error'
    assert classify_alert_level('retry failed') == 'warning'


def test_summarize_failure_categories():
    stats = summarize_failure_categories([
        {'status': 'failed', 'failure_category': 'data'},
        {'status': 'failed', 'failure_category': 'model'},
        {'status': 'ok'},
        {'status': 'failed', 'error': 'unknown type'},
    ])
    assert stats['data'] == 1
    assert stats['model'] == 1
    assert stats['other'] == 1


def test_calculate_daily_health_score_penalty():
    runs = [
        {'status': 'ok'},
        {'status': 'failed', 'failure_category': 'data'},
        {'status': 'failed', 'failure_category': 'model'},
        {'status': 'skipped_due_to_failure'},
    ]
    health = calculate_daily_health_score(runs)
    assert health['success_rate'] == 0.3333
    assert health['failure_penalty'] == 10.0
    assert health['category_penalty'] == 14.0
    assert health['score'] == 9.33


def test_load_recent_health_entries_and_write_csv(tmp_path):
    history_path = tmp_path / 'daily_research_history.jsonl'
    history_path.write_text(
        '\n'.join([
            '{"generated_at":"2026-03-15T10:00:00","health_score":{"score":70,"success_rate":0.8,"failed_count":1},"alerts":[]}',
            '{"generated_at":"2026-03-16T10:00:00","health_score":{"score":75,"success_rate":0.9,"failed_count":1},"alerts":[{"x":1}]}',
        ]) + '\n',
        encoding='utf-8',
    )
    entries = load_recent_health_entries(history_path, limit=7)
    assert len(entries) == 2
    assert entries[0]['score'] == 70.0
    assert entries[1]['alerts_count'] == 1

    csv_path = tmp_path / 'trend.csv'
    written = write_health_trend_csv(csv_path, entries)
    assert written.endswith('trend.csv')
    text = csv_path.read_text(encoding='utf-8')
    assert 'generated_at,score,success_rate,failed_count,alerts_count' in text
    assert '2026-03-16T10:00:00,75.00,0.9000,1,1' in text


def test_detect_consecutive_health_decline():
    entries = [
        {'score': 80.0},
        {'score': 78.5},
        {'score': 77.0},
    ]
    info = detect_consecutive_health_decline(entries, days=3, min_total_drop=1.0)
    assert info['triggered'] is True
    assert info['drop'] == 3.0

    info2 = detect_consecutive_health_decline(entries, days=3, min_total_drop=5.0)
    assert info2['triggered'] is False


def test_daily_summary_markdown_contains_trend_alert():
    content = build_daily_summary_markdown(
        runs=[{'profile': 'short', 'status': 'ok', 'attempts': 1, 'duration_sec': 0.1, 'executed_runs': 1, 'planned_runs': 1, 'early_stopped': False, 'top_result': {}}],
        health_trend=[
            {'generated_at': '2026-03-15', 'score': 80.0, 'success_rate': 1.0, 'failed_count': 0.0, 'alerts_count': 0},
            {'generated_at': '2026-03-16', 'score': 78.0, 'success_rate': 1.0, 'failed_count': 0.0, 'alerts_count': 0},
            {'generated_at': '2026-03-17', 'score': 76.0, 'success_rate': 1.0, 'failed_count': 0.0, 'alerts_count': 1},
        ],
        health_trend_decline={'triggered': True, 'days': 3, 'drop': 4.0},
        recovery_runs=[{
            'profile': 'medium',
            'status': 'recovered',
            'duration_sec': 1.2,
            'top_result': {'run_id': 'replay_1'},
            'error': '',
        }],
    )
    assert 'trend_alert: consecutive decline for 3 days, total_drop=4.00' in content
    assert '## Failed Profile Recovery' in content
    assert '| medium | recovered | 1.20 | replay_1 |  |' in content


def test_run_with_resolved_profile_rejects_overlapping_validation_window():
    class DummyRunner:
        def run(self, **kwargs):
            return kwargs

    args = SimpleNamespace(stocks=['000001.SZ'])
    with pytest.raises(ValueError, match='overlaps search window'):
        _run_with_resolved_profile(
            DummyRunner(),
            args,
            {
                'profile': 'medium',
                'grid_size': 'small',
                'max_runs': 1,
                'batch_size': 1,
                'early_stop_patience': None,
                'min_improve': 0.0,
                'replay_top_k': 2,
                'start_date': '2024-01-01',
                'end_date': '2024-12-31',
                'replay_start_date': '2024-06-01',
                'replay_end_date': '2025-01-31',
                'require_validation_window': True,
            },
        )


def test_daily_summary_markdown_contains_quality_and_cost_metrics():
    content = build_daily_summary_markdown(
        runs=[{
            'profile': 'medium',
            'status': 'ok',
            'attempts': 1,
            'duration_sec': 0.3,
            'executed_runs': 1,
            'planned_runs': 1,
            'early_stopped': False,
            'top_result': {
                'run_id': 'm1',
                'params': {'buy_score': 34},
                'calmar_ratio': 0.88,
                'reward_risk_ratio': 1.45,
                'avg_holding_days': 18.0,
                'median_holding_days': 15.0,
                'low_liquidity_blocks': 7,
                'total_commission': 123.45,
                'total_slippage_cost': 67.89,
                'return_mean': 0.001,
                'return_median': 0.0008,
                'return_p05': -0.01,
                'return_p95': 0.02,
                'positive_day_ratio': 0.56,
            },
            'latest_md_path': '',
            'replay_md_path': '',
        }],
    )
    assert 'quality_metrics: calmar=0.8800, reward_risk=1.4500' in content
    assert 'cost_and_blocks: low_liquidity_blocks=7, commission=123.45, slippage=67.89' in content
    assert 'return_distribution: mean=0.0010, median=0.0008, p05=-0.0100, p95=0.0200, positive_day_ratio=0.5600' in content


def test_send_webhook_notification_empty_url():
    ok, detail = send_webhook_notification('', {'x': 1})
    assert ok is False
    assert 'empty webhook url' in detail


def test_resolve_default_stocks_uses_config_pool():
    stocks = _resolve_default_stocks(None)
    assert stocks == ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH']
