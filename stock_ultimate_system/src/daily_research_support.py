from __future__ import annotations

import csv
import json
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any


ERROR_KEYWORDS = ('traceback', 'fatal', 'crash', 'critical')
FAILURE_CATEGORY_KEYWORDS: dict[str, tuple[str, ...]] = {
    'data': ('tushare', 'qlib', 'data', 'calendar', 'fetch', 'token', 'network', 'http'),
    'model': ('lightgbm', 'xgboost', 'train', 'predict', 'feature', 'label', 'model'),
    'backtest': ('backtest', 'order', 'execution', 'trade', 'slippage', 'commission'),
}
FAILURE_CATEGORY_PENALTY: dict[str, float] = {
    'data': 8.0,
    'model': 6.0,
    'backtest': 7.0,
    'other': 5.0,
    'unknown': 4.0,
}


def jsonable(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [jsonable(v) for v in obj]
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    return str(obj)


def diff_params(current: dict[str, Any], previous: dict[str, Any]) -> list[str]:
    diffs: list[str] = []
    keys = sorted(set(current) | set(previous))
    for key in keys:
        in_cur = key in current
        in_prev = key in previous
        if in_cur and not in_prev:
            diffs.append(f'+ {key}={current[key]}')
        elif in_prev and not in_cur:
            diffs.append(f'- {key}={previous[key]}')
        elif current.get(key) != previous.get(key):
            diffs.append(f'~ {key}: {previous.get(key)} -> {current.get(key)}')
    return diffs


def load_last_profile_top_params(history_path: Path) -> dict[str, dict[str, Any]]:
    if not history_path.exists():
        return {}
    last: dict[str, dict[str, Any]] = {}
    try:
        lines = history_path.read_text(encoding='utf-8').splitlines()
    except Exception:
        return {}
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        for run in obj.get('runs', []):
            profile = run.get('profile')
            top = run.get('top_result', {}) or {}
            params = top.get('params', {})
            if profile and profile not in last and isinstance(params, dict):
                last[profile] = params
        if last:
            break
    return last


def append_history(history_path: Path, payload: dict[str, Any]) -> None:
    history_path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(jsonable(payload), ensure_ascii=False)
    with history_path.open('a', encoding='utf-8') as f:
        f.write(line + '\n')


def load_recent_health_entries(history_path: Path, limit: int = 7) -> list[dict[str, Any]]:
    if not history_path.exists():
        return []
    entries: list[dict[str, Any]] = []
    try:
        lines = history_path.read_text(encoding='utf-8').splitlines()
    except Exception:
        return []
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        health = obj.get('health_score')
        if not isinstance(health, dict):
            continue
        entries.append({
            'generated_at': obj.get('generated_at', ''),
            'score': float(health.get('score', 0.0)),
            'success_rate': float(health.get('success_rate', 0.0)),
            'failed_count': float(health.get('failed_count', 0.0)),
            'alerts_count': len(obj.get('alerts', []) or []),
        })
        if len(entries) >= limit:
            break
    entries.reverse()
    return entries


def write_health_trend_csv(csv_path: Path, entries: list[dict[str, Any]]) -> str:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fields = ['generated_at', 'score', 'success_rate', 'failed_count', 'alerts_count']
    with csv_path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for entry in entries:
            writer.writerow({
                'generated_at': entry.get('generated_at', ''),
                'score': f'{float(entry.get("score", 0.0)):.2f}',
                'success_rate': f'{float(entry.get("success_rate", 0.0)):.4f}',
                'failed_count': f'{float(entry.get("failed_count", 0.0)):.0f}',
                'alerts_count': f'{int(entry.get("alerts_count", 0) or 0)}',
            })
    return str(csv_path)


def send_webhook_notification(
    webhook_url: str,
    payload: dict[str, Any],
    timeout_sec: float = 5.0,
) -> tuple[bool, str]:
    if not webhook_url.strip():
        return False, 'empty webhook url'
    data = json.dumps(jsonable(payload), ensure_ascii=False).encode('utf-8')
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={'Content-Type': 'application/json; charset=utf-8'},
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            code = getattr(resp, 'status', 200)
            if int(code) >= 400:
                return False, f'http status {code}'
            return True, f'http status {code}'
    except urllib.error.URLError as e:
        return False, str(e)
    except Exception as e:
        return False, str(e)


def detect_consecutive_health_decline(
    entries: list[dict[str, Any]],
    days: int = 3,
    min_total_drop: float = 0.1,
) -> dict[str, Any]:
    if days <= 1 or len(entries) < days:
        return {'triggered': False, 'drop': 0.0}
    window = entries[-days:]
    scores = [float(entry.get('score', 0.0)) for entry in window]
    strictly_declining = all(scores[i] < scores[i - 1] for i in range(1, len(scores)))
    drop = scores[0] - scores[-1]
    return {
        'triggered': strictly_declining and drop >= min_total_drop,
        'drop': round(drop, 2),
        'days': days,
        'scores': scores,
    }


def classify_failure_reason(error_message: str) -> str:
    msg = (error_message or '').lower()
    if not msg:
        return 'unknown'
    for category, keys in FAILURE_CATEGORY_KEYWORDS.items():
        if any(key in msg for key in keys):
            return category
    return 'other'


def classify_alert_level(message: str) -> str:
    msg = (message or '').lower()
    return 'error' if any(key in msg for key in ERROR_KEYWORDS) else 'warning'


def summarize_failure_categories(runs: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {'data': 0, 'model': 0, 'backtest': 0, 'other': 0, 'unknown': 0}
    for run in runs:
        if run.get('status') != 'failed':
            continue
        category = run.get('failure_category') or classify_failure_reason(str(run.get('error', '')))
        if category not in counts:
            category = 'other'
        counts[category] += 1
    return counts


def calculate_daily_health_score(
    runs: list[dict[str, Any]],
    category_stats: dict[str, int] | None = None,
) -> dict[str, float]:
    if not runs:
        return {
            'score': 100.0,
            'success_rate': 1.0,
            'failed_count': 0.0,
            'success_component': 100.0,
            'failure_penalty': 0.0,
            'category_penalty': 0.0,
        }
    category_stats = category_stats or summarize_failure_categories(runs)
    executable = [run for run in runs if run.get('status') != 'skipped_due_to_failure']
    total_executable = len(executable)
    success_count = sum(1 for run in executable if run.get('status') == 'ok')
    failed_count = sum(1 for run in executable if run.get('status') == 'failed')
    success_rate = (success_count / total_executable) if total_executable > 0 else 1.0
    success_component = success_rate * 100.0
    failure_penalty = failed_count * 5.0
    category_penalty = sum(
        FAILURE_CATEGORY_PENALTY.get(category, FAILURE_CATEGORY_PENALTY['other']) * float(count)
        for category, count in category_stats.items()
    )
    score = max(0.0, min(100.0, success_component - failure_penalty - category_penalty))
    return {
        'score': round(score, 2),
        'success_rate': round(success_rate, 4),
        'failed_count': float(failed_count),
        'success_component': round(success_component, 2),
        'failure_penalty': round(failure_penalty, 2),
        'category_penalty': round(category_penalty, 2),
    }


def build_daily_summary_markdown(
    runs: list[dict[str, Any]],
    previous_top_params: dict[str, dict[str, Any]] | None = None,
    total_duration_sec: float | None = None,
    alerts: list[dict[str, str]] | None = None,
    health_score: dict[str, float] | None = None,
    health_trend: list[dict[str, Any]] | None = None,
    health_trend_decline: dict[str, Any] | None = None,
    recovery_runs: list[dict[str, Any]] | None = None,
) -> str:
    previous_top_params = previous_top_params or {}
    alerts = alerts or []
    health_trend = health_trend or []
    recovery_runs = recovery_runs or []
    category_stats = summarize_failure_categories(runs)
    health_score = health_score or calculate_daily_health_score(runs, category_stats)
    lines = [
        '# Daily Research Summary',
        f'\nGenerated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        '\n| profile | status | attempts | duration_s | executed/planned | early_stop | top_run | sharpe | return | calmar | rr | trades | low_liq |',
        '|---------|--------|----------|------------|------------------|------------|---------|--------|--------|--------|----|--------|---------|',
    ]
    if total_duration_sec is not None:
        lines.append(f'\nTotal duration: {total_duration_sec:.2f}s')
    lines.extend([
        '\n## Daily Health Score',
        f'- score: {health_score["score"]:.2f}/100',
        f'- success_rate: {health_score["success_rate"]:.2%}',
        f'- success_component: {health_score["success_component"]:.2f}',
        f'- failure_penalty: -{health_score["failure_penalty"]:.2f}',
        f'- category_penalty: -{health_score["category_penalty"]:.2f}',
    ])
    if health_trend:
        lines.extend([
            '\n## Health Trend (Recent)',
            '\n| generated_at | score | success_rate | failed | alerts |',
            '|--------------|-------|--------------|--------|--------|',
        ])
        for item in health_trend:
            lines.append(
                f'| {item.get("generated_at", "")} | {float(item.get("score", 0.0)):.2f} | '
                f'{float(item.get("success_rate", 0.0)):.2%} | {int(float(item.get("failed_count", 0.0)))} | '
                f'{int(item.get("alerts_count", 0) or 0)} |'
            )
        if len(health_trend) >= 2:
            prev = health_trend[-2]
            cur = health_trend[-1]
            delta = float(cur.get('score', 0.0)) - float(prev.get('score', 0.0))
            lines.append(f'\n- score_delta_vs_previous: {delta:+.2f}')
    if health_trend_decline and health_trend_decline.get('triggered'):
        lines.append(
            f'- trend_alert: consecutive decline for {int(health_trend_decline.get("days", 0))} days, '
            f'total_drop={float(health_trend_decline.get("drop", 0.0)):.2f}'
        )
    lines.extend([
        '\n## Failure Category Stats',
        f'- data: {category_stats["data"]}',
        f'- model: {category_stats["model"]}',
        f'- backtest: {category_stats["backtest"]}',
        f'- other: {category_stats["other"]}',
        f'- unknown: {category_stats["unknown"]}',
    ])
    if alerts:
        lines.append('\n## Alerts')
        for alert in alerts:
            lines.append(
                f'- [{alert.get("level", "warning").upper()}]'
                f' [{alert.get("category", "other")}] {alert.get("message", "")}'
            )
    if recovery_runs:
        lines.extend([
            '\n## Failed Profile Recovery',
            '\n| profile | status | duration_s | top_run | error |',
            '|---------|--------|------------|---------|-------|',
        ])
        for rec in recovery_runs:
            top = rec.get('top_result', {}) or {}
            lines.append(
                f'| {rec.get("profile", "")} | {rec.get("status", "")} | '
                f'{float(rec.get("duration_sec", 0.0)):.2f} | {top.get("run_id", "")} | '
                f'{str(rec.get("error", "")).replace("|", "/")} |'
            )
    for run in runs:
        top = run.get('top_result', {}) or {}
        lines.append(
            f'| {run.get("profile", "")} | {run.get("status", "ok")} | {run.get("attempts", 1)} | {float(run.get("duration_sec", 0.0)):.2f} | '
            f'{run.get("executed_runs", 0)}/{run.get("planned_runs", 0)} | '
            f'{run.get("early_stopped", False)} | {top.get("run_id", "")} | '
            f'{float(top.get("sharpe_ratio", 0.0)):.4f} | {float(top.get("total_return", 0.0)):.4f} | '
            f'{float(top.get("calmar_ratio", 0.0)):.4f} | {float(top.get("reward_risk_ratio", 0.0)):.4f} | '
            f'{int(top.get("total_trades", 0) or 0)} | {int(top.get("low_liquidity_blocks", 0) or 0)} |'
        )
    for run in runs:
        top = run.get('top_result', {}) or {}
        lines.extend([
            f'\n## {run.get("profile", "").upper()}',
            f'- status: {run.get("status", "ok")} (attempts={run.get("attempts", 1)}, duration={float(run.get("duration_sec", 0.0)):.2f}s)',
            f'- failure_category: {run.get("failure_category", "")}',
            f'- top_run: {top.get("run_id", "")}',
            f'- top_params: `{json.dumps(top.get("params", {}), ensure_ascii=False)}`',
            f'- quality_metrics: calmar={float(top.get("calmar_ratio", 0.0)):.4f}, reward_risk={float(top.get("reward_risk_ratio", 0.0)):.4f}, '
            f'avg_holding_days={float(top.get("avg_holding_days", 0.0)):.2f}, median_holding_days={float(top.get("median_holding_days", 0.0)):.2f}',
            f'- cost_and_blocks: low_liquidity_blocks={int(top.get("low_liquidity_blocks", 0) or 0)}, '
            f'commission={float(top.get("total_commission", 0.0)):.2f}, slippage={float(top.get("total_slippage_cost", 0.0)):.2f}',
            f'- return_distribution: mean={float(top.get("return_mean", 0.0)):.4f}, median={float(top.get("return_median", 0.0)):.4f}, '
            f'p05={float(top.get("return_p05", 0.0)):.4f}, p95={float(top.get("return_p95", 0.0)):.4f}, '
            f'positive_day_ratio={float(top.get("positive_day_ratio", 0.0)):.4f}',
            f'- ranked_markdown: `{run.get("latest_md_path", "")}`',
            f'- replay_markdown: `{run.get("replay_md_path", "")}`',
        ])
        prev = previous_top_params.get(run.get('profile', ''), {})
        curr = top.get('params', {}) if isinstance(top.get('params', {}), dict) else {}
        if prev:
            diffs = diff_params(curr, prev)
            if diffs:
                lines.append('- param_changes_vs_previous:')
                for diff in diffs:
                    lines.append(f'  - {diff}')
            else:
                lines.append('- param_changes_vs_previous: none')
    return '\n'.join(lines) + '\n'
