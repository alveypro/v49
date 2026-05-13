from __future__ import annotations

import html
import json
from pathlib import Path
from urllib.parse import urlencode

from src.stock_ai_runner_query_service import StockAIRunnerQueryService
from src.utils.project_paths import resolve_project_path


STOCK_AI_RUNNER_API_PATH = "/api/stock-ai-runner"
STOCK_AI_RUNNER_OPS_PATH = "/ops/stock-ai-runner"
STOCK_AI_RUNNER_RESULT_REPLAY_OPS_PATH = "/ops/stock-ai-runner/result-replay"


def _normalize_base_path(base_path: str) -> str:
    raw = (base_path or "").strip()
    if not raw or raw == "/":
        return ""
    return "/" + raw.strip("/")


def _base_href(base_path: str, suffix: str = "/") -> str:
    base = _normalize_base_path(base_path)
    normalized_suffix = "/" + str(suffix or "/").lstrip("/")
    if normalized_suffix == "/":
        return base or "/"
    return f"{base}{normalized_suffix}" if base else normalized_suffix


def build_stock_ai_runner_api_body(
    root: Path,
    *,
    resource: str = "",
    provider_name: str = "",
    result_id: str = "",
    replay_window: int = 8,
    recorded_at_from: str = "",
    recorded_at_to: str = "",
    health_window: int = 8,
    trend_short_window: int = 8,
    trend_long_window: int = 16,
    top_n: int = 5,
) -> bytes:
    artifacts_dir = (root or resolve_project_path(".")) / "artifacts" / "stock_ai_runner"
    service = StockAIRunnerQueryService(artifacts_dir)
    normalized_resource = str(resource or "").strip().strip("/")
    payload = {
        "schema_version": "stock_ai_runner_read_api.v1",
        "resource": normalized_resource or "aggregate",
        "storage_dir": str(artifacts_dir),
    }
    if normalized_resource == "latest-health":
        payload["latest_health"] = service.read_latest_health()
    elif normalized_resource == "health-rollups":
        payload["health_rollups"] = service.read_health_rollups(window=max(int(health_window or 0), 0))
    elif normalized_resource == "trend-summaries":
        payload["trend_summaries"] = service.read_trend_summaries(
            short_window=max(int(trend_short_window or 0), 0),
            long_window=max(int(trend_long_window or 0), 0),
        )
    elif normalized_resource == "failure-top-causes":
        payload["failure_top_causes"] = service.read_failure_top_causes(top_n=max(int(top_n or 0), 0))
    elif normalized_resource == "provider-detail":
        payload["provider_detail"] = service.read_provider_detail(
            provider_name=str(provider_name or "").strip(),
            replay_window=max(int(replay_window or 0), 0),
            recorded_at_from=str(recorded_at_from or ""),
            recorded_at_to=str(recorded_at_to or ""),
            health_window=max(int(health_window or 0), 0),
            trend_short_window=max(int(trend_short_window or 0), 0),
            trend_long_window=max(int(trend_long_window or 0), 0),
        )
    elif normalized_resource == "result-replay":
        payload["result_replay"] = service.read_result_replay(
            result_id=str(result_id or "").strip(),
            window=max(int(replay_window or 0), 0),
            recorded_at_from=str(recorded_at_from or ""),
            recorded_at_to=str(recorded_at_to or ""),
        )
    else:
        normalized_result_id = str(result_id or "").strip()
        payload["latest_health"] = service.read_latest_health()
        payload["health_rollups"] = service.read_health_rollups(window=max(int(health_window or 0), 0))
        payload["trend_summaries"] = service.read_trend_summaries(
            short_window=max(int(trend_short_window or 0), 0),
            long_window=max(int(trend_long_window or 0), 0),
        )
        payload["failure_top_causes"] = service.read_failure_top_causes(top_n=max(int(top_n or 0), 0))
        payload["result_replay"] = (
            service.read_result_replay(
                result_id=normalized_result_id,
                window=max(int(replay_window or 0), 0),
                recorded_at_from=str(recorded_at_from or ""),
                recorded_at_to=str(recorded_at_to or ""),
            )
            if normalized_result_id
            else None
        )
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def render_stock_ai_runner_ops_page(root: Path, *, base_path: str = "", provider_name: str = "") -> str:
    artifacts_dir = (root or resolve_project_path(".")) / "artifacts" / "stock_ai_runner"
    service = StockAIRunnerQueryService(artifacts_dir)
    latest_health = service.read_latest_health()
    trend_summaries = service.read_trend_summaries(short_window=8, long_window=16)
    failure_top_causes = service.read_failure_top_causes(top_n=5)
    selected_provider_name = str(provider_name or "").strip() or (
        str(latest_health[0].get("provider_name", "") or "") if latest_health else ""
    )
    provider_detail = (
        service.read_provider_detail(provider_name=selected_provider_name)
        if selected_provider_name
        else None
    )

    def _ops_provider_href(value: str) -> str:
        return _base_href(base_path, f"{STOCK_AI_RUNNER_OPS_PATH}?{urlencode({'provider': value})}")

    latest_health_rows = "".join(
        (
            "<tr>"
            f'<td><a href="{html.escape(_ops_provider_href(str(item.get("provider_name", "") or "")))}">{html.escape(str(item.get("provider_name", "") or ""))}</a></td>'
            f"<td>{html.escape(str(item.get('latest_state', '') or ''))}</td>"
            f"<td>{html.escape(str(item.get('last_result_id', '') or ''))}</td>"
            f"<td>{html.escape(str(item.get('last_reason', '') or ''))}</td>"
            f"<td>{html.escape(str(item.get('last_recorded_at', '') or ''))}</td>"
            "</tr>"
        )
        for item in latest_health
    ) or '<tr><td colspan="5">暂无 provider 健康数据</td></tr>'
    trend_rows = "".join(
        (
            "<tr>"
            f"<td>{html.escape(str(item.get('provider_name', '') or ''))}</td>"
            f"<td>{html.escape(str(item.get('short_timeout_rate', '') or ''))}</td>"
            f"<td>{html.escape(str(item.get('long_timeout_rate', '') or ''))}</td>"
            f"<td>{html.escape(str(item.get('blocked_trend_delta', '') or ''))}</td>"
            f"<td>{html.escape(str(item.get('is_worsening', '') or ''))}</td>"
            f"<td>{html.escape(str(item.get('is_flapping', '') or ''))}</td>"
            "</tr>"
        )
        for item in trend_summaries
    ) or '<tr><td colspan="6">暂无趋势数据</td></tr>'
    cause_rows = "".join(
        (
            "<tr>"
            f"<td>{html.escape(str(item.get('reason', '') or ''))}</td>"
            f"<td>{html.escape(str(item.get('count', '') or ''))}</td>"
            f"<td>{html.escape(str(item.get('latest_provider_name', '') or ''))}</td>"
            f"<td>{html.escape(str(item.get('latest_result_id', '') or ''))}</td>"
            "</tr>"
        )
        for item in failure_top_causes
    ) or '<tr><td colspan="4">暂无失败原因</td></tr>'
    provider_detail_rows = ""
    provider_detail_title = "暂无 provider 下钻数据"
    if provider_detail:
        provider_detail_title = f"Provider Drill-Down · {provider_detail['provider_name']}"
        replay = dict(provider_detail.get("provider_replay", {}) or {})
        provider_detail_rows = "".join(
            (
                "<tr>"
                f"<td>{html.escape(str(item.get('recorded_at', '') or ''))}</td>"
                f"<td>{html.escape(str(item.get('result_id', '') or ''))}</td>"
                f"<td>{html.escape(str(item.get('state', '') or ''))}</td>"
                f"<td>{html.escape(str(item.get('reason', '') or ''))}</td>"
                "</tr>"
            )
            for item in list(replay.get("attempts", []) or [])
        ) or '<tr><td colspan="4">该 provider 暂无最近 attempt</td></tr>'
    api_root = _base_href(base_path, STOCK_AI_RUNNER_API_PATH)
    latest_health_href = _base_href(base_path, f"{STOCK_AI_RUNNER_API_PATH}/latest-health")
    health_rollups_href = _base_href(base_path, f"{STOCK_AI_RUNNER_API_PATH}/health-rollups")
    trend_summaries_href = _base_href(base_path, f"{STOCK_AI_RUNNER_API_PATH}/trend-summaries")
    failure_top_causes_href = _base_href(base_path, f"{STOCK_AI_RUNNER_API_PATH}/failure-top-causes")
    provider_detail_href = _base_href(
        base_path,
        f"{STOCK_AI_RUNNER_API_PATH}/provider-detail?{urlencode({'provider_name': selected_provider_name})}",
    ) if selected_provider_name else _base_href(base_path, f"{STOCK_AI_RUNNER_API_PATH}/provider-detail")
    result_replay_href = _base_href(base_path, f"{STOCK_AI_RUNNER_API_PATH}/result-replay")
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Stock AI Runner Ops</title>
  <style>
    body {{ margin: 0; font-family: "Helvetica Neue", "PingFang SC", sans-serif; background: #f5f7fa; color: #142033; }}
    .ops-shell {{ max-width: 1280px; margin: 0 auto; padding: 32px 24px 48px; }}
    .ops-hero, .ops-card {{ background: #fff; border: 1px solid rgba(20,32,51,.1); border-radius: 20px; box-shadow: 0 12px 36px rgba(20,32,51,.06); }}
    .ops-hero {{ padding: 28px; margin-bottom: 20px; }}
    .ops-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
    .ops-card {{ padding: 20px; }}
    .ops-links {{ display: flex; gap: 12px; flex-wrap: wrap; margin-top: 16px; }}
    .ops-links a {{ color: #0f766e; text-decoration: none; font-weight: 600; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid rgba(20,32,51,.08); vertical-align: top; }}
    .ops-note {{ color: #5c6a79; line-height: 1.7; }}
    @media (max-width: 960px) {{ .ops-grid {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
  <main class="ops-shell" id="stock-ai-runner-ops">
    <section class="ops-hero">
      <div>Stock AI Runner Ops</div>
      <h1>独立运维只读入口</h1>
      <p class="ops-note">这个页面只消费 stock AI runner 的 query service / 只读 API，不反向参与 /stock 主结果裁决，也不在 /T12 启用。</p>
      <div class="ops-links">
        <a href="{api_root}">aggregate</a>
        <a href="{latest_health_href}">latest-health</a>
        <a href="{health_rollups_href}">health-rollups</a>
        <a href="{trend_summaries_href}">trend-summaries</a>
        <a href="{failure_top_causes_href}">failure-top-causes</a>
        <a href="{provider_detail_href}">provider-detail</a>
        <a href="{result_replay_href}">result-replay</a>
      </div>
    </section>
    <section class="ops-grid">
      <section class="ops-card">
        <h2>Latest Health</h2>
        <table>
          <thead><tr><th>Provider</th><th>State</th><th>Last Result</th><th>Reason</th><th>Recorded At</th></tr></thead>
          <tbody>{latest_health_rows}</tbody>
        </table>
      </section>
      <section class="ops-card">
        <h2>Trend Summaries</h2>
        <table>
          <thead><tr><th>Provider</th><th>Short Timeout</th><th>Long Timeout</th><th>Blocked Delta</th><th>Worsening</th><th>Flapping</th></tr></thead>
          <tbody>{trend_rows}</tbody>
        </table>
      </section>
      <section class="ops-card">
        <h2>Failure Top Causes</h2>
        <table>
          <thead><tr><th>Reason</th><th>Count</th><th>Latest Provider</th><th>Latest Result</th></tr></thead>
          <tbody>{cause_rows}</tbody>
        </table>
      </section>
      <section class="ops-card">
        <h2>{html.escape(provider_detail_title)}</h2>
        <table>
          <thead><tr><th>Recorded At</th><th>Result</th><th>State</th><th>Reason</th></tr></thead>
          <tbody>{provider_detail_rows or '<tr><td colspan="4">暂无 provider 下钻数据</td></tr>'}</tbody>
        </table>
      </section>
    </section>
  </main>
</body>
</html>"""


def render_stock_ai_runner_result_replay_page(
    root: Path,
    *,
    base_path: str = "",
    result_id: str = "",
    replay_window: int = 8,
    recorded_at_from: str = "",
    recorded_at_to: str = "",
) -> str:
    artifacts_dir = (root or resolve_project_path(".")) / "artifacts" / "stock_ai_runner"
    service = StockAIRunnerQueryService(artifacts_dir)
    replay = service.read_result_replay(
        result_id=str(result_id or "").strip(),
        window=max(int(replay_window or 0), 0),
        recorded_at_from=str(recorded_at_from or ""),
        recorded_at_to=str(recorded_at_to or ""),
    )
    attempts = list(replay.get("attempts", []) or [])
    latest_attempt = dict(attempts[-1]) if attempts else {}
    latest_provider = str(latest_attempt.get("provider_name", "") or "")
    latest_state = str(replay.get("latest_state", "") or "")
    last_recorded_at = str(latest_attempt.get("recorded_at", "") or "")
    failure_counts = dict(replay.get("failure_counts", {}) or {})
    attempts_rows = "".join(
        (
            "<tr>"
            f"<td>{html.escape(str(item.get('recorded_at', '') or ''))}</td>"
            f"<td>{html.escape(str(item.get('provider_name', '') or ''))}</td>"
            f"<td>{html.escape(str(item.get('state', '') or ''))}</td>"
            f"<td>{html.escape(str(item.get('reason', '') or ''))}</td>"
            f"<td>{html.escape(str(item.get('final_status', '') or ''))}</td>"
            "</tr>"
        )
        for item in attempts
    ) or '<tr><td colspan="5">当前时间窗口内没有 attempt</td></tr>'
    failure_rows = "".join(
        (
            "<tr>"
            f"<td>{html.escape(str(reason or ''))}</td>"
            f"<td>{html.escape(str(count or 0))}</td>"
            "</tr>"
        )
        for reason, count in failure_counts.items()
    ) or '<tr><td colspan="2">当前窗口内没有失败记录</td></tr>'
    replay_api_href = _base_href(
        base_path,
        f"{STOCK_AI_RUNNER_API_PATH}/result-replay?{urlencode({'result_id': str(replay.get('result_id', '') or ''), 'replay_window': str(replay.get('window', 0) or 0), 'recorded_at_from': str(replay.get('recorded_at_from', '') or ''), 'recorded_at_to': str(replay.get('recorded_at_to', '') or '')})}",
    )
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Stock AI Runner Result Replay</title>
  <style>
    body {{ margin: 0; font-family: "Helvetica Neue", "PingFang SC", sans-serif; background: #f5f7fa; color: #142033; }}
    .replay-shell {{ max-width: 1280px; margin: 0 auto; padding: 32px 24px 48px; }}
    .replay-hero, .replay-card {{ background: #fff; border: 1px solid rgba(20,32,51,.1); border-radius: 20px; box-shadow: 0 12px 36px rgba(20,32,51,.06); }}
    .replay-hero {{ padding: 28px; margin-bottom: 20px; }}
    .replay-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
    .replay-card {{ padding: 20px; }}
    .replay-kpis {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-top: 18px; }}
    .replay-kpi {{ padding: 14px; border: 1px solid rgba(20,32,51,.08); border-radius: 16px; background: #f9fbfc; }}
    .replay-links {{ display: flex; gap: 12px; flex-wrap: wrap; margin-top: 16px; }}
    .replay-links a {{ color: #0f766e; text-decoration: none; font-weight: 600; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid rgba(20,32,51,.08); vertical-align: top; }}
    .replay-note {{ color: #5c6a79; line-height: 1.7; }}
    @media (max-width: 960px) {{ .replay-grid, .replay-kpis {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
  <main class="replay-shell" id="stock-ai-runner-result-replay">
    <section class="replay-hero">
      <div>Stock AI Runner Result Replay</div>
      <h1>Result Replay · {html.escape(str(replay.get('result_id', '') or '未指定 result_id'))}</h1>
      <p class="replay-note">这个页面只做单个 result 的只读复盘，帮助运维看清最近几次 attempt 顺序、失败原因、时间窗口状态变化和对应 provider 痕迹。</p>
      <div class="replay-links">
        <a href="{html.escape(_base_href(base_path, STOCK_AI_RUNNER_OPS_PATH))}">返回 ops 总览</a>
        <a href="{html.escape(replay_api_href)}">只读 API</a>
      </div>
      <div class="replay-kpis">
        <div class="replay-kpi"><strong>Latest State</strong><div>{html.escape(latest_state or '-')}</div></div>
        <div class="replay-kpi"><strong>Latest Provider</strong><div>{html.escape(latest_provider or '-')}</div></div>
        <div class="replay-kpi"><strong>Last Recorded At</strong><div>{html.escape(last_recorded_at or '-')}</div></div>
        <div class="replay-kpi"><strong>Attempt Count</strong><div>{html.escape(str(replay.get('attempt_count', 0) or 0))}</div></div>
      </div>
    </section>
    <section class="replay-grid">
      <section class="replay-card">
        <h2>Attempt Timeline</h2>
        <table>
          <thead><tr><th>Recorded At</th><th>Provider</th><th>State</th><th>Reason</th><th>Final Status</th></tr></thead>
          <tbody>{attempts_rows}</tbody>
        </table>
      </section>
      <section class="replay-card">
        <h2>Failure Panel</h2>
        <table>
          <thead><tr><th>Reason / State</th><th>Count</th></tr></thead>
          <tbody>{failure_rows}</tbody>
        </table>
        <p class="replay-note" style="margin-top:16px;">当前窗口：from={html.escape(str(replay.get('recorded_at_from', '') or '-'))}，to={html.escape(str(replay.get('recorded_at_to', '') or '-'))}，window={html.escape(str(replay.get('window', 0) or 0))}</p>
      </section>
    </section>
  </main>
</body>
</html>"""
