import argparse
import html
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlencode, urlparse

from src.dashboard_context import build_dashboard_context
from src.dashboard_operations import build_operations_render_contract, render_operations_section
from src.dashboard_reports import build_reports_render_contract, render_reports_section
from src.stock_dashboard_constants import REPORT_LABELS, VIEW_LABELS, VIEW_SUBTITLES
from src.stock_dashboard_assets import compose_dashboard_stylesheet
from src.stock_dashboard_assets import compose_dashboard_script_tag
from src.stock_dashboard_assets import compose_fail_closed_stylesheet
from src.stock_dashboard_assets import compose_inline_style_tag
from src.stock_dashboard_sections import (
    render_actions_section,
    render_architecture_section,
    render_charts_section,
    render_candidate_focus_section,
    render_candidate_compare_section,
    render_candidate_visuals_section,
    render_command_deck_section,
    render_control_strip_section,
    render_diagnostics_appendix_section,
    render_diagnosis_section,
    render_display_contract,
    render_dual_track_rows,
    render_external_decision_spine_section,
    render_guide_section,
    render_hero_side_section,
    render_home_headline_section,
    render_jump_strip,
    render_kpi_grid_rows,
    render_links_section,
    render_market_snapshot_section,
    render_nav_links,
    render_overview_kpi_section,
    render_overview_operations_disclosure_section,
    render_overview_system_summary_section,
    render_overview_visuals_disclosure_section,
    render_overview_visuals_section,
    render_sidebar_status_section,
    render_spotlight_section,
    render_metric_ribbon,
    render_opportunities_section,
    render_prefilter_section,
    render_research_visuals_section,
    render_selection_funnel_section,
    render_summary_section,
    render_top1_section,
    render_topbar_pills_section,
    render_primary_result_home_brief_section,
    render_validation_section,
    render_view_banner_section,
)
from src.stock_dashboard_page_composer import (
    compose_fail_closed_page_html,
    compose_main_content_html,
    compose_page_shell_html,
    compose_page_interaction_script,
    compose_primary_result_bridge_client_script,
    compose_primary_result_bridge_bootstrap_script,
    compose_primary_result_shell_html,
    compose_table_export_script,
    compose_top_story_html,
)
from src.stock_dashboard_domain_context import build_dashboard_domain_context
from src.stock_dashboard_render_context import (
    build_dashboard_asset_contract,
    build_candidate_focus_render_contract,
    build_dashboard_page_shell_contract,
    build_jump_strip_context,
    build_main_content_sections,
    build_page_shell_context,
    build_page_interaction_context,
    build_primary_result_bridge_context,
    build_section_visibility_context,
    build_top_story_context,
)
from src.dashboard_support import (
    build_candidate_actions_render_contract,
    stock_primary_result_card_html,
    stock_primary_result_bridge_shell_html,
    translate_guardrail_mode,
    translate_strategy_mode,
    translate_strategy_strictness,
    translate_weak_market_action,
)
from src.main_site_home import render_main_site_home
from src.utils.project_paths import resolve_project_path
from src.stock_dashboard_view_model import (
    build_stock_actions_view_model,
    build_stock_candidate_compare_view_model,
    build_stock_candidate_diagnostics_view_model,
    build_stock_candidate_focus_view_model,
    build_stock_charts_view_model,
    build_stock_guide_view_model,
    build_stock_home_view_model,
    build_stock_links_view_model,
    build_stock_overview_chrome_view_model,
    build_stock_overview_disclosure_view_model,
    build_stock_overview_kpi_view_model,
    build_stock_opportunities_view_model,
    build_stock_prefilter_view_model,
    build_stock_runtime_view_model,
    build_stock_selection_funnel_view_model,
    build_stock_summary_view_model,
    build_stock_top1_view_model,
    build_stock_validation_view_model,
)
from src.stock_entry_guard import evaluate_stock_entry_guard
from src.stock_ai_runner_query_service import StockAIRunnerQueryService
from src.t12_governance_summary import (
    build_t12_governance_summary_view_model,
    render_t12_governance_summary_template,
)
from src.t12_overview_card import build_t12_overview_card_view_model, render_t12_overview_card_template
from src.unified_result_builder import build_primary_result_api_payload
from src.utils.project_paths import resolve_artifacts_path, resolve_experiments_path, resolve_project_path
from src.airivo_scope_registry import get_airivo_namespace, get_airivo_scope, resolve_airivo_namespace_scope

PRIMARY_RESULT_API_PATH = "/api/primary-result"
STOCK_AI_RUNNER_API_PATH = "/api/stock-ai-runner"
STOCK_AI_RUNNER_OPS_PATH = "/ops/stock-ai-runner"
STOCK_AI_RUNNER_RESULT_REPLAY_OPS_PATH = "/ops/stock-ai-runner/result-replay"
PRIMARY_RESULT_BRIDGE_ENABLED = True
PRIMARY_RESULT_CORE_COMPARE_FIELDS = (
    "result_lifecycle_stage",
    "result_type",
    "risk_level",
    "audit_status",
    "terminal_outcome",
)

def _display_missing(value: object, fallback: str) -> str:
    text = str(value or "").strip()
    return text if text and text != "-" else fallback


def _display_status_label(value: object) -> str:
    text = str(value or "").strip()
    if not text or text == "-":
        return "待确认"
    normalized = text.lower()
    mapping = {
        "manual_review": "人工复核",
        "blocked": "已阻断",
        "conditional": "受控放行",
        "running": "运行中",
        "running_daily_research": "每日研究运行中",
        "completed": "已完成",
        "done": "已完成",
        "up_to_date": "已更新",
        "pending_window": "受控等待",
        "ready_for_data_check": "等待数据门检查",
        "unknown": "待确认",
        "pass": "通过",
        "failed": "失败",
        "yellow": "黄色观察",
    }
    return mapping.get(normalized, text)


def _compact_date_key(value: object) -> str:
    text = str(value or "").strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10].replace("-", "")
    if len(text) >= 10 and text[4] == "." and text[7] == ".":
        return text[:10].replace(".", "")
    if len(text) >= 8 and text[:8].isdigit():
        return text[:8]
    return ""


def _display_trade_date(value: object, fallback: str = "-") -> str:
    text = str(value or "").strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    if len(text) >= 8 and text[:8].isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text or fallback


def _display_timestamp(value: object, fallback: str = "-") -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    if "T" in text and len(text) >= 19:
        return text[:19].replace("T", " ")
    return text


def _load_json_file(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _external_promotion_label(value: object) -> str:
    text = str(value or "").strip()
    if text.lower() == "blocked":
        return "晋级锁定"
    return _display_status_label(text)


def _is_t12_scope(base_path: str) -> bool:
    # `/T12` is a read-only scope. Treat this helper as a boundary guard,
    # not as an entry point for adding new interactive console views.
    return _resolve_scope_id(base_path) == "t12"


def _is_main_site_scope(base_path: str) -> bool:
    return _resolve_scope_id(base_path) == "main_site"


def _resolve_namespace_scope_with_fallback(base_path: str):
    normalized = _normalize_base_path(base_path) or "/"
    try:
        return resolve_airivo_namespace_scope(normalized), None
    except KeyError:
        # Fail closed to the canonical public entry instead of crashing render
        # on an unregistered mount path or proxy rewrite mistake.
        return (get_airivo_namespace("production"), get_airivo_scope("main_site")), normalized


def _resolve_namespace_id(base_path: str) -> str:
    (namespace, _scope), _fallback_route = _resolve_namespace_scope_with_fallback(base_path)
    return namespace.namespace_id


def _resolve_scope_id(base_path: str) -> str:
    (_namespace, scope), _fallback_route = _resolve_namespace_scope_with_fallback(base_path)
    return scope.scope_id


def _view_labels(base_path: str) -> dict[str, str]:
    labels = dict(VIEW_LABELS)
    if _is_t12_scope(base_path):
        # Only add the minimal read-only T12 view in `/T12` scope.
        labels["t12"] = "T12"
    return labels


def _view_subtitles(base_path: str) -> dict[str, str]:
    subtitles = dict(VIEW_SUBTITLES)
    if _is_t12_scope(base_path):
        # Keep the subtitle explicit so later changes do not reinterpret
        # `view=t12` as a console or interaction surface.
        subtitles["t12"] = "聚焦 T12 最小制度镜像与治理摘要，不扩成完整控制台。"
    return subtitles


def _normalize_base_path(base_path: str) -> str:
    raw = (base_path or "").strip()
    if not raw or raw == "/":
        return ""
    return "/" + raw.strip("/")


def _base_href(base_path: str, suffix: str = "/") -> str:
    base = _normalize_base_path(base_path)
    if not base:
        return suffix
    if suffix == "/":
        return f"{base}/"
    if suffix.startswith("/"):
        return f"{base}{suffix}"
    return f"{base}/{suffix}"


def _view_href(view: str, candidate_index: int, base_path: str) -> str:
    query = {"view": view}
    if view == "candidates":
        query["candidate"] = str(candidate_index)
    return _base_href(base_path, f"/?{urlencode(query)}")


def _report_href(report_key: str, candidate_index: int, base_path: str) -> str:
    return _base_href(base_path, f"/?{urlencode({'view': 'reports', 'candidate': candidate_index, 'report': report_key})}")


def _build_primary_result_api_body(root: Path, candidate_index: int = 0) -> bytes:
    exp_dir = resolve_experiments_path()
    payload = build_primary_result_api_payload(
        exp_dir,
        candidate_index=max(candidate_index, 0),
        require_current_pointer=True,
    )
    entry_guard = evaluate_stock_entry_guard(
        exp_dir=exp_dir,
        artifacts_dir=resolve_artifacts_path(),
    )
    if not entry_guard["ok"]:
        payload = _apply_entry_guard_fail_closed(payload, entry_guard)
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _build_stock_ai_runner_api_body(
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


def _render_stock_ai_runner_ops_page(root: Path, *, base_path: str = "", provider_name: str = "") -> str:
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


def _render_stock_ai_runner_result_replay_page(
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


def _json_script_tag(element_id: str, payload: dict[str, object]) -> str:
    json_text = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    return f'<script type="application/json" id="{html.escape(element_id)}">{json_text}</script>'


def _is_valid_primary_result_payload(payload: dict[str, object]) -> bool:
    if str(payload.get("schema_version", "") or "") != "primary_result_v1":
        return False
    stage = str(payload.get("result_lifecycle_stage", "") or "").strip()
    return bool(stage)


def _merge_primary_result_for_card(server_fact: dict[str, object], api_fact: dict[str, object]) -> dict[str, object]:
    merged = dict(server_fact)
    for key, value in api_fact.items():
        if key == "schema_version":
            continue
        merged[key] = value
    return merged


def _compare_primary_result_facts(server_fact: dict[str, object], api_fact: dict[str, object]) -> dict[str, tuple[object, object]]:
    diff: dict[str, tuple[object, object]] = {}
    for key in PRIMARY_RESULT_CORE_COMPARE_FIELDS:
        if server_fact.get(key) != api_fact.get(key):
            diff[key] = (server_fact.get(key), api_fact.get(key))
    return diff


def _apply_entry_guard_fail_closed(payload: dict[str, object], entry_guard: dict[str, object]) -> dict[str, object]:
    guarded = dict(payload)
    problems = [str(item) for item in (entry_guard.get("problems") or []) if str(item).strip()]
    guarded["disabled_reason"] = "stock entry guard blocked primary result publication."
    guarded["invalid_reason"] = None
    guarded["history_generation_mode"] = "blocked"
    guarded["data_sync_note"] = f"fail closed：{'；'.join(problems) if problems else 'stock entry guard failed'}。"
    guarded["entry_guard"] = {
        "ok": False,
        "problems": problems,
        "lifecycle_evidence_path": entry_guard.get("lifecycle_evidence_path"),
    }
    return guarded


def _render_stock_fail_closed_page(*, base_path: str, entry_guard: dict[str, object]) -> str:
    problems = [str(item) for item in (entry_guard.get("problems") or []) if str(item).strip()]
    problems_html = "".join(f"<li>{html.escape(item)}</li>" for item in problems) or "<li>未提供具体阻断原因</li>"
    fail_closed_style_tag = compose_inline_style_tag(compose_fail_closed_stylesheet())
    return compose_fail_closed_page_html(
        fail_closed_style_tag=fail_closed_style_tag,
        problems_html=problems_html,
        primary_result_api_href=_base_href(base_path, PRIMARY_RESULT_API_PATH),
    )


def _render_dashboard(
    root: Path,
    current_view: str = "overview",
    candidate_index: int = 0,
    current_report: str = "research",
    base_path: str = "",
) -> str:
    (_namespace, _scope), fallback_route = _resolve_namespace_scope_with_fallback(base_path)
    if _is_main_site_scope(base_path):
        html_text = render_main_site_home(base_path="" if fallback_route else base_path)
        if fallback_route:
            return f"<!-- airivo-route-fallback:{html.escape(fallback_route)} -->\n{html_text}"
        return html_text
    entry_guard = evaluate_stock_entry_guard(
        exp_dir=resolve_experiments_path(),
        artifacts_dir=resolve_artifacts_path(),
    )
    hard_fail_problems = [
        str(problem)
        for problem in (entry_guard.get("problems") or [])
        if "primary_result_lifecycle_evidence_latest.json" in str(problem)
    ]
    if current_view == "overview" and hard_fail_problems:
        return _render_stock_fail_closed_page(
            base_path=base_path,
            entry_guard={
                **entry_guard,
                "problems": hard_fail_problems,
            },
        )
    context = build_dashboard_context(root, candidate_index=candidate_index, base_path=base_path)
    domain = build_dashboard_domain_context(context)
    core = domain["core"]
    reports = domain["reports"]
    candidate = domain["candidate"]
    governance = domain["governance"]
    runtime = domain["runtime"]
    primary = domain["primary"]
    t12 = domain["t12"]
    semantics = domain["semantics"]
    view_labels = _view_labels(base_path)
    view_subtitles = _view_subtitles(base_path)
    candidate_index = int(core["candidate_index"])
    exp_dir = core["exp_dir"]
    daily_md_text = str(reports["daily_md_text"])
    translated_daily_md_text = str(reports["translated_daily_md_text"])
    latest_report_text = str(reports["latest_report_text"])
    health = core["health"]
    health_status = str(core["health_status"])
    health_tag = str(core["health_tag"])
    candidate_cards = candidate["cards"]
    top1 = candidate["top1"]
    top1_signal = str(candidate["top1_signal"])
    top1_risk = str(candidate["top1_risk"])
    bt_diag = core["bt_diag"]
    top1_label = str(candidate["top1_label"])
    summary_lines = candidate["summary_lines"]
    update_status = core["update_status"]
    update_health = core["update_health"]
    daily_md_href = str(reports["daily_md_href"])
    daily_md_download_href = str(reports["daily_md_download_href"])
    health_csv = reports["health_csv"]
    health_csv_href = str(reports["health_csv_href"])
    health_csv_download_href = str(reports["health_csv_download_href"])
    leaderboard_csv = reports["leaderboard_csv"]
    leaderboard_href = str(reports["leaderboard_href"])
    leaderboard_download_href = str(reports["leaderboard_download_href"])
    candidates_csv = reports["candidates_csv"]
    candidates_md_href = str(reports["candidates_md_href"])
    candidates_md_download_href = str(reports["candidates_md_download_href"])
    candidates_csv_href = str(reports["candidates_csv_href"])
    candidates_csv_download_href = str(reports["candidates_csv_download_href"])
    latest_report_href = str(reports["latest_report_href"])
    latest_report_download_href = str(reports["latest_report_download_href"])
    health_chart_html = str(reports["health_chart_html"])
    backtest_equity_html = str(reports["backtest_equity_html"])
    backtest_drawdown_html = str(reports["backtest_drawdown_html"])
    backtest_chart_html = str(reports["backtest_chart_html"])
    backtest_map_chart_html = str(reports["backtest_map_chart_html"])
    candidate_chart_html = str(reports["candidate_chart_html"])
    candidate_map_chart_html = str(reports["candidate_map_chart_html"])
    candidate_detail_html = str(reports["candidate_detail_html"])
    market_snapshot = candidate["market_snapshot"]
    basket_summary = candidate["basket_summary"]
    basket_validation = candidate["basket_validation"]
    candidate_basket_feedback = candidate["candidate_basket_feedback"]
    governance_cycle = governance["cycle"]
    governance_cycle_state = str(governance["cycle_state"])
    governance_recommended_action = str(governance["recommended_action"])
    governance_operator_message = str(governance["operator_message"])
    governance_release_readiness = governance["release_readiness"]
    governance_fully_release_ready = bool(governance["fully_release_ready"])
    previous_stable_run_id = str(governance["previous_stable_run_id"])
    research_batch_status = runtime["research_batch_status"]
    daily_research_runtime = runtime["daily_research_runtime"]
    effective_update_status = runtime["effective_update_status"]
    automation_health = runtime["automation_health"]
    candidate_artifact_status = candidate["candidate_artifact_status"]
    prefilter_artifact_status = candidate["prefilter_artifact_status"]
    research_topology = runtime["research_topology"]
    grid_backtest_status = runtime["grid_backtest_status"]
    evolution_status = runtime["evolution_status"]
    decision_semantics = semantics["decision"]
    blocker_semantics = semantics["blocker"]
    execution_semantics = semantics["execution"]
    evidence_semantics = semantics["evidence"]
    governance_semantics = semantics["governance"]
    update_timeline_panel = str(runtime["update_timeline_panel"])
    update_alerts_panel = str(runtime["update_alerts_panel"])
    backtest_scope = core["backtest_scope"]
    if current_report not in REPORT_LABELS:
        current_report = "research"
    if current_view not in view_labels:
        current_view = "overview"
    view_subtitle = view_subtitles.get(current_view, view_subtitles["overview"])
    report_state = str(core["report_state"])
    update_stage = str(core["view_update_stage"])
    run_freshness = str(core["run_freshness"])
    candidate_score = str(candidate["candidate_score"])
    candidate_name = str(candidate["candidate_name"])
    candidate_count = int(candidate["candidate_count"])
    candidate_source_label = str(candidate["candidate_source_label"])
    generation_mode_label = str(candidate["generation_mode_label"])
    current_basket_pointer_status = str(candidate["current_basket_pointer_status"])
    current_basket_pointer_updated_at = str(candidate["current_basket_pointer_updated_at"])
    current_basket_pointer_basket_id = str(candidate["current_basket_pointer_basket_id"])
    latest_basket_attempt_status = str(candidate["latest_basket_attempt_status"])
    latest_basket_attempt_generated_at = str(candidate["latest_basket_attempt_generated_at"])
    latest_basket_attempt_blocking_reason = str(candidate["latest_basket_attempt_blocking_reason"])
    headline_tone = str(core["headline_tone"])
    headline_detail = str(core["headline_detail"])
    primary_result = primary["primary_result"]
    first_place_evidence_cockpit_section = str(primary["first_place_evidence_cockpit_section"])
    primary_result_card_section = str(primary["primary_result_card_section"])
    stock_ai_explainer = primary["stock_ai_explainer"]
    t12_minimal_facts = t12["minimal_facts"]
    t12_governance_source_facts = t12["governance_source_facts"]
    primary_result_query = primary["primary_result_query"]
    primary_conclusion = primary_result_query["primary_conclusion"]
    t12_overview_card_section = render_t12_overview_card_template(
        build_t12_overview_card_view_model(t12_minimal_facts)
    )
    t12_governance_summary_section = render_t12_governance_summary_template(
        build_t12_governance_summary_view_model(t12_governance_source_facts)
    )
    primary_result_bridge_context = build_primary_result_bridge_context(
        current_view=current_view,
        primary_result_bridge_enabled=PRIMARY_RESULT_BRIDGE_ENABLED,
        primary_result_api_url=_base_href(base_path, PRIMARY_RESULT_API_PATH),
        primary_result_initial_json_html=_json_script_tag("primary-result-initial-json", primary_result),
    )
    cockpit_model = primary["cockpit_model"]
    artifacts_root = resolve_artifacts_path()
    observation_wait_status = {}
    daily_closure_latest = {}
    if isinstance(artifacts_root, Path):
        wait_path = artifacts_root / "primary_result_observation_wait_status_latest.json"
        if wait_path.exists():
            observation_wait_status = _load_json_file(wait_path)
    daily_closure_path = exp_dir / "primary_result_daily_closure_latest.json"
    if daily_closure_path.exists():
        daily_closure_latest = _load_json_file(daily_closure_path)
    runtime_view_model = build_stock_runtime_view_model(
        effective_update_status=effective_update_status,
        prefilter_artifact_status=prefilter_artifact_status,
        automation_health=automation_health,
        governance_recommended_action=governance_recommended_action,
        cockpit_model=cockpit_model,
        candidate_artifact_status=candidate_artifact_status,
        current_basket_pointer_status=current_basket_pointer_status,
        current_basket_pointer_updated_at=current_basket_pointer_updated_at,
        current_basket_pointer_basket_id=current_basket_pointer_basket_id,
        latest_basket_attempt_status=latest_basket_attempt_status,
        latest_basket_attempt_generated_at=latest_basket_attempt_generated_at,
        latest_basket_attempt_blocking_reason=latest_basket_attempt_blocking_reason,
        observation_wait_status=observation_wait_status,
        daily_closure_latest=daily_closure_latest,
    )
    update_status_label = str(runtime_view_model["update_status_label"])
    update_stage_label = str(runtime_view_model["update_stage_label"])
    prefilter_freshness_label = str(runtime_view_model["prefilter_freshness_label"])
    automation_health_label = str(runtime_view_model["automation_health_label"])
    governance_recommended_action_label = str(runtime_view_model["governance_recommended_action_label"])
    promotion_decision_label = str(runtime_view_model["promotion_decision_label"])
    db_latest_trade_date = str(runtime_view_model["db_latest_trade_date"])
    candidate_generated_at_label = str(runtime_view_model["candidate_generated_at_label"])
    candidate_basket_generated_at_label = str(runtime_view_model["candidate_basket_generated_at_label"])
    candidate_pointer_updated_label = str(runtime_view_model["candidate_pointer_updated_label"])
    observation_window_start_label = str(runtime_view_model["observation_window_start_label"])
    observation_generated_at_label = str(runtime_view_model["observation_generated_at_label"])
    observation_timeline_stale = bool(runtime_view_model["observation_timeline_stale"])
    observation_timeline_label = str(runtime_view_model["observation_timeline_label"])
    candidate_timeline_label = str(runtime_view_model["candidate_timeline_label"])
    timeline_consistency_note = str(runtime_view_model["timeline_consistency_note"])
    current_basket_pointer_label = str(runtime_view_model["current_basket_pointer_label"])
    latest_basket_attempt_label = str(runtime_view_model["latest_basket_attempt_label"])
    home_view_model = build_stock_home_view_model(
        headline_tone=headline_tone,
        headline_detail=headline_detail,
        current_basket_pointer_label=current_basket_pointer_label,
        latest_basket_attempt_label=latest_basket_attempt_label,
        current_basket_pointer_status=current_basket_pointer_status,
        current_basket_pointer_basket_id=current_basket_pointer_basket_id,
        latest_basket_attempt_status=latest_basket_attempt_status,
        latest_basket_attempt_blocking_reason=latest_basket_attempt_blocking_reason,
        health_status=health_status,
        health_score=str(health["score"]),
        top_code=str(top1.get("ts_code", "暂无")),
        candidate_name=candidate_name,
        candidate_generated_at=str(candidate_artifact_status.get("generated_at", "-")),
        generation_mode_label=generation_mode_label,
        update_status_label=update_status_label,
        update_stage_label=update_stage_label,
        candidate_count=candidate_count,
        candidate_score=candidate_score,
        candidate_timeline_label=candidate_timeline_label,
        run_freshness=run_freshness,
        db_latest_trade_date=db_latest_trade_date,
        observation_timeline_label=observation_timeline_label,
        prefilter_freshness_label=prefilter_freshness_label,
        backtest_scope_label=backtest_scope["label"],
        governance_cycle_state=governance_cycle_state,
        governance_recommended_action_label=governance_recommended_action_label,
        governance_ready_for_release=governance_release_readiness.get("ready_for_release", False),
        governance_fully_release_ready=governance_fully_release_ready,
        result_id=str(primary_conclusion.get("result_id", "primary:unavailable")),
        stage_label=str(primary_conclusion.get("stage_label", "阶段待确认")),
        result_subject=(
            f"{str(primary_conclusion.get('ts_code', '对象暂缺'))} "
            f"{str(primary_conclusion.get('stock_name', '名称暂缺'))}"
        ).strip(),
        dominant_regime=str(decision_semantics.get("market_regime", market_snapshot.get("dominant_regime", "-"))),
        risk_preference=str(decision_semantics.get("risk_preference", market_snapshot.get("risk_preference", "-"))),
        backtest_conclusion=str(decision_semantics.get("strategy_conclusion", bt_diag.get("结论", "未评估"))),
        avg_risk_pressure=str(market_snapshot.get("avg_risk_pressure", "0.0")),
        disabled_reason=str(primary_conclusion.get("disabled_reason", "")),
        invalid_reason=str(primary_conclusion.get("invalid_reason", "")),
        blocker_semantics=blocker_semantics,
        cockpit_model=cockpit_model,
        promotion_decision_label=promotion_decision_label,
    )
    home_hero_facts = home_view_model["home_hero_facts"]
    primary_result_home_facts = home_view_model["primary_result_home_facts"]
    control_strip_html = render_control_strip_section(home_view_model["control_strip_cards"])
    basket_dual_track_html = render_dual_track_rows(home_view_model["basket_dual_track_rows"])
    command_pointer_sentence = str(home_view_model["command_pointer_sentence"])
    command_attempt_sentence = str(home_view_model["command_attempt_sentence"])
    validation_basket_kpis = list(home_view_model["validation_basket_kpis"])
    headline_html = render_home_headline_section(home_hero_facts)
    external_decision_spine_html = render_external_decision_spine_section(home_view_model["external_decision_spine"])
    overview_disclosure_view_model = build_stock_overview_disclosure_view_model(
        update_status_label=update_status_label,
        update_stage_label=update_stage_label,
    )
    external_system_summary_html = render_overview_system_summary_section(
        control_strip_html=control_strip_html,
        disclosure_view_model=overview_disclosure_view_model,
    )
    view_title = view_labels.get(current_view, view_labels["overview"])
    overview_chrome_view_model = build_stock_overview_chrome_view_model(
        home_hero_facts=home_hero_facts,
        health_tag=health_tag,
        top1_label=top1_label,
        top1_signal=top1_signal,
        top1_risk=top1_risk,
        candidate_score=candidate_score,
        run_freshness=run_freshness,
        update_stage_label=update_stage_label,
        update_status_label=update_status_label,
        candidate_name=candidate_name,
        result_subject=(
            f"{str(primary_conclusion.get('ts_code', '对象暂缺'))} "
            f"{str(primary_conclusion.get('stock_name', '名称暂缺'))}"
        ).strip(),
        db_latest_trade_date=db_latest_trade_date,
        timeline_consistency_note=timeline_consistency_note,
        view_title=view_title,
        view_subtitle=view_subtitle,
        command_pointer_sentence=command_pointer_sentence,
        command_attempt_sentence=command_attempt_sentence,
    )
    page_shell_context = build_page_shell_context(
        current_view=current_view,
        candidate_index=candidate_index,
        base_path=base_path,
        view_labels=view_labels,
        health_status=health_status,
        update_status_label=update_status_label,
        update_stage_label=update_stage_label,
        report_state=report_state,
        db_latest_trade_date=db_latest_trade_date,
        candidate_timeline_label=candidate_timeline_label,
        observation_timeline_label=observation_timeline_label,
        prefilter_freshness_label=prefilter_freshness_label,
        automation_health_label=automation_health_label,
        server_sync_preflight=context.get("server_sync_preflight", {}),
    )
    sidebar_status_html = render_sidebar_status_section(page_shell_context["sidebar_stats"])
    topbar_pills_html = render_topbar_pills_section(page_shell_context["topbar_pills"])
    hero_side_html = render_hero_side_section(
        hero_side_view_model=overview_chrome_view_model["hero_side"],
        basket_dual_track_html=basket_dual_track_html,
    )
    command_deck_html = render_command_deck_section(
        command_focus_view_model=overview_chrome_view_model["command_focus"],
        command_runtime_view_model=overview_chrome_view_model["command_runtime"],
    )
    view_banner_html = render_view_banner_section(overview_chrome_view_model["view_banner"])
    spotlight_html = render_spotlight_section(overview_chrome_view_model["spotlight"])
    if not primary_result_card_section:
        primary_result_card_section = stock_primary_result_card_html(primary_result)
    primary_result_card_section = compose_primary_result_shell_html(
        primary_result_card_html=primary_result_card_section,
        primary_result_bridge_shell_html=stock_primary_result_bridge_shell_html(primary_result),
    )
    nav_html = render_nav_links(page_shell_context["nav_items"])
    jump_strip_html = render_jump_strip(build_jump_strip_context(current_view=current_view))
    section_visibility = build_section_visibility_context(current_view=current_view)

    def visible(name: str) -> bool:
        return bool(section_visibility.get(name))

    kpi_html = ""
    if visible("kpi"):
        kpi_html = render_overview_kpi_section(
            build_stock_overview_kpi_view_model(
                health_status=health_status,
                health_score=str(health["score"]),
                update_status_label=update_status_label,
                execution_semantics=execution_semantics,
                cockpit_model=cockpit_model,
                governance_semantics=governance_semantics,
            )
        )

    overview_visuals_section = render_overview_visuals_section(
        health_chart_html=health_chart_html,
        backtest_equity_html=backtest_equity_html,
        backtest_drawdown_html=backtest_drawdown_html,
        backtest_chart_html=backtest_chart_html,
    )

    candidate_visuals_section = render_candidate_visuals_section(
        candidate_map_chart_html=candidate_map_chart_html,
        candidate_chart_html=candidate_chart_html,
    )

    candidate_compare_section = render_candidate_compare_section(
        build_stock_candidate_compare_view_model(candidate_cards=candidate_cards)
    )

    basket_validation_summary = basket_validation.get("summary", {}) or {}
    basket_validation_variants = basket_validation.get("variants", {}) or {}
    best_variant = "-"
    if int(basket_validation_summary.get("rebalance_dates", 0) or 0) > 0 and basket_validation_variants:
        best_variant = str(
            max(
                basket_validation_variants.items(),
                key=lambda item: float((item[1] or {}).get("avg_excess_return_5d", float("-inf")) or float("-inf")),
            )[0]
        )
    diagnostics_view_model = build_stock_candidate_diagnostics_view_model(
        basket_validation_summary=basket_validation_summary,
        basket_validation_variants=basket_validation_variants,
        candidate_basket_feedback=candidate_basket_feedback,
        evolution_status=evolution_status,
        basket_summary=basket_summary,
        candidate_artifact_status=candidate_artifact_status,
        best_variant_label=translate_strategy_mode(best_variant),
    )
    validation_view_model = build_stock_validation_view_model(
        rebalance_dates=int(diagnostics_view_model["rebalance_dates"]),
        basket_validation_summary=basket_validation_summary,
        expected_basket_return=float(basket_summary.get("expected_basket_return", 0.0) or 0.0),
        risk_pressure_score=basket_summary.get("risk_pressure_score", 0.0),
        liquidity_capacity_state=str(diagnostics_view_model["liquidity_capacity_state"]),
        weighted_liquidity_score=float(diagnostics_view_model["weighted_liquidity_score"]),
        liquidity_capacity_weight=float(diagnostics_view_model["liquidity_capacity_weight"]),
        candidate_feedback_window=str(diagnostics_view_model["candidate_feedback_window"]),
        candidate_feedback_level=str(diagnostics_view_model["candidate_feedback_level"]),
        candidate_feedback_summary=str(diagnostics_view_model["candidate_feedback_summary"]),
        candidate_feedback_changes=diagnostics_view_model["candidate_feedback_changes"],
        candidate_runtime_stage_label=str(diagnostics_view_model["candidate_runtime_stage_label"]),
        candidate_runtime_status_label=str(diagnostics_view_model["candidate_runtime_status_label"]),
        candidate_runtime_detail=str(diagnostics_view_model["candidate_runtime_detail"]),
        candidate_runtime_results_ready=str(diagnostics_view_model["candidate_runtime_results_ready"]),
        candidate_runtime_skipped=str(diagnostics_view_model["candidate_runtime_skipped"]),
        candidate_runtime_updated_label=str(diagnostics_view_model["candidate_runtime_updated_label"]),
        candidate_runtime_elapsed=str(diagnostics_view_model["candidate_runtime_elapsed"]),
        guardrail_mode_label=translate_guardrail_mode(str(basket_summary.get("guardrail_mode", "normal"))),
        guardrail_reasons_label=",".join(basket_summary.get("guardrail_reasons", [])) or "未触发防守降级",
        generation_mode_label=generation_mode_label,
        generation_reason=str(candidate_artifact_status.get("generation_reason", "-")),
        validation_basket_kpis=validation_basket_kpis,
        skipped_count=basket_summary.get("skipped_count", 0),
        best_variant_label=str(diagnostics_view_model["best_variant_label"]),
        strategy_mode_label=translate_strategy_mode(candidate_artifact_status.get("strategy_mode", "-")),
        strategy_strictness_label=translate_strategy_strictness(candidate_artifact_status.get("strategy_strictness", "-")),
        strategy_weak_market_action_label=translate_weak_market_action(candidate_artifact_status.get("strategy_weak_market_action", "-")),
        diversified_avg_excess_return_5d_label=str(diagnostics_view_model["diversified_avg_excess_return_5d_label"]),
        raw_avg_excess_return_5d_label=str(diagnostics_view_model["raw_avg_excess_return_5d_label"]),
        top1_avg_excess_return_5d_label=str(diagnostics_view_model["top1_avg_excess_return_5d_label"]),
        avg_basket_return_5d_label=str(diagnostics_view_model["avg_basket_return_5d_label"]),
        avg_excess_return_5d_label=str(diagnostics_view_model["avg_excess_return_5d_label"]),
        basket_win_rate_5d_label=str(diagnostics_view_model["basket_win_rate_5d_label"]),
        avg_top1_return_5d_label=str(diagnostics_view_model["avg_top1_return_5d_label"]),
    )
    validation_section = render_validation_section(validation_view_model)

    market_snapshot_section = render_market_snapshot_section(market_snapshot=market_snapshot)

    today_brief_html = render_primary_result_home_brief_section(primary_result_home_facts)

    opportunity_cards_html = ""
    if candidate_cards:
        opportunities_view_model = build_stock_opportunities_view_model(
            top_code=str(top1.get("ts_code", "暂无")),
            candidate_name=candidate_name,
            top1_signal=top1_signal,
            top1_risk=top1_risk,
            candidate_count=candidate_count,
            candidate_generated_at=str(candidate_artifact_status.get("generated_at", "-")),
            generation_mode_label=generation_mode_label,
            basket_dual_track_rows=home_view_model["basket_dual_track_rows"],
            candidate_cards=candidate_cards,
            candidate_hrefs=[_view_href("candidates", idx, base_path) for idx, _ in enumerate(candidate_cards[:5])],
        )
        opportunity_cards_html = render_opportunities_section(opportunities_view_model=opportunities_view_model)

    research_visuals_section = render_research_visuals_section(
        health_chart_html=health_chart_html,
        backtest_equity_html=backtest_equity_html,
        backtest_drawdown_html=backtest_drawdown_html,
        backtest_map_chart_html=backtest_map_chart_html,
    )

    architecture_section = ""
    if visible("architecture"):
        architecture_section = render_architecture_section(page_shell_context["architecture_steps"])

    summary_section = ""
    if visible("summary"):
        summary_view_model = build_stock_summary_view_model(
            health_status=health_status,
            backtest_conclusion=str(bt_diag.get("结论", "未评估")),
            top_code=str(top1.get("ts_code", "暂无")),
            candidate_name=candidate_name,
            health_score=str(health["score"]),
            candidate_generated_at=str(candidate_artifact_status.get("generated_at", "-")),
            backtest_scope_label=backtest_scope["label"],
            basket_dual_track_rows=home_view_model["basket_dual_track_rows"],
            execution_semantics=execution_semantics,
            blocker_semantics=blocker_semantics,
            governance_semantics=governance_semantics,
            evidence_semantics=evidence_semantics,
            evolution_capacity_gate_status=str(diagnostics_view_model["evolution_capacity_gate_status"]),
            evolution_capacity_state=str(diagnostics_view_model["evolution_capacity_state"]),
            evolution_capacity_profile=str(diagnostics_view_model["evolution_capacity_profile"]),
            evolution_capacity_stress_score=str(diagnostics_view_model["evolution_capacity_stress_score"]),
            liquidity_capacity_state=str(diagnostics_view_model["liquidity_capacity_state"]),
            governance_cycle_state_label=_display_status_label(governance_cycle_state),
            governance_decision_label=_display_status_label(governance_cycle.get("governance_inputs", {}).get("governance_decision", "unknown")),
            governance_recommended_action_label=governance_recommended_action_label,
            governance_audit_status_label=_display_status_label(governance_cycle.get("governance_inputs", {}).get("governance_audit_status", "unknown")),
            governance_ready_for_release=governance_release_readiness.get("ready_for_release", False),
            governance_fully_release_ready=governance_fully_release_ready,
            previous_stable_run_id=previous_stable_run_id,
            governance_operator_message=governance_operator_message,
            summary_lines=summary_lines,
            ai_explainer=stock_ai_explainer,
        )
        summary_section = render_summary_section(summary_view_model)

    actions_section = ""
    if visible("actions"):
        actions_view_model = build_stock_actions_view_model(
            candidate_generated_at=str(candidate_artifact_status.get("generated_at", "-")),
            basket_generated_at=str(candidate_artifact_status.get("basket_generated_at", "-")),
            candidate_source_label=candidate_source_label,
            generation_mode_label=generation_mode_label,
            dominant_regime=str(market_snapshot.get("dominant_regime", "-")),
            risk_preference=str(market_snapshot.get("risk_preference", "-")),
            avg_risk_pressure=str(market_snapshot.get("avg_risk_pressure", "0.0")),
            basket_dual_track_rows=home_view_model["basket_dual_track_rows"],
        )
        actions_render_contract = build_candidate_actions_render_contract(
            candidates_csv,
            card_top_n=5,
            table_top_n=10,
            card_hrefs=[_view_href("candidates", idx, base_path) for idx in range(5)],
        )
        actions_section = render_actions_section(
            actions_view_model=actions_view_model,
            actions_render_contract=actions_render_contract,
        )

    prefilter_view_model = build_stock_prefilter_view_model(
        prefilter_artifact_status=prefilter_artifact_status,
        db_latest_trade_date=db_latest_trade_date,
        top_candidates=prefilter_artifact_status.get("top_candidates", []) or [],
        exclusion_summary_rows=prefilter_artifact_status.get("exclusion_summary", []) or [],
        top_exclusion_rows=prefilter_artifact_status.get("top_exclusions", []) or [],
    )
    prefilter_section = ""
    if visible("prefilter"):
        prefilter_section = render_prefilter_section(prefilter_view_model)

    def _safe_int(value: str | int | float, default: int = 0) -> int:
        try:
            return int(float(value))
        except Exception:
            return default

    market_sample_count = _safe_int(prefilter_artifact_status.get("market_symbol_count", "0"))
    prefilter_pass_count = _safe_int(prefilter_artifact_status.get("row_count", "0"))
    final_candidate_count = int(candidate_count)
    funnel_view_model = build_stock_selection_funnel_view_model(
        market_sample_count=market_sample_count,
        prefilter_pass_count=prefilter_pass_count,
        final_candidate_count=final_candidate_count,
        top_code=str(top1.get("ts_code", "-")),
        pass_rate_pct=str(prefilter_artifact_status.get("pass_rate_pct", "0.0%")),
        top_exclusion_reason=str(prefilter_view_model["top_exclusion_reason"]),
        top_exclusion_reason_count=str(prefilter_artifact_status.get("top_exclusion_reason_count", "0")),
        configured_liquidity_min_turnover=str(prefilter_artifact_status.get("configured_liquidity_min_turnover", "0")),
        effective_liquidity_min_turnover=str(prefilter_artifact_status.get("effective_liquidity_min_turnover", "0")),
    )
    selection_funnel_section = render_selection_funnel_section(funnel_view_model)

    operations_effective_update_status = dict(effective_update_status)
    operations_effective_update_status["status"] = _display_status_label(effective_update_status.get("status"))
    operations_effective_update_status["stage"] = _display_status_label(effective_update_status.get("stage"))
    operations_daily_research_runtime = dict(daily_research_runtime)
    operations_daily_research_runtime["state"] = _display_status_label(daily_research_runtime.get("state"))
    operations_daily_research_runtime["stage"] = _display_status_label(daily_research_runtime.get("stage"))
    operations_research_batch_status = dict(research_batch_status)
    operations_research_batch_status["status"] = _display_status_label(research_batch_status.get("status"))
    operations_section = render_operations_section(
        build_operations_render_contract(
            visible=visible("operations"),
            effective_update_status=operations_effective_update_status,
            automation_health=automation_health,
            update_health=update_health,
            update_timeline_panel=update_timeline_panel,
            update_alerts_panel=update_alerts_panel,
            daily_research_runtime=operations_daily_research_runtime,
            research_topology=research_topology,
            research_batch_status=operations_research_batch_status,
            evolution_status=evolution_status,
            grid_backtest_status=grid_backtest_status,
            progress_pct_label=runtime["progress_pct_label"],
            server_sync_preflight=context.get("server_sync_preflight", {}),
        )
    )
    overview_visuals_disclosure_section = render_overview_visuals_disclosure_section(
        overview_visuals_section=overview_visuals_section
    )
    overview_operations_disclosure_section = (
        render_overview_operations_disclosure_section(
            operations_section=operations_section,
            external_system_summary_html=external_system_summary_html,
            disclosure_view_model=overview_disclosure_view_model,
        )
    )

    top1_section = ""
    if visible("top1"):
        top1_section = render_top1_section(
            top1_view_model=build_stock_top1_view_model(top1=top1, top1_signal=top1_signal, top1_risk=top1_risk)
        )

    candidate_focus_section = ""
    if visible("candidate_focus"):
        candidate_focus_view_model = build_stock_candidate_focus_view_model(
            top1=top1,
            top1_signal=top1_signal,
            top1_risk=top1_risk,
            candidate_cards=candidate_cards,
            candidate_index=candidate_index,
        )
        candidate_focus_section = render_candidate_focus_section(
            candidate_focus_view_model=candidate_focus_view_model,
            candidate_focus_render_contract=build_candidate_focus_render_contract(
                candidate_focus_view_model=candidate_focus_view_model,
                candidate_cards=candidate_cards,
                candidate_index=candidate_index,
                base_path=base_path,
            ),
            candidate_detail_html=candidate_detail_html,
        )

    diagnosis_section = ""
    if visible("diagnosis"):
        diagnosis_section = render_diagnosis_section(
            bt_diag=bt_diag,
            next_check=_display_missing(cockpit_model.get("next_check"), "-"),
        )

    diagnostics_appendix_section = ""
    if current_view == "overview":
        diagnostics_appendix_section = render_diagnostics_appendix_section(
            validation_section=validation_section,
            diagnosis_section=diagnosis_section,
            prefilter_section=prefilter_section,
        )

    links_section = ""
    if visible("links"):
        links_section = render_links_section(
            build_stock_links_view_model(
                daily_md_href=daily_md_href,
                daily_md_download_href=daily_md_download_href,
                health_csv_href=health_csv_href,
                health_csv_download_href=health_csv_download_href,
                leaderboard_href=leaderboard_href,
                leaderboard_download_href=leaderboard_download_href,
                candidates_md_href=candidates_md_href,
                candidates_md_download_href=candidates_md_download_href,
                candidates_csv_href=candidates_csv_href,
                candidates_csv_download_href=candidates_csv_download_href,
                latest_report_href=latest_report_href,
                latest_report_download_href=latest_report_download_href,
                evolution_report_href=_report_href("evolution", candidate_index, base_path),
            )
        )

    guide_section = ""
    if visible("guide"):
        guide_section = render_guide_section(build_stock_guide_view_model())

    charts_section = ""
    if visible("charts"):
        charts_section = render_charts_section(
            build_stock_charts_view_model(
                health_chart_html=health_chart_html,
                backtest_equity_html=backtest_equity_html,
                backtest_drawdown_html=backtest_drawdown_html,
                backtest_chart_html=backtest_chart_html,
                backtest_map_chart_html=backtest_map_chart_html,
                candidate_map_chart_html=candidate_map_chart_html,
                candidate_chart_html=candidate_chart_html,
            )
        )

    reports_section = render_reports_section(build_reports_render_contract(
        visible=visible("reports"),
        current_report=current_report,
        candidate_index=candidate_index,
        base_path=base_path,
        report_labels=REPORT_LABELS,
        daily_md_text=daily_md_text,
        translated_daily_md_text=translated_daily_md_text,
        health_csv=health_csv,
        leaderboard_csv=leaderboard_csv,
        latest_report_text=latest_report_text,
        evolution_status=evolution_status,
        backtest_scope=backtest_scope,
        daily_md_download_href=daily_md_download_href,
        health_csv_download_href=health_csv_download_href,
        leaderboard_download_href=leaderboard_download_href,
        latest_report_download_href=latest_report_download_href,
        report_href_builder=_report_href,
    ))

    main_content_sections = build_main_content_sections(
        first_place_evidence_cockpit_section=first_place_evidence_cockpit_section,
        summary_section=summary_section,
        primary_result_card_section=primary_result_card_section,
        kpi_html=kpi_html,
        t12_overview_card_section=t12_overview_card_section,
        spotlight_html=spotlight_html,
        overview_visuals_disclosure_section=overview_visuals_disclosure_section,
        overview_operations_disclosure_section=overview_operations_disclosure_section,
        links_section=links_section,
        opportunity_cards_html=opportunity_cards_html,
        candidate_compare_section=candidate_compare_section,
        market_snapshot_section=market_snapshot_section,
        selection_funnel_section=selection_funnel_section,
        diagnostics_appendix_section=diagnostics_appendix_section,
        validation_section=validation_section,
        diagnosis_section=diagnosis_section,
        prefilter_section=prefilter_section,
        architecture_section=architecture_section,
        research_visuals_section=research_visuals_section,
        candidate_focus_section=candidate_focus_section,
        actions_section=actions_section,
        candidate_visuals_section=candidate_visuals_section,
        operations_section=operations_section,
        reports_section=reports_section,
        t12_governance_summary_section=t12_governance_summary_section,
        charts_section=charts_section,
        top1_section=top1_section,
        guide_section=guide_section,
    )
    main_content_html = compose_main_content_html(current_view=current_view, sections=main_content_sections)
    if current_view == "overview":
        main_content_html = (
            '<div class="stack">'
            f"{first_place_evidence_cockpit_section}"
            f"{summary_section}"
            f"{primary_result_card_section}"
            f"{kpi_html}"
            f"{opportunity_cards_html}"
            f"{main_content_html}"
            "</div>"
        )

    top_story_context = build_top_story_context(
        current_view=current_view,
        external_decision_spine_html=external_decision_spine_html,
        external_system_summary_html=external_system_summary_html,
        jump_strip_html=jump_strip_html,
        view_banner_html=view_banner_html,
        control_strip_html=control_strip_html,
        headline_html=headline_html,
        today_brief_html=today_brief_html,
        hero_side_html=hero_side_html,
        command_deck_html=command_deck_html,
        top1_label=top1_label,
        top1_signal=top1_signal,
        top1_risk=top1_risk,
    )
    top_story_html = compose_top_story_html(**top_story_context)
    page_shell_contract = build_dashboard_page_shell_contract(
        nav_html=nav_html,
        sidebar_status_html=sidebar_status_html,
        topbar_pills_html=topbar_pills_html,
        top_story_html=top_story_html,
        current_view=current_view,
        kpi_html=kpi_html,
        primary_result_bridge_json=str(primary_result_bridge_context["initial_json_html"]),
        main_content_html=main_content_html,
    )
    page_shell_html = compose_page_shell_html(**page_shell_contract)
    primary_result_bridge_bootstrap_script = compose_primary_result_bridge_bootstrap_script(
        primary_result_bridge_context
    )
    primary_result_bridge_client_script = compose_primary_result_bridge_client_script(
        primary_result_bridge_bootstrap_script=primary_result_bridge_bootstrap_script,
        primary_result_core_compare_fields=PRIMARY_RESULT_CORE_COMPARE_FIELDS,
    )
    table_export_script = compose_table_export_script()
    page_interaction_script = compose_page_interaction_script(
        build_page_interaction_context(
            current_view=current_view,
            candidate_index=candidate_index,
            candidate_count=len(candidate_cards),
            candidate_base_href=_view_href("candidates", 0, base_path),
        )
    )
    dashboard_style_tag = compose_inline_style_tag(
        compose_dashboard_stylesheet(is_t12_scope=_is_t12_scope(base_path))
    )
    dashboard_script_tag = compose_dashboard_script_tag(
        primary_result_bridge_client_script,
        table_export_script,
        page_interaction_script,
    )
    dashboard_asset_contract = build_dashboard_asset_contract(
        dashboard_style_tag=dashboard_style_tag,
        dashboard_script_tag=dashboard_script_tag,
    )

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>{dashboard_asset_contract["document_title"]}</title>
  {dashboard_asset_contract["dashboard_style_tag"]}
</head>
  {page_shell_html}
  {dashboard_asset_contract["dashboard_script_tag"]}
  </div>
</body>
</html>"""


class DashboardHandler(BaseHTTPRequestHandler):
    root_dir: Path = Path(".").resolve()
    base_path: str = ""

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        base_path = _normalize_base_path(self.base_path)
        request_path = parsed.path
        if base_path:
            if request_path == base_path:
                request_path = "/"
            elif request_path.startswith(base_path + "/"):
                request_path = request_path[len(base_path):]

        if request_path == PRIMARY_RESULT_API_PATH:
            query = parse_qs(parsed.query)
            try:
                candidate_index = int(query.get("candidate", ["0"])[0])
            except Exception:
                candidate_index = 0
            body = _build_primary_result_api_body(
                self.root_dir,
                candidate_index=max(candidate_index, 0),
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if request_path == STOCK_AI_RUNNER_OPS_PATH:
            if _is_t12_scope(self.base_path):
                self.send_error(404, "stock ai runner ops is disabled on /T12")
                return
            query = parse_qs(parsed.query)
            body = _render_stock_ai_runner_ops_page(
                self.root_dir,
                base_path=base_path,
                provider_name=str(query.get("provider", [""])[0] or ""),
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if request_path == STOCK_AI_RUNNER_RESULT_REPLAY_OPS_PATH:
            if _is_t12_scope(self.base_path):
                self.send_error(404, "stock ai runner result replay is disabled on /T12")
                return
            query = parse_qs(parsed.query)
            body = _render_stock_ai_runner_result_replay_page(
                self.root_dir,
                base_path=base_path,
                result_id=str(query.get("result_id", [""])[0] or ""),
                replay_window=int(query.get("replay_window", ["8"])[0] or 8),
                recorded_at_from=str(query.get("recorded_at_from", [""])[0] or ""),
                recorded_at_to=str(query.get("recorded_at_to", [""])[0] or ""),
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if request_path == STOCK_AI_RUNNER_API_PATH or request_path.startswith(STOCK_AI_RUNNER_API_PATH + "/"):
            if _is_t12_scope(self.base_path):
                self.send_error(404, "stock ai runner api is disabled on /T12")
                return
            query = parse_qs(parsed.query)
            resource = ""
            if request_path.startswith(STOCK_AI_RUNNER_API_PATH + "/"):
                resource = request_path[len(STOCK_AI_RUNNER_API_PATH) + 1:]
            body = _build_stock_ai_runner_api_body(
                self.root_dir,
                resource=resource,
                provider_name=str(query.get("provider_name", [""])[0] or ""),
                result_id=str(query.get("result_id", [""])[0] or ""),
                replay_window=int(query.get("replay_window", ["8"])[0] or 8),
                recorded_at_from=str(query.get("recorded_at_from", [""])[0] or ""),
                recorded_at_to=str(query.get("recorded_at_to", [""])[0] or ""),
                health_window=int(query.get("health_window", ["8"])[0] or 8),
                trend_short_window=int(query.get("trend_short_window", ["8"])[0] or 8),
                trend_long_window=int(query.get("trend_long_window", ["16"])[0] or 16),
                top_n=int(query.get("top_n", ["5"])[0] or 5),
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if request_path.startswith("/file/") or request_path.startswith("/download/"):
            is_download = request_path.startswith("/download/")
            prefix = "/download/" if is_download else "/file/"
            rel = unquote(request_path[len(prefix):]).lstrip("/")
            target = (self.root_dir / rel).resolve()
            if not str(target).startswith(str(self.root_dir)):
                self.send_error(403, "禁止访问")
                return
            if not target.exists() or not target.is_file():
                self.send_error(404, "文件不存在")
                return
            content = target.read_bytes()
            content_type = "text/plain; charset=utf-8"
            if target.suffix.lower() == ".csv":
                content_type = "text/csv; charset=utf-8"
            elif target.suffix.lower() == ".md":
                content_type = "text/markdown; charset=utf-8"
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            if is_download:
                self.send_header("Content-Disposition", f'attachment; filename="{target.name}"')
            else:
                self.send_header("Content-Disposition", f'inline; filename="{target.name}"')
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
            return

        query = parse_qs(parsed.query)
        default_view = "t12" if _is_t12_scope(self.base_path) else "overview"
        view = query.get("view", [default_view])[0]
        if view not in _view_labels(self.base_path):
            view = default_view
        try:
            candidate_index = int(query.get("candidate", ["0"])[0])
        except Exception:
            candidate_index = 0
        report_key = query.get("report", ["research"])[0]

        body = _render_dashboard(
            self.root_dir,
            current_view=view,
            candidate_index=candidate_index,
            current_report=report_key,
            base_path=base_path,
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    parser = argparse.ArgumentParser(description="股票系统浏览器看板")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址")
    parser.add_argument("--port", type=int, default=8765, help="监听端口")
    parser.add_argument("--base-path", default="", help="Optional URL base path such as /stock")
    args = parser.parse_args()

    DashboardHandler.root_dir = resolve_project_path('.')
    DashboardHandler.base_path = _normalize_base_path(args.base_path)
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print(f"看板已启动: http://{args.host}:{args.port}{DashboardHandler.base_path or ''}")
    server.serve_forever()


if __name__ == "__main__":
    main()
