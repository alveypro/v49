import argparse
import html
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from src.stock_dashboard_constants import REPORT_LABELS, VIEW_LABELS
from src import stock_dashboard_http_routes
from src.stock_dashboard_http_routes import (
    DashboardRouteBuilders,
    RouteError,
    RouteResponse,
    build_dashboard_page_request,
    build_dashboard_route_response,
    build_file_route_response,
)
from src.stock_top5_trader_brief_health_api import build_top5_trader_brief_health_body
from src.stock_dashboard_url import (
    base_href as _base_href,
    is_main_site_scope as _is_main_site_scope,
    is_t12_scope as _is_t12_scope,
    normalize_base_path as _normalize_base_path,
    report_href as _report_href,
    resolve_namespace_id as _resolve_namespace_id,
    resolve_namespace_scope_with_fallback as _resolve_namespace_scope_with_fallback,
    resolve_scope_id as _resolve_scope_id,
    view_href as _view_href,
)
from src.stock_dashboard_view_contract import view_labels as _view_labels, view_subtitles as _view_subtitles
from src.stock_dashboard_fail_closed_page import render_stock_fail_closed_page, select_hard_fail_closed_problems
from src.stock_dashboard_render_inputs import build_stock_dashboard_render_inputs
from src.stock_dashboard_page_sections import compose_stock_dashboard_page_html
from src.main_site_home import render_main_site_home
from src.stock_entry_guard import evaluate_stock_entry_guard
from src import stock_ai_runner_routes
from src.unified_result_builder import build_primary_result_api_payload
from src.utils.project_paths import resolve_artifacts_path, resolve_experiments_path, resolve_project_path

PRIMARY_RESULT_API_PATH = stock_dashboard_http_routes.PRIMARY_RESULT_API_PATH
TOP5_TRADER_BRIEF_HEALTH_PATH = stock_dashboard_http_routes.TOP5_TRADER_BRIEF_HEALTH_PATH
STOCK_AI_RUNNER_API_PATH = stock_ai_runner_routes.STOCK_AI_RUNNER_API_PATH
STOCK_AI_RUNNER_OPS_PATH = stock_ai_runner_routes.STOCK_AI_RUNNER_OPS_PATH
STOCK_AI_RUNNER_RESULT_REPLAY_OPS_PATH = stock_ai_runner_routes.STOCK_AI_RUNNER_RESULT_REPLAY_OPS_PATH
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


_build_stock_ai_runner_api_body = stock_ai_runner_routes.build_stock_ai_runner_api_body
_render_stock_ai_runner_ops_page = stock_ai_runner_routes.render_stock_ai_runner_ops_page
_render_stock_ai_runner_result_replay_page = stock_ai_runner_routes.render_stock_ai_runner_result_replay_page


def _dashboard_route_builders() -> DashboardRouteBuilders:
    return DashboardRouteBuilders(
        build_primary_result_api_body=_build_primary_result_api_body,
        build_top5_trader_brief_health_body=build_top5_trader_brief_health_body,
        build_stock_ai_runner_api_body=_build_stock_ai_runner_api_body,
        render_stock_ai_runner_ops_page=_render_stock_ai_runner_ops_page,
        render_stock_ai_runner_result_replay_page=_render_stock_ai_runner_result_replay_page,
        is_t12_scope=_is_t12_scope,
    )


def _send_route_response(handler: BaseHTTPRequestHandler, response: RouteResponse) -> None:
    handler.send_response(response.status)
    handler.send_header("Content-Type", response.content_type)
    if response.content_disposition:
        handler.send_header("Content-Disposition", response.content_disposition)
    handler.send_header("Content-Length", str(len(response.body)))
    handler.end_headers()
    handler.wfile.write(response.body)


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
    hard_fail_problems = select_hard_fail_closed_problems(entry_guard)
    if current_view == "overview" and hard_fail_problems:
        return render_stock_fail_closed_page(
            base_path=base_path,
            entry_guard={
                **entry_guard,
                "problems": hard_fail_problems,
            },
        )
    ri = build_stock_dashboard_render_inputs(
        root=root,
        current_view=current_view,
        candidate_index=candidate_index,
        current_report=current_report,
        base_path=base_path,
        report_labels=REPORT_LABELS,
        view_labels_builder=_view_labels,
        view_subtitles_builder=_view_subtitles,
        primary_result_bridge_enabled=PRIMARY_RESULT_BRIDGE_ENABLED,
    )
    return compose_stock_dashboard_page_html(
        render_inputs=ri,
        base_path=base_path,
        primary_result_core_compare_fields=PRIMARY_RESULT_CORE_COMPARE_FIELDS,
        display_missing=_display_missing,
        display_status_label=_display_status_label,
        is_t12_scope=_is_t12_scope,
        view_href=_view_href,
    )


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

        query = parse_qs(parsed.query)
        route_response = build_dashboard_route_response(
            root_dir=self.root_dir,
            request_path=request_path,
            query=query,
            base_path=base_path,
            raw_base_path=self.base_path,
            builders=_dashboard_route_builders(),
        )
        if isinstance(route_response, RouteError):
            self.send_error(route_response.status, route_response.message)
            return
        if isinstance(route_response, RouteResponse):
            _send_route_response(self, route_response)
            return

        file_response = build_file_route_response(root_dir=self.root_dir, request_path=request_path)
        if isinstance(file_response, RouteError):
            self.send_error(file_response.status, file_response.message)
            return
        if isinstance(file_response, RouteResponse):
            _send_route_response(self, file_response)
            return

        page_request = build_dashboard_page_request(
            query=query,
            raw_base_path=self.base_path,
            view_labels=_view_labels,
            is_t12_scope=_is_t12_scope,
        )

        body = _render_dashboard(
            self.root_dir,
            current_view=page_request.view,
            candidate_index=page_request.candidate_index,
            current_report=page_request.report_key,
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
