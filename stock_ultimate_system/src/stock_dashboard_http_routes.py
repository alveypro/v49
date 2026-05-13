from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib.parse import unquote

from src import stock_ai_runner_routes


@dataclass(frozen=True)
class RouteResponse:
    status: int
    content_type: str
    body: bytes
    content_disposition: str = ""


@dataclass(frozen=True)
class RouteError:
    status: int
    message: str


@dataclass(frozen=True)
class DashboardRouteBuilders:
    build_primary_result_api_body: Callable[[Path, int], bytes]
    build_top5_trader_brief_health_body: Callable[[Path], bytes]
    build_stock_ai_runner_api_body: Callable[..., bytes]
    render_stock_ai_runner_ops_page: Callable[..., str]
    render_stock_ai_runner_result_replay_page: Callable[..., str]
    is_t12_scope: Callable[[str], bool]


@dataclass(frozen=True)
class DashboardPageRequest:
    view: str
    candidate_index: int
    report_key: str


PRIMARY_RESULT_API_PATH = "/api/primary-result"
TOP5_TRADER_BRIEF_HEALTH_PATH = "/api/top5-trader-brief-health"
STOCK_AI_RUNNER_API_PATH = stock_ai_runner_routes.STOCK_AI_RUNNER_API_PATH
STOCK_AI_RUNNER_OPS_PATH = stock_ai_runner_routes.STOCK_AI_RUNNER_OPS_PATH
STOCK_AI_RUNNER_RESULT_REPLAY_OPS_PATH = stock_ai_runner_routes.STOCK_AI_RUNNER_RESULT_REPLAY_OPS_PATH


def _query_value(query: dict[str, list[str]], key: str, default: str = "") -> str:
    return str(query.get(key, [default])[0] or default)


def _query_int(query: dict[str, list[str]], key: str, default: int) -> int:
    return int(query.get(key, [str(default)])[0] or default)


def build_dashboard_page_request(
    *,
    query: dict[str, list[str]],
    raw_base_path: str,
    view_labels: Callable[[str], dict[str, str]],
    is_t12_scope: Callable[[str], bool],
) -> DashboardPageRequest:
    default_view = "t12" if is_t12_scope(raw_base_path) else "overview"
    view = _query_value(query, "view", default_view)
    if view not in view_labels(raw_base_path):
        view = default_view

    try:
        candidate_index = int(query.get("candidate", ["0"])[0])
    except Exception:
        candidate_index = 0

    return DashboardPageRequest(
        view=view,
        candidate_index=candidate_index,
        report_key=_query_value(query, "report", "research"),
    )


def build_dashboard_route_response(
    *,
    root_dir: Path,
    request_path: str,
    query: dict[str, list[str]],
    base_path: str,
    raw_base_path: str,
    builders: DashboardRouteBuilders,
) -> RouteResponse | RouteError | None:
    if request_path == PRIMARY_RESULT_API_PATH:
        try:
            candidate_index = int(query.get("candidate", ["0"])[0])
        except Exception:
            candidate_index = 0
        return RouteResponse(
            status=200,
            content_type="application/json; charset=utf-8",
            body=builders.build_primary_result_api_body(root_dir, max(candidate_index, 0)),
        )

    if request_path == TOP5_TRADER_BRIEF_HEALTH_PATH:
        return RouteResponse(
            status=200,
            content_type="application/json; charset=utf-8",
            body=builders.build_top5_trader_brief_health_body(root_dir),
        )

    if request_path == STOCK_AI_RUNNER_OPS_PATH:
        if builders.is_t12_scope(raw_base_path):
            return RouteError(status=404, message="stock ai runner ops is disabled on /T12")
        body = builders.render_stock_ai_runner_ops_page(
            root_dir,
            base_path=base_path,
            provider_name=_query_value(query, "provider"),
        ).encode("utf-8")
        return RouteResponse(status=200, content_type="text/html; charset=utf-8", body=body)

    if request_path == STOCK_AI_RUNNER_RESULT_REPLAY_OPS_PATH:
        if builders.is_t12_scope(raw_base_path):
            return RouteError(status=404, message="stock ai runner result replay is disabled on /T12")
        body = builders.render_stock_ai_runner_result_replay_page(
            root_dir,
            base_path=base_path,
            result_id=_query_value(query, "result_id"),
            replay_window=_query_int(query, "replay_window", 8),
            recorded_at_from=_query_value(query, "recorded_at_from"),
            recorded_at_to=_query_value(query, "recorded_at_to"),
        ).encode("utf-8")
        return RouteResponse(status=200, content_type="text/html; charset=utf-8", body=body)

    if request_path == STOCK_AI_RUNNER_API_PATH or request_path.startswith(STOCK_AI_RUNNER_API_PATH + "/"):
        if builders.is_t12_scope(raw_base_path):
            return RouteError(status=404, message="stock ai runner api is disabled on /T12")
        resource = ""
        if request_path.startswith(STOCK_AI_RUNNER_API_PATH + "/"):
            resource = request_path[len(STOCK_AI_RUNNER_API_PATH) + 1 :]
        return RouteResponse(
            status=200,
            content_type="application/json; charset=utf-8",
            body=builders.build_stock_ai_runner_api_body(
                root_dir,
                resource=resource,
                provider_name=_query_value(query, "provider_name"),
                result_id=_query_value(query, "result_id"),
                replay_window=_query_int(query, "replay_window", 8),
                recorded_at_from=_query_value(query, "recorded_at_from"),
                recorded_at_to=_query_value(query, "recorded_at_to"),
                health_window=_query_int(query, "health_window", 8),
                trend_short_window=_query_int(query, "trend_short_window", 8),
                trend_long_window=_query_int(query, "trend_long_window", 16),
                top_n=_query_int(query, "top_n", 5),
            ),
        )

    return None


def build_file_route_response(*, root_dir: Path, request_path: str) -> RouteResponse | RouteError | None:
    if not (request_path.startswith("/file/") or request_path.startswith("/download/")):
        return None

    root = Path(root_dir).resolve()
    is_download = request_path.startswith("/download/")
    prefix = "/download/" if is_download else "/file/"
    rel = unquote(request_path[len(prefix):]).lstrip("/")
    target = (root / rel).resolve()
    if not str(target).startswith(str(root)):
        return RouteError(status=403, message="禁止访问")
    if not target.exists() or not target.is_file():
        return RouteError(status=404, message="文件不存在")

    content_type = "text/plain; charset=utf-8"
    if target.suffix.lower() == ".csv":
        content_type = "text/csv; charset=utf-8"
    elif target.suffix.lower() == ".md":
        content_type = "text/markdown; charset=utf-8"
    disposition = "attachment" if is_download else "inline"
    return RouteResponse(
        status=200,
        content_type=content_type,
        content_disposition=f'{disposition}; filename="{target.name}"',
        body=target.read_bytes(),
    )
