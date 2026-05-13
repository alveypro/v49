from pathlib import Path

from src.stock_dashboard_http_routes import (
    RouteError,
    RouteResponse,
    build_dashboard_page_request,
    build_file_route_response,
)


def _view_labels(_base_path: str) -> dict[str, str]:
    return {"overview": "Overview", "candidates": "Candidates", "t12": "T12"}


def _is_t12_scope(base_path: str) -> bool:
    return base_path == "/T12"


def test_build_dashboard_page_request_defaults_stock_scope_to_overview():
    request = build_dashboard_page_request(
        query={"candidate": ["bad"]},
        raw_base_path="/stock",
        view_labels=_view_labels,
        is_t12_scope=_is_t12_scope,
    )

    assert request.view == "overview"
    assert request.candidate_index == 0
    assert request.report_key == "research"


def test_build_dashboard_page_request_defaults_t12_scope_to_t12():
    request = build_dashboard_page_request(
        query={"view": ["missing"], "candidate": ["3"], "report": ["ops"]},
        raw_base_path="/T12",
        view_labels=_view_labels,
        is_t12_scope=_is_t12_scope,
    )

    assert request.view == "t12"
    assert request.candidate_index == 3
    assert request.report_key == "ops"


def test_build_dashboard_page_request_preserves_valid_view_and_report():
    request = build_dashboard_page_request(
        query={"view": ["candidates"], "candidate": ["2"], "report": ["research"]},
        raw_base_path="/stock",
        view_labels=_view_labels,
        is_t12_scope=_is_t12_scope,
    )

    assert request.view == "candidates"
    assert request.candidate_index == 2
    assert request.report_key == "research"


def test_build_file_route_response_serves_inline_markdown(tmp_path: Path):
    target = tmp_path / "reports" / "daily.md"
    target.parent.mkdir()
    target.write_text("# daily\n", encoding="utf-8")

    response = build_file_route_response(root_dir=tmp_path, request_path="/file/reports/daily.md")

    assert isinstance(response, RouteResponse)
    assert response.status == 200
    assert response.content_type == "text/markdown; charset=utf-8"
    assert response.content_disposition == 'inline; filename="daily.md"'
    assert response.body == b"# daily\n"


def test_build_file_route_response_serves_download_csv(tmp_path: Path):
    target = tmp_path / "exports" / "top5.csv"
    target.parent.mkdir()
    target.write_text("code,name\n", encoding="utf-8")

    response = build_file_route_response(root_dir=tmp_path, request_path="/download/exports/top5.csv")

    assert isinstance(response, RouteResponse)
    assert response.status == 200
    assert response.content_type == "text/csv; charset=utf-8"
    assert response.content_disposition == 'attachment; filename="top5.csv"'


def test_build_file_route_response_blocks_path_escape(tmp_path: Path):
    response = build_file_route_response(root_dir=tmp_path, request_path="/file/../secret.txt")

    assert isinstance(response, RouteError)
    assert response.status == 403


def test_build_file_route_response_reports_missing_file(tmp_path: Path):
    response = build_file_route_response(root_dir=tmp_path, request_path="/file/missing.md")

    assert isinstance(response, RouteError)
    assert response.status == 404
