from __future__ import annotations

import html

from src import stock_dashboard_http_routes
from src.stock_dashboard_assets import compose_fail_closed_stylesheet, compose_inline_style_tag
from src.stock_dashboard_page_composer import compose_fail_closed_page_html
from src.stock_dashboard_url import base_href


def select_hard_fail_closed_problems(entry_guard: dict[str, object]) -> list[str]:
    return [
        str(problem)
        for problem in (entry_guard.get("problems") or [])
        if "primary_result_lifecycle_evidence_latest.json" in str(problem)
    ]


def render_stock_fail_closed_page(*, base_path: str, entry_guard: dict[str, object]) -> str:
    problems = [str(item) for item in (entry_guard.get("problems") or []) if str(item).strip()]
    problems_html = "".join(f"<li>{html.escape(item)}</li>" for item in problems) or "<li>未提供具体阻断原因</li>"
    fail_closed_style_tag = compose_inline_style_tag(compose_fail_closed_stylesheet())
    return compose_fail_closed_page_html(
        fail_closed_style_tag=fail_closed_style_tag,
        problems_html=problems_html,
        primary_result_api_href=base_href(base_path, stock_dashboard_http_routes.PRIMARY_RESULT_API_PATH),
    )
