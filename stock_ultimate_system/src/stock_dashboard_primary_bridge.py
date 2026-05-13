from __future__ import annotations

import html
import json

from src import stock_dashboard_http_routes
from src.stock_dashboard_render_context import build_primary_result_bridge_context
from src.stock_dashboard_url import base_href, is_t12_scope


PRIMARY_RESULT_INITIAL_JSON_SCRIPT_ID = "primary-result-initial-json"


def json_script_tag(element_id: str, payload: dict[str, object]) -> str:
    json_text = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    return f'<script type="application/json" id="{html.escape(element_id)}">{json_text}</script>'


def build_stock_primary_result_bridge_context(
    *,
    current_view: str,
    base_path: str,
    primary_result: dict[str, object],
    bridge_enabled: bool,
) -> dict[str, object]:
    return build_primary_result_bridge_context(
        current_view=current_view,
        primary_result_bridge_enabled=bridge_enabled,
        primary_result_api_url=base_href(base_path, stock_dashboard_http_routes.PRIMARY_RESULT_API_PATH),
        primary_result_initial_json_html=json_script_tag(PRIMARY_RESULT_INITIAL_JSON_SCRIPT_ID, primary_result),
        top5_trader_brief_health_enabled=(not is_t12_scope(base_path)),
        top5_trader_brief_health_url=base_href(base_path, stock_dashboard_http_routes.TOP5_TRADER_BRIEF_HEALTH_PATH),
    )
