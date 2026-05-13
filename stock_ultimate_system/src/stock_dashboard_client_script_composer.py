from __future__ import annotations

import json
from pathlib import Path


_PRIMARY_RESULT_BRIDGE_TEMPLATE = Path(__file__).with_name(
    "stock_dashboard_primary_result_bridge.js"
).read_text(encoding="utf-8")
_TABLE_EXPORT_TEMPLATE = Path(__file__).with_name(
    "stock_dashboard_table_export.js"
).read_text(encoding="utf-8")
_PAGE_INTERACTION_TEMPLATE = Path(__file__).with_name(
    "stock_dashboard_page_interaction.js"
).read_text(encoding="utf-8")


def compose_primary_result_bridge_bootstrap_script(primary_result_bridge_context: dict[str, object]) -> str:
    enabled = "true" if bool(primary_result_bridge_context.get("enabled")) else "false"
    api_url = repr(str(primary_result_bridge_context.get("api_url", "")))
    th = "true" if bool(primary_result_bridge_context.get("top5_health_enabled")) else "false"
    health_url = repr(str(primary_result_bridge_context.get("top5_health_api_url", "") or ""))
    return (
        f"const PRIMARY_RESULT_BRIDGE_ENABLED = {enabled};\n"
        f"    const PRIMARY_RESULT_API_URL = {api_url};\n"
        f"    const TOP5_MANIFEST_HEALTH_ENABLED = {th};\n"
        f"    const TOP5_MANIFEST_HEALTH_URL = {health_url};"
    )


def compose_primary_result_bridge_client_script(
    *,
    primary_result_bridge_bootstrap_script: str,
    primary_result_core_compare_fields: tuple[str, ...],
) -> str:
    return (
        _PRIMARY_RESULT_BRIDGE_TEMPLATE
        .replace("__PRIMARY_RESULT_BRIDGE_BOOTSTRAP_SCRIPT__", primary_result_bridge_bootstrap_script)
        .replace(
            "__PRIMARY_RESULT_CORE_COMPARE_FIELDS__",
            json.dumps(primary_result_core_compare_fields, ensure_ascii=False),
        )
    )


def compose_table_export_script() -> str:
    return _TABLE_EXPORT_TEMPLATE


def compose_page_interaction_script(page_interaction_context: dict[str, object]) -> str:
    return (
        _PAGE_INTERACTION_TEMPLATE
        .replace(
            "__CURRENT_VIEW__",
            json.dumps(str(page_interaction_context.get("current_view", "")), ensure_ascii=False),
        )
        .replace(
            "__CANDIDATE_INDEX__",
            str(int(page_interaction_context.get("candidate_index", 0) or 0)),
        )
        .replace(
            "__CANDIDATE_COUNT__",
            str(int(page_interaction_context.get("candidate_count", 0) or 0)),
        )
        .replace(
            "__CANDIDATE_BASE_HREF__",
            json.dumps(str(page_interaction_context.get("candidate_base_href", "")), ensure_ascii=False),
        )
    )
