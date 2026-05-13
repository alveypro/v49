from src.stock_dashboard_primary_bridge import (
    PRIMARY_RESULT_INITIAL_JSON_SCRIPT_ID,
    build_stock_primary_result_bridge_context,
    json_script_tag,
)


def test_json_script_tag_escapes_script_close_sequence():
    html = json_script_tag(PRIMARY_RESULT_INITIAL_JSON_SCRIPT_ID, {"note": "</script><script>alert(1)</script>"})

    assert f'id="{PRIMARY_RESULT_INITIAL_JSON_SCRIPT_ID}"' in html
    assert "</script><script>" not in html
    assert "<\\/script>" in html


def test_build_stock_primary_result_bridge_context_enables_stock_overview():
    context = build_stock_primary_result_bridge_context(
        current_view="overview",
        base_path="/stock",
        primary_result={"schema_version": "primary_result_v1"},
        bridge_enabled=True,
    )

    assert context["enabled"] is True
    assert context["api_url"] == "/stock/api/primary-result"
    assert context["top5_health_enabled"] is True
    assert context["top5_health_api_url"] == "/stock/api/top5-trader-brief-health"
    assert PRIMARY_RESULT_INITIAL_JSON_SCRIPT_ID in str(context["initial_json_html"])


def test_build_stock_primary_result_bridge_context_disables_t12_initial_json():
    context = build_stock_primary_result_bridge_context(
        current_view="t12",
        base_path="/T12",
        primary_result={"schema_version": "primary_result_v1"},
        bridge_enabled=True,
    )

    assert context["enabled"] is False
    assert context["enabled_for_view"] is False
    assert context["initial_json_html"] == ""
    assert context["top5_health_enabled"] is False
