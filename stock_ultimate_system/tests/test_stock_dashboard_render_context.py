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


def test_build_main_content_sections_collects_named_blocks():
    sections = build_main_content_sections(
        first_place_evidence_cockpit_section="cockpit",
        summary_section="summary",
        primary_result_card_section="primary",
        kpi_html="kpi",
        t12_overview_card_section="t12",
        spotlight_html="spotlight",
        overview_visuals_disclosure_section="visuals",
        overview_operations_disclosure_section="ops",
        links_section="links",
        opportunity_cards_html="opps",
        candidate_compare_section="compare",
        market_snapshot_section="market",
        selection_funnel_section="funnel",
        diagnostics_appendix_section="appendix",
        validation_section="validation",
        diagnosis_section="diagnosis",
        prefilter_section="prefilter",
        architecture_section="architecture",
        research_visuals_section="research-visuals",
        candidate_focus_section="focus",
        actions_section="actions",
        candidate_visuals_section="candidate-visuals",
        operations_section="operations",
        reports_section="reports",
        t12_governance_summary_section="t12-governance",
        charts_section="charts",
        top1_section="top1",
        guide_section="guide",
    )
    assert sections["summary_section"] == "summary"
    assert sections["selection_funnel_section"] == "funnel"
    assert sections["diagnostics_appendix_section"] == "appendix"
    assert sections["t12_governance_summary_section"] == "t12-governance"


def test_build_top_story_context_collects_page_story_inputs():
    payload = build_top_story_context(
        current_view="research",
        external_decision_spine_html="spine",
        external_system_summary_html="summary",
        jump_strip_html="jump",
        view_banner_html="banner",
        control_strip_html="control",
        headline_html="headline",
        today_brief_html="brief",
        hero_side_html="hero-side",
        command_deck_html="command",
        top1_label="000001.SZ 平安银行",
        top1_signal="strong_buy",
        top1_risk="low",
    )
    assert payload["current_view"] == "research"
    assert payload["command_deck_html"] == "command"
    assert payload["top1_label"] == "000001.SZ 平安银行"


def test_build_jump_strip_context_collects_links_and_tool_button():
    payload = build_jump_strip_context(current_view="overview")
    assert payload["links"][0]["anchor"] == "summary"
    assert payload["links"][3]["anchor"] == "selection-funnel"
    assert payload["links"][4]["anchor"] == "diagnostics-appendix"
    assert payload["tool_button"]["body_class"] == "overview-focus"


def test_build_section_visibility_context_returns_view_flags():
    payload = build_section_visibility_context(current_view="candidates")
    assert payload["candidate_focus"] is True
    assert payload["top1"] is False
    assert payload["prefilter"] is True


def test_build_page_shell_context_collects_nav_sidebar_and_architecture():
    payload = build_page_shell_context(
        current_view="candidates",
        candidate_index=2,
        base_path="/stock",
        view_labels={"overview": "总览", "candidates": "候选股"},
        health_status="稳健",
        update_status_label="已更新",
        update_stage_label="已完成",
        report_state="最新",
        db_latest_trade_date="2026-04-28",
        candidate_timeline_label="2026-04-28 08:00:00",
        observation_timeline_label="2026-04-23",
        prefilter_freshness_label="预筛 fresh",
        automation_health_label="自动链路 稳健",
        server_sync_preflight={
            "preflight_version": "server_sync_preflight.v1",
            "sync_decision": {"allowed_to_sync": True, "blocking_checks": []},
        },
    )
    assert payload["nav_items"][1]["href"] == "/stock/?view=candidates&candidate=2"
    assert payload["nav_items"][1]["active"] is True
    assert payload["sidebar_stats"][0]["value"] == "稳健"
    assert payload["architecture_steps"][0]["index"] == "01"
    assert payload["topbar_pills"][0] == "数据最新交易日 2026-04-28"
    assert "服务器同步 可同步" in payload["topbar_pills"]


def test_build_page_shell_context_softens_internal_update_states_for_formal_surface():
    payload = build_page_shell_context(
        current_view="overview",
        candidate_index=0,
        base_path="/stock",
        view_labels={"overview": "总览"},
        health_status="告警",
        update_status_label="partial_success",
        update_stage_label="blocked",
        report_state="已生成",
        db_latest_trade_date="2026-04-28",
        candidate_timeline_label="2026-04-28 08:00:00",
        observation_timeline_label="2026-04-23",
        prefilter_freshness_label="预筛待补齐",
        automation_health_label="自动链路 待复核",
        server_sync_preflight={
            "preflight_version": "server_sync_preflight.v1",
            "sync_decision": {
                "allowed_to_sync": False,
                "blocking_checks": ["manifest_classification", "required_sync_files"],
            },
        },
    )
    assert payload["sidebar_stats"][1]["value"] == "待补齐"
    assert payload["sidebar_stats"][2]["value"] == "待复核"
    assert "服务器同步 2项阻断" in payload["topbar_pills"]
    assert payload["topbar_pills"][-1] == "更新 待补齐"


def test_build_primary_result_bridge_context_enables_only_supported_views():
    payload = build_primary_result_bridge_context(
        current_view="overview",
        primary_result_bridge_enabled=True,
        primary_result_api_url="/stock/api/primary-result",
        primary_result_initial_json_html="<script>json</script>",
    )
    assert payload["enabled"] is True
    assert payload["enabled_for_view"] is True
    assert payload["initial_json_html"] == "<script>json</script>"

    t12_payload = build_primary_result_bridge_context(
        current_view="t12",
        primary_result_bridge_enabled=True,
        primary_result_api_url="/stock/api/primary-result",
        primary_result_initial_json_html="<script>json</script>",
    )
    assert t12_payload["enabled"] is False
    assert t12_payload["enabled_for_view"] is False
    assert t12_payload["initial_json_html"] == ""


def test_build_page_interaction_context_collects_candidate_navigation_inputs():
    payload = build_page_interaction_context(
        current_view="candidates",
        candidate_index=2,
        candidate_count=5,
        candidate_base_href="/stock?view=candidates&candidate=0",
    )
    assert payload["current_view"] == "candidates"
    assert payload["candidate_index"] == 2
    assert payload["candidate_count"] == 5
    assert payload["candidate_base_href"] == "/stock?view=candidates&candidate=0"


def test_build_dashboard_page_shell_contract_collects_final_shell_inputs():
    payload = build_dashboard_page_shell_contract(
        nav_html="<a>nav</a>",
        sidebar_status_html="<div>sidebar</div>",
        topbar_pills_html="<div>pill</div>",
        top_story_html="<div>story</div>",
        current_view="overview",
        kpi_html="<div>kpi</div>",
        primary_result_bridge_json="<script>json</script>",
        main_content_html="<div>main</div>",
    )
    assert payload["nav_html"] == "<a>nav</a>"
    assert payload["current_view"] == "overview"
    assert payload["primary_result_bridge_json"] == "<script>json</script>"


def test_build_dashboard_asset_contract_collects_style_and_script_tags():
    payload = build_dashboard_asset_contract(
        dashboard_style_tag="<style>body { color: red; }</style>",
        dashboard_script_tag="<script>console.log(1)</script>",
    )
    assert payload["document_title"] == "Airivo Alpha | 股票研究终端"
    assert payload["dashboard_style_tag"].startswith("<style>")
    assert payload["dashboard_script_tag"].startswith("<script>")


def test_build_candidate_focus_render_contract_collects_nav_and_switcher_items():
    payload = build_candidate_focus_render_contract(
        candidate_focus_view_model={
            "current_index": 1,
            "prev_index": 0,
            "next_index": 2,
            "current_position_label": "候选 2 / 3",
            "total_candidates": 3,
            "quick_links": [
                {"label": "Top1", "ts_code": "000001.SZ", "index": 0, "active": False},
                {"label": "Top2", "ts_code": "000002.SZ", "index": 1, "active": True},
            ],
        },
        candidate_cards=[
            {"ts_code": "000001.SZ", "stock_name": "平安银行", "signal": "buy", "final_score": "95"},
            {"ts_code": "000002.SZ", "stock_name": "万科A", "signal": "watch", "final_score": "88"},
            {"ts_code": "000004.SZ", "stock_name": "国华网安", "signal": "watch", "final_score": "80"},
        ],
        candidate_index=1,
        base_path="/stock",
    )
    assert payload["nav"]["position_label"] == "候选 2 / 3"
    assert payload["quick_links"][1]["href"] == "/stock/?view=candidates&candidate=1"
    assert payload["switcher_items"][0]["rank"] == "#1"
