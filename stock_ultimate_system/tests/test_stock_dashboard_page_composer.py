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


def test_compose_main_content_html_overview_uses_deep_dive_layout():
    html = compose_main_content_html(
        current_view="overview",
        sections={
            "first_place_evidence_cockpit_section": "<div>cockpit</div>",
            "summary_section": "<div>summary</div>",
            "primary_result_card_section": "<div>primary</div>",
            "kpi_html": "<div>kpi</div>",
            "t12_overview_card_section": "<div>t12</div>",
            "spotlight_html": "<div>spotlight</div>",
            "overview_visuals_disclosure_section": "<div>visuals</div>",
            "overview_operations_disclosure_section": "<div>ops</div>",
            "links_section": "<div>links</div>",
            "opportunity_cards_html": "<div>opps</div>",
            "candidate_compare_section": "<div>compare</div>",
            "market_snapshot_section": "<div>market</div>",
            "selection_funnel_section": "<div>funnel</div>",
            "diagnostics_appendix_section": "<div>appendix</div>",
            "validation_section": "<div>validation</div>",
            "diagnosis_section": "<div>diagnosis</div>",
            "prefilter_section": "<div>prefilter</div>",
            "architecture_section": "",
            "research_visuals_section": "",
            "candidate_focus_section": "",
            "actions_section": "<div>actions</div>",
            "candidate_visuals_section": "",
            "operations_section": "",
            "reports_section": "",
            "t12_governance_summary_section": "",
            "charts_section": "",
            "top1_section": "",
            "guide_section": "",
        },
    )
    assert "overview-layout" in html
    assert "<div>cockpit</div>" not in html
    assert "<div>opps</div>" not in html
    assert "professional-evidence" in html
    assert "<div>funnel</div>" in html
    assert "<div>appendix</div>" in html
    assert "<div>actions</div>" in html
    assert html.index("<div>actions</div>") < html.index("professional-evidence")
    assert "<div>validation</div>" not in html


def test_compose_main_content_html_candidates_uses_sidebar():
    html = compose_main_content_html(
        current_view="candidates",
        sections={
            "first_place_evidence_cockpit_section": "",
            "summary_section": "",
            "primary_result_card_section": "<div>primary</div>",
            "kpi_html": "",
            "t12_overview_card_section": "",
            "spotlight_html": "",
            "overview_visuals_disclosure_section": "",
            "overview_operations_disclosure_section": "",
            "links_section": "",
            "opportunity_cards_html": "",
            "candidate_compare_section": "<div>compare</div>",
            "market_snapshot_section": "<div>market</div>",
            "selection_funnel_section": "<div>funnel</div>",
            "diagnostics_appendix_section": "",
            "validation_section": "<div>validation</div>",
            "diagnosis_section": "",
            "prefilter_section": "<div>prefilter</div>",
            "architecture_section": "",
            "research_visuals_section": "",
            "candidate_focus_section": "<div>focus</div>",
            "actions_section": "<div>actions</div>",
            "candidate_visuals_section": "<div>candidate-visuals</div>",
            "operations_section": "",
            "reports_section": "",
            "t12_governance_summary_section": "",
            "charts_section": "",
            "top1_section": "",
            "guide_section": "",
        },
    )
    assert "候选决策侧栏" in html
    assert "<div>candidate-visuals</div>" in html


def test_compose_main_content_html_t12_renders_readonly_note():
    html = compose_main_content_html(
        current_view="t12",
        sections={key: "" for key in [
            "first_place_evidence_cockpit_section",
            "summary_section",
            "primary_result_card_section",
            "kpi_html",
            "spotlight_html",
            "overview_visuals_disclosure_section",
            "overview_operations_disclosure_section",
            "links_section",
            "opportunity_cards_html",
            "candidate_compare_section",
            "market_snapshot_section",
            "selection_funnel_section",
            "diagnostics_appendix_section",
            "validation_section",
            "diagnosis_section",
            "prefilter_section",
            "architecture_section",
            "research_visuals_section",
            "candidate_focus_section",
            "actions_section",
            "candidate_visuals_section",
            "operations_section",
            "reports_section",
            "charts_section",
            "top1_section",
            "guide_section",
        ]} | {
            "t12_overview_card_section": "<div>t12-overview</div>",
            "t12_governance_summary_section": "<div>t12-governance</div>",
        },
    )
    assert "只读镜像范围" in html
    assert "<div>t12-governance</div>" in html


def test_compose_top_story_html_overview_is_compact():
    html = compose_top_story_html(
        current_view="overview",
        external_decision_spine_html="<div>spine</div>",
        external_system_summary_html="<div>summary</div>",
        jump_strip_html="<div>jump</div>",
        view_banner_html="<div>banner</div>",
        control_strip_html="<div>control</div>",
        headline_html="<div>headline</div>",
        today_brief_html="<div>brief</div>",
        hero_side_html="<div>hero-side</div>",
        command_deck_html="<div>command</div>",
        top1_label="000001.SZ 平安银行",
        top1_signal="strong_buy",
        top1_risk="low",
    )
    assert html == "<div>spine</div>"


def test_compose_top_story_html_non_overview_uses_distinct_view_header():
    html = compose_top_story_html(
        current_view="research",
        external_decision_spine_html="<div>spine</div>",
        external_system_summary_html="<div>summary</div>",
        jump_strip_html="<div>jump</div>",
        view_banner_html="<div>banner</div>",
        control_strip_html="<div>control</div>",
        headline_html="<div>headline</div>",
        today_brief_html="<div>brief</div>",
        hero_side_html="<div>hero-side</div>",
        command_deck_html="<div>command</div>",
        top1_label="000001.SZ 平安银行",
        top1_signal="strong_buy",
        top1_risk="low",
    )
    assert html == "<div>banner</div><div>jump</div>"
    assert "服务器主结果" not in html


def test_compose_primary_result_shell_html_appends_bridge_shell():
    html = compose_primary_result_shell_html(
        primary_result_card_html="<div>card</div>",
        primary_result_bridge_shell_html="<script>bridge</script>",
    )
    assert html == "<div>card</div><script>bridge</script>"


def test_compose_primary_result_bridge_bootstrap_script_renders_constants():
    html = compose_primary_result_bridge_bootstrap_script(
        {
            "enabled": True,
            "api_url": "/stock/api/primary-result",
            "top5_health_enabled": True,
            "top5_health_api_url": "/stock/api/top5-trader-brief-health",
        }
    )
    assert "const PRIMARY_RESULT_BRIDGE_ENABLED = true;" in html
    assert "const PRIMARY_RESULT_API_URL = '/stock/api/primary-result';" in html
    assert "const TOP5_MANIFEST_HEALTH_ENABLED = true;" in html
    assert "const TOP5_MANIFEST_HEALTH_URL = '/stock/api/top5-trader-brief-health';" in html


def test_compose_primary_result_bridge_client_script_contains_bridge_contract():
    html = compose_primary_result_bridge_client_script(
        primary_result_bridge_bootstrap_script=(
            "const PRIMARY_RESULT_BRIDGE_ENABLED = true;\n"
            "    const PRIMARY_RESULT_API_URL = '/stock/api/primary-result';\n"
            "    const TOP5_MANIFEST_HEALTH_ENABLED = true;\n"
            "    const TOP5_MANIFEST_HEALTH_URL = '/stock/api/top5-trader-brief-health';"
        ),
        primary_result_core_compare_fields=("result_lifecycle_stage", "audit_status"),
    )
    assert "function isValidPrimaryResultPayload(payload)" in html
    assert "function comparePrimaryResultFacts(serverFact, apiFact)" in html
    assert '"result_lifecycle_stage"' in html
    assert '"audit_status"' in html
    assert "async function loadPrimaryResultCard()" in html


def test_compose_table_export_script_contains_csv_export_logic():
    html = compose_table_export_script()
    assert "function exportTableToCsv(tableId, filename)" in html
    assert "URL.createObjectURL(blob)" in html
    assert "querySelectorAll('tr')" in html


def test_compose_page_interaction_script_contains_copy_mode_and_navigation():
    html = compose_page_interaction_script(
        {
            "current_view": "candidates",
            "candidate_index": 2,
            "candidate_count": 5,
            "candidate_base_href": "/stock?view=candidates&candidate=0",
        }
    )
    assert "loadPrimaryResultCard();" in html
    assert "data-copy-link" in html
    assert "data-toggle-mode" in html
    assert "candidateBaseHref" in html
    assert "ArrowLeft" in html


def test_compose_fail_closed_page_html_renders_message_and_api_link():
    html = compose_fail_closed_page_html(
        fail_closed_style_tag="<style>body { color: red; }</style>",
        problems_html="<li>pointer missing</li>",
        primary_result_api_href="/stock/api/primary-result",
    )
    assert "/stock Fail Closed" in html
    assert "<li>pointer missing</li>" in html
    assert 'href="/stock/api/primary-result"' in html


def test_compose_page_shell_html_wraps_app_sidebar_topbar_and_content():
    html = compose_page_shell_html(
        nav_html="<a>nav</a>",
        sidebar_status_html="<div>sidebar</div>",
        topbar_pills_html="<div>pill</div>",
        top_story_html="<div>story</div>",
        current_view="research",
        kpi_html="<div>kpi</div>",
        primary_result_bridge_json="<script>json</script>",
        main_content_html="<div>main</div>",
    )
    assert '<div class="app-shell">' in html
    assert 'id="top5-manifest-freshness-banner"' in html
    assert "<a>nav</a>" in html
    assert "<div>pill</div>" in html
    assert "<div>kpi</div>" in html
    assert "<script>json</script>" in html
