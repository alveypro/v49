from __future__ import annotations

from typing import Callable

from src.dashboard_support import stock_primary_result_bridge_shell_html, stock_primary_result_card_html
from src.stock_dashboard_assets import compose_dashboard_script_tag, compose_dashboard_stylesheet, compose_inline_style_tag
from src.stock_dashboard_candidate_sections import build_stock_candidate_sections
from src.stock_dashboard_diagnostics_sections import build_stock_diagnostics_sections
from src.stock_dashboard_operations_section import build_stock_operations_section
from src.stock_dashboard_overview_sections import build_stock_overview_sections
from src.stock_dashboard_page_composer import (
    compose_main_content_html,
    compose_page_interaction_script,
    compose_page_shell_html,
    compose_primary_result_bridge_bootstrap_script,
    compose_primary_result_bridge_client_script,
    compose_primary_result_shell_html,
    compose_table_export_script,
)
from src.stock_dashboard_render_context import (
    build_dashboard_asset_contract,
    build_dashboard_page_shell_contract,
    build_main_content_sections,
    build_page_interaction_context,
    build_section_visibility_context,
)
from src.stock_dashboard_render_inputs import StockDashboardRenderInputs
from src.stock_dashboard_sections import (
    render_architecture_section,
    render_diagnosis_section,
    render_market_snapshot_section,
    render_primary_result_home_brief_section,
    render_research_visuals_section,
    render_top1_section,
)
from src.stock_dashboard_sections_builder import build_stock_resource_sections
from src.stock_dashboard_shell_sections import build_stock_dashboard_shell_sections, compose_stock_top_story_html
from src.stock_dashboard_view_model import build_stock_top1_view_model


def compose_stock_dashboard_page_html(
    *,
    render_inputs: StockDashboardRenderInputs,
    base_path: str,
    primary_result_core_compare_fields: tuple[str, ...],
    display_missing: Callable[[object, str], str],
    display_status_label: Callable[[object], str],
    is_t12_scope: Callable[[str], bool],
    view_href: Callable[[str, int, str], str],
) -> str:
    ri = render_inputs
    shell_sections = build_stock_dashboard_shell_sections(
        current_view=ri.current_view,
        candidate_index=ri.candidate_index,
        base_path=base_path,
        view_labels=ri.view_labels,
        view_subtitle=ri.view_subtitle,
        health_status=ri.health_status,
        health_score=str(ri.health["score"]),
        health_tag=ri.health_tag,
        headline_tone=ri.headline_tone,
        headline_detail=ri.headline_detail,
        current_basket_pointer_label=ri.current_basket_pointer_label,
        latest_basket_attempt_label=ri.latest_basket_attempt_label,
        current_basket_pointer_status=ri.current_basket_pointer_status,
        current_basket_pointer_basket_id=ri.current_basket_pointer_basket_id,
        latest_basket_attempt_status=ri.latest_basket_attempt_status,
        latest_basket_attempt_blocking_reason=ri.latest_basket_attempt_blocking_reason,
        top1=ri.top1,
        top1_label=ri.top1_label,
        top1_signal=ri.top1_signal,
        top1_risk=ri.top1_risk,
        candidate_name=ri.candidate_name,
        candidate_artifact_status=ri.candidate_artifact_status,
        generation_mode_label=ri.generation_mode_label,
        update_status_label=ri.update_status_label,
        update_stage_label=ri.update_stage_label,
        candidate_count=ri.candidate_count,
        candidate_score=ri.candidate_score,
        candidate_timeline_label=ri.candidate_timeline_label,
        run_freshness=ri.run_freshness,
        report_state=ri.report_state,
        db_latest_trade_date=ri.db_latest_trade_date,
        observation_timeline_label=ri.observation_timeline_label,
        prefilter_freshness_label=ri.prefilter_freshness_label,
        backtest_scope=ri.backtest_scope,
        governance_cycle_state=ri.governance_cycle_state,
        governance_recommended_action_label=ri.governance_recommended_action_label,
        governance_release_readiness=ri.governance_release_readiness,
        governance_fully_release_ready=ri.governance_fully_release_ready,
        primary_conclusion=ri.primary_conclusion,
        decision_semantics=ri.decision_semantics,
        market_snapshot=ri.market_snapshot,
        bt_diag=ri.bt_diag,
        blocker_semantics=ri.blocker_semantics,
        cockpit_model=ri.cockpit_model,
        promotion_decision_label=ri.promotion_decision_label,
        timeline_consistency_note=ri.timeline_consistency_note,
        automation_health_label=ri.automation_health_label,
        context=ri.context,
    )
    home_view_model = shell_sections.home_view_model
    primary_result_card_section = ri.primary_result_card_section
    if not primary_result_card_section:
        primary_result_card_section = stock_primary_result_card_html(ri.primary_result)
    primary_result_card_section = compose_primary_result_shell_html(
        primary_result_card_html=primary_result_card_section,
        primary_result_bridge_shell_html=stock_primary_result_bridge_shell_html(ri.primary_result),
    )
    section_visibility = build_section_visibility_context(current_view=ri.current_view)

    def visible(name: str) -> bool:
        return bool(section_visibility.get(name))

    candidate_sections = build_stock_candidate_sections(
        visible=visible,
        top1=ri.top1,
        top1_signal=ri.top1_signal,
        top1_risk=ri.top1_risk,
        candidate_cards=ri.candidate_cards,
        candidate_index=ri.candidate_index,
        candidate_detail_html=ri.candidate_detail_html,
        base_path=base_path,
        candidate_map_chart_html=ri.candidate_map_chart_html,
        candidate_chart_html=ri.candidate_chart_html,
        candidates_csv=ri.candidates_csv,
        candidate_generated_at=str(ri.candidate_artifact_status.get("generated_at", "-")),
        basket_generated_at=str(ri.candidate_artifact_status.get("basket_generated_at", "-")),
        candidate_source_label=ri.candidate_source_label,
        generation_mode_label=ri.generation_mode_label,
        dominant_regime=str(ri.market_snapshot.get("dominant_regime", "-")),
        risk_preference=str(ri.market_snapshot.get("risk_preference", "-")),
        avg_risk_pressure=str(ri.market_snapshot.get("avg_risk_pressure", "0.0")),
        basket_dual_track_rows=home_view_model["basket_dual_track_rows"],
        view_href=view_href,
    )

    market_snapshot_section = render_market_snapshot_section(market_snapshot=ri.market_snapshot)
    today_brief_html = render_primary_result_home_brief_section(shell_sections.primary_result_home_facts)
    research_visuals_section = render_research_visuals_section(
        health_chart_html=ri.health_chart_html,
        backtest_equity_html=ri.backtest_equity_html,
        backtest_drawdown_html=ri.backtest_drawdown_html,
        backtest_map_chart_html=ri.backtest_map_chart_html,
    )

    architecture_section = ""
    if visible("architecture"):
        architecture_section = render_architecture_section(shell_sections.page_shell_context["architecture_steps"])

    top1_section = ""
    if visible("top1"):
        top1_section = render_top1_section(
            top1_view_model=build_stock_top1_view_model(top1=ri.top1, top1_signal=ri.top1_signal, top1_risk=ri.top1_risk)
        )

    diagnosis_section = ""
    if visible("diagnosis"):
        diagnosis_section = render_diagnosis_section(
            bt_diag=ri.bt_diag,
            next_check=display_missing(ri.cockpit_model.get("next_check"), "-"),
        )

    diagnostics_sections = build_stock_diagnostics_sections(
        visible=visible,
        current_view=ri.current_view,
        basket_validation=ri.basket_validation,
        candidate_basket_feedback=ri.candidate_basket_feedback,
        evolution_status=ri.evolution_status,
        basket_summary=ri.basket_summary,
        candidate_artifact_status=ri.candidate_artifact_status,
        validation_basket_kpis=shell_sections.validation_basket_kpis,
        generation_mode_label=ri.generation_mode_label,
        candidate_count=ri.candidate_count,
        top1=ri.top1,
        prefilter_artifact_status=ri.prefilter_artifact_status,
        db_latest_trade_date=ri.db_latest_trade_date,
        diagnosis_section=diagnosis_section,
    )

    operations_section = build_stock_operations_section(
        visible=visible("operations"),
        effective_update_status=ri.effective_update_status,
        automation_health=ri.automation_health,
        update_health=ri.update_health,
        update_timeline_panel=ri.update_timeline_panel,
        update_alerts_panel=ri.update_alerts_panel,
        daily_research_runtime=ri.daily_research_runtime,
        research_topology=ri.research_topology,
        research_batch_status=ri.research_batch_status,
        evolution_status=ri.evolution_status,
        grid_backtest_status=ri.grid_backtest_status,
        progress_pct_label=ri.progress_pct_label,
        server_sync_preflight=ri.context.get("server_sync_preflight", {}),
        status_label=display_status_label,
    )
    overview_sections = build_stock_overview_sections(
        visible=visible,
        health_status=ri.health_status,
        health_score=str(ri.health["score"]),
        update_status_label=ri.update_status_label,
        execution_semantics=ri.execution_semantics,
        cockpit_model=ri.cockpit_model,
        governance_semantics=ri.governance_semantics,
        bt_diag=ri.bt_diag,
        top1=ri.top1,
        candidate_name=ri.candidate_name,
        candidate_artifact_status=ri.candidate_artifact_status,
        backtest_scope=ri.backtest_scope,
        home_view_model=home_view_model,
        blocker_semantics=ri.blocker_semantics,
        evidence_semantics=ri.evidence_semantics,
        diagnostics_view_model=diagnostics_sections.diagnostics_view_model,
        governance_cycle_state=ri.governance_cycle_state,
        governance_cycle=ri.governance_cycle,
        governance_recommended_action_label=ri.governance_recommended_action_label,
        governance_release_readiness=ri.governance_release_readiness,
        governance_fully_release_ready=ri.governance_fully_release_ready,
        previous_stable_run_id=ri.previous_stable_run_id,
        governance_operator_message=ri.governance_operator_message,
        summary_lines=ri.summary_lines,
        stock_ai_explainer=ri.stock_ai_explainer,
        top1_signal=ri.top1_signal,
        top1_risk=ri.top1_risk,
        candidate_count=ri.candidate_count,
        generation_mode_label=ri.generation_mode_label,
        candidate_cards=ri.candidate_cards,
        base_path=base_path,
        health_chart_html=ri.health_chart_html,
        backtest_equity_html=ri.backtest_equity_html,
        backtest_drawdown_html=ri.backtest_drawdown_html,
        backtest_chart_html=ri.backtest_chart_html,
        operations_section=operations_section,
        external_system_summary_html=shell_sections.external_system_summary_html,
        overview_disclosure_view_model=shell_sections.overview_disclosure_view_model,
        status_label=display_status_label,
        view_href=view_href,
    )

    resource_sections = build_stock_resource_sections(
        visible=visible,
        daily_md_href=ri.daily_md_href,
        daily_md_download_href=ri.daily_md_download_href,
        health_csv_href=ri.health_csv_href,
        health_csv_download_href=ri.health_csv_download_href,
        leaderboard_href=ri.leaderboard_href,
        leaderboard_download_href=ri.leaderboard_download_href,
        candidates_md_href=ri.candidates_md_href,
        candidates_md_download_href=ri.candidates_md_download_href,
        candidates_csv_href=ri.candidates_csv_href,
        candidates_csv_download_href=ri.candidates_csv_download_href,
        latest_report_href=ri.latest_report_href,
        latest_report_download_href=ri.latest_report_download_href,
        candidate_index=ri.candidate_index,
        base_path=base_path,
        health_chart_html=ri.health_chart_html,
        backtest_equity_html=ri.backtest_equity_html,
        backtest_drawdown_html=ri.backtest_drawdown_html,
        backtest_chart_html=ri.backtest_chart_html,
        backtest_map_chart_html=ri.backtest_map_chart_html,
        candidate_map_chart_html=ri.candidate_map_chart_html,
        candidate_chart_html=ri.candidate_chart_html,
        current_report=ri.current_report,
        daily_md_text=ri.daily_md_text,
        translated_daily_md_text=ri.translated_daily_md_text,
        health_csv=ri.health_csv,
        leaderboard_csv=ri.leaderboard_csv,
        latest_report_text=ri.latest_report_text,
        evolution_status=ri.evolution_status,
        backtest_scope=ri.backtest_scope,
    )

    main_content_sections = build_main_content_sections(
        first_place_evidence_cockpit_section=ri.first_place_evidence_cockpit_section,
        summary_section=overview_sections.summary_section,
        primary_result_card_section=primary_result_card_section,
        kpi_html=overview_sections.kpi_html,
        t12_overview_card_section=ri.t12_sections.overview_card_section,
        spotlight_html=shell_sections.spotlight_html,
        overview_visuals_disclosure_section=overview_sections.overview_visuals_disclosure_section,
        overview_operations_disclosure_section=overview_sections.overview_operations_disclosure_section,
        links_section=resource_sections.links_section,
        opportunity_cards_html=overview_sections.opportunity_cards_html,
        candidate_compare_section=candidate_sections.candidate_compare_section,
        market_snapshot_section=market_snapshot_section,
        selection_funnel_section=diagnostics_sections.selection_funnel_section,
        diagnostics_appendix_section=diagnostics_sections.diagnostics_appendix_section,
        validation_section=diagnostics_sections.validation_section,
        diagnosis_section=diagnosis_section,
        prefilter_section=diagnostics_sections.prefilter_section,
        architecture_section=architecture_section,
        research_visuals_section=research_visuals_section,
        candidate_focus_section=candidate_sections.candidate_focus_section,
        actions_section=candidate_sections.actions_section,
        candidate_visuals_section=candidate_sections.candidate_visuals_section,
        operations_section=operations_section,
        reports_section=resource_sections.reports_section,
        t12_governance_summary_section=ri.t12_sections.governance_summary_section,
        charts_section=resource_sections.charts_section,
        top1_section=top1_section,
        guide_section=resource_sections.guide_section,
    )
    main_content_html = compose_main_content_html(current_view=ri.current_view, sections=main_content_sections)
    if ri.current_view == "overview":
        main_content_html = (
            '<div class="stack">'
            f"{ri.first_place_evidence_cockpit_section}"
            f"{overview_sections.summary_section}"
            f"{primary_result_card_section}"
            f"{overview_sections.kpi_html}"
            f"{overview_sections.opportunity_cards_html}"
            f"{main_content_html}"
            "</div>"
        )

    top_story_html = compose_stock_top_story_html(
        current_view=ri.current_view,
        shell_sections=shell_sections,
        today_brief_html=today_brief_html,
        top1_label=ri.top1_label,
        top1_signal=ri.top1_signal,
        top1_risk=ri.top1_risk,
    )
    page_shell_contract = build_dashboard_page_shell_contract(
        nav_html=shell_sections.nav_html,
        sidebar_status_html=shell_sections.sidebar_status_html,
        topbar_pills_html=shell_sections.topbar_pills_html,
        top_story_html=top_story_html,
        current_view=ri.current_view,
        kpi_html=overview_sections.kpi_html,
        primary_result_bridge_json=str(ri.primary_result_bridge_context["initial_json_html"]),
        main_content_html=main_content_html,
    )
    page_shell_html = compose_page_shell_html(**page_shell_contract)
    primary_result_bridge_bootstrap_script = compose_primary_result_bridge_bootstrap_script(
        ri.primary_result_bridge_context
    )
    primary_result_bridge_client_script = compose_primary_result_bridge_client_script(
        primary_result_bridge_bootstrap_script=primary_result_bridge_bootstrap_script,
        primary_result_core_compare_fields=primary_result_core_compare_fields,
    )
    table_export_script = compose_table_export_script()
    page_interaction_script = compose_page_interaction_script(
        build_page_interaction_context(
            current_view=ri.current_view,
            candidate_index=ri.candidate_index,
            candidate_count=len(ri.candidate_cards),
            candidate_base_href=view_href("candidates", 0, base_path),
        )
    )
    dashboard_style_tag = compose_inline_style_tag(
        compose_dashboard_stylesheet(is_t12_scope=is_t12_scope(base_path))
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
