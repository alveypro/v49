from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.stock_dashboard_page_composer import compose_top_story_html
from src.stock_dashboard_render_context import build_jump_strip_context, build_page_shell_context, build_top_story_context
from src.stock_dashboard_sections import (
    render_command_deck_section,
    render_control_strip_section,
    render_dual_track_rows,
    render_external_decision_spine_section,
    render_hero_side_section,
    render_home_headline_section,
    render_jump_strip,
    render_nav_links,
    render_overview_system_summary_section,
    render_sidebar_status_section,
    render_spotlight_section,
    render_topbar_pills_section,
    render_view_banner_section,
)
from src.stock_dashboard_view_model import (
    build_stock_home_view_model,
    build_stock_overview_chrome_view_model,
    build_stock_overview_disclosure_view_model,
)


@dataclass(frozen=True)
class StockDashboardShellSections:
    home_view_model: dict[str, Any]
    primary_result_home_facts: dict[str, Any]
    validation_basket_kpis: list[dict[str, Any]]
    control_strip_html: str
    overview_disclosure_view_model: dict[str, str]
    external_system_summary_html: str
    external_decision_spine_html: str
    headline_html: str
    hero_side_html: str
    command_deck_html: str
    view_banner_html: str
    spotlight_html: str
    page_shell_context: dict[str, Any]
    sidebar_status_html: str
    topbar_pills_html: str
    nav_html: str
    jump_strip_html: str


def build_stock_dashboard_shell_sections(
    *,
    current_view: str,
    candidate_index: int,
    base_path: str,
    view_labels: dict[str, str],
    view_subtitle: str,
    report_state: dict[str, Any],
    context: dict[str, Any],
    headline_tone: str,
    headline_detail: str,
    current_basket_pointer_label: str,
    latest_basket_attempt_label: str,
    current_basket_pointer_status: str,
    current_basket_pointer_basket_id: str,
    latest_basket_attempt_status: str,
    latest_basket_attempt_blocking_reason: str,
    health_status: str,
    health_score: str,
    health_tag: str,
    top1: dict[str, Any],
    top1_label: str,
    top1_signal: str,
    top1_risk: str,
    candidate_name: str,
    candidate_artifact_status: dict[str, Any],
    generation_mode_label: str,
    update_status_label: str,
    update_stage_label: str,
    candidate_count: int,
    candidate_score: str,
    candidate_timeline_label: str,
    run_freshness: str,
    db_latest_trade_date: str,
    observation_timeline_label: str,
    prefilter_freshness_label: str,
    backtest_scope: dict[str, Any],
    governance_cycle_state: str,
    governance_recommended_action_label: str,
    governance_release_readiness: dict[str, Any],
    governance_fully_release_ready: bool,
    primary_conclusion: dict[str, Any],
    decision_semantics: dict[str, Any],
    market_snapshot: dict[str, Any],
    bt_diag: dict[str, Any],
    blocker_semantics: dict[str, Any],
    cockpit_model: dict[str, Any],
    promotion_decision_label: str,
    timeline_consistency_note: str,
    automation_health_label: str,
) -> StockDashboardShellSections:
    home_view_model = build_stock_home_view_model(
        headline_tone=headline_tone,
        headline_detail=headline_detail,
        current_basket_pointer_label=current_basket_pointer_label,
        latest_basket_attempt_label=latest_basket_attempt_label,
        current_basket_pointer_status=current_basket_pointer_status,
        current_basket_pointer_basket_id=current_basket_pointer_basket_id,
        latest_basket_attempt_status=latest_basket_attempt_status,
        latest_basket_attempt_blocking_reason=latest_basket_attempt_blocking_reason,
        health_status=health_status,
        health_score=health_score,
        top_code=str(top1.get("ts_code", "暂无")),
        candidate_name=candidate_name,
        candidate_generated_at=str(candidate_artifact_status.get("generated_at", "-")),
        generation_mode_label=generation_mode_label,
        update_status_label=update_status_label,
        update_stage_label=update_stage_label,
        candidate_count=candidate_count,
        candidate_score=candidate_score,
        candidate_timeline_label=candidate_timeline_label,
        run_freshness=run_freshness,
        db_latest_trade_date=db_latest_trade_date,
        observation_timeline_label=observation_timeline_label,
        prefilter_freshness_label=prefilter_freshness_label,
        backtest_scope_label=backtest_scope["label"],
        governance_cycle_state=governance_cycle_state,
        governance_recommended_action_label=governance_recommended_action_label,
        governance_ready_for_release=governance_release_readiness.get("ready_for_release", False),
        governance_fully_release_ready=governance_fully_release_ready,
        result_id=str(primary_conclusion.get("result_id", "primary:unavailable")),
        stage_label=str(primary_conclusion.get("stage_label", "阶段待确认")),
        result_subject=(
            f"{str(primary_conclusion.get('ts_code', '对象暂缺'))} "
            f"{str(primary_conclusion.get('stock_name', '名称暂缺'))}"
        ).strip(),
        dominant_regime=str(decision_semantics.get("market_regime", market_snapshot.get("dominant_regime", "-"))),
        risk_preference=str(decision_semantics.get("risk_preference", market_snapshot.get("risk_preference", "-"))),
        backtest_conclusion=str(decision_semantics.get("strategy_conclusion", bt_diag.get("结论", "未评估"))),
        avg_risk_pressure=str(market_snapshot.get("avg_risk_pressure", "0.0")),
        disabled_reason=str(primary_conclusion.get("disabled_reason", "")),
        invalid_reason=str(primary_conclusion.get("invalid_reason", "")),
        blocker_semantics=blocker_semantics,
        cockpit_model=cockpit_model,
        promotion_decision_label=promotion_decision_label,
    )
    home_hero_facts = home_view_model["home_hero_facts"]
    control_strip_html = render_control_strip_section(home_view_model["control_strip_cards"])
    basket_dual_track_html = render_dual_track_rows(home_view_model["basket_dual_track_rows"])
    overview_disclosure_view_model = build_stock_overview_disclosure_view_model(
        update_status_label=update_status_label,
        update_stage_label=update_stage_label,
    )
    external_system_summary_html = render_overview_system_summary_section(
        control_strip_html=control_strip_html,
        disclosure_view_model=overview_disclosure_view_model,
    )
    view_title = view_labels.get(current_view, view_labels["overview"])
    overview_chrome_view_model = build_stock_overview_chrome_view_model(
        home_hero_facts=home_hero_facts,
        health_tag=health_tag,
        top1_label=top1_label,
        top1_signal=top1_signal,
        top1_risk=top1_risk,
        candidate_score=candidate_score,
        run_freshness=run_freshness,
        update_stage_label=update_stage_label,
        update_status_label=update_status_label,
        candidate_name=candidate_name,
        result_subject=(
            f"{str(primary_conclusion.get('ts_code', '对象暂缺'))} "
            f"{str(primary_conclusion.get('stock_name', '名称暂缺'))}"
        ).strip(),
        db_latest_trade_date=db_latest_trade_date,
        timeline_consistency_note=timeline_consistency_note,
        view_title=view_title,
        view_subtitle=view_subtitle,
        command_pointer_sentence=str(home_view_model["command_pointer_sentence"]),
        command_attempt_sentence=str(home_view_model["command_attempt_sentence"]),
    )
    page_shell_context = build_page_shell_context(
        current_view=current_view,
        candidate_index=candidate_index,
        base_path=base_path,
        view_labels=view_labels,
        health_status=health_status,
        update_status_label=update_status_label,
        update_stage_label=update_stage_label,
        report_state=report_state,
        db_latest_trade_date=db_latest_trade_date,
        candidate_timeline_label=candidate_timeline_label,
        observation_timeline_label=observation_timeline_label,
        prefilter_freshness_label=prefilter_freshness_label,
        automation_health_label=automation_health_label,
        server_sync_preflight=context.get("server_sync_preflight", {}),
    )

    return StockDashboardShellSections(
        home_view_model=home_view_model,
        primary_result_home_facts=home_view_model["primary_result_home_facts"],
        validation_basket_kpis=list(home_view_model["validation_basket_kpis"]),
        control_strip_html=control_strip_html,
        overview_disclosure_view_model=overview_disclosure_view_model,
        external_system_summary_html=external_system_summary_html,
        external_decision_spine_html=render_external_decision_spine_section(home_view_model["external_decision_spine"]),
        headline_html=render_home_headline_section(home_hero_facts),
        hero_side_html=render_hero_side_section(
            hero_side_view_model=overview_chrome_view_model["hero_side"],
            basket_dual_track_html=basket_dual_track_html,
        ),
        command_deck_html=render_command_deck_section(
            command_focus_view_model=overview_chrome_view_model["command_focus"],
            command_runtime_view_model=overview_chrome_view_model["command_runtime"],
        ),
        view_banner_html=render_view_banner_section(overview_chrome_view_model["view_banner"]),
        spotlight_html=render_spotlight_section(overview_chrome_view_model["spotlight"]),
        page_shell_context=page_shell_context,
        sidebar_status_html=render_sidebar_status_section(page_shell_context["sidebar_stats"]),
        topbar_pills_html=render_topbar_pills_section(page_shell_context["topbar_pills"]),
        nav_html=render_nav_links(page_shell_context["nav_items"]),
        jump_strip_html=render_jump_strip(build_jump_strip_context(current_view=current_view)),
    )


def compose_stock_top_story_html(
    *,
    current_view: str,
    shell_sections: StockDashboardShellSections,
    today_brief_html: str,
    top1_label: str,
    top1_signal: str,
    top1_risk: str,
) -> str:
    top_story_context = build_top_story_context(
        current_view=current_view,
        external_decision_spine_html=shell_sections.external_decision_spine_html,
        external_system_summary_html=shell_sections.external_system_summary_html,
        jump_strip_html=shell_sections.jump_strip_html,
        view_banner_html=shell_sections.view_banner_html,
        control_strip_html=shell_sections.control_strip_html,
        headline_html=shell_sections.headline_html,
        today_brief_html=today_brief_html,
        hero_side_html=shell_sections.hero_side_html,
        command_deck_html=shell_sections.command_deck_html,
        top1_label=top1_label,
        top1_signal=top1_signal,
        top1_risk=top1_risk,
    )
    return compose_top_story_html(**top_story_context)
