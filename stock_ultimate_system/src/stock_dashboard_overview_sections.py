from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from src.stock_dashboard_sections import (
    render_opportunities_section,
    render_overview_kpi_section,
    render_overview_operations_disclosure_section,
    render_overview_visuals_disclosure_section,
    render_overview_visuals_section,
    render_summary_section,
)
from src.stock_dashboard_view_model import (
    build_stock_opportunities_view_model,
    build_stock_overview_kpi_view_model,
    build_stock_summary_view_model,
)


@dataclass(frozen=True)
class StockOverviewSections:
    kpi_html: str
    summary_section: str
    opportunity_cards_html: str
    overview_visuals_disclosure_section: str
    overview_operations_disclosure_section: str


def build_stock_overview_sections(
    *,
    visible: Callable[[str], bool],
    health_status: str,
    health_score: str,
    update_status_label: str,
    execution_semantics: dict[str, Any],
    cockpit_model: dict[str, Any],
    governance_semantics: dict[str, Any],
    bt_diag: dict[str, Any],
    top1: dict[str, Any],
    candidate_name: str,
    candidate_artifact_status: dict[str, Any],
    backtest_scope: dict[str, Any],
    home_view_model: dict[str, Any],
    blocker_semantics: dict[str, Any],
    evidence_semantics: dict[str, Any],
    diagnostics_view_model: dict[str, Any],
    governance_cycle_state: str,
    governance_cycle: dict[str, Any],
    governance_recommended_action_label: str,
    governance_release_readiness: dict[str, Any],
    governance_fully_release_ready: bool,
    previous_stable_run_id: str,
    governance_operator_message: str,
    summary_lines: list[str],
    stock_ai_explainer: dict[str, Any],
    top1_signal: str,
    top1_risk: str,
    candidate_count: int,
    generation_mode_label: str,
    candidate_cards: list[dict[str, Any]],
    base_path: str,
    health_chart_html: str,
    backtest_equity_html: str,
    backtest_drawdown_html: str,
    backtest_chart_html: str,
    operations_section: str,
    external_system_summary_html: str,
    overview_disclosure_view_model: dict[str, str],
    status_label: Callable[[object], str],
    view_href: Callable[[str, int, str], str],
) -> StockOverviewSections:
    kpi_html = ""
    if visible("kpi"):
        kpi_html = render_overview_kpi_section(
            build_stock_overview_kpi_view_model(
                health_status=health_status,
                health_score=health_score,
                update_status_label=update_status_label,
                execution_semantics=execution_semantics,
                cockpit_model=cockpit_model,
                governance_semantics=governance_semantics,
            )
        )

    summary_section = ""
    if visible("summary"):
        summary_section = render_summary_section(
            build_stock_summary_view_model(
                health_status=health_status,
                backtest_conclusion=str(bt_diag.get("结论", "未评估")),
                top_code=str(top1.get("ts_code", "暂无")),
                candidate_name=candidate_name,
                health_score=health_score,
                candidate_generated_at=str(candidate_artifact_status.get("generated_at", "-")),
                backtest_scope_label=str(backtest_scope["label"]),
                basket_dual_track_rows=home_view_model["basket_dual_track_rows"],
                execution_semantics=execution_semantics,
                blocker_semantics=blocker_semantics,
                governance_semantics=governance_semantics,
                evidence_semantics=evidence_semantics,
                evolution_capacity_gate_status=str(diagnostics_view_model["evolution_capacity_gate_status"]),
                evolution_capacity_state=str(diagnostics_view_model["evolution_capacity_state"]),
                evolution_capacity_profile=str(diagnostics_view_model["evolution_capacity_profile"]),
                evolution_capacity_stress_score=str(diagnostics_view_model["evolution_capacity_stress_score"]),
                liquidity_capacity_state=str(diagnostics_view_model["liquidity_capacity_state"]),
                governance_cycle_state_label=status_label(governance_cycle_state),
                governance_decision_label=status_label(
                    governance_cycle.get("governance_inputs", {}).get("governance_decision", "unknown")
                ),
                governance_recommended_action_label=governance_recommended_action_label,
                governance_audit_status_label=status_label(
                    governance_cycle.get("governance_inputs", {}).get("governance_audit_status", "unknown")
                ),
                governance_ready_for_release=governance_release_readiness.get("ready_for_release", False),
                governance_fully_release_ready=governance_fully_release_ready,
                previous_stable_run_id=previous_stable_run_id,
                governance_operator_message=governance_operator_message,
                summary_lines=summary_lines,
                ai_explainer=stock_ai_explainer,
            )
        )

    opportunity_cards_html = ""
    if candidate_cards:
        opportunity_cards_html = render_opportunities_section(
            opportunities_view_model=build_stock_opportunities_view_model(
                top_code=str(top1.get("ts_code", "暂无")),
                candidate_name=candidate_name,
                top1_signal=top1_signal,
                top1_risk=top1_risk,
                candidate_count=candidate_count,
                candidate_generated_at=str(candidate_artifact_status.get("generated_at", "-")),
                generation_mode_label=generation_mode_label,
                basket_dual_track_rows=home_view_model["basket_dual_track_rows"],
                candidate_cards=candidate_cards,
                candidate_hrefs=[view_href("candidates", idx, base_path) for idx, _ in enumerate(candidate_cards[:5])],
            )
        )

    overview_visuals_section = render_overview_visuals_section(
        health_chart_html=health_chart_html,
        backtest_equity_html=backtest_equity_html,
        backtest_drawdown_html=backtest_drawdown_html,
        backtest_chart_html=backtest_chart_html,
    )
    overview_visuals_disclosure_section = render_overview_visuals_disclosure_section(
        overview_visuals_section=overview_visuals_section
    )
    overview_operations_disclosure_section = render_overview_operations_disclosure_section(
        operations_section=operations_section,
        external_system_summary_html=external_system_summary_html,
        disclosure_view_model=overview_disclosure_view_model,
    )

    return StockOverviewSections(
        kpi_html=kpi_html,
        summary_section=summary_section,
        opportunity_cards_html=opportunity_cards_html,
        overview_visuals_disclosure_section=overview_visuals_disclosure_section,
        overview_operations_disclosure_section=overview_operations_disclosure_section,
    )
