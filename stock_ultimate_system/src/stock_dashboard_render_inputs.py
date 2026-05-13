from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from src.dashboard_context import build_dashboard_context
from src.stock_dashboard_domain_context import build_dashboard_domain_context
from src.stock_dashboard_primary_bridge import build_stock_primary_result_bridge_context
from src.stock_dashboard_runtime_evidence import load_dashboard_runtime_evidence
from src.stock_dashboard_t12_sections import T12ReadOnlySections, build_t12_read_only_sections
from src.stock_dashboard_view_model import build_stock_runtime_view_model
from src.utils.project_paths import resolve_artifacts_path


@dataclass(frozen=True)
class StockDashboardRenderInputs:
    context: dict[str, Any]
    current_view: str
    candidate_index: int
    current_report: str
    view_labels: dict[str, str]
    view_subtitles: dict[str, str]
    view_subtitle: str
    report_state: str
    exp_dir: Path
    daily_md_text: str
    translated_daily_md_text: str
    latest_report_text: str
    health: dict[str, Any]
    health_status: str
    health_tag: str
    candidate_cards: list[dict[str, Any]]
    top1: dict[str, Any]
    top1_signal: str
    top1_risk: str
    bt_diag: dict[str, Any]
    top1_label: str
    summary_lines: list[str]
    update_health: dict[str, Any]
    daily_md_href: str
    daily_md_download_href: str
    health_csv: str
    health_csv_href: str
    health_csv_download_href: str
    leaderboard_csv: str
    leaderboard_href: str
    leaderboard_download_href: str
    candidates_csv: str
    candidates_md_href: str
    candidates_md_download_href: str
    candidates_csv_href: str
    candidates_csv_download_href: str
    latest_report_href: str
    latest_report_download_href: str
    health_chart_html: str
    backtest_equity_html: str
    backtest_drawdown_html: str
    backtest_chart_html: str
    backtest_map_chart_html: str
    candidate_chart_html: str
    candidate_map_chart_html: str
    candidate_detail_html: str
    market_snapshot: dict[str, Any]
    basket_summary: dict[str, Any]
    basket_validation: dict[str, Any]
    candidate_basket_feedback: dict[str, Any]
    governance_cycle: dict[str, Any]
    governance_cycle_state: str
    governance_recommended_action_label: str
    governance_operator_message: str
    governance_release_readiness: dict[str, Any]
    governance_fully_release_ready: bool
    previous_stable_run_id: str
    research_batch_status: dict[str, Any]
    daily_research_runtime: dict[str, Any]
    progress_pct_label: str
    effective_update_status: dict[str, Any]
    automation_health: dict[str, Any]
    candidate_artifact_status: dict[str, Any]
    prefilter_artifact_status: dict[str, Any]
    research_topology: dict[str, Any]
    grid_backtest_status: dict[str, Any]
    evolution_status: dict[str, Any]
    decision_semantics: dict[str, Any]
    blocker_semantics: dict[str, Any]
    execution_semantics: dict[str, Any]
    evidence_semantics: dict[str, Any]
    governance_semantics: dict[str, Any]
    update_timeline_panel: str
    update_alerts_panel: str
    backtest_scope: dict[str, Any]
    run_freshness: str
    candidate_score: str
    candidate_name: str
    candidate_count: int
    candidate_source_label: str
    generation_mode_label: str
    current_basket_pointer_status: str
    current_basket_pointer_basket_id: str
    latest_basket_attempt_status: str
    latest_basket_attempt_blocking_reason: str
    headline_tone: str
    headline_detail: str
    primary_result: dict[str, Any]
    first_place_evidence_cockpit_section: str
    primary_result_card_section: str
    stock_ai_explainer: dict[str, Any]
    primary_conclusion: dict[str, Any]
    t12_sections: T12ReadOnlySections
    primary_result_bridge_context: dict[str, Any]
    cockpit_model: dict[str, Any]
    update_status_label: str
    update_stage_label: str
    prefilter_freshness_label: str
    automation_health_label: str
    promotion_decision_label: str
    db_latest_trade_date: str
    candidate_timeline_label: str
    observation_timeline_label: str
    timeline_consistency_note: str
    current_basket_pointer_label: str
    latest_basket_attempt_label: str


def build_stock_dashboard_render_inputs(
    *,
    root: Path,
    current_view: str,
    candidate_index: int,
    current_report: str,
    base_path: str,
    report_labels: dict[str, str],
    view_labels_builder: Callable[[str], dict[str, str]],
    view_subtitles_builder: Callable[[str], dict[str, str]],
    primary_result_bridge_enabled: bool,
) -> StockDashboardRenderInputs:
    context = build_dashboard_context(root, candidate_index=candidate_index, base_path=base_path)
    domain = build_dashboard_domain_context(context)
    core = domain["core"]
    reports = domain["reports"]
    candidate = domain["candidate"]
    governance = domain["governance"]
    runtime = domain["runtime"]
    primary = domain["primary"]
    t12 = domain["t12"]
    semantics = domain["semantics"]
    view_labels = view_labels_builder(base_path)
    view_subtitles = view_subtitles_builder(base_path)
    resolved_candidate_index = int(core["candidate_index"])
    resolved_current_report = current_report if current_report in report_labels else "research"
    resolved_current_view = current_view if current_view in view_labels else "overview"
    view_subtitle = view_subtitles.get(resolved_current_view, view_subtitles["overview"])

    candidate_artifact_status = candidate["candidate_artifact_status"]
    prefilter_artifact_status = candidate["prefilter_artifact_status"]
    governance_recommended_action = str(governance["recommended_action"])
    current_basket_pointer_status = str(candidate["current_basket_pointer_status"])
    current_basket_pointer_updated_at = str(candidate["current_basket_pointer_updated_at"])
    current_basket_pointer_basket_id = str(candidate["current_basket_pointer_basket_id"])
    latest_basket_attempt_status = str(candidate["latest_basket_attempt_status"])
    latest_basket_attempt_generated_at = str(candidate["latest_basket_attempt_generated_at"])
    latest_basket_attempt_blocking_reason = str(candidate["latest_basket_attempt_blocking_reason"])
    primary_result = primary["primary_result"]
    primary_result_query = primary["primary_result_query"]
    cockpit_model = primary["cockpit_model"]
    exp_dir = core["exp_dir"]

    runtime_evidence = load_dashboard_runtime_evidence(
        artifacts_root=resolve_artifacts_path(),
        exp_dir=exp_dir,
    )
    runtime_view_model = build_stock_runtime_view_model(
        effective_update_status=runtime["effective_update_status"],
        prefilter_artifact_status=prefilter_artifact_status,
        automation_health=runtime["automation_health"],
        governance_recommended_action=governance_recommended_action,
        cockpit_model=cockpit_model,
        candidate_artifact_status=candidate_artifact_status,
        current_basket_pointer_status=current_basket_pointer_status,
        current_basket_pointer_updated_at=current_basket_pointer_updated_at,
        current_basket_pointer_basket_id=current_basket_pointer_basket_id,
        latest_basket_attempt_status=latest_basket_attempt_status,
        latest_basket_attempt_generated_at=latest_basket_attempt_generated_at,
        latest_basket_attempt_blocking_reason=latest_basket_attempt_blocking_reason,
        observation_wait_status=runtime_evidence.observation_wait_status,
        daily_closure_latest=runtime_evidence.daily_closure_latest,
    )

    return StockDashboardRenderInputs(
        context=context,
        current_view=resolved_current_view,
        candidate_index=resolved_candidate_index,
        current_report=resolved_current_report,
        view_labels=view_labels,
        view_subtitles=view_subtitles,
        view_subtitle=view_subtitle,
        report_state=str(core["report_state"]),
        exp_dir=exp_dir,
        daily_md_text=str(reports["daily_md_text"]),
        translated_daily_md_text=str(reports["translated_daily_md_text"]),
        latest_report_text=str(reports["latest_report_text"]),
        health=core["health"],
        health_status=str(core["health_status"]),
        health_tag=str(core["health_tag"]),
        candidate_cards=candidate["cards"],
        top1=candidate["top1"],
        top1_signal=str(candidate["top1_signal"]),
        top1_risk=str(candidate["top1_risk"]),
        bt_diag=core["bt_diag"],
        top1_label=str(candidate["top1_label"]),
        summary_lines=candidate["summary_lines"],
        update_health=core["update_health"],
        daily_md_href=str(reports["daily_md_href"]),
        daily_md_download_href=str(reports["daily_md_download_href"]),
        health_csv=reports["health_csv"],
        health_csv_href=str(reports["health_csv_href"]),
        health_csv_download_href=str(reports["health_csv_download_href"]),
        leaderboard_csv=reports["leaderboard_csv"],
        leaderboard_href=str(reports["leaderboard_href"]),
        leaderboard_download_href=str(reports["leaderboard_download_href"]),
        candidates_csv=reports["candidates_csv"],
        candidates_md_href=str(reports["candidates_md_href"]),
        candidates_md_download_href=str(reports["candidates_md_download_href"]),
        candidates_csv_href=str(reports["candidates_csv_href"]),
        candidates_csv_download_href=str(reports["candidates_csv_download_href"]),
        latest_report_href=str(reports["latest_report_href"]),
        latest_report_download_href=str(reports["latest_report_download_href"]),
        health_chart_html=str(reports["health_chart_html"]),
        backtest_equity_html=str(reports["backtest_equity_html"]),
        backtest_drawdown_html=str(reports["backtest_drawdown_html"]),
        backtest_chart_html=str(reports["backtest_chart_html"]),
        backtest_map_chart_html=str(reports["backtest_map_chart_html"]),
        candidate_chart_html=str(reports["candidate_chart_html"]),
        candidate_map_chart_html=str(reports["candidate_map_chart_html"]),
        candidate_detail_html=str(reports["candidate_detail_html"]),
        market_snapshot=candidate["market_snapshot"],
        basket_summary=candidate["basket_summary"],
        basket_validation=candidate["basket_validation"],
        candidate_basket_feedback=candidate["candidate_basket_feedback"],
        governance_cycle=governance["cycle"],
        governance_cycle_state=str(governance["cycle_state"]),
        governance_recommended_action_label=str(runtime_view_model["governance_recommended_action_label"]),
        governance_operator_message=str(governance["operator_message"]),
        governance_release_readiness=governance["release_readiness"],
        governance_fully_release_ready=bool(governance["fully_release_ready"]),
        previous_stable_run_id=str(governance["previous_stable_run_id"]),
        research_batch_status=runtime["research_batch_status"],
        daily_research_runtime=runtime["daily_research_runtime"],
        progress_pct_label=str(runtime["progress_pct_label"]),
        effective_update_status=runtime["effective_update_status"],
        automation_health=runtime["automation_health"],
        candidate_artifact_status=candidate_artifact_status,
        prefilter_artifact_status=prefilter_artifact_status,
        research_topology=runtime["research_topology"],
        grid_backtest_status=runtime["grid_backtest_status"],
        evolution_status=runtime["evolution_status"],
        decision_semantics=semantics["decision"],
        blocker_semantics=semantics["blocker"],
        execution_semantics=semantics["execution"],
        evidence_semantics=semantics["evidence"],
        governance_semantics=semantics["governance"],
        update_timeline_panel=str(runtime["update_timeline_panel"]),
        update_alerts_panel=str(runtime["update_alerts_panel"]),
        backtest_scope=core["backtest_scope"],
        run_freshness=str(core["run_freshness"]),
        candidate_score=str(candidate["candidate_score"]),
        candidate_name=str(candidate["candidate_name"]),
        candidate_count=int(candidate["candidate_count"]),
        candidate_source_label=str(candidate["candidate_source_label"]),
        generation_mode_label=str(candidate["generation_mode_label"]),
        current_basket_pointer_status=current_basket_pointer_status,
        current_basket_pointer_basket_id=current_basket_pointer_basket_id,
        latest_basket_attempt_status=latest_basket_attempt_status,
        latest_basket_attempt_blocking_reason=latest_basket_attempt_blocking_reason,
        headline_tone=str(core["headline_tone"]),
        headline_detail=str(core["headline_detail"]),
        primary_result=primary_result,
        first_place_evidence_cockpit_section=str(primary["first_place_evidence_cockpit_section"]),
        primary_result_card_section=str(primary["primary_result_card_section"]),
        stock_ai_explainer=primary["stock_ai_explainer"],
        primary_conclusion=primary_result_query["primary_conclusion"],
        t12_sections=build_t12_read_only_sections(
            minimal_facts=t12["minimal_facts"],
            governance_source_facts=t12["governance_source_facts"],
        ),
        primary_result_bridge_context=build_stock_primary_result_bridge_context(
            current_view=resolved_current_view,
            base_path=base_path,
            primary_result=primary_result,
            bridge_enabled=primary_result_bridge_enabled,
        ),
        cockpit_model=cockpit_model,
        update_status_label=str(runtime_view_model["update_status_label"]),
        update_stage_label=str(runtime_view_model["update_stage_label"]),
        prefilter_freshness_label=str(runtime_view_model["prefilter_freshness_label"]),
        automation_health_label=str(runtime_view_model["automation_health_label"]),
        promotion_decision_label=str(runtime_view_model["promotion_decision_label"]),
        db_latest_trade_date=str(runtime_view_model["db_latest_trade_date"]),
        candidate_timeline_label=str(runtime_view_model["candidate_timeline_label"]),
        observation_timeline_label=str(runtime_view_model["observation_timeline_label"]),
        timeline_consistency_note=str(runtime_view_model["timeline_consistency_note"]),
        current_basket_pointer_label=str(runtime_view_model["current_basket_pointer_label"]),
        latest_basket_attempt_label=str(runtime_view_model["latest_basket_attempt_label"]),
    )
