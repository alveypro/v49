from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from src.dashboard_support import (
    translate_guardrail_mode,
    translate_strategy_mode,
    translate_strategy_strictness,
    translate_weak_market_action,
)
from src.stock_dashboard_sections import (
    render_diagnostics_appendix_section,
    render_prefilter_section,
    render_selection_funnel_section,
    render_validation_section,
)
from src.stock_dashboard_view_model import (
    build_stock_candidate_diagnostics_view_model,
    build_stock_prefilter_view_model,
    build_stock_selection_funnel_view_model,
    build_stock_validation_view_model,
)


@dataclass(frozen=True)
class StockDiagnosticsSections:
    diagnostics_view_model: dict[str, Any]
    validation_section: str
    prefilter_section: str
    selection_funnel_section: str
    diagnostics_appendix_section: str


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _best_variant_label(basket_validation_summary: dict[str, Any], basket_validation_variants: dict[str, Any]) -> str:
    if _safe_int(basket_validation_summary.get("rebalance_dates", 0)) <= 0 or not basket_validation_variants:
        return "-"
    best_variant = str(
        max(
            basket_validation_variants.items(),
            key=lambda item: float((item[1] or {}).get("avg_excess_return_5d", float("-inf")) or float("-inf")),
        )[0]
    )
    return translate_strategy_mode(best_variant)


def build_stock_diagnostics_sections(
    *,
    visible: Callable[[str], bool],
    current_view: str,
    basket_validation: dict[str, Any],
    candidate_basket_feedback: dict[str, Any],
    evolution_status: dict[str, Any],
    basket_summary: dict[str, Any],
    candidate_artifact_status: dict[str, Any],
    validation_basket_kpis: list[dict[str, Any]],
    generation_mode_label: str,
    candidate_count: int,
    top1: dict[str, Any],
    prefilter_artifact_status: dict[str, Any],
    db_latest_trade_date: str,
    diagnosis_section: str,
) -> StockDiagnosticsSections:
    basket_validation_summary = basket_validation.get("summary", {}) or {}
    basket_validation_variants = basket_validation.get("variants", {}) or {}
    diagnostics_view_model = build_stock_candidate_diagnostics_view_model(
        basket_validation_summary=basket_validation_summary,
        basket_validation_variants=basket_validation_variants,
        candidate_basket_feedback=candidate_basket_feedback,
        evolution_status=evolution_status,
        basket_summary=basket_summary,
        candidate_artifact_status=candidate_artifact_status,
        best_variant_label=_best_variant_label(basket_validation_summary, basket_validation_variants),
    )

    validation_view_model = build_stock_validation_view_model(
        rebalance_dates=int(diagnostics_view_model["rebalance_dates"]),
        basket_validation_summary=basket_validation_summary,
        expected_basket_return=float(basket_summary.get("expected_basket_return", 0.0) or 0.0),
        risk_pressure_score=basket_summary.get("risk_pressure_score", 0.0),
        liquidity_capacity_state=str(diagnostics_view_model["liquidity_capacity_state"]),
        weighted_liquidity_score=float(diagnostics_view_model["weighted_liquidity_score"]),
        liquidity_capacity_weight=float(diagnostics_view_model["liquidity_capacity_weight"]),
        candidate_feedback_window=str(diagnostics_view_model["candidate_feedback_window"]),
        candidate_feedback_level=str(diagnostics_view_model["candidate_feedback_level"]),
        candidate_feedback_summary=str(diagnostics_view_model["candidate_feedback_summary"]),
        candidate_feedback_changes=diagnostics_view_model["candidate_feedback_changes"],
        candidate_runtime_stage_label=str(diagnostics_view_model["candidate_runtime_stage_label"]),
        candidate_runtime_status_label=str(diagnostics_view_model["candidate_runtime_status_label"]),
        candidate_runtime_detail=str(diagnostics_view_model["candidate_runtime_detail"]),
        candidate_runtime_results_ready=str(diagnostics_view_model["candidate_runtime_results_ready"]),
        candidate_runtime_skipped=str(diagnostics_view_model["candidate_runtime_skipped"]),
        candidate_runtime_updated_label=str(diagnostics_view_model["candidate_runtime_updated_label"]),
        candidate_runtime_elapsed=str(diagnostics_view_model["candidate_runtime_elapsed"]),
        guardrail_mode_label=translate_guardrail_mode(str(basket_summary.get("guardrail_mode", "normal"))),
        guardrail_reasons_label=",".join(basket_summary.get("guardrail_reasons", [])) or "未触发防守降级",
        generation_mode_label=generation_mode_label,
        generation_reason=str(candidate_artifact_status.get("generation_reason", "-")),
        validation_basket_kpis=validation_basket_kpis,
        skipped_count=basket_summary.get("skipped_count", 0),
        best_variant_label=str(diagnostics_view_model["best_variant_label"]),
        strategy_mode_label=translate_strategy_mode(candidate_artifact_status.get("strategy_mode", "-")),
        strategy_strictness_label=translate_strategy_strictness(candidate_artifact_status.get("strategy_strictness", "-")),
        strategy_weak_market_action_label=translate_weak_market_action(candidate_artifact_status.get("strategy_weak_market_action", "-")),
        diversified_avg_excess_return_5d_label=str(diagnostics_view_model["diversified_avg_excess_return_5d_label"]),
        raw_avg_excess_return_5d_label=str(diagnostics_view_model["raw_avg_excess_return_5d_label"]),
        top1_avg_excess_return_5d_label=str(diagnostics_view_model["top1_avg_excess_return_5d_label"]),
        avg_basket_return_5d_label=str(diagnostics_view_model["avg_basket_return_5d_label"]),
        avg_excess_return_5d_label=str(diagnostics_view_model["avg_excess_return_5d_label"]),
        basket_win_rate_5d_label=str(diagnostics_view_model["basket_win_rate_5d_label"]),
        avg_top1_return_5d_label=str(diagnostics_view_model["avg_top1_return_5d_label"]),
    )
    validation_section = render_validation_section(validation_view_model)

    prefilter_view_model = build_stock_prefilter_view_model(
        prefilter_artifact_status=prefilter_artifact_status,
        db_latest_trade_date=db_latest_trade_date,
        top_candidates=prefilter_artifact_status.get("top_candidates", []) or [],
        exclusion_summary_rows=prefilter_artifact_status.get("exclusion_summary", []) or [],
        top_exclusion_rows=prefilter_artifact_status.get("top_exclusions", []) or [],
    )
    prefilter_section = render_prefilter_section(prefilter_view_model) if visible("prefilter") else ""

    funnel_view_model = build_stock_selection_funnel_view_model(
        market_sample_count=_safe_int(prefilter_artifact_status.get("market_symbol_count", "0")),
        prefilter_pass_count=_safe_int(prefilter_artifact_status.get("row_count", "0")),
        final_candidate_count=int(candidate_count),
        top_code=str(top1.get("ts_code", "-")),
        pass_rate_pct=str(prefilter_artifact_status.get("pass_rate_pct", "0.0%")),
        top_exclusion_reason=str(prefilter_view_model["top_exclusion_reason"]),
        top_exclusion_reason_count=str(prefilter_artifact_status.get("top_exclusion_reason_count", "0")),
        configured_liquidity_min_turnover=str(prefilter_artifact_status.get("configured_liquidity_min_turnover", "0")),
        effective_liquidity_min_turnover=str(prefilter_artifact_status.get("effective_liquidity_min_turnover", "0")),
    )
    selection_funnel_section = render_selection_funnel_section(funnel_view_model)

    diagnostics_appendix_section = ""
    if current_view == "overview":
        diagnostics_appendix_section = render_diagnostics_appendix_section(
            validation_section=validation_section,
            diagnosis_section=diagnosis_section,
            prefilter_section=prefilter_section,
        )

    return StockDiagnosticsSections(
        diagnostics_view_model=diagnostics_view_model,
        validation_section=validation_section,
        prefilter_section=prefilter_section,
        selection_funnel_section=selection_funnel_section,
        diagnostics_appendix_section=diagnostics_appendix_section,
    )
