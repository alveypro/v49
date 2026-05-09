from __future__ import annotations

from typing import Any, Dict, Iterable, List

from openclaw.services.ensemble_core_contract_service import build_ensemble_core_contract_review


JsonDict = Dict[str, Any]


def build_strategy_maturity_plan(unified_recommendation: JsonDict) -> JsonDict:
    """Build a promotion roadmap from current evidence without changing gates."""

    reviews = [dict(item) for item in unified_recommendation.get("all_strategy_reviews") or [] if isinstance(item, dict)]
    pool_status = _pool_status_by_strategy(unified_recommendation)
    items = []
    for review in reviews:
        strategy = str(review.get("strategy") or "")
        status = pool_status.get(strategy, "diagnostic")
        backtest = review.get("backtest_component") if isinstance(review.get("backtest_component"), dict) else {}
        item = {
            "strategy": strategy,
            "current_pool": status,
            "truth": _truth_label(status=status, backtest=backtest),
            "can_compete_for_formal_top": status == "formal_eligible",
            "evidence_snapshot": {
                "backtest_passed": bool(backtest.get("passed") is True),
                "quality_floor_passed": bool(backtest.get("quality_floor_passed") is True),
                "eligible_for_formal_ranking": bool(backtest.get("eligible_for_formal_ranking") is True),
                "source_run_id": str(backtest.get("source_run_id") or ""),
                "source_artifact_path": str(backtest.get("source_artifact_path") or ""),
                "blocking_reasons": list(review.get("blocking_reasons") or []),
                "backtest_blocking_reasons": list(backtest.get("blocking_reasons") or []),
            },
            "promotion_requirements": _promotion_requirements(strategy=strategy, status=status, backtest=backtest),
            "next_experiment": _next_experiment(strategy=strategy, status=status, backtest=backtest),
        }
        items.append(item)
    return {
        "maturity_plan_version": "strategy_maturity_plan.v1",
        "policy": {
            "formal_top_requires": [
                "credible_point_in_time_backtest",
                "cost_slippage_tradeability_constraints",
                "parameter_sensitivity",
                "quality_floor_win_rate_signal_density_drawdown",
                "traceable_artifact_and_run_id",
            ],
            "forbidden_shortcuts": [
                "lower_threshold_to_create_signals",
                "promote_observation_pool_without_quality_floor",
                "treat_research_only_ai_as_backtested_strategy",
                "reuse_rejected_artifact_as_runtime_default",
            ],
            "industry_reference_design": [
                "single_source_of_truth_for_evidence",
                "walk_forward_out_of_sample_validation",
                "execution_cost_and_capacity_constraints",
                "model_risk_governance_before_promotion",
                "shadow_or_canary_execution_before_production",
            ],
        },
        "items": sorted(items, key=lambda item: (_status_rank(str(item.get("current_pool") or "")), str(item.get("strategy") or ""))),
    }


def _pool_status_by_strategy(recommendation: JsonDict) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in recommendation.get("eligible_pool") or []:
        if isinstance(item, dict):
            out[str(item.get("strategy") or "")] = "formal_eligible"
    for item in recommendation.get("observation_pool") or []:
        if isinstance(item, dict):
            out[str(item.get("strategy") or "")] = "observation"
    for item in recommendation.get("diagnostic_pool") or []:
        if isinstance(item, dict):
            out[str(item.get("strategy") or "")] = "diagnostic"
    for item in recommendation.get("research_only_pool") or []:
        if isinstance(item, dict):
            out[str(item.get("strategy") or "")] = "research_only"
    return out


def _status_rank(status: str) -> int:
    return {"formal_eligible": 0, "observation": 1, "diagnostic": 2, "research_only": 3}.get(status, 9)


def _truth_label(*, status: str, backtest: JsonDict) -> str:
    if status == "formal_eligible":
        return "formal_strategy_with_current_backtest_evidence"
    if status == "observation":
        return "credible_backtest_but_not_alpha_quality"
    if status == "research_only":
        return "research_concept_without_runtime_backtest_contract"
    if backtest.get("source_run_id"):
        return "runtime_strategy_with_failed_or_incomplete_backtest_evidence"
    return "registered_strategy_without_enough_runtime_evidence"


def _promotion_requirements(*, strategy: str, status: str, backtest: JsonDict) -> List[str]:
    if status == "formal_eligible":
        return ["maintain_current_evidence_and_add_shadow_execution_before_production_promotion"]
    if strategy == "ensemble_core":
        return list(build_ensemble_core_contract_review({}).get("blocking_reasons") or [])
    if status == "research_only" or strategy == "ai":
        return [
            "implement_real_runtime_scan_and_backtest_handler",
            "persist_explainable_signal_fact_chain_per_stock",
            "prove_point_in_time_inputs_no_future_leakage",
            "route_backtest_through_cost_slippage_and_volume_constraints",
            "pass_same_quality_floor_as_non_ai_strategies",
        ]
    requirements: List[str] = []
    if backtest.get("passed") is not True:
        requirements.append("pass_backtest_credibility_gate_without_lowering_thresholds")
    if backtest.get("quality_floor_passed") is not True:
        requirements.append("raise_out_of_sample_win_rate_to_at_least_0_45_with_positive_signal_density_and_drawdown_le_0_25")
    for reason in backtest.get("blocking_reasons") or []:
        if reason == "missing_or_failed:parameter_sensitivity":
            requirements.append("run_multi_parameter_sweep_and_keep_failed_runs_recorded")
        if reason == "missing_positive_signal_density":
            requirements.append("produce_nonzero_signal_density_under_current_formal_gate")
        if reason == "missing_successful_test_windows":
            requirements.append("produce_successful_rolling_out_of_sample_windows")
        if str(reason).startswith("missing_or_failed:"):
            requirements.append("close_" + str(reason).replace("missing_or_failed:", "") + "_evidence")
    return _unique(requirements) or ["complete_current_backtest_blocking_reasons"]


def _next_experiment(*, strategy: str, status: str, backtest: JsonDict) -> JsonDict:
    if status == "formal_eligible":
        return {"type": "shadow_execution", "action": "collect_traceable_execution_cases_before_any_production_increase"}
    if strategy == "stable":
        return {
            "type": "defensive_allocator_rebuild",
            "action": "rebuild_stable_as_portfolio_overlay_with_regime_filter_and_weight_bounds_not_as_standalone_alpha",
            "success_metric": "portfolio_drawdown_reduction_with_non_negative_excess_return_and_no_hidden_turnover_cost",
            "required_contract": [
                "portfolio_overlay_backtest_against_formal_strategy_pool",
                "formal_pool_benchmark_return_series",
                "allocation_weight_bounds_and_cash_fallback_rule",
                "market_regime_filter_for_risk_off_activation",
                "correlation_and_overlap_report_vs_formal_top_strategies",
                "no_formal_top_promotion_until_standalone_alpha_quality_floor_passes",
            ],
        }
    if strategy == "v4":
        return {
            "type": "legacy_factor_refresh",
            "action": "filter_v4_entries_by_v5_style_launch_confirmation_or_regime_gate_and_rerun_sweep",
            "success_metric": "win_rate>=0.45 without reducing score_threshold_below_60",
        }
    if strategy == "v6":
        return {
            "type": "signal_generation_and_rolling_window_repair",
            "action": "repair_v6_entry_signal_conversion_and_rolling_window_success_before_any_quality_floor_tuning",
            "success_metric": "successful_rolling_windows>0 and signal_density>0",
            "required_contract": [
                "point_in_time_signal_generation_trace",
                "near_threshold_to_passed_threshold_conversion_report",
                "rolling_window_success_rate_report",
                "no_threshold_lowering_to_create_density",
            ],
        }
    if strategy == "v7":
        return {
            "type": "signal_generation_and_rolling_window_repair",
            "action": "prove_v7_can_generate_enough_point_in_time_signals_and_successful_rolling_windows_before_quality_floor_review",
            "success_metric": "successful_rolling_windows>0 and signal_density>0_under_threshold_60",
            "required_contract": [
                "score_distribution_with_near_threshold_samples",
                "rolling_window_success_rate_report",
                "decision_to_rebase_on_v8_filters_or_retire_if_signal_density_stays_zero",
                "no_threshold_lowering_to_create_density",
            ],
        }
    if strategy == "ai":
        return {
            "type": "research_to_runtime_contract",
            "action": "build_fact_chain_first_then_backtest_handler",
            "success_metric": "same_backtest_and_quality_gate_as_registered_runtime_strategies",
            "required_contract": [
                "runtime_scan_handler",
                "runtime_backtest_handler",
                "persisted_explainable_fact_chain_per_signal",
                "point_in_time_feature_snapshot",
                "cost_slippage_volume_constraints",
            ],
        }
    if strategy == "ensemble_core":
        contract = build_ensemble_core_contract_review({})
        return {
            "type": "multi_alpha_portfolio_research_contract",
            "action": "build_ensemble_core_as_risk_budgeted_alpha_sleeve_portfolio_not_new_single_score_strategy",
            "success_metric": "walk_forward_oos_improvement_vs_v4_v5_v8_v9_combo_after_cost_capacity_and_alpha_attribution",
            "required_contract": list(contract.get("blocking_reasons") or []),
            "hard_boundaries": list(contract.get("hard_boundaries") or []),
            "donor_strategy_policy": dict(contract.get("donor_strategy_policy") or {}),
        }
    return {"type": "diagnostic_sweep", "action": "rerun_evidence_sweep_after_closing_blocking_reasons"}


def _unique(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        text = str(value or "")
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out
