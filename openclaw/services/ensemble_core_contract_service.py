from __future__ import annotations

from typing import Any, Dict, List


JsonDict = Dict[str, Any]


REQUIRED_ALPHA_SLEEVES = (
    "momentum",
    "reversal",
    "money_flow",
    "sector_rotation",
    "quality_low_vol",
    "event_risk",
)

REQUIRED_PIT_INPUTS = (
    "price_volume",
    "money_flow",
    "sector_heat",
    "suspension_limit",
    "volume_capacity",
)

REQUIRED_PORTFOLIO_CONTROLS = (
    "risk_budget",
    "industry_caps",
    "single_name_caps",
    "turnover_budget",
    "drawdown_budget",
    "capacity_constraint",
)

REQUIRED_ATTRIBUTION = (
    "alpha_ic",
    "alpha_decay",
    "alpha_correlation",
    "risk_contribution",
    "cost_slippage",
    "regime_split",
)


def build_ensemble_core_contract_review(payload: JsonDict | None = None) -> JsonDict:
    """Review whether ensemble_core has a real top-level portfolio contract.

    This is deliberately stricter than a single strategy score gate.  A
    multi-alpha ensemble is only useful if the system can replay alpha sleeves,
    portfolio construction, execution costs, and attribution from point-in-time
    facts.  Without that contract it remains research-only.
    """

    payload = payload if isinstance(payload, dict) else {}
    missing = []
    missing.extend(_missing("alpha_sleeves", payload.get("alpha_sleeves"), REQUIRED_ALPHA_SLEEVES))
    missing.extend(_missing("pit_inputs", payload.get("pit_inputs"), REQUIRED_PIT_INPUTS))
    missing.extend(_missing("portfolio_controls", payload.get("portfolio_controls"), REQUIRED_PORTFOLIO_CONTROLS))
    missing.extend(_missing("attribution", payload.get("attribution"), REQUIRED_ATTRIBUTION))

    if payload.get("runtime_scan_handler") is not True:
        missing.append("runtime_scan_handler")
    if payload.get("runtime_backtest_handler") is not True:
        missing.append("runtime_backtest_handler")
    if payload.get("walk_forward_backtest") is not True:
        missing.append("walk_forward_backtest")
    if payload.get("formal_pool_shadow_benchmark") is not True:
        missing.append("formal_pool_shadow_benchmark")

    return {
        "contract_version": "ensemble_core_contract.v1",
        "strategy": "ensemble_core",
        "research_only": bool(missing),
        "eligible_for_formal_ranking": not missing,
        "required_alpha_sleeves": list(REQUIRED_ALPHA_SLEEVES),
        "required_pit_inputs": list(REQUIRED_PIT_INPUTS),
        "required_portfolio_controls": list(REQUIRED_PORTFOLIO_CONTROLS),
        "required_attribution": list(REQUIRED_ATTRIBUTION),
        "blocking_reasons": missing,
        "hard_boundaries": [
            "do_not_treat_weighted_average_scores_as_portfolio_construction",
            "do_not_promote_without_alpha_sleeve_attribution",
            "do_not_hide_capacity_turnover_or_cost_in_combo_score",
            "do_not_use_v6_or_v7_as_standalone_formal_gate_without_rebase_evidence",
        ],
        "recommended_build_order": [
            "persist_alpha_sleeve_fact_chain",
            "compute_ic_decay_correlation_by_sleeve",
            "build_risk_budgeted_portfolio_constructor",
            "replay_walk_forward_with_cost_slippage_capacity",
            "shadow_compare_against_v4_v5_v8_v9_combo_formal_pool",
        ],
        "donor_strategy_policy": {
            "v6": "money_flow_event_alpha_sleeve_only_until_pit_and_runtime_contract_pass",
            "v7": "v8_filter_rebase_shadow_only_or_retire_standalone_gate",
        },
    }


def _missing(prefix: str, value: Any, required: tuple[str, ...]) -> List[str]:
    provided = set(str(item or "") for item in value) if isinstance(value, list) else set()
    return [f"missing_{prefix}:{item}" for item in required if item not in provided]
