from __future__ import annotations

from typing import Any, Dict


JsonDict = Dict[str, Any]


def build_ai_runtime_backtest_contract_failure(*, strategy: str = "ai") -> JsonDict:
    return {
        "status": "failed",
        "reason": "backtest_not_implemented",
        "strategy": str(strategy or "ai"),
        "research_only": True,
        "promotion_blocked": True,
        "eligible_for_formal_ranking": False,
        "required_to_compete": [
            "real_runtime_scan_handler",
            "real_runtime_backtest_handler",
            "explainable_signal_fact_chain",
            "point_in_time_inputs",
            "cost_slippage_fill_constraints",
            "stage_audit_promotion_decision",
        ],
        "runtime_backtest_contract": {
            "required_input_fields": [
                "run_id",
                "strategy",
                "date_from",
                "date_to",
                "as_of_date",
                "universe",
                "model_or_rule_version",
            ],
            "required_signal_fact_fields": [
                "signal_id",
                "ts_code",
                "as_of_date",
                "input_snapshot_id",
                "feature_values",
                "model_or_rule_version",
                "score",
                "reason_codes",
                "execution_constraints",
            ],
            "required_output_fields": [
                "status",
                "summary",
                "backtest_data",
                "backtest_diagnostics",
                "fact_chain",
            ],
            "hard_boundaries": [
                "no_research_notebook_backtest_as_runtime_evidence",
                "no_signal_without_point_in_time_feature_snapshot",
                "no_formal_competition_until_same_gate_as_runtime_strategies_passes",
            ],
        },
        "fact_chain": {
            "available": False,
            "required_fields": [
                "signal_id",
                "ts_code",
                "as_of_date",
                "input_snapshot_id",
                "feature_values",
                "model_or_rule_version",
                "score",
                "reason_codes",
                "execution_constraints",
            ],
        },
    }


def evaluate_ai_runtime_candidate_contract(payload: JsonDict) -> JsonDict:
    payload = dict(payload or {})
    required_top_level = [
        "runtime_scan_handler",
        "runtime_backtest_handler",
        "point_in_time_feature_snapshot",
        "cost_slippage_volume_constraints",
        "fact_chain",
    ]
    blocking: list[str] = []
    for field in required_top_level:
        if payload.get(field) in (None, "", False):
            blocking.append(f"missing_{field}")

    fact_chain = payload.get("fact_chain") if isinstance(payload.get("fact_chain"), dict) else {}
    required_fact_fields = [
        "signal_id",
        "ts_code",
        "as_of_date",
        "input_snapshot_id",
        "feature_values",
        "model_or_rule_version",
        "score",
        "reason_codes",
        "execution_constraints",
    ]
    for field in required_fact_fields:
        if field not in fact_chain or fact_chain.get(field) in (None, "", []):
            blocking.append(f"missing_fact_chain_{field}")

    backtest = payload.get("backtest_result") if isinstance(payload.get("backtest_result"), dict) else {}
    diagnostics = (
        backtest.get("strategy_backtest_diagnostics")
        if isinstance(backtest.get("strategy_backtest_diagnostics"), dict)
        else {}
    )
    if diagnostics.get("eligible_for_formal_ranking") is not True:
        blocking.append("same_backtest_gate_not_passed")

    return {
        "contract_version": "ai_runtime_candidate_contract.v1",
        "research_only": bool(blocking),
        "eligible_for_runtime_candidate": not blocking,
        "eligible_for_formal_ranking": False,
        "blocking_reasons": sorted(set(blocking)),
        "hard_boundaries": [
            "runtime_candidate_is_not_formal_ranking",
            "same_backtest_gate_required_before_formal_competition",
            "no_ai_signal_without_explainable_point_in_time_fact_chain",
        ],
    }
