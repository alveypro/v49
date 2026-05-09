from __future__ import annotations

from openclaw.services.ai_runtime_backtest_contract_service import (
    build_ai_runtime_backtest_contract_failure,
    evaluate_ai_runtime_candidate_contract,
)


def test_ai_runtime_backtest_contract_failure_is_structured_and_non_competing():
    payload = build_ai_runtime_backtest_contract_failure(strategy="ai")

    assert payload["status"] == "failed"
    assert payload["research_only"] is True
    assert payload["promotion_blocked"] is True
    assert payload["eligible_for_formal_ranking"] is False
    assert "real_runtime_backtest_handler" in payload["required_to_compete"]
    assert "required_signal_fact_fields" in payload["runtime_backtest_contract"]
    assert "no_formal_competition_until_same_gate_as_runtime_strategies_passes" in payload["runtime_backtest_contract"]["hard_boundaries"]


def test_ai_runtime_candidate_contract_requires_fact_chain_and_same_gate():
    review = evaluate_ai_runtime_candidate_contract(
        {
            "runtime_scan_handler": True,
            "runtime_backtest_handler": True,
            "point_in_time_feature_snapshot": True,
            "cost_slippage_volume_constraints": True,
            "fact_chain": {
                "signal_id": "sig-1",
                "ts_code": "000001.SZ",
                "as_of_date": "20260102",
                "input_snapshot_id": "snap-1",
                "feature_values": {"momentum": 1.2},
                "model_or_rule_version": "ai.runtime.v1",
                "score": 72.0,
                "reason_codes": ["momentum"],
                "execution_constraints": {"volume": True},
            },
            "backtest_result": {
                "strategy_backtest_diagnostics": {
                    "eligible_for_formal_ranking": True,
                }
            },
        }
    )

    assert review["eligible_for_runtime_candidate"] is True
    assert review["eligible_for_formal_ranking"] is False
    assert review["research_only"] is False


def test_ai_runtime_candidate_contract_blocks_incomplete_fact_chain():
    review = evaluate_ai_runtime_candidate_contract({"runtime_scan_handler": True, "fact_chain": {}})

    assert review["eligible_for_runtime_candidate"] is False
    assert "missing_runtime_backtest_handler" in review["blocking_reasons"]
    assert "missing_fact_chain_signal_id" in review["blocking_reasons"]
    assert "same_backtest_gate_not_passed" in review["blocking_reasons"]
