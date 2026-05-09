from __future__ import annotations

import json
from pathlib import Path

from openclaw.services.ensemble_observation_promotion_decision_service import (
    build_ensemble_observation_promotion_decision,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _shadow_payload() -> dict:
    return {
        "candidate": "hard_event_alpha_candidate",
        "rule_freeze": {
            "frozen": True,
            "rule_version": "hard_event_alpha_candidate.v2",
            "rule_hash": "abc123",
            "sleeve_policy_approved": False,
        },
        "benchmark": {
            "passed": True,
            "after_cost_excess_return": 1.016334,
            "hit_rate": 0.875,
            "turnover": 0.59019,
            "industry_concentration": 0.240416,
            "capacity_utilization": 0.032271,
            "blocking_reasons": [],
            "regime_split": {
                "neutral": {"hit_rate": 0.75, "window_count": 4},
                "risk_on": {"hit_rate": 1.0, "window_count": 4},
            },
        },
    }


def test_ensemble_observation_promotion_decision_allows_manual_review_without_pool_mutation(tmp_path: Path):
    shadow = _write_json(tmp_path / "shadow.json", _shadow_payload())
    gate = _write_json(
        tmp_path / "gate.json",
        {
            "gate": {
                "candidate": "hard_event_alpha_candidate",
                "research_only": True,
                "observation_gate_passed": True,
                "observation_review_eligible": True,
                "observation_pool_eligible": False,
                "formal_pool_eligible": False,
                "blocking_reasons": [],
                "evidence_summary": {"window_count": 8},
            },
            "source_shadow_benchmark_json": str(shadow),
        },
    )
    audit = _write_json(
        tmp_path / "stage_audit.json",
        {
            "passed": True,
            "blocking_reasons": [],
            "top_strategies": ["v4", "v5", "v9"],
            "observation_pool": [],
        },
    )

    payload = build_ensemble_observation_promotion_decision(
        observation_gate_artifact_path=str(gate),
        stage_audit_artifact_path=str(audit),
        output_dir=str(tmp_path / "decision"),
        operator_name="test_operator",
        decision_id="dec_ensemble_obs_ready",
    )

    assert payload["decision_id"] == "dec_ensemble_obs_ready"
    assert payload["status"] == "observation_promotion_review_ready"
    assert payload["observation_promotion_allowed"] is True
    assert payload["strategy_pool_mutation_allowed"] is False
    assert payload["formal_candidate_allowed"] is False
    assert payload["formal_ranking_allowed"] is False
    assert payload["production_candidate_allowed"] is False
    assert payload["blocking_reasons"] == []
    assert payload["required_next_action"] == "manual_apply_research_only_to_observation_pool_transition"
    assert Path(payload["artifacts"]["json"]).exists()
    assert Path(payload["artifacts"]["markdown"]).exists()


def test_ensemble_observation_promotion_decision_blocks_failed_gate(tmp_path: Path):
    shadow = _write_json(tmp_path / "shadow.json", _shadow_payload())
    gate = _write_json(
        tmp_path / "gate.json",
        {
            "gate": {
                "candidate": "hard_event_alpha_candidate",
                "research_only": True,
                "observation_gate_passed": False,
                "observation_review_eligible": False,
                "observation_pool_eligible": False,
                "formal_pool_eligible": False,
                "blocking_reasons": ["neutral_regime_hit_rate_below_floor"],
            },
            "source_shadow_benchmark_json": str(shadow),
        },
    )
    audit = _write_json(tmp_path / "stage_audit.json", {"passed": True})

    payload = build_ensemble_observation_promotion_decision(
        observation_gate_artifact_path=str(gate),
        stage_audit_artifact_path=str(audit),
        output_dir=str(tmp_path / "decision"),
    )

    assert payload["status"] == "observation_promotion_blocked"
    assert payload["observation_promotion_allowed"] is False
    assert "observation_gate_not_passed" in payload["blocking_reasons"]
    assert "observation_review_not_eligible" in payload["blocking_reasons"]
    assert "observation_gate_blocked:neutral_regime_hit_rate_below_floor" in payload["blocking_reasons"]


def test_ensemble_observation_promotion_decision_blocks_attempted_direct_formal_eligibility(tmp_path: Path):
    shadow = _write_json(tmp_path / "shadow.json", _shadow_payload())
    gate = _write_json(
        tmp_path / "gate.json",
        {
            "gate": {
                "candidate": "hard_event_alpha_candidate",
                "research_only": True,
                "observation_gate_passed": True,
                "observation_review_eligible": True,
                "observation_pool_eligible": True,
                "formal_pool_eligible": True,
                "blocking_reasons": [],
            },
            "source_shadow_benchmark_json": str(shadow),
        },
    )
    audit = _write_json(tmp_path / "stage_audit.json", {"passed": True})

    payload = build_ensemble_observation_promotion_decision(
        observation_gate_artifact_path=str(gate),
        stage_audit_artifact_path=str(audit),
        output_dir=str(tmp_path / "decision"),
    )

    assert payload["observation_promotion_allowed"] is False
    assert "gate_attempted_direct_observation_pool_eligibility" in payload["blocking_reasons"]
    assert "gate_attempted_formal_pool_eligibility" in payload["blocking_reasons"]
