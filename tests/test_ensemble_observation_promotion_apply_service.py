from __future__ import annotations

import json
from pathlib import Path

from openclaw.services.ensemble_observation_promotion_apply_service import (
    build_ensemble_observation_promotion_apply,
    load_observation_promotion_records,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _ready_decision() -> dict:
    return {
        "artifact_version": "ensemble_observation_promotion_decision.v1",
        "decision_id": "dec_ready",
        "status": "observation_promotion_review_ready",
        "strategy": "ensemble_core",
        "candidate": "hard_event_alpha_candidate",
        "observation_promotion_allowed": True,
        "strategy_pool_mutation_allowed": False,
        "formal_candidate_allowed": False,
        "formal_ranking_allowed": False,
        "production_candidate_allowed": False,
        "blocking_reasons": [],
    }


def test_ensemble_observation_promotion_apply_records_observation_only_transition(tmp_path: Path):
    decision = _write_json(tmp_path / "decision.json", _ready_decision())
    ledger = tmp_path / "observation_records.jsonl"

    payload = build_ensemble_observation_promotion_apply(
        promotion_decision_artifact_path=str(decision),
        output_dir=str(tmp_path / "apply"),
        observation_ledger_path=str(ledger),
        decision_id="dec_apply",
        operator_name="test_operator",
    )

    assert payload["status"] == "observation_pool_record_applied"
    assert payload["observation_pool_record_applied"] is True
    assert payload["strategy_pool_mutation"] == "observation_ledger_only"
    assert payload["formal_candidate_allowed"] is False
    assert payload["formal_ranking_allowed"] is False
    assert Path(payload["artifacts"]["json"]).exists()
    records = load_observation_promotion_records(str(ledger))
    assert len(records) == 1
    assert records[0]["strategy"] == "ensemble_core"
    assert records[0]["from_pool"] == "research_only"
    assert records[0]["to_pool"] == "observation"
    assert records[0]["formal_ranking_allowed"] is False


def test_ensemble_observation_promotion_apply_blocks_formal_attempt(tmp_path: Path):
    source = _ready_decision()
    source["formal_ranking_allowed"] = True
    decision = _write_json(tmp_path / "decision.json", source)

    payload = build_ensemble_observation_promotion_apply(
        promotion_decision_artifact_path=str(decision),
        output_dir=str(tmp_path / "apply"),
        observation_ledger_path=str(tmp_path / "ledger.jsonl"),
    )

    assert payload["status"] == "observation_pool_record_blocked"
    assert payload["observation_pool_record_applied"] is False
    assert "source_decision_attempts_formal_eligibility" in payload["blocking_reasons"]
    assert not (tmp_path / "ledger.jsonl").exists()
