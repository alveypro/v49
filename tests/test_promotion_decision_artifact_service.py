from __future__ import annotations

import json
from pathlib import Path

from openclaw.services.promotion_decision_artifact_service import build_promotion_decision_artifact


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_promotion_decision_artifact_blocks_production_without_execution_evidence(tmp_path: Path):
    sweep = _write_json(
        tmp_path / "stable_sweep.json",
        {
            "run_id": "sweep_stable_ok",
            "strategy": "stable",
            "backtest_credibility": {"passed": True, "parameter_sensitivity": True},
            "strategy_backtest_diagnostics": {
                "eligible_for_formal_ranking": True,
                "credible_evidence_present": True,
                "quality_floor_passed": True,
            },
        },
    )
    audit = _write_json(tmp_path / "stage_audit.json", {"passed": True, "blocking_reasons": []})
    ledger = tmp_path / "rejected.jsonl"

    payload = build_promotion_decision_artifact(
        strategy="stable",
        sweep_artifact_path=str(sweep),
        stage_audit_artifact_path=str(audit),
        rejected_ledger_path=str(ledger),
        output_dir=str(tmp_path),
        decision_id="dec_stable_candidate",
    )

    assert payload["decision_id"] == "dec_stable_candidate"
    assert payload["decision_type"] == "candidate_discussion"
    assert payload["production_candidate_allowed"] is False
    assert payload["formal_ranking_allowed"] is False
    assert payload["linked_run_id"] == "sweep_stable_ok"
    assert payload["blocking_reasons"] == ["execution_fact_chain_missing"]
    assert "order_fill_attribution" in payload["evidence"]["execution_evidence"]["missing_fields"]
    assert Path(payload["artifacts"]["json"]).exists()


def test_promotion_decision_artifact_blocks_rejected_sweep_artifact(tmp_path: Path):
    sweep = _write_json(
        tmp_path / "stable_sweep_rejected.json",
        {
            "run_id": "sweep_stable_rejected",
            "strategy": "stable",
            "backtest_credibility": {"passed": True},
            "strategy_backtest_diagnostics": {
                "eligible_for_formal_ranking": True,
                "credible_evidence_present": True,
                "quality_floor_passed": True,
            },
        },
    )
    audit = _write_json(tmp_path / "stage_audit.json", {"passed": True})
    ledger = tmp_path / "rejected.jsonl"
    ledger.write_text(
        json.dumps(
            {
                "artifact_path": str(sweep),
                "strategy": "stable",
                "reason": "superseded_by_bad_evidence",
                "reused_as_runtime_default": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    payload = build_promotion_decision_artifact(
        strategy="stable",
        sweep_artifact_path=str(sweep),
        stage_audit_artifact_path=str(audit),
        rejected_ledger_path=str(ledger),
        output_dir=str(tmp_path),
    )

    assert "sweep_artifact_present_in_rejected_ledger" in payload["blocking_reasons"]
    assert payload["evidence"]["rejected_ledger_check"]["matched_rejected_count"] == 1


def test_promotion_decision_artifact_accepts_complete_execution_evidence_but_not_production(tmp_path: Path):
    sweep = _write_json(
        tmp_path / "stable_sweep.json",
        {
            "run_id": "sweep_stable_ok",
            "strategy": "stable",
            "backtest_credibility": {"passed": True, "parameter_sensitivity": True},
            "strategy_backtest_diagnostics": {
                "eligible_for_formal_ranking": True,
                "credible_evidence_present": True,
                "quality_floor_passed": True,
            },
        },
    )
    audit = _write_json(tmp_path / "stage_audit.json", {"passed": True, "blocking_reasons": []})

    payload = build_promotion_decision_artifact(
        strategy="stable",
        sweep_artifact_path=str(sweep),
        stage_audit_artifact_path=str(audit),
        rejected_ledger_path=str(tmp_path / "missing.jsonl"),
        output_dir=str(tmp_path),
        execution_evidence={
            "passed": True,
            "blocking_reasons": [],
            "total_orders": 2,
            "linked_run_ids": ["sweep_stable_ok"],
            "cases": [{"order_id": "ord_1", "decision_id": "dec_1"}],
        },
    )

    assert payload["status"] == "candidate_execution_evidence_ready"
    assert payload["blocking_reasons"] == []
    assert payload["production_candidate_allowed"] is False
    assert payload["formal_ranking_allowed"] is False
    assert payload["evidence"]["execution_evidence"]["present"] is True
