from __future__ import annotations

import json
import sqlite3
from hashlib import sha256
from pathlib import Path

from openclaw.services.strategy_competition_formal_rerun_output_submission_service import (
    build_strategy_competition_formal_rerun_output_submission,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _plan(tmp_path: Path, ready: bool = True) -> Path:
    return _write_json(
        tmp_path / "plan.json",
        {
            "artifact_version": "strategy_competition_formal_rerun_plan.v1",
            "competition_run_id": "comp_test",
            "rerun_plan_status": "formal_rerun_plan_ready" if ready else "formal_rerun_plan_blocked",
            "passed": ready,
            "rerun_plan_hash": "plan_hash",
            "source_manifest_hash": "manifest_hash",
            "rerun_steps": [
                {"step": "shadow_execution_evidence", "command": "python3 tools/record_strategy_competition_shadow_feedback.py"},
                {"step": "independent_validation", "command": "python3 tools/build_strategy_competition_independent_validation.py"},
            ] if ready else [],
        },
    )


def test_formal_rerun_output_submission_blocks_missing_outputs(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))

    submission = build_strategy_competition_formal_rerun_output_submission(
        conn,
        rerun_plan_artifact_path=_plan(tmp_path),
        step_output_artifact_paths={},
        output_dir=tmp_path / "submission",
    )

    conn.close()
    assert submission["rerun_output_submission_status"] == "formal_rerun_output_submission_blocked"
    assert submission["passed"] is False
    assert submission["production_candidate_allowed"] is False
    assert submission["live_order_authority_granted"] is False
    assert "formal_rerun_plan_not_ready" not in submission["blocking_reasons"]
    assert "rerun_output_submission_missing" in submission["blocking_reasons"]


def test_formal_rerun_output_submission_accepts_ordered_passed_outputs(tmp_db: Path, tmp_path: Path):
    plan = _plan(tmp_path)
    out1 = _write_json(tmp_path / "shadow.json", {"passed": True})
    out2 = _write_json(tmp_path / "independent.json", {"passed": True})
    conn = sqlite3.connect(str(tmp_db))

    submission = build_strategy_competition_formal_rerun_output_submission(
        conn,
        rerun_plan_artifact_path=plan,
        step_output_artifact_paths={
            "shadow_execution_evidence": str(out1),
            "independent_validation": str(out2),
        },
        output_dir=tmp_path / "submission",
    )

    conn.close()
    assert submission["rerun_output_submission_status"] == "formal_rerun_output_submission_ready"
    assert submission["passed"] is True
    assert submission["production_candidate_allowed"] is False
    assert [item["step"] for item in submission["rerun_outputs"]] == ["shadow_execution_evidence", "independent_validation"]
    assert submission["allowed_next_actions"] == ["submit_to_formal_rerun_result_review"]


def test_formal_rerun_output_submission_blocks_order_or_payload_mismatch(tmp_db: Path, tmp_path: Path):
    plan = _plan(tmp_path)
    out1 = _write_json(tmp_path / "shadow_failed.json", {"passed": False})
    conn = sqlite3.connect(str(tmp_db))

    submission = build_strategy_competition_formal_rerun_output_submission(
        conn,
        rerun_plan_artifact_path=plan,
        step_output_artifact_paths={
            "independent_validation": str(out1),
            "shadow_execution_evidence": str(out1),
        },
        output_dir=tmp_path / "submission",
    )

    conn.close()
    assert submission["passed"] is False
    assert "rerun_output_submission_step_order_invalid" in submission["blocking_reasons"]
    assert any(reason.endswith(":rerun_output_not_passed") for reason in submission["blocking_reasons"])
