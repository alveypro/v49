from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_formal_rerun_plan_service import (
    RERUN_ORDER,
    build_strategy_competition_formal_rerun_plan,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def test_formal_rerun_plan_blocks_unaccepted_closure_review(tmp_db: Path, tmp_path: Path):
    closure = _write_json(
        tmp_path / "closure_blocked.json",
        {
            "artifact_version": "strategy_competition_remediation_closure_review.v1",
            "competition_run_id": "comp_test",
            "closure_review_status": "remediation_closure_blocked",
            "passed": False,
            "source_manifest_hash": "manifest_hash",
            "closure_review_hash": "closure_hash",
        },
    )
    conn = sqlite3.connect(str(tmp_db))

    plan = build_strategy_competition_formal_rerun_plan(conn, closure_review_artifact_path=closure, output_dir=tmp_path / "plan")

    conn.close()
    assert plan["rerun_plan_status"] == "formal_rerun_plan_blocked"
    assert plan["passed"] is False
    assert plan["rerun_steps"] == []
    assert plan["production_candidate_allowed"] is False
    assert plan["blocking_reasons"] == ["remediation_closure_review_not_accepted"]


def test_formal_rerun_plan_ready_never_grants_permission(tmp_db: Path, tmp_path: Path):
    closure = _write_json(
        tmp_path / "closure_accepted.json",
        {
            "artifact_version": "strategy_competition_remediation_closure_review.v1",
            "competition_run_id": "comp_test",
            "closure_review_status": "remediation_closure_accepted_for_rerun",
            "passed": True,
            "source_manifest_hash": "manifest_hash",
            "closure_review_hash": "closure_hash",
        },
    )
    conn = sqlite3.connect(str(tmp_db))

    plan = build_strategy_competition_formal_rerun_plan(conn, closure_review_artifact_path=closure, output_dir=tmp_path / "plan")

    conn.close()
    assert plan["rerun_plan_status"] == "formal_rerun_plan_ready"
    assert plan["passed"] is True
    assert plan["production_candidate_allowed"] is False
    assert plan["live_order_authority_granted"] is False
    assert [step["step"] for step in plan["rerun_steps"]] == list(RERUN_ORDER)
    assert plan["rerun_plan_contract"]["each_step_output_must_pass_before_next_step"] is True
    assert plan["rerun_plan_hash"]
