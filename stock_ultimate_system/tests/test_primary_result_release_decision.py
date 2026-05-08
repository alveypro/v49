import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.primary_result_release_decision import PrimaryResultReleaseDecisionRegistry


def _write_checklist(path: Path, *, status: str = "complete", missing=None, blocking_reason=None) -> Path:
    payload = {
        "checklist_version": "primary_result_release_evidence_checklist.v1",
        "checklist_id": "checklist-001",
        "generated_at": "2026-04-20T08:40:00Z",
        "status": status,
        "review_id": "review-001",
        "result_id": "primary:000001.SZ",
        "ts_code": "000001.SZ",
        "stock_name": "平安银行",
        "requires_baseline_revalidation": True,
        "required_evidence": [],
        "missing_evidence": list(missing or []),
        "blocking_gate_reason": blocking_reason,
        "do_not_auto_apply": True,
        "release_boundary": "checklist only",
        "source_review_item_path": "review.json",
        "source_review_item_hash": "abc123",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def test_primary_result_release_decision_approves_complete_checklist(tmp_path):
    registry = PrimaryResultReleaseDecisionRegistry(decisions_dir=tmp_path / "decisions")
    checklist_path = _write_checklist(tmp_path / "checklist.json")

    decision = registry.create_decision(
        checklist_path=checklist_path,
        decision="approved",
        actor="release_manager",
        reason="all required evidence is complete",
        decision_id="decision-001",
        decided_at="2026-04-20T08:50:00Z",
    )

    assert decision["decision_version"] == "primary_result_release_decision.v1"
    assert decision["decision"] == "approved"
    assert decision["baseline_promotion_allowed"] is True
    assert decision["release_pipeline_allowed"] is True
    assert decision["source_checklist_hash"]
    assert decision["do_not_auto_apply"] is True
    assert registry.get_current_pointer()["decision_id"] == "decision-001"
    assert (tmp_path / "decisions" / "history" / "decision-001.json").exists()


def test_primary_result_release_decision_rejects_approval_for_incomplete_checklist(tmp_path):
    registry = PrimaryResultReleaseDecisionRegistry(decisions_dir=tmp_path / "decisions")
    checklist_path = _write_checklist(tmp_path / "checklist.json", status="incomplete", missing=["release_gates"])

    with pytest.raises(ValueError, match="complete checklist"):
        registry.create_decision(
            checklist_path=checklist_path,
            decision="approved",
            actor="release_manager",
            reason="should not approve",
            decision_id="decision-incomplete",
        )


def test_primary_result_release_decision_rejects_approval_for_blocked_checklist(tmp_path):
    registry = PrimaryResultReleaseDecisionRegistry(decisions_dir=tmp_path / "decisions")
    checklist_path = _write_checklist(
        tmp_path / "checklist.json",
        status="blocked",
        blocking_reason="release gates contain blocking_failures",
    )

    with pytest.raises(ValueError, match="complete checklist"):
        registry.create_decision(
            checklist_path=checklist_path,
            decision="approved",
            actor="release_manager",
            reason="should not approve",
            decision_id="decision-blocked",
        )


def test_primary_result_release_decision_can_record_rejection_for_blocked_checklist(tmp_path):
    registry = PrimaryResultReleaseDecisionRegistry(decisions_dir=tmp_path / "decisions")
    checklist_path = _write_checklist(
        tmp_path / "checklist.json",
        status="blocked",
        blocking_reason="release gates contain blocking_failures",
    )

    decision = registry.create_decision(
        checklist_path=checklist_path,
        decision="rejected",
        actor="release_manager",
        reason="blocking gate remains open",
        decision_id="decision-rejected",
    )

    assert decision["decision"] == "rejected"
    assert decision["baseline_promotion_allowed"] is False


def test_primary_result_release_decision_preserves_history(tmp_path):
    registry = PrimaryResultReleaseDecisionRegistry(decisions_dir=tmp_path / "decisions")
    checklist_path = _write_checklist(tmp_path / "checklist.json")

    registry.create_decision(
        checklist_path=checklist_path,
        decision="approved",
        actor="release_manager",
        reason="complete",
        decision_id="decision-001",
    )

    with pytest.raises(FileExistsError, match="already exists"):
        registry.create_decision(
            checklist_path=checklist_path,
            decision="approved",
            actor="release_manager",
            reason="complete",
            decision_id="decision-001",
        )


def test_decide_primary_result_release_cli(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "decide_primary_result_release.py"
    checklist_path = _write_checklist(tmp_path / "checklist.json")
    decisions_dir = tmp_path / "decisions"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--decisions-dir",
            str(decisions_dir),
            "--checklist-json",
            str(checklist_path),
            "--decision",
            "approved",
            "--actor",
            "release_manager",
            "--reason",
            "all required evidence is complete",
            "--decision-id",
            "decision-cli",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "ok"
    assert payload["decision"] == "approved"
    assert payload["baseline_promotion_allowed"] is True
    assert json.loads((decisions_dir / "current.json").read_text(encoding="utf-8"))["decision_id"] == "decision-cli"
