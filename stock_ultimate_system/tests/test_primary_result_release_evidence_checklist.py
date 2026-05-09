import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.primary_result_release_evidence_checklist import PrimaryResultReleaseEvidenceChecklistRegistry


def _write_review_item(path: Path, *, status: str = "accepted") -> Path:
    payload = {
        "queue_version": "primary_result_feedback_review_queue.v1",
        "review_id": "review-001",
        "status": status,
        "owner": "reviewer",
        "created_at": "2026-04-20T08:00:00Z",
        "updated_at": "2026-04-20T08:30:00Z",
        "result_id": "primary:000001.SZ",
        "ts_code": "000001.SZ",
        "stock_name": "平安银行",
        "primary_failure_category": "risk_control_failure",
        "requires_baseline_revalidation": True,
        "do_not_auto_apply": True,
        "recommended_changes": [
            {
                "change_id": "tighten_drawdown_controls",
                "affected_module": "risk_control",
                "recommendation": "review drawdown floor",
                "severity": "high",
                "requires_baseline_revalidation": True,
                "do_not_auto_apply": True,
            }
        ],
        "decision_reason": "benchmark execution passed",
        "decision_at": "2026-04-20T08:30:00Z",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_json(path: Path, payload: dict[str, object] | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload or {"status": "passed"}, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _evidence_paths(tmp_path: Path) -> dict[str, Path]:
    return {
        "benchmark_report_path": _write_json(tmp_path / "evidence" / "benchmark_report.json"),
        "benchmark_diff_path": _write_json(tmp_path / "evidence" / "benchmark_diff.json"),
        "release_gates_path": _write_json(tmp_path / "evidence" / "release_gates.json", {"status": "passed"}),
        "release_evidence_bundle_path": _write_json(tmp_path / "evidence" / "release_evidence_bundle.json"),
        "manifest_path": _write_json(tmp_path / "evidence" / "manifest.json"),
        "baseline_policy_decision_path": _write_json(tmp_path / "evidence" / "baseline_policy_decision.json"),
    }


def test_primary_result_release_evidence_checklist_builds_complete_checklist(tmp_path):
    registry = PrimaryResultReleaseEvidenceChecklistRegistry(checklists_dir=tmp_path / "checklists")
    review_item = _write_review_item(tmp_path / "review.json")

    checklist = registry.create_checklist(
        review_item_path=review_item,
        checklist_id="checklist-001",
        generated_at="2026-04-20T08:40:00Z",
        **_evidence_paths(tmp_path),
    )

    assert checklist["checklist_version"] == "primary_result_release_evidence_checklist.v1"
    assert checklist["checklist_id"] == "checklist-001"
    assert checklist["status"] == "complete"
    assert checklist["missing_evidence"] == []
    assert checklist["blocking_gate_reason"] is None
    assert checklist["do_not_auto_apply"] is True
    assert all(entry["sha256"] for entry in checklist["required_evidence"])
    assert (tmp_path / "checklists" / "history" / "checklist-001.json").exists()


def test_primary_result_release_evidence_checklist_reports_missing_evidence(tmp_path):
    registry = PrimaryResultReleaseEvidenceChecklistRegistry(checklists_dir=tmp_path / "checklists")
    review_item = _write_review_item(tmp_path / "review.json")

    checklist = registry.create_checklist(
        review_item_path=review_item,
        benchmark_report_path=tmp_path / "missing_report.json",
        checklist_id="checklist-missing",
    )

    assert checklist["status"] == "incomplete"
    assert "benchmark_report" in checklist["missing_evidence"]
    assert "release_gates" in checklist["missing_evidence"]


def test_primary_result_release_evidence_checklist_blocks_on_blocking_gate(tmp_path):
    registry = PrimaryResultReleaseEvidenceChecklistRegistry(checklists_dir=tmp_path / "checklists")
    review_item = _write_review_item(tmp_path / "review.json")
    evidence = _evidence_paths(tmp_path)
    _write_json(evidence["release_gates_path"], {"status": "failed", "blocking_failures": [{"gate": "risk"}]})

    checklist = registry.create_checklist(
        review_item_path=review_item,
        checklist_id="checklist-blocked",
        **evidence,
    )

    assert checklist["status"] == "blocked"
    assert checklist["blocking_gate_reason"] == "release gates status is failed"


def test_primary_result_release_evidence_checklist_rejects_non_accepted_review_item(tmp_path):
    registry = PrimaryResultReleaseEvidenceChecklistRegistry(checklists_dir=tmp_path / "checklists")
    review_item = _write_review_item(tmp_path / "review.json", status="needs_benchmark")

    with pytest.raises(ValueError, match="accepted"):
        registry.create_checklist(review_item_path=review_item, checklist_id="checklist-needs-benchmark")


def test_primary_result_release_evidence_checklist_preserves_history_and_switches_current(tmp_path):
    registry = PrimaryResultReleaseEvidenceChecklistRegistry(checklists_dir=tmp_path / "checklists")
    review_item = _write_review_item(tmp_path / "review.json")

    registry.create_checklist(review_item_path=review_item, checklist_id="checklist-001", **_evidence_paths(tmp_path))
    registry.create_checklist(review_item_path=review_item, checklist_id="checklist-002", **_evidence_paths(tmp_path))

    with pytest.raises(FileExistsError, match="already exists"):
        registry.create_checklist(review_item_path=review_item, checklist_id="checklist-001", **_evidence_paths(tmp_path))
    assert registry.get_current_pointer()["checklist_id"] == "checklist-002"
    assert (tmp_path / "checklists" / "history" / "checklist-001.json").exists()
    assert (tmp_path / "checklists" / "history" / "checklist-002.json").exists()


def test_build_primary_result_release_evidence_checklist_cli(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "build_primary_result_release_evidence_checklist.py"
    review_item = _write_review_item(tmp_path / "review.json")
    evidence = _evidence_paths(tmp_path)
    checklists_dir = tmp_path / "checklists"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--checklists-dir",
            str(checklists_dir),
            "--review-item-json",
            str(review_item),
            "--benchmark-report-json",
            str(evidence["benchmark_report_path"]),
            "--benchmark-diff-json",
            str(evidence["benchmark_diff_path"]),
            "--release-gates-json",
            str(evidence["release_gates_path"]),
            "--release-evidence-bundle-json",
            str(evidence["release_evidence_bundle_path"]),
            "--manifest-json",
            str(evidence["manifest_path"]),
            "--baseline-policy-decision-json",
            str(evidence["baseline_policy_decision_path"]),
            "--checklist-id",
            "checklist-cli",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "complete"
    assert payload["checklist_id"] == "checklist-cli"
    assert payload["missing_evidence"] == []
    assert json.loads((checklists_dir / "current.json").read_text(encoding="utf-8"))["checklist_id"] == "checklist-cli"
