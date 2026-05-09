import json
import subprocess
import sys
from pathlib import Path

from src.primary_result_promotion_readiness_gate import build_primary_result_promotion_readiness_gate


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _evidence(status: str) -> dict[str, object]:
    return {
        "evidence_version": "primary_result_performance_evidence.v1",
        "status": status,
        "next_actions": ["primary_result needs 19 more ledger entries"] if status == "accumulating" else [],
        "streams": [
            {"stream_id": "primary_result", "status": "evidence_ready", "entry_total": 20, "windows": [{"floor": 20, "status": "passed", "blocking_reasons": []}]},
            {"stream_id": "candidate_basket", "status": "evidence_ready", "entry_total": 20, "windows": [{"floor": 20, "status": "passed", "blocking_reasons": []}]},
        ],
    }


def test_promotion_readiness_gate_allows_review_when_evidence_ready_and_queue_closed(tmp_path):
    evidence = tmp_path / "evidence.json"
    queue = tmp_path / "queue_summary.json"
    baseline = tmp_path / "current.json"
    _write_json(evidence, _evidence("ready"))
    _write_json(
        queue,
        {
            "status_counts": {"open": 0},
            "open_high_severity_total": 0,
            "open_high_priority_total": 0,
            "priority_counts": {"critical": 0, "high": 0, "medium": 0, "low": 0, "none": 0},
            "taxonomy_hotspots": {},
        },
    )
    _write_json(baseline, {"baseline_id": "baseline-001"})

    exit_code, payload = build_primary_result_promotion_readiness_gate(
        performance_evidence_path=evidence,
        feedback_queue_summary_path=queue,
        baseline_current_path=baseline,
        output_path=tmp_path / "gate.json",
    )

    assert exit_code == 0
    assert payload["gate_version"] == "primary_result_promotion_readiness_gate.v1"
    assert payload["status"] == "passed"
    assert payload["decision"] == "promotion_review_allowed"
    assert payload["review_queue_priority"]["open_high_priority_total"] == 0
    assert (tmp_path / "gate.json").exists()


def test_promotion_readiness_gate_blocks_accumulating_evidence(tmp_path):
    evidence = tmp_path / "evidence.json"
    queue = tmp_path / "queue_summary.json"
    _write_json(evidence, _evidence("accumulating"))
    _write_json(
        queue,
        {
            "status_counts": {"open": 0},
            "open_high_severity_total": 0,
            "open_high_priority_total": 0,
            "priority_counts": {"critical": 0, "high": 0, "medium": 0, "low": 0, "none": 0},
            "taxonomy_hotspots": {},
        },
    )

    exit_code, payload = build_primary_result_promotion_readiness_gate(
        performance_evidence_path=evidence,
        feedback_queue_summary_path=queue,
        baseline_current_path=tmp_path / "missing_current.json",
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert payload["decision"] == "blocked"
    assert "performance evidence must be ready before promotion review" in payload["blocking_reasons"]
    assert "primary_result needs 19 more ledger entries" in payload["next_actions"]


def test_promotion_readiness_gate_freezes_failed_evidence(tmp_path):
    evidence = tmp_path / "evidence.json"
    queue = tmp_path / "queue_summary.json"
    _write_json(evidence, _evidence("failed"))
    _write_json(
        queue,
        {
            "status_counts": {"open": 0},
            "open_high_severity_total": 0,
            "open_high_priority_total": 0,
            "priority_counts": {"critical": 0, "high": 0, "medium": 0, "low": 0, "none": 0},
            "taxonomy_hotspots": {},
        },
    )

    exit_code, payload = build_primary_result_promotion_readiness_gate(
        performance_evidence_path=evidence,
        feedback_queue_summary_path=queue,
        baseline_current_path=tmp_path / "missing_current.json",
    )

    assert exit_code == 1
    assert payload["decision"] == "freeze"
    assert "freeze promotion" in payload["next_actions"][0]


def test_promotion_readiness_gate_blocks_open_review_items(tmp_path):
    evidence = tmp_path / "evidence.json"
    queue = tmp_path / "queue_summary.json"
    _write_json(evidence, _evidence("ready"))
    _write_json(
        queue,
        {
            "status_counts": {"open": 2},
            "open_high_severity_total": 1,
            "open_high_priority_total": 2,
            "priority_counts": {"critical": 0, "high": 2, "medium": 0, "low": 0, "none": 0},
            "taxonomy_hotspots": {"risk_control_failure": 2},
        },
    )

    exit_code, payload = build_primary_result_promotion_readiness_gate(
        performance_evidence_path=evidence,
        feedback_queue_summary_path=queue,
        baseline_current_path=tmp_path / "missing_current.json",
    )

    assert exit_code == 1
    assert payload["decision"] == "blocked"
    assert "open high-severity review items block promotion" in payload["blocking_reasons"]
    assert "open high-priority review items block promotion until benchmark and review handling is completed" in payload["blocking_reasons"]
    assert payload["review_queue_priority"]["priority_counts"]["high"] == 2
    assert "open review items should be closed or explicitly decided before promotion" in payload["blocking_reasons"]


def test_promotion_readiness_gate_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script = project_root / "scripts" / "build_primary_result_promotion_readiness_gate.py"
    evidence = tmp_path / "evidence.json"
    queue = tmp_path / "queue_summary.json"
    baseline = tmp_path / "current.json"
    _write_json(evidence, _evidence("ready"))
    _write_json(
        queue,
        {
            "status_counts": {"open": 0},
            "open_high_severity_total": 0,
            "open_high_priority_total": 0,
            "priority_counts": {"critical": 0, "high": 0, "medium": 0, "low": 0, "none": 0},
            "taxonomy_hotspots": {},
        },
    )
    _write_json(baseline, {"baseline_id": "baseline-001"})

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--performance-evidence-json",
            str(evidence),
            "--feedback-queue-summary-json",
            str(queue),
            "--baseline-current-json",
            str(baseline),
            "--output",
            str(tmp_path / "gate.json"),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["decision"] == "promotion_review_allowed"
    assert json.loads((tmp_path / "gate.json").read_text(encoding="utf-8"))["status"] == "passed"
