import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.primary_result_benchmark_execution_review_update import apply_benchmark_execution_to_review_queue
from src.primary_result_feedback_review_queue import PrimaryResultFeedbackReviewQueue


def _write_feedback(path: Path) -> Path:
    payload = {
        "feedback_version": "primary_result_learning_feedback.v1",
        "generated_at": "2026-04-20T08:00:00Z",
        "status": "passed",
        "result_id": "primary:000001.SZ",
        "ts_code": "000001.SZ",
        "stock_name": "平安银行",
        "outcome": "failed",
        "attribution_required": True,
        "primary_failure_category": "risk_control_failure",
        "recommended_changes": [
            {
                "change_id": "tighten_drawdown_controls",
                "affected_module": "risk_control",
                "recommendation": "review drawdown floor",
                "severity": "high",
                "requires_baseline_revalidation": True,
                "evidence_category": "risk_control_failure",
                "do_not_auto_apply": True,
            }
        ],
        "change_total": 1,
        "max_severity": "high",
        "review_priority": "high",
        "priority_reasons": ["risk_control_failure_requires_risk_model_review"],
        "requires_baseline_revalidation": True,
        "do_not_auto_apply": True,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _queue_item_needs_benchmark(tmp_path: Path) -> PrimaryResultFeedbackReviewQueue:
    queue = PrimaryResultFeedbackReviewQueue(queue_dir=tmp_path / "queue")
    feedback = _write_feedback(tmp_path / "feedback.json")
    queue.enqueue(feedback_path=feedback, review_id="review-001", owner="reviewer-a")
    queue.decide(
        review_id="review-001",
        status="needs_benchmark",
        reason="benchmark validation required",
        actor="reviewer-b",
    )
    return queue


def _write_execution(path: Path, *, status: str = "passed", review_id: str = "review-001") -> Path:
    payload = {
        "execution_version": "primary_result_benchmark_plan_execution.v1",
        "executed_at": "2026-04-20T08:30:00Z",
        "status": status,
        "plan_id": "plan-001",
        "review_id": review_id,
        "result_id": "primary:000001.SZ",
        "ts_code": "000001.SZ",
        "source_plan_path": "/tmp/plan.json",
        "source_plan_hash": "abc123",
        "command": ["python", "-m", "pytest"],
        "required_tests": ["tests/test_primary_result_learning_feedback.py"],
        "required_test_total": 1,
        "exit_code": 0 if status == "passed" else 1,
        "stdout": "ok" if status == "passed" else "failed",
        "stderr": "" if status == "passed" else "boom",
        "release_gates_required": True,
        "baseline_policy_required": True,
        "requires_baseline_revalidation": True,
        "do_not_auto_apply": True,
        "execution_priority": "expedite",
        "execution_batch": "batch_01_expedite",
        "expected_evidence_artifacts": [],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def test_benchmark_execution_pass_updates_review_item_to_accepted(tmp_path):
    queue = _queue_item_needs_benchmark(tmp_path)
    execution = _write_execution(tmp_path / "execution.json", status="passed")

    payload = apply_benchmark_execution_to_review_queue(
        execution_json_path=execution,
        queue_dir=queue.queue_dir,
        actor="executor",
    )

    assert payload["status"] == "updated"
    assert payload["item_status"] == "accepted"
    assert payload["review_priority"] == "high"
    assert payload["execution_priority"] == "expedite"
    assert payload["execution_batch"] == "batch_01_expedite"
    item = queue.get_item("review-001")
    assert item["status"] == "accepted"
    history = queue.list_decision_history()
    assert len(history) == 3
    assert history[-1]["payload"]["benchmark_execution_status"] == "passed"
    assert history[-1]["payload"]["source_execution_hash"]
    assert history[-1]["payload"]["do_not_auto_apply"] is True


def test_benchmark_execution_failure_updates_review_item_to_rejected(tmp_path):
    queue = _queue_item_needs_benchmark(tmp_path)
    execution = _write_execution(tmp_path / "execution.json", status="failed")

    payload = apply_benchmark_execution_to_review_queue(execution_json_path=execution, queue_dir=queue.queue_dir)

    assert payload["item_status"] == "rejected"
    assert queue.get_item("review-001")["status"] == "rejected"


def test_benchmark_execution_update_rejects_wrong_queue_status(tmp_path):
    queue = PrimaryResultFeedbackReviewQueue(queue_dir=tmp_path / "queue")
    feedback = _write_feedback(tmp_path / "feedback.json")
    queue.enqueue(feedback_path=feedback, review_id="review-001")
    execution = _write_execution(tmp_path / "execution.json")

    with pytest.raises(ValueError, match="needs_benchmark"):
        apply_benchmark_execution_to_review_queue(execution_json_path=execution, queue_dir=queue.queue_dir)


def test_benchmark_execution_update_rejects_unsafe_execution(tmp_path):
    queue = _queue_item_needs_benchmark(tmp_path)
    execution = _write_execution(tmp_path / "execution.json")
    payload = json.loads(execution.read_text(encoding="utf-8"))
    payload["do_not_auto_apply"] = False
    execution.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(ValueError, match="do_not_auto_apply"):
        apply_benchmark_execution_to_review_queue(execution_json_path=execution, queue_dir=queue.queue_dir)


def test_apply_primary_result_benchmark_execution_decision_cli(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "apply_primary_result_benchmark_execution_decision.py"
    queue = _queue_item_needs_benchmark(tmp_path)
    execution = _write_execution(tmp_path / "execution.json")

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--execution-json",
            str(execution),
            "--queue-dir",
            str(queue.queue_dir),
            "--actor",
            "executor",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "updated"
    assert payload["item_status"] == "accepted"
