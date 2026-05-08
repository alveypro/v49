import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.primary_result_feedback_review_queue import PrimaryResultFeedbackReviewQueue


def _write_feedback(path: Path, *, do_not_auto_apply: bool = True, change_total: int = 2) -> Path:
    changes = [
        {
            "change_id": "tighten_drawdown_controls",
            "affected_module": "risk_control",
            "recommendation": "review drawdown floor",
            "severity": "high",
            "requires_baseline_revalidation": True,
            "evidence_category": "risk_control_failure",
            "do_not_auto_apply": do_not_auto_apply,
        },
        {
            "change_id": "review_selection_factors",
            "affected_module": "candidate_selection",
            "recommendation": "review ranking factors",
            "severity": "high",
            "requires_baseline_revalidation": True,
            "evidence_category": "benchmark_underperformance",
            "do_not_auto_apply": do_not_auto_apply,
        },
    ][:change_total]
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
        "recommended_changes": changes,
        "change_total": len(changes),
        "max_severity": "high" if changes else "none",
        "review_priority": "high" if changes else "none",
        "priority_reasons": ["risk_control_failure_requires_risk_model_review"] if changes else [],
        "requires_baseline_revalidation": bool(changes),
        "do_not_auto_apply": do_not_auto_apply,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def test_feedback_review_queue_enqueues_feedback_with_decision_history(tmp_path):
    queue = PrimaryResultFeedbackReviewQueue(queue_dir=tmp_path / "artifacts" / "primary_result_feedback_review_queue")
    feedback = _write_feedback(tmp_path / "feedback.json")

    item = queue.enqueue(feedback_path=feedback, review_id="review-001", owner="reviewer-a")

    assert item["review_id"] == "review-001"
    assert item["status"] == "open"
    assert item["review_priority"] == "high"
    assert item["priority_reasons"] == ["risk_control_failure_requires_risk_model_review"]
    assert item["requires_baseline_revalidation"] is True
    assert item["do_not_auto_apply"] is True
    assert item["source_feedback_hash"]
    assert len(queue.list_decision_history()) == 1
    summary = json.loads(queue.summary_path.read_text(encoding="utf-8"))
    assert summary["item_total"] == 1
    assert summary["status_counts"]["open"] == 1
    assert summary["priority_counts"]["high"] == 1
    assert summary["open_high_severity_total"] == 1
    assert summary["open_high_priority_total"] == 1
    assert summary["taxonomy_hotspots"]["risk_control_failure"] == 1
    assert summary["open_owner_workloads"]["reviewer-a"]["open_total"] == 1
    assert summary["open_owner_workloads"]["reviewer-a"]["high_priority_total"] == 1


def test_feedback_review_queue_rejects_feedback_that_can_auto_apply(tmp_path):
    queue = PrimaryResultFeedbackReviewQueue(queue_dir=tmp_path / "queue")
    feedback = _write_feedback(tmp_path / "feedback.json", do_not_auto_apply=False)

    with pytest.raises(ValueError, match="do_not_auto_apply"):
        queue.enqueue(feedback_path=feedback, review_id="review-unsafe")

    assert queue.list_items() == []


def test_feedback_review_queue_decision_updates_item_and_appends_immutable_event(tmp_path):
    queue = PrimaryResultFeedbackReviewQueue(queue_dir=tmp_path / "queue")
    feedback = _write_feedback(tmp_path / "feedback.json")
    queue.enqueue(feedback_path=feedback, review_id="review-001", owner="reviewer-a")

    item = queue.decide(
        review_id="review-001",
        status="needs_benchmark",
        reason="risk and selection changes require benchmark validation",
        actor="reviewer-b",
    )

    assert item["status"] == "needs_benchmark"
    assert item["decision_reason"] == "risk and selection changes require benchmark validation"
    assert item["do_not_auto_apply"] is True
    history = queue.list_decision_history()
    assert [event["action"] for event in history] == ["enqueue", "decision"]
    assert history[-1]["status"] == "needs_benchmark"
    summary = json.loads(queue.summary_path.read_text(encoding="utf-8"))
    assert summary["status_counts"]["needs_benchmark"] == 1
    assert summary["priority_counts"]["high"] == 1
    assert summary["decision_event_total"] == 2


def test_feedback_review_queue_lists_higher_priority_items_first(tmp_path):
    queue = PrimaryResultFeedbackReviewQueue(queue_dir=tmp_path / "queue")
    low_feedback = _write_feedback(tmp_path / "low.json", change_total=0)
    high_feedback = _write_feedback(tmp_path / "high.json", change_total=2)

    queue.enqueue(feedback_path=low_feedback, review_id="review-low")
    queue.enqueue(feedback_path=high_feedback, review_id="review-high")

    items = queue.list_items()
    assert [item["review_id"] for item in items] == ["review-high", "review-low"]


def test_feedback_review_queue_rejects_duplicate_review_id(tmp_path):
    queue = PrimaryResultFeedbackReviewQueue(queue_dir=tmp_path / "queue")
    feedback = _write_feedback(tmp_path / "feedback.json")

    queue.enqueue(feedback_path=feedback, review_id="review-001")

    with pytest.raises(FileExistsError, match="already exists"):
        queue.enqueue(feedback_path=feedback, review_id="review-001")


def test_feedback_review_queue_cli_enqueue_and_decide(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "manage_primary_result_feedback_review_queue.py"
    feedback = _write_feedback(tmp_path / "feedback.json")
    queue_dir = tmp_path / "queue"

    enqueued = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--queue-dir",
            str(queue_dir),
            "--feedback-json",
            str(feedback),
            "--review-id",
            "review-cli",
            "--owner",
            "reviewer-a",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )
    decided = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--queue-dir",
            str(queue_dir),
            "--review-id",
            "review-cli",
            "--decision-status",
            "accepted",
            "--reason",
            "accepted for benchmark planning",
            "--owner",
            "reviewer-b",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    assert json.loads(enqueued.stdout)["status"] == "enqueued"
    payload = json.loads(decided.stdout)
    assert payload["status"] == "decided"
    assert payload["item_status"] == "accepted"
    assert payload["do_not_auto_apply"] is True
    assert len((queue_dir / "decision_history.jsonl").read_text(encoding="utf-8").splitlines()) == 2
