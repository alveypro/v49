import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.primary_result_benchmark_plan import PrimaryResultBenchmarkPlanRegistry


def _write_review_item(path: Path, *, status: str = "needs_benchmark", requires_baseline: bool = True) -> Path:
    payload = {
        "queue_version": "primary_result_feedback_review_queue.v1",
        "review_id": "review-001",
        "status": status,
        "owner": "reviewer",
        "created_at": "2026-04-20T08:00:00Z",
        "updated_at": "2026-04-20T08:10:00Z",
        "result_id": "primary:000001.SZ",
        "ts_code": "000001.SZ",
        "stock_name": "平安银行",
        "primary_failure_category": "risk_control_failure",
        "change_total": 2,
        "max_severity": "high",
        "review_priority": "high",
        "priority_reasons": ["risk_control_failure_requires_risk_model_review"],
        "requires_baseline_revalidation": requires_baseline,
        "do_not_auto_apply": True,
        "recommended_changes": [
            {
                "change_id": "tighten_drawdown_controls",
                "affected_module": "risk_control",
                "recommendation": "review drawdown floor",
                "severity": "high",
                "requires_baseline_revalidation": True,
                "evidence_category": "risk_control_failure",
                "do_not_auto_apply": True,
            },
            {
                "change_id": "review_selection_factors",
                "affected_module": "candidate_selection",
                "recommendation": "review ranking factors",
                "severity": "high",
                "requires_baseline_revalidation": True,
                "evidence_category": "benchmark_underperformance",
                "do_not_auto_apply": True,
            },
        ],
        "source_feedback_hash": "abc123",
        "decision_reason": "risk and selection changes require benchmark validation",
        "decision_at": "2026-04-20T08:10:00Z",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def test_primary_result_benchmark_plan_builds_from_needs_benchmark_review_item(tmp_path):
    registry = PrimaryResultBenchmarkPlanRegistry(plans_dir=tmp_path / "artifacts" / "primary_result_benchmark_plans")
    review_item = _write_review_item(tmp_path / "review.json")

    plan = registry.create_plan(review_item_path=review_item, plan_id="plan-001", planned_at="2026-04-20T08:20:00Z")

    assert plan["plan_version"] == "primary_result_benchmark_plan.v1"
    assert plan["plan_id"] == "plan-001"
    assert plan["status"] == "planned"
    assert plan["affected_modules"] == ["risk_control", "candidate_selection"]
    assert plan["review_priority"] == "high"
    assert plan["execution_priority"] == "expedite"
    assert plan["execution_batch"] == "batch_01_expedite"
    assert plan["priority_reasons"] == ["risk_control_failure_requires_risk_model_review"]
    assert "tests/test_primary_result_rollback_terminal.py" in plan["required_tests"]
    assert "tests/test_stock_primary_result_benchmark_diff.py" in plan["required_tests"]
    assert "tests/test_run_stock_release_pipeline_fast.py" in plan["required_tests"]
    assert "tests/test_run_stock_release_pipeline_functional.py" in plan["required_tests"]
    assert "tests/test_run_stock_release_pipeline_integration.py" in plan["required_tests"]
    assert "tests/test_run_stock_release_pipeline_e2e.py" in plan["required_tests"]
    assert plan["release_gates_required"] is True
    assert plan["baseline_policy_required"] is True
    assert plan["do_not_auto_apply"] is True
    current = registry.get_current_pointer()
    assert current["plan_id"] == "plan-001"
    assert (tmp_path / "artifacts" / "primary_result_benchmark_plans" / "history" / "plan-001.json").exists()


def test_primary_result_benchmark_plan_rejects_open_review_item(tmp_path):
    registry = PrimaryResultBenchmarkPlanRegistry(plans_dir=tmp_path / "plans")
    review_item = _write_review_item(tmp_path / "review.json", status="open")

    with pytest.raises(ValueError, match="needs_benchmark"):
        registry.create_plan(review_item_path=review_item, plan_id="plan-open")

    assert registry.get_current_pointer()["plan_id"] is None


def test_primary_result_benchmark_plan_rejects_review_without_baseline_revalidation(tmp_path):
    registry = PrimaryResultBenchmarkPlanRegistry(plans_dir=tmp_path / "plans")
    review_item = _write_review_item(tmp_path / "review.json", requires_baseline=False)

    with pytest.raises(ValueError, match="baseline revalidation"):
        registry.create_plan(review_item_path=review_item, plan_id="plan-no-baseline")


def test_primary_result_benchmark_plan_rejects_invalid_review_priority(tmp_path):
    registry = PrimaryResultBenchmarkPlanRegistry(plans_dir=tmp_path / "plans")
    review_item = _write_review_item(tmp_path / "review.json")
    payload = json.loads(review_item.read_text(encoding="utf-8"))
    payload["review_priority"] = "urgent"
    review_item.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    with pytest.raises(ValueError, match="review_priority"):
        registry.create_plan(review_item_path=review_item, plan_id="plan-invalid-priority")


def test_primary_result_benchmark_plan_preserves_immutable_history(tmp_path):
    registry = PrimaryResultBenchmarkPlanRegistry(plans_dir=tmp_path / "plans")
    review_item = _write_review_item(tmp_path / "review.json")

    registry.create_plan(review_item_path=review_item, plan_id="plan-001")

    with pytest.raises(FileExistsError, match="already exists"):
        registry.create_plan(review_item_path=review_item, plan_id="plan-001")


def test_build_primary_result_benchmark_plan_cli(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "build_primary_result_benchmark_plan.py"
    review_item = _write_review_item(tmp_path / "review.json")
    plans_dir = tmp_path / "plans"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--plans-dir",
            str(plans_dir),
            "--review-item-json",
            str(review_item),
            "--plan-id",
            "plan-cli",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "planned"
    assert payload["plan_id"] == "plan-cli"
    assert payload["requires_baseline_revalidation"] is True
    assert json.loads((plans_dir / "current.json").read_text(encoding="utf-8"))["plan_id"] == "plan-cli"
