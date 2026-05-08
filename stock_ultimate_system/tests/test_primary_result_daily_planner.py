import json
import subprocess
import sys
from pathlib import Path

from src.primary_result_daily_planner import build_primary_result_daily_planner


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_primary_result_daily_planner_combines_owner_workload_batches_and_iteration_schedule(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    artifacts = tmp_path / "artifacts"
    _write_json(
        artifacts / "primary_result_daily_operations_scoreboard_latest.json",
        {
            "overall_status": "yellow",
            "next_actions": ["resolve 1 critical-priority feedback review items before any promotion or baseline move"],
        },
    )
    _write_json(
        artifacts / "primary_result_feedback_review_queue" / "summary.json",
        {
            "open_owner_workloads": {
                "reviewer-a": {"open_total": 2, "critical_priority_total": 1, "high_priority_total": 2},
                "reviewer-b": {"open_total": 1, "critical_priority_total": 0, "high_priority_total": 1},
            }
        },
    )
    _write_json(
        artifacts / "primary_result_promotion_readiness_gate_latest.json",
        {"decision": "blocked"},
    )
    _write_json(
        exp_dir / "candidate_quality_diff.json",
        {
            "recommended_remediation_actions": [
                "prioritize risk-control review items before ranking or timing adjustments"
            ],
            "iteration_schedule": [
                {
                    "sequence": 1,
                    "failure_field": "risk_control_failure_cases",
                    "priority_band": "critical",
                    "delta": 1,
                    "recommended_action": "prioritize risk-control review items before ranking or timing adjustments",
                }
            ],
        },
    )
    _write_json(
        exp_dir / "candidate_quality_summary.json",
        {
            "multiwindow_sample_density": {
                "60d": {"sample_total": 3, "minimum_required": 3, "status": "passed"},
                "120d": {"sample_total": 4, "minimum_required": 5, "status": "blocked"},
            }
        },
    )
    _write_json(
        exp_dir / "candidate_quality_density_progress.json",
        {
            "progress": {
                "120d": {
                    "sample_total": 4,
                    "minimum_required": 5,
                    "remaining_samples_needed": 1,
                    "progress_ratio": 0.8,
                    "status": "blocked",
                    "latest_validation_date": "2026-04-10",
                    "earliest_validation_date": "2026-01-10",
                }
            }
        },
    )
    _write_json(
        artifacts / "primary_result_benchmark_plans" / "current.json",
        {
            "plan_id": "plan-001",
            "review_id": "review-001",
            "execution_priority": "expedite",
            "execution_batch": "batch_01_expedite",
        },
    )

    exit_code, payload = build_primary_result_daily_planner(
        artifacts_dir=artifacts,
        exp_dir=exp_dir,
        output_path=tmp_path / "planner.json",
    )

    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["owner_workload_schedule"][0]["owner"] == "reviewer-a"
    assert payload["benchmark_execution_batches"][0]["execution_batch"] == "batch_01_expedite"
    assert payload["candidate_iteration_schedule"][0]["priority_band"] == "critical"
    assert payload["candidate_quality_sample_density"]["120d"]["status"] == "blocked"
    assert payload["candidate_quality_density_progress"]["120d"]["remaining_samples_needed"] == 1
    assert any("120d sample density is insufficient" in action for action in payload["next_actions"])
    assert any("120d density progress requires 1 more formal validation samples" in action for action in payload["next_actions"])
    assert any("prioritize risk-control review items" in action for action in payload["next_actions"])
    assert (tmp_path / "planner.json").exists()


def test_build_primary_result_daily_planner_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script = project_root / "scripts" / "build_primary_result_daily_planner.py"
    exp_dir = tmp_path / "data" / "experiments"
    artifacts = tmp_path / "artifacts"
    _write_json(artifacts / "primary_result_daily_operations_scoreboard_latest.json", {"overall_status": "green", "next_actions": []})
    _write_json(artifacts / "primary_result_feedback_review_queue" / "summary.json", {"open_owner_workloads": {}})
    _write_json(artifacts / "primary_result_promotion_readiness_gate_latest.json", {"decision": "promotion_review_allowed"})
    _write_json(
        exp_dir / "candidate_quality_summary.json",
        {"multiwindow_sample_density": {"60d": {"sample_total": 3, "minimum_required": 3, "status": "passed"}}},
    )
    _write_json(
        exp_dir / "candidate_quality_density_progress.json",
        {"progress": {"60d": {"sample_total": 3, "minimum_required": 3, "remaining_samples_needed": 0, "progress_ratio": 1.0, "status": "passed"}}},
    )
    _write_json(exp_dir / "candidate_quality_diff.json", {"recommended_remediation_actions": [], "iteration_schedule": []})
    _write_json(artifacts / "primary_result_benchmark_plans" / "current.json", {"plan_id": None, "review_id": None})

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--artifacts-dir",
            str(artifacts),
            "--exp-dir",
            str(exp_dir),
            "--output",
            str(tmp_path / "planner.json"),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "passed"
    assert json.loads((tmp_path / "planner.json").read_text(encoding="utf-8"))["planner_version"] == payload["planner_version"]
