import json
import subprocess
import sys
from pathlib import Path

from src.primary_result_morning_operations_brief import build_primary_result_morning_operations_brief


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_primary_result_morning_operations_brief_renders_from_daily_planner(tmp_path):
    planner = tmp_path / "artifacts" / "primary_result_daily_planner_latest.json"
    _write_json(
        planner,
        {
            "generated_at": "2026-05-02T08:00:00Z",
            "scoreboard_status": "yellow",
            "promotion_decision": "blocked",
            "owner_workload_schedule": [
                {"owner": "reviewer-a", "open_total": 2, "critical_priority_total": 1, "high_priority_total": 2}
            ],
            "benchmark_execution_batches": [
                {"execution_batch": "batch_01_expedite", "execution_priority": "expedite", "plan_id": "plan-001", "review_id": "review-001"}
            ],
            "candidate_iteration_schedule": [
                {
                    "sequence": 1,
                    "priority_band": "critical",
                    "failure_field": "risk_control_failure_cases",
                    "recommended_action": "prioritize risk-control review items before ranking or timing adjustments",
                }
            ],
            "candidate_quality_sample_density": {
                "120d": {"status": "blocked", "sample_total": 4, "minimum_required": 5}
            },
            "candidate_quality_density_progress": {
                "120d": {
                    "remaining_samples_needed": 1,
                    "progress_ratio": 0.8,
                    "latest_validation_date": "2026-04-10",
                }
            },
            "next_actions": ["120d sample density is insufficient: sample_total=4 minimum_required=5"],
        },
    )

    output = tmp_path / "brief.md"
    exit_code, payload = build_primary_result_morning_operations_brief(planner_path=planner, output_path=output)

    assert exit_code == 0
    assert payload["status"] == "passed"
    text = output.read_text(encoding="utf-8")
    assert "# /stock Morning Operations Brief" in text
    assert "reviewer-a: open=2" in text
    assert "batch_01_expedite / expedite" in text
    assert "120d: status=blocked" in text
    assert "120d: remaining=1, progress_ratio=0.8, latest_validation_date=2026-04-10" in text


def test_build_primary_result_morning_operations_brief_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script = project_root / "scripts" / "build_primary_result_morning_operations_brief.py"
    planner = tmp_path / "artifacts" / "primary_result_daily_planner_latest.json"
    _write_json(planner, {"generated_at": "2026-05-02T08:00:00Z", "scoreboard_status": "green", "promotion_decision": "promotion_review_allowed"})

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--planner-json",
            str(planner),
            "--output",
            str(tmp_path / "brief.md"),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "passed"
    assert Path(payload["output_path"]).exists()
