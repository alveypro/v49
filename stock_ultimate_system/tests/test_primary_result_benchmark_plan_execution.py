import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.primary_result_benchmark_plan_execution import run_primary_result_benchmark_plan_execution


class _Completed:
    def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _write_plan(path: Path, *, status: str = "planned", do_not_auto_apply: bool = True) -> Path:
    payload = {
        "plan_version": "primary_result_benchmark_plan.v1",
        "plan_id": "plan-001",
        "planned_at": "2026-04-20T08:20:00Z",
        "status": status,
        "review_id": "review-001",
        "result_id": "primary:000001.SZ",
        "ts_code": "000001.SZ",
        "affected_modules": ["risk_control"],
        "recommended_changes": [],
        "required_tests": [
            "tests/test_primary_result_rollback_terminal.py",
            "tests/test_primary_result_observation_metrics.py",
        ],
        "expected_evidence_artifacts": ["stock_primary_result_benchmark_report.json", "release_gates.json"],
        "release_gates_required": True,
        "baseline_policy_required": True,
        "requires_baseline_revalidation": True,
        "do_not_auto_apply": do_not_auto_apply,
        "execution_priority": "expedite",
        "execution_batch": "batch_01_expedite",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def test_benchmark_plan_execution_runs_required_tests_and_writes_evidence(tmp_path):
    plan_path = _write_plan(tmp_path / "plan.json")
    output_path = tmp_path / "execution.json"
    calls = []

    def fake_runner(command, cwd=None, capture_output=None, text=None, timeout=None):
        calls.append(
            {
                "command": command,
                "cwd": cwd,
                "capture_output": capture_output,
                "text": text,
                "timeout": timeout,
            }
        )
        return _Completed(0, stdout="passed")

    exit_code, payload = run_primary_result_benchmark_plan_execution(
        plan_path=plan_path,
        output_path=output_path,
        runner=fake_runner,
        cwd=tmp_path,
        timeout=30,
        executed_at="2026-04-20T08:30:00Z",
    )

    assert exit_code == 0
    assert payload["execution_version"] == "primary_result_benchmark_plan_execution.v1"
    assert payload["status"] == "passed"
    assert payload["source_plan_hash"]
    assert payload["required_test_total"] == 2
    assert payload["do_not_auto_apply"] is True
    assert payload["execution_priority"] == "expedite"
    assert payload["execution_batch"] == "batch_01_expedite"
    assert calls[0]["command"][:3] == [sys.executable, "-m", "pytest"]
    assert "tests/test_primary_result_observation_metrics.py" in calls[0]["command"]
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "passed"


def test_benchmark_plan_execution_records_failed_test_run(tmp_path):
    plan_path = _write_plan(tmp_path / "plan.json")
    output_path = tmp_path / "execution.json"

    exit_code, payload = run_primary_result_benchmark_plan_execution(
        plan_path=plan_path,
        output_path=output_path,
        runner=lambda *args, **kwargs: _Completed(1, stdout="failed", stderr="boom"),
        cwd=tmp_path,
    )

    assert exit_code == 1
    assert payload["status"] == "failed"
    assert payload["exit_code"] == 1
    assert payload["stderr"] == "boom"


def test_benchmark_plan_execution_rejects_non_planned_plan(tmp_path):
    plan_path = _write_plan(tmp_path / "plan.json", status="executed")

    with pytest.raises(ValueError, match="status must be planned"):
        run_primary_result_benchmark_plan_execution(
            plan_path=plan_path,
            output_path=tmp_path / "execution.json",
            runner=lambda *args, **kwargs: _Completed(0),
        )


def test_benchmark_plan_execution_rejects_auto_apply_plan(tmp_path):
    plan_path = _write_plan(tmp_path / "plan.json", do_not_auto_apply=False)

    with pytest.raises(ValueError, match="do_not_auto_apply"):
        run_primary_result_benchmark_plan_execution(
            plan_path=plan_path,
            output_path=tmp_path / "execution.json",
            runner=lambda *args, **kwargs: _Completed(0),
        )


def test_run_primary_result_benchmark_plan_execution_cli(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "run_primary_result_benchmark_plan_execution.py"
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps(
            {
                "plan_version": "primary_result_benchmark_plan.v1",
                "plan_id": "plan-cli",
                "status": "planned",
                "review_id": "review-cli",
                "result_id": "primary:000001.SZ",
                "ts_code": "000001.SZ",
                "required_tests": ["tests/test_primary_result_learning_feedback.py"],
                "expected_evidence_artifacts": [],
                "release_gates_required": True,
                "baseline_policy_required": True,
                "requires_baseline_revalidation": True,
                "do_not_auto_apply": True,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    output_path = tmp_path / "execution.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--plan-json",
            str(plan_path),
            "--output",
            str(output_path),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "passed"
    assert payload["plan_id"] == "plan-cli"
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "passed"
