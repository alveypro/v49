import json
import subprocess
import sys
from pathlib import Path

from scripts.run_airivo_scope_full_readiness import build_scope_full_readiness_plan, run_scope_full_readiness


def test_scope_full_readiness_plan_uses_full_required_tests():
    plan = build_scope_full_readiness_plan()

    assert set(plan.keys()) == {"main_site", "stock", "t12"}
    assert "tests/test_main_chain_authenticity_integration.py" in plan["stock"]
    assert "tests/test_main_chain_recovery_integration.py" in plan["stock"]
    assert "tests/test_candidate_quality_evaluation.py" in plan["stock"]
    assert "tests/test_candidate_quality_multiwindow_source.py" in plan["stock"]
    assert "tests/test_candidate_quality_multiwindow.py" in plan["stock"]
    assert "tests/test_register_candidate_quality_baseline.py" in plan["stock"]
    assert "tests/test_primary_result_failure_attribution.py" in plan["stock"]
    assert "tests/test_primary_result_failure_attribution_ledger.py" in plan["stock"]
    assert "tests/test_primary_result_feedback_loop.py" in plan["stock"]
    assert "tests/test_run_stock_release_pipeline_fast.py" in plan["stock"]
    assert "tests/test_run_stock_release_pipeline_functional.py" in plan["stock"]
    assert "tests/test_run_stock_release_pipeline_integration.py" in plan["stock"]
    assert "tests/test_run_stock_release_pipeline_e2e.py" in plan["stock"]
    assert "tests/test_run_dashboard_primary_result_api.py" in plan["t12"]
    assert "tests/test_main_site_event_sink_jsonl.py" in plan["main_site"]


def test_scope_full_readiness_runner_supports_fake_runner(tmp_path):
    commands: list[list[str]] = []

    def fake_runner(command, cwd=None, capture_output=False, text=False):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="ok\n", stderr="")

    output_path = tmp_path / "scope_full_readiness.json"
    exit_code, payload = run_scope_full_readiness(
        selected_scopes=["main_site", "t12"],
        capture_output=True,
        output_path=output_path,
        runner=fake_runner,
    )

    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["scope_total"] == 2
    assert [result["scope_id"] for result in payload["results"]] == ["main_site", "t12"]
    assert all(command[:3] == [sys.executable, "-m", "pytest"] for command in commands)
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "passed"


def test_scope_full_readiness_dry_run_cli_outputs_plan():
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "run_airivo_scope_full_readiness.py"

    completed = subprocess.run(
        [sys.executable, str(script_path), "--dry-run", "--json"],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(completed.stdout)

    assert payload["mode"] == "dry_run"
    assert payload["scope_total"] == 3
    assert "tests/test_main_chain_authenticity_integration.py" in payload["plan"]["stock"]
    assert "tests/test_main_chain_recovery_integration.py" in payload["plan"]["stock"]
    assert "tests/test_candidate_quality_evaluation.py" in payload["plan"]["stock"]
    assert "tests/test_candidate_quality_multiwindow_source.py" in payload["plan"]["stock"]
    assert "tests/test_candidate_quality_multiwindow.py" in payload["plan"]["stock"]
    assert "tests/test_register_candidate_quality_baseline.py" in payload["plan"]["stock"]
    assert "tests/test_primary_result_failure_attribution.py" in payload["plan"]["stock"]
    assert "tests/test_primary_result_failure_attribution_ledger.py" in payload["plan"]["stock"]
    assert "tests/test_primary_result_feedback_loop.py" in payload["plan"]["stock"]
    assert "tests/test_run_stock_release_pipeline_fast.py" in payload["plan"]["stock"]
    assert "tests/test_run_stock_release_pipeline_functional.py" in payload["plan"]["stock"]
    assert "tests/test_run_stock_release_pipeline_integration.py" in payload["plan"]["stock"]
    assert "tests/test_run_stock_release_pipeline_e2e.py" in payload["plan"]["stock"]
