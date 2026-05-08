import json
import subprocess
import sys
from pathlib import Path

import pytest


pytestmark = pytest.mark.integration


def test_check_release_gates_dry_run_outputs_expected_groups():
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "check_release_gates.py"

    completed = subprocess.run(
        [sys.executable, str(script_path), "--dry-run"],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(completed.stdout)

    assert set(payload.keys()) == {
        "scope_readiness",
        "benchmark",
        "content_quality",
        "layout_contract",
        "runtime_metadata",
        "main_site_conversion_path",
    }
    assert "tests/test_airivo_scope_registry.py" in payload["scope_readiness"]
    assert "tests/test_t12_governance_summary.py" in payload["scope_readiness"]
    assert "tests/test_main_site_event_bridge.py" in payload["scope_readiness"]
    assert "tests/test_artifact_registry.py" in payload["scope_readiness"]
    assert "tests/test_run_stock_release_pipeline_fast.py" not in payload["scope_readiness"]
    assert "tests/test_run_stock_release_pipeline_functional.py" not in payload["scope_readiness"]
    assert "tests/test_run_stock_release_pipeline_integration.py" not in payload["scope_readiness"]
    assert "tests/test_run_stock_release_pipeline_e2e.py" not in payload["scope_readiness"]
    assert "tests/test_stock_primary_result_benchmark_report.py" in payload["benchmark"]
    assert "tests/test_candidate_quality_evaluation.py" in payload["benchmark"]
    assert "tests/test_candidate_quality_diff.py" in payload["benchmark"]
    assert "tests/test_candidate_quality_multiwindow_source.py" in payload["benchmark"]
    assert "tests/test_candidate_quality_multiwindow.py" in payload["benchmark"]
    assert "tests/test_register_candidate_quality_baseline.py" in payload["benchmark"]
    assert "tests/test_primary_result_failure_attribution.py" in payload["benchmark"]
    assert "tests/test_primary_result_failure_attribution_ledger.py" in payload["benchmark"]
    assert "tests/test_primary_result_feedback_loop.py" in payload["benchmark"]
    assert "tests/test_main_chain_authenticity_integration.py" in payload["runtime_metadata"]
    assert "tests/test_main_chain_recovery_integration.py" in payload["runtime_metadata"]
    assert "tests/test_main_site_home_contract.py" in payload["main_site_conversion_path"]


def test_check_release_gates_supports_json_output_and_gate_filter(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "check_release_gates.py"
    output_path = tmp_path / "release-gates.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--gate",
            "benchmark",
            "--json",
            "--output",
            str(output_path),
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    payload = json.loads(completed.stdout)
    assert payload["status"] == "passed"
    assert payload["gate_total"] == 3
    assert payload["failed_total"] == 0
    assert payload["artifact_support"]["benchmark_diff_script"] == "scripts/compare_stock_primary_result_benchmark_reports.py"
    assert payload["artifact_support"]["release_evidence_bundle_script"] == "scripts/build_release_evidence_bundle.py"
    assert payload["artifact_support"]["release_pipeline_script"] == "scripts/run_stock_release_pipeline.py"
    assert payload["scope_registry"]["scope_ids"] == ["main_site", "stock", "t12"]
    benchmark_result = next(item for item in payload["results"] if item["gate"] == "benchmark")
    assert benchmark_result["gate_level"] == "blocking"
    assert benchmark_result["passed"] is True
    assert next(item for item in json.loads(output_path.read_text(encoding="utf-8"))["results"] if item["gate"] == "benchmark")["gate"] == "benchmark"


def test_check_release_gates_dry_run_includes_latest_path_static_gate():
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "check_release_gates.py"

    completed = subprocess.run(
        [sys.executable, str(script_path), "--dry-run", "--json"],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(completed.stdout)

    assert payload["gate_levels"]["latest_path"] == "blocking"
    assert payload["gate_levels"]["pointer_integrity"] == "blocking"
    static_checks = payload["static_gates"]["latest_path"]
    assert {"path": "src/dashboard_context.py", "function": "build_primary_result"} in static_checks
    assert {"path": "run_dashboard.py", "function": "build_primary_result_api_payload"} in static_checks
    assert {"path": "scripts/run_primary_result_lifecycle.py", "function": "build_primary_result_api_payload"} in static_checks
    assert {"path": "scripts/run_primary_result_audit.py", "function": "build_primary_result_api_payload"} in static_checks
    assert {"path": "scripts/run_current_primary_result_daily_closure.py", "function": "build_primary_result_api_payload"} in static_checks
    assert {"path": "src/primary_result_observation_closure_preflight.py", "function": "build_primary_result_api_payload"} in static_checks
    assert payload["static_gates"]["pointer_integrity"]["script"] == "scripts/check_current_result_pointer_integrity.py"


def test_check_release_gates_runs_latest_path_gate():
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "check_release_gates.py"

    completed = subprocess.run(
        [sys.executable, str(script_path), "--gate", "benchmark", "--json"],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )

    payload = json.loads(completed.stdout)
    latest_path_result = next(item for item in payload["results"] if item["gate"] == "latest_path")
    pointer_integrity_result = next(item for item in payload["results"] if item["gate"] == "pointer_integrity")
    assert latest_path_result["passed"] is True
    assert latest_path_result["gate_level"] == "blocking"
    assert latest_path_result["checks"]
    assert pointer_integrity_result["gate_level"] == "blocking"
    assert pointer_integrity_result["status"] in {"passed", "skipped_no_active_pointer"}


def test_check_release_gates_require_active_pointer_fails_without_pointer():
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "check_release_gates.py"

    completed = subprocess.run(
        [sys.executable, str(script_path), "--gate", "benchmark", "--json", "--require-active-pointer"],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )

    payload = json.loads(completed.stdout)
    pointer_integrity_result = next(item for item in payload["results"] if item["gate"] == "pointer_integrity")
    assert completed.returncode == 1
    assert payload["gate_policy"]["require_active_pointer"] is True
    assert pointer_integrity_result["passed"] is False
    assert pointer_integrity_result["status"] == "failed_missing_active_pointer"
