#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.airivo_scope_registry import AIRIVO_SCOPE_REGISTRY
from scripts.check_current_result_pointer_integrity import check_current_result_pointer_integrity


GATE_LEVELS: dict[str, str] = {
    "scope_readiness": "blocking",
    "benchmark": "blocking",
    "content_quality": "blocking",
    "layout_contract": "blocking",
    "runtime_metadata": "blocking",
    "main_site_conversion_path": "blocking",
    "latest_path": "blocking",
    "pointer_integrity": "blocking",
}

RELEASE_GATE_TEST_GROUPS: dict[str, list[str]] = {
    "scope_readiness": [
        "tests/test_airivo_scope_registry.py",
    ],
    "benchmark": [
        "tests/test_stock_primary_result_benchmarks.py",
        "tests/test_stock_primary_result_benchmark_report.py",
        "tests/test_candidate_quality_evaluation.py",
        "tests/test_candidate_quality_diff.py",
        "tests/test_candidate_quality_multiwindow_source.py",
        "tests/test_candidate_quality_multiwindow.py",
        "tests/test_register_candidate_quality_baseline.py",
        "tests/test_primary_result_failure_attribution.py",
        "tests/test_primary_result_failure_attribution_ledger.py",
        "tests/test_primary_result_feedback_loop.py",
    ],
    "content_quality": [
        "tests/test_stock_primary_result_content_quality.py",
        "tests/test_stock_primary_result.py",
        "tests/test_stock_primary_result_rendering.py",
    ],
    "layout_contract": [
        "tests/test_stock_primary_result_layout_contract.py",
        "tests/test_main_site_home_contract.py",
        "tests/test_main_site_home.py",
    ],
    "runtime_metadata": [
        "tests/test_stock_primary_result_runtime_metadata.py",
        "tests/test_dashboard_context.py",
        "tests/test_stock_primary_result_integration.py",
        "tests/test_main_chain_authenticity_integration.py",
        "tests/test_main_chain_recovery_integration.py",
    ],
    "main_site_conversion_path": [
        "tests/test_main_site_home.py",
        "tests/test_main_site_home_contract.py",
    ],
}

PRIMARY_RESULT_STRICT_CALLSITES: tuple[tuple[str, str], ...] = (
    ("src/dashboard_context.py", "build_primary_result"),
    ("run_dashboard.py", "build_primary_result_api_payload"),
    ("scripts/run_primary_result_audit.py", "build_primary_result_api_payload"),
    ("scripts/run_primary_result_execution.py", "build_primary_result_api_payload"),
    ("scripts/run_primary_result_observation.py", "build_primary_result_api_payload"),
    ("scripts/run_primary_result_rollback.py", "build_primary_result_api_payload"),
    ("scripts/run_primary_result_observation_metrics.py", "build_primary_result_api_payload"),
    ("scripts/record_primary_result_terminal.py", "build_primary_result_api_payload"),
    ("scripts/run_current_primary_result_daily_closure.py", "build_primary_result_api_payload"),
    ("scripts/run_primary_result_lifecycle.py", "build_primary_result_api_payload"),
    ("src/primary_result_observation_closure_preflight.py", "build_primary_result_api_payload"),
    ("src/primary_result_observation_wait_status.py", "build_primary_result_api_payload"),
    ("src/primary_result_candidate_handoff_gate.py", "build_primary_result_api_payload"),
)


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def build_release_gate_plan() -> dict[str, list[str]]:
    plan = {gate_name: list(test_paths) for gate_name, test_paths in RELEASE_GATE_TEST_GROUPS.items()}
    for scope in AIRIVO_SCOPE_REGISTRY.values():
        plan["scope_readiness"].extend(scope.smoke_required_tests)
    plan["scope_readiness"] = _dedupe_preserve_order(plan["scope_readiness"])
    return plan


def _build_result_payload(results: list[dict[str, object]]) -> dict[str, object]:
    return {
        "status": "passed" if all(item["passed"] for item in results) else "failed",
        "gate_total": len(results),
        "passed_total": sum(1 for item in results if item["passed"]),
        "failed_total": sum(1 for item in results if not item["passed"]),
        "artifact_support": {
            "benchmark_diff_script": "scripts/compare_stock_primary_result_benchmark_reports.py",
            "release_evidence_bundle_script": "scripts/build_release_evidence_bundle.py",
            "release_pipeline_script": "scripts/run_stock_release_pipeline.py",
        },
        "scope_registry": {
            "scope_ids": sorted(AIRIVO_SCOPE_REGISTRY.keys()),
            "source": "src/airivo_scope_registry.py",
        },
        "results": results,
    }


def _call_requires_current_result_pointer(path: Path, function_name: str) -> tuple[bool, str]:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        called_name = None
        if isinstance(func, ast.Name):
            called_name = func.id
        elif isinstance(func, ast.Attribute):
            called_name = func.attr
        if called_name != function_name:
            continue
        for keyword in node.keywords:
            if keyword.arg != "require_current_pointer":
                continue
            if isinstance(keyword.value, ast.Constant) and keyword.value.value is True:
                return True, ""
            return False, f"{path.relative_to(PROJECT_ROOT)} calls {function_name} without require_current_pointer=True"
        return False, f"{path.relative_to(PROJECT_ROOT)} calls {function_name} without require_current_pointer keyword"
    return False, f"{path.relative_to(PROJECT_ROOT)} does not call {function_name}"


def _run_latest_path_gate() -> dict[str, object]:
    checks: list[dict[str, object]] = []
    failures: list[str] = []
    for relative_path, function_name in PRIMARY_RESULT_STRICT_CALLSITES:
        path = PROJECT_ROOT / relative_path
        ok, detail = _call_requires_current_result_pointer(path, function_name)
        checks.append(
            {
                "path": relative_path,
                "function": function_name,
                "passed": ok,
                "detail": detail or "ok",
            }
        )
        if not ok:
            failures.append(detail)
    return {
        "gate": "latest_path",
        "gate_level": GATE_LEVELS["latest_path"],
        "passed": not failures,
        "exit_code": 0 if not failures else 1,
        "checks": checks,
        "failures": failures,
    }


def _run_pointer_integrity_gate(
    *,
    require_active_pointer: bool = False,
    pointer_dir: str | Path | None = None,
    results_dir: str | Path | None = None,
    runs_dir: str | Path | None = None,
    artifact_registry_path: str | Path | None = None,
) -> dict[str, object]:
    if pointer_dir is None:
        pointer_dir = PROJECT_ROOT / ".release_gate_runtime" / "current_result_pointer"
    if results_dir is None:
        results_dir = PROJECT_ROOT / ".release_gate_runtime" / "result_registry"
    if runs_dir is None:
        runs_dir = PROJECT_ROOT / ".release_gate_runtime" / "run_registry"
    exit_code, payload = check_current_result_pointer_integrity(
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )
    pointer = dict(payload.get("pointer") or {})
    pointer_active = bool(pointer.get("result_id") and pointer.get("run_id") and pointer.get("lifecycle_id"))
    if not pointer_active:
        if require_active_pointer:
            problems = list(payload.get("problems", []))
            problems.insert(0, "active current_result_pointer is required for release-context execution")
            return {
                "gate": "pointer_integrity",
                "gate_level": GATE_LEVELS["pointer_integrity"],
                "passed": False,
                "exit_code": 1,
                "status": "failed_missing_active_pointer",
                "detail": "release-context execution requires an active current_result_pointer chain",
                "pointer": pointer,
                "problems": problems,
            }
        return {
            "gate": "pointer_integrity",
            "gate_level": GATE_LEVELS["pointer_integrity"],
            "passed": True,
            "exit_code": 0,
            "status": "skipped_no_active_pointer",
            "detail": "current_result_pointer is not active in the default artifacts scope; integrity check skipped",
            "pointer": pointer,
            "problems": payload.get("problems", []),
        }
    return {
        "gate": "pointer_integrity",
        "gate_level": GATE_LEVELS["pointer_integrity"],
        "passed": exit_code == 0,
        "exit_code": exit_code,
        "status": "passed" if exit_code == 0 else "failed",
        "detail": "current_result_pointer/result_registry/run_registry chain must be consistent",
        "pointer": pointer,
        "problems": payload.get("problems", []),
    }


def _run_gate(name: str, tests: list[str], *, capture_output: bool) -> dict[str, object]:
    command = [sys.executable, "-m", "pytest", *tests]
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=capture_output,
        text=True,
    )
    passed = completed.returncode == 0
    result = {
        "gate": name,
        "gate_level": GATE_LEVELS[name],
        "passed": passed,
        "exit_code": completed.returncode,
        "tests": tests,
    }
    if capture_output:
        result["stdout"] = completed.stdout
        result["stderr"] = completed.stderr
    if not passed:
        print(f"[release-gates] {name}: failed", file=sys.stderr)
    return result


def _write_output(path: str | None, payload: dict[str, object]) -> None:
    if not path:
        return
    Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run_release_gates(
    *,
    selected_gates: list[str] | None = None,
    capture_output: bool = False,
    output_path: str | Path | None = None,
    require_active_pointer: bool = False,
    pointer_dir: str | Path | None = None,
    results_dir: str | Path | None = None,
    runs_dir: str | Path | None = None,
    artifact_registry_path: str | Path | None = None,
) -> tuple[int, dict[str, object]]:
    plan = build_release_gate_plan()
    if selected_gates:
        plan = {gate_name: plan[gate_name] for gate_name in selected_gates}
    results: list[dict[str, object]] = []
    for gate_name, test_paths in plan.items():
        results.append(_run_gate(gate_name, test_paths, capture_output=capture_output))
    latest_path_result = _run_latest_path_gate()
    results.append(latest_path_result)
    if not latest_path_result["passed"]:
        print("[release-gates] latest_path: failed", file=sys.stderr)
    pointer_integrity_result = _run_pointer_integrity_gate(
        require_active_pointer=require_active_pointer,
        pointer_dir=pointer_dir,
        results_dir=results_dir,
        runs_dir=runs_dir,
        artifact_registry_path=artifact_registry_path,
    )
    results.append(pointer_integrity_result)
    if not pointer_integrity_result["passed"]:
        print("[release-gates] pointer_integrity: failed", file=sys.stderr)
    payload = _build_result_payload(results)
    payload["gate_policy"] = {"require_active_pointer": require_active_pointer}
    _write_output(str(output_path) if output_path else None, payload)
    exit_code = 1 if any((not item["passed"]) and item["gate_level"] == "blocking" for item in results) else 0
    return exit_code, payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Airivo release gate checks.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the gate plan without executing pytest.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON result.",
    )
    parser.add_argument(
        "--output",
        help="Write machine-readable JSON result to a file.",
    )
    parser.add_argument(
        "--gate",
        action="append",
        choices=sorted(RELEASE_GATE_TEST_GROUPS.keys()),
        help="Run only the specified gate. Can be passed multiple times.",
    )
    parser.add_argument(
        "--require-active-pointer",
        action="store_true",
        help="Treat missing active current_result_pointer as a blocking failure.",
    )
    parser.add_argument("--pointer-dir")
    parser.add_argument("--results-dir")
    parser.add_argument("--runs-dir")
    parser.add_argument("--artifact-registry-path")
    args = parser.parse_args()

    plan = build_release_gate_plan()
    if args.gate:
        plan = {gate_name: plan[gate_name] for gate_name in args.gate}
    if args.dry_run:
        payload = {
            "mode": "dry_run",
            "gate_total": len(plan) + 2,
            "plan": plan,
            "gate_levels": {gate_name: GATE_LEVELS[gate_name] for gate_name in plan}
            | {
                "latest_path": GATE_LEVELS["latest_path"],
                "pointer_integrity": GATE_LEVELS["pointer_integrity"],
            },
            "artifact_support": {
                "benchmark_diff_script": "scripts/compare_stock_primary_result_benchmark_reports.py",
                "release_evidence_bundle_script": "scripts/build_release_evidence_bundle.py",
                "release_pipeline_script": "scripts/run_stock_release_pipeline.py",
            },
            "scope_registry": {
                "scope_ids": sorted(AIRIVO_SCOPE_REGISTRY.keys()),
                "source": "src/airivo_scope_registry.py",
            },
            "gate_policy": {
                "require_active_pointer": bool(args.require_active_pointer),
            },
            "static_gates": {
                "latest_path": [
                    {"path": relative_path, "function": function_name}
                    for relative_path, function_name in PRIMARY_RESULT_STRICT_CALLSITES
                ],
                "pointer_integrity": {
                    "script": "scripts/check_current_result_pointer_integrity.py",
                    "pointer_dir": args.pointer_dir,
                    "results_dir": args.results_dir,
                    "runs_dir": args.runs_dir,
                    "artifact_registry_path": args.artifact_registry_path,
                },
            },
        }
        if args.json or args.output:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            _write_output(args.output, payload)
        else:
            print(json.dumps(plan, ensure_ascii=False, indent=2))
        return 0

    capture_output = args.json or bool(args.output)
    exit_code, payload = run_release_gates(
        selected_gates=args.gate,
        capture_output=capture_output,
        output_path=args.output,
        require_active_pointer=args.require_active_pointer,
        pointer_dir=args.pointer_dir,
        results_dir=args.results_dir,
        runs_dir=args.runs_dir,
        artifact_registry_path=args.artifact_registry_path,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
