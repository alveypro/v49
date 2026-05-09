from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Protocol

from src.artifact_registry import sha256_file
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_BENCHMARK_PLAN_EXECUTION_VERSION = "primary_result_benchmark_plan_execution.v1"
SUPPORTED_PLAN_VERSION = "primary_result_benchmark_plan.v1"


class CompletedRun(Protocol):
    returncode: int
    stdout: str | None
    stderr: str | None


Runner = Callable[..., CompletedRun]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _validate_plan(plan: dict[str, object]) -> None:
    if plan.get("plan_version") != SUPPORTED_PLAN_VERSION:
        raise ValueError("primary result benchmark plan version is invalid")
    if plan.get("status") != "planned":
        raise ValueError("primary result benchmark plan status must be planned")
    if plan.get("do_not_auto_apply") is not True:
        raise ValueError("primary result benchmark plan must keep do_not_auto_apply=true")
    if plan.get("requires_baseline_revalidation") is not True:
        raise ValueError("primary result benchmark plan must require baseline revalidation")
    tests = plan.get("required_tests")
    if not isinstance(tests, list) or not tests:
        raise ValueError("primary result benchmark plan missing required_tests")
    if any(not isinstance(test, str) or not test.strip() for test in tests):
        raise ValueError("primary result benchmark plan required_tests must contain non-empty strings")


def run_primary_result_benchmark_plan_execution(
    *,
    plan_path: str | Path,
    output_path: str | Path,
    runner: Runner = subprocess.run,
    cwd: str | Path | None = None,
    capture_output: bool = True,
    timeout: int | None = None,
    executed_at: str | None = None,
) -> tuple[int, dict[str, object]]:
    resolved_plan_path = resolve_project_path(plan_path)
    if not resolved_plan_path.exists():
        raise FileNotFoundError(f"primary result benchmark plan missing: {resolved_plan_path}")
    plan = _read_json(resolved_plan_path)
    _validate_plan(plan)

    tests = [str(test) for test in plan["required_tests"]]
    command = [sys.executable, "-m", "pytest", *tests]
    completed = runner(
        command,
        cwd=resolve_project_path(cwd) if cwd is not None else resolved_plan_path.parents[2],
        capture_output=capture_output,
        text=True,
        timeout=timeout,
    )
    status = "passed" if completed.returncode == 0 else "failed"
    payload = {
        "execution_version": PRIMARY_RESULT_BENCHMARK_PLAN_EXECUTION_VERSION,
        "executed_at": executed_at or _utc_now_iso(),
        "status": status,
        "plan_id": plan.get("plan_id"),
        "review_id": plan.get("review_id"),
        "result_id": plan.get("result_id"),
        "ts_code": plan.get("ts_code"),
        "execution_priority": plan.get("execution_priority"),
        "execution_batch": plan.get("execution_batch"),
        "source_plan_path": str(resolved_plan_path),
        "source_plan_hash": sha256_file(resolved_plan_path),
        "command": command,
        "required_tests": tests,
        "required_test_total": len(tests),
        "exit_code": completed.returncode,
        "stdout": completed.stdout if capture_output else None,
        "stderr": completed.stderr if capture_output else None,
        "release_gates_required": bool(plan.get("release_gates_required")),
        "baseline_policy_required": bool(plan.get("baseline_policy_required")),
        "requires_baseline_revalidation": bool(plan.get("requires_baseline_revalidation")),
        "do_not_auto_apply": True,
        "execution_boundary": (
            "benchmark plan execution only runs validation tests and records evidence; it does not apply changes or promote baselines"
        ),
        "expected_evidence_artifacts": list(plan.get("expected_evidence_artifacts", []) or []),
    }
    _write_json(resolve_project_path(output_path), payload)
    return (0 if status == "passed" else 1), payload
