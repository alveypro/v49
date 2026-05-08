#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.airivo_scope_registry import AIRIVO_SCOPE_REGISTRY, AIRIVO_SCOPE_REGISTRY_VERSION


Runner = Callable[..., subprocess.CompletedProcess[str]]


def _dedupe_preserve_order(items: tuple[str, ...]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def build_scope_full_readiness_plan(selected_scopes: list[str] | None = None) -> dict[str, list[str]]:
    scope_ids = selected_scopes or list(AIRIVO_SCOPE_REGISTRY.keys())
    return {
        scope_id: _dedupe_preserve_order(AIRIVO_SCOPE_REGISTRY[scope_id].required_tests)
        for scope_id in scope_ids
    }


def _run_scope(
    *,
    scope_id: str,
    tests: list[str],
    capture_output: bool,
    runner: Runner = subprocess.run,
) -> dict[str, object]:
    command = [sys.executable, "-m", "pytest", *tests]
    completed = runner(
        command,
        cwd=PROJECT_ROOT,
        capture_output=capture_output,
        text=True,
    )
    passed = completed.returncode == 0
    result: dict[str, object] = {
        "scope_id": scope_id,
        "route": AIRIVO_SCOPE_REGISTRY[scope_id].route,
        "role": AIRIVO_SCOPE_REGISTRY[scope_id].role,
        "passed": passed,
        "exit_code": completed.returncode,
        "tests": tests,
    }
    if capture_output:
        result["stdout"] = completed.stdout
        result["stderr"] = completed.stderr
    if not passed:
        print(f"[scope-full-readiness] {scope_id}: failed", file=sys.stderr)
    return result


def _build_payload(results: list[dict[str, object]]) -> dict[str, object]:
    return {
        "readiness_version": "airivo_scope_full_readiness.v1",
        "registry_version": AIRIVO_SCOPE_REGISTRY_VERSION,
        "mode": "full",
        "status": "passed" if all(item["passed"] for item in results) else "failed",
        "scope_total": len(results),
        "passed_total": sum(1 for item in results if item["passed"]),
        "failed_total": sum(1 for item in results if not item["passed"]),
        "scope_registry": {
            "scope_ids": sorted(AIRIVO_SCOPE_REGISTRY.keys()),
            "source": "src/airivo_scope_registry.py",
        },
        "results": results,
    }


def _write_output(path: str | Path | None, payload: dict[str, object]) -> None:
    if not path:
        return
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run_scope_full_readiness(
    *,
    selected_scopes: list[str] | None = None,
    capture_output: bool = False,
    output_path: str | Path | None = None,
    runner: Runner = subprocess.run,
) -> tuple[int, dict[str, object]]:
    plan = build_scope_full_readiness_plan(selected_scopes)
    results = [
        _run_scope(
            scope_id=scope_id,
            tests=tests,
            capture_output=capture_output,
            runner=runner,
        )
        for scope_id, tests in plan.items()
    ]
    payload = _build_payload(results)
    _write_output(output_path, payload)
    return (0 if payload["status"] == "passed" else 1), payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Run full readiness tests for Airivo's three product scopes.")
    parser.add_argument("--scope", action="append", choices=sorted(AIRIVO_SCOPE_REGISTRY.keys()))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--output")
    args = parser.parse_args()

    plan = build_scope_full_readiness_plan(args.scope)
    if args.dry_run:
        payload = {
            "mode": "dry_run",
            "readiness_version": "airivo_scope_full_readiness.v1",
            "registry_version": AIRIVO_SCOPE_REGISTRY_VERSION,
            "scope_total": len(plan),
            "plan": plan,
            "scope_registry": {
                "scope_ids": sorted(AIRIVO_SCOPE_REGISTRY.keys()),
                "source": "src/airivo_scope_registry.py",
            },
        }
        print(json.dumps(payload if (args.json or args.output) else plan, ensure_ascii=False, indent=2))
        _write_output(args.output, payload)
        return 0

    capture_output = args.json or bool(args.output)
    exit_code, payload = run_scope_full_readiness(
        selected_scopes=args.scope,
        capture_output=capture_output,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
