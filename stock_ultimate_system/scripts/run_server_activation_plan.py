#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


ACTIVATION_EXECUTION_VERSION = "server_activation_execution.v1"

CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def _load_plan(path: str | Path) -> dict[str, object]:
    plan_path = Path(path)
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"activation plan is not a JSON object: {plan_path}")
    if payload.get("activation_plan_version") != "server_activation_plan.v1":
        raise ValueError(f"activation plan has invalid version: {plan_path}")
    if payload.get("status") != "passed":
        raise ValueError("activation plan must be passed before execution")
    return payload


def _run_shell_command(command: str, timeout: float | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, shell=True, capture_output=True, text=True, timeout=timeout)


def _execute_commands(
    *,
    commands: list[str],
    dry_run: bool,
    timeout: float | None,
    command_runner: CommandRunner,
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    for index, command in enumerate(commands, start=1):
        if dry_run:
            results.append(
                {
                    "index": index,
                    "command": command,
                    "passed": True,
                    "dry_run": True,
                    "exit_code": None,
                    "stdout": "",
                    "stderr": "",
                }
            )
            continue
        try:
            completed = command_runner(command, timeout=timeout)
            passed = completed.returncode == 0
            results.append(
                {
                    "index": index,
                    "command": command,
                    "passed": passed,
                    "dry_run": False,
                    "exit_code": completed.returncode,
                    "stdout": completed.stdout,
                    "stderr": completed.stderr,
                }
            )
        except subprocess.TimeoutExpired as exc:
            passed = False
            results.append(
                {
                    "index": index,
                    "command": command,
                    "passed": False,
                    "dry_run": False,
                    "exit_code": None,
                    "stdout": exc.stdout or "",
                    "stderr": f"command timed out after {exc.timeout} seconds",
                }
            )
        if not passed:
            break
    return results


def _write_output(path: str | Path | None, payload: dict[str, object]) -> None:
    if not path:
        return
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run_server_activation_plan(
    *,
    plan_path: str | Path,
    confirm_release_id: str,
    action: str = "activate",
    dry_run: bool = False,
    output_path: str | Path | None = None,
    timeout: float | None = 600.0,
    command_runner: CommandRunner = _run_shell_command,
) -> tuple[int, dict[str, object]]:
    plan = _load_plan(plan_path)
    release_id = str(plan.get("release_id") or "")
    if confirm_release_id != release_id:
        raise ValueError("confirm_release_id must match activation plan release_id")
    command_key = "rollback_commands" if action == "rollback" else "activation_commands"
    commands = plan.get(command_key)
    if not isinstance(commands, list) or not all(isinstance(item, str) for item in commands) or not commands:
        raise ValueError(f"activation plan does not contain valid {command_key}")

    results = _execute_commands(
        commands=commands,
        dry_run=dry_run,
        timeout=timeout,
        command_runner=command_runner,
    )
    payload = {
        "activation_execution_version": ACTIVATION_EXECUTION_VERSION,
        "status": "passed" if all(item["passed"] for item in results) else "failed",
        "action": action,
        "dry_run": dry_run,
        "release_id": release_id,
        "plan_path": str(Path(plan_path)),
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "command_total": len(commands),
        "executed_total": len(results),
        "failed_total": sum(1 for item in results if not item["passed"]),
        "results": results,
    }
    _write_output(output_path, payload)
    return (0 if payload["status"] == "passed" else 1), payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Execute a passed server activation plan with release-id confirmation.")
    parser.add_argument("--plan", required=True)
    parser.add_argument("--confirm-release-id", required=True)
    parser.add_argument("--action", choices=("activate", "rollback"), default="activate")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--timeout", type=float, default=600.0)
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    try:
        exit_code, payload = run_server_activation_plan(
            plan_path=args.plan,
            confirm_release_id=args.confirm_release_id,
            action=args.action,
            dry_run=args.dry_run,
            output_path=args.output,
            timeout=args.timeout,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "activation_execution_version": payload["activation_execution_version"],
                    "action": payload["action"],
                    "dry_run": payload["dry_run"],
                    "release_id": payload["release_id"],
                    "executed_total": payload["executed_total"],
                    "failed_total": payload["failed_total"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
