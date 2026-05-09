#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.project_paths import resolve_experiments_path, resolve_project_path


DAILY_CLOSURE_VERSION = "candidate_quality_daily_closure.v1"
CommandRunner = Callable[[list[str]], subprocess.CompletedProcess[str]]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_json_command(command: list[str], runner: CommandRunner) -> dict[str, Any]:
    completed = runner(command)
    payload: dict[str, Any]
    try:
        payload = json.loads(completed.stdout or "{}")
        if not isinstance(payload, dict):
            payload = {"raw_stdout": completed.stdout}
    except Exception:
        payload = {"raw_stdout": completed.stdout}
    return {
        "command": command,
        "returncode": completed.returncode,
        "ok": completed.returncode == 0,
        "payload": payload,
        "stderr": completed.stderr,
    }


def _default_runner(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )


def run_candidate_quality_daily_closure(
    *,
    exp_dir: str | Path | None = None,
    generate_candidates: bool = False,
    expanded_universe_size: int = 300,
    top_n: int = 5,
    quick_mode: bool = True,
    runner: CommandRunner = _default_runner,
) -> dict[str, Any]:
    resolved_exp_dir = resolve_project_path(exp_dir) if exp_dir else resolve_experiments_path()
    steps: list[dict[str, Any]] = []
    python = sys.executable

    if generate_candidates:
        command = [
            python,
            "run_top_candidates.py",
            "--universe-size",
            str(max(int(expanded_universe_size), 0)),
            "--top-n",
            str(max(int(top_n), 1)),
        ]
        if quick_mode:
            command.append("--quick-mode")
        steps.append({"id": "R3_candidate_generation", **_run_json_command(command, runner)})
    else:
        steps.append(
            {
                "id": "R3_candidate_generation",
                "ok": True,
                "returncode": 0,
                "payload": {
                    "status": "skipped",
                    "reason": "candidate generation requires explicit --generate-candidates to avoid overwriting current formal sample chain",
                    "recommended_command": (
                        f"{python} run_top_candidates.py --universe-size {int(expanded_universe_size)} "
                        f"--top-n {int(top_n)}{' --quick-mode' if quick_mode else ''}"
                    ),
                },
                "stderr": "",
            }
        )

    command_specs = [
        ("R1_freeze_observation", [python, "scripts/freeze_candidate_observation.py", "--exp-dir", str(resolved_exp_dir), "--json"]),
        (
            "R1_observation_closure",
            [python, "scripts/build_candidate_observation_closure.py", "--exp-dir", str(resolved_exp_dir), "--json"],
        ),
        (
            "R2_realistic_backtest",
            [python, "scripts/build_candidate_realistic_backtest.py", "--output-dir", str(resolved_exp_dir), "--json"],
        ),
        ("R3_R4_R5_portfolio_quality", [python, "scripts/build_candidate_portfolio.py", "--exp-dir", str(resolved_exp_dir), "--json"]),
        ("R6_risk_state", [python, "scripts/build_candidate_risk_state.py", "--exp-dir", str(resolved_exp_dir), "--json"]),
        ("P5_explanations", [python, "scripts/build_candidate_explanations.py", "--exp-dir", str(resolved_exp_dir), "--json"]),
        (
            "P6_quality_proof_report",
            [python, "scripts/build_candidate_quality_proof_report.py", "--exp-dir", str(resolved_exp_dir), "--json"],
        ),
        (
            "remediation_plan",
            [python, "scripts/build_candidate_quality_remediation_plan.py", "--exp-dir", str(resolved_exp_dir), "--json"],
        ),
    ]
    for step_id, command in command_specs:
        steps.append({"id": step_id, **_run_json_command(command, runner)})

    failed_steps = [step for step in steps if not step.get("ok")]
    remediation_step = next((step for step in steps if step["id"] == "remediation_plan"), {})
    remediation_payload = remediation_step.get("payload", {}) if isinstance(remediation_step.get("payload"), dict) else {}
    proof_step = next((step for step in steps if step["id"] == "P6_quality_proof_report"), {})
    proof_payload = proof_step.get("payload", {}) if isinstance(proof_step.get("payload"), dict) else {}
    status = "failed" if failed_steps else "blocked" if remediation_payload.get("status") == "blocked" else "passed"
    return {
        "schema_version": DAILY_CLOSURE_VERSION,
        "status": status,
        "generated_at": _utc_now(),
        "exp_dir": str(resolved_exp_dir),
        "generate_candidates": bool(generate_candidates),
        "failed_step_ids": [step["id"] for step in failed_steps],
        "quality_proof_status": proof_payload.get("status"),
        "remediation_status": remediation_payload.get("status"),
        "remediation_action_count": remediation_payload.get("action_count"),
        "next_run_order": remediation_payload.get("next_run_order", []),
        "steps": steps,
    }


def write_candidate_quality_daily_closure(payload: dict[str, Any], *, output_path: str | Path) -> str:
    resolved = Path(output_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(resolved)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the daily R1-R6 candidate quality closure loop.")
    parser.add_argument("--exp-dir", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--generate-candidates", action="store_true")
    parser.add_argument("--expanded-universe-size", type=int, default=300)
    parser.add_argument("--top-n", type=int, default=5)
    parser.add_argument("--no-quick-mode", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exp_dir = resolve_project_path(args.exp_dir) if args.exp_dir else resolve_experiments_path()
    payload = run_candidate_quality_daily_closure(
        exp_dir=exp_dir,
        generate_candidates=args.generate_candidates,
        expanded_universe_size=args.expanded_universe_size,
        top_n=args.top_n,
        quick_mode=not args.no_quick_mode,
    )
    output = resolve_project_path(args.output) if args.output else exp_dir / "candidate_quality_daily_closure_latest.json"
    output_path = write_candidate_quality_daily_closure(payload, output_path=output)
    response = {
        "status": payload.get("status"),
        "output_path": output_path,
        "quality_proof_status": payload.get("quality_proof_status"),
        "remediation_status": payload.get("remediation_status"),
        "remediation_action_count": payload.get("remediation_action_count"),
        "next_run_order": payload.get("next_run_order", []),
        "failed_step_ids": payload.get("failed_step_ids", []),
    }
    if args.json:
        print(json.dumps(response, ensure_ascii=False, indent=2))
    else:
        print(f"{response['status']}: actions={response['remediation_action_count']}")
    return 0 if not payload.get("failed_step_ids") else 1


if __name__ == "__main__":
    raise SystemExit(main())
