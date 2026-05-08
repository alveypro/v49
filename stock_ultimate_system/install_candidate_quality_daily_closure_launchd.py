from __future__ import annotations

import argparse
import json
import os
import plistlib
import subprocess
import sys
from pathlib import Path


LABEL = "com.stockultimate.candidate-quality-daily-closure"


def build_plist(
    project_root: Path,
    *,
    hour: int,
    minute: int,
    python_exec: str | None = None,
    exp_dir: str | Path | None = None,
    output_path: str | Path | None = None,
) -> dict:
    logs_dir = project_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    resolved_python = python_exec or sys.executable
    script = project_root / "scripts" / "run_candidate_quality_daily_closure.py"
    args = [
        resolved_python,
        str(script),
        "--json",
    ]
    if exp_dir:
        args.extend(["--exp-dir", str(exp_dir)])
    if output_path:
        args.extend(["--output", str(output_path)])
    return {
        "Label": LABEL,
        "ProgramArguments": args,
        "WorkingDirectory": str(project_root),
        "StartCalendarInterval": {
            "Hour": int(hour),
            "Minute": int(minute),
        },
        "RunAtLoad": False,
        "EnvironmentVariables": {
            "PYTHONUNBUFFERED": "1",
        },
        "StandardOutPath": str(logs_dir / "candidate_quality_daily_closure.launchd.log"),
        "StandardErrorPath": str(logs_dir / "candidate_quality_daily_closure.launchd.err"),
    }


def _existing_python_exec(plist_path: Path) -> str | None:
    if not plist_path.exists():
        return None
    try:
        with plist_path.open("rb") as f:
            plist = plistlib.load(f)
    except Exception:
        return None
    args = plist.get("ProgramArguments") if isinstance(plist, dict) else None
    if not isinstance(args, list) or not args:
        return None
    python_exec = str(args[0] or "").strip()
    if python_exec and Path(python_exec).exists():
        return python_exec
    return None


def install_launchd(
    project_root: Path,
    *,
    hour: int,
    minute: int,
    python_exec: str | None = None,
    exp_dir: str | Path | None = None,
    output_path: str | Path | None = None,
    launch_agents_dir: Path | None = None,
) -> Path:
    launch_agents = launch_agents_dir or Path.home() / "Library" / "LaunchAgents"
    launch_agents.mkdir(parents=True, exist_ok=True)
    plist_path = launch_agents / f"{LABEL}.plist"
    plist = build_plist(
        project_root,
        hour=hour,
        minute=minute,
        python_exec=python_exec or _existing_python_exec(plist_path),
        exp_dir=exp_dir,
        output_path=output_path,
    )
    with plist_path.open("wb") as f:
        plistlib.dump(plist, f)
    return plist_path


def reload_launchd(plist_path: Path) -> None:
    domain = f"gui/{os.getuid()}"
    subprocess.run(["launchctl", "bootout", domain, str(plist_path)], check=False)
    subprocess.run(["launchctl", "bootstrap", domain, str(plist_path)], check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Install daily candidate quality closure launchd job.")
    parser.add_argument("--hour", type=int, default=16)
    parser.add_argument("--minute", type=int, default=10)
    parser.add_argument("--python-exec", help="Python interpreter used by launchd; defaults to current or existing plist value.")
    parser.add_argument("--exp-dir", help="Optional experiment directory passed to the daily closure runner.")
    parser.add_argument("--output", help="Optional output JSON path passed to the daily closure runner.")
    parser.add_argument("--no-load", action="store_true", help="Write plist but do not reload launchd.")
    parser.add_argument("--dry-run", action="store_true", help="Print the plist JSON and do not write or load launchd.")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent
    plist = build_plist(
        project_root,
        hour=args.hour,
        minute=args.minute,
        python_exec=args.python_exec,
        exp_dir=args.exp_dir,
        output_path=args.output,
    )
    if args.dry_run:
        print(json.dumps(plist, ensure_ascii=False, indent=2))
        return 0

    plist_path = install_launchd(
        project_root,
        hour=args.hour,
        minute=args.minute,
        python_exec=args.python_exec,
        exp_dir=args.exp_dir,
        output_path=args.output,
    )
    if not args.no_load:
        reload_launchd(plist_path)
    print(f"installed: {plist_path}")
    print(f"schedule: daily {args.hour:02d}:{args.minute:02d}")
    print("command: scripts/run_candidate_quality_daily_closure.py --json")
    print("candidate generation: disabled by default; use runner flags manually for explicit expanded-universe generation.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
