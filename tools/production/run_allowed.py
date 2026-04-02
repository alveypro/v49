#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ALLOW_FILE = ROOT / "tools" / "production" / "ALLOWED_SCRIPTS.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run only allowlisted production scripts")
    parser.add_argument("script", help="script path relative to repo root")
    parser.add_argument("script_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    allow = json.loads(ALLOW_FILE.read_text(encoding="utf-8"))
    allowed = set(allow.get("allowed", []))
    script = args.script.strip()

    if script not in allowed:
        print(f"[blocked] script not allowlisted: {script}")
        return 2

    target = ROOT / script
    if not target.exists():
        print(f"[error] script not found: {target}")
        return 1

    cmd = [str(target), *args.script_args]
    if target.suffix == ".py":
        cmd = [sys.executable, str(target), *args.script_args]

    proc = subprocess.run(cmd, cwd=str(ROOT))
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
