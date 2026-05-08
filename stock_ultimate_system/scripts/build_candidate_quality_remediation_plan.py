#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.candidate_quality.remediation import (
    build_candidate_quality_remediation_plan,
    write_candidate_quality_remediation_plan,
)
from src.utils.project_paths import resolve_experiments_path, resolve_project_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build actionable remediation plan for blocked candidate quality proof.")
    parser.add_argument("--exp-dir", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exp_dir = resolve_project_path(args.exp_dir) if args.exp_dir else resolve_experiments_path()
    output = resolve_project_path(args.output) if args.output else exp_dir / "candidate_quality_remediation_plan_latest.json"
    plan = build_candidate_quality_remediation_plan(exp_dir=exp_dir)
    output_path = write_candidate_quality_remediation_plan(plan, output_path=output)
    payload = {
        "status": plan.get("status"),
        "output_path": output_path,
        "action_count": plan.get("action_count", 0),
        "next_run_order": plan.get("next_run_order", []),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"{payload['status']}: actions={payload['action_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
