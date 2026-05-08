#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_daily_planner import build_primary_result_daily_planner


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the /stock daily planner artifact.")
    parser.add_argument("--artifacts-dir", default="artifacts")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--output", default="artifacts/primary_result_daily_planner_latest.json")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exit_code, payload = build_primary_result_daily_planner(
        artifacts_dir=args.artifacts_dir,
        exp_dir=args.exp_dir,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "scoreboard_status": payload["scoreboard_status"],
                    "promotion_decision": payload["promotion_decision"],
                    "owner_workload_schedule": payload["owner_workload_schedule"],
                    "benchmark_execution_batches": payload["benchmark_execution_batches"],
                    "candidate_iteration_schedule": payload["candidate_iteration_schedule"],
                    "next_actions": payload["next_actions"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
