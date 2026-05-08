#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_daily_operations_scoreboard import build_primary_result_daily_operations_scoreboard


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the /stock daily operations scoreboard from local evidence reports.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--artifacts-dir", default="artifacts")
    parser.add_argument("--output", default="artifacts/primary_result_daily_operations_scoreboard_latest.json")
    parser.add_argument("--zero-on-red", action="store_true", help="Write a red scoreboard but return 0 for systemd evidence capture.")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exit_code, payload = build_primary_result_daily_operations_scoreboard(
        exp_dir=args.exp_dir,
        artifacts_dir=args.artifacts_dir,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "overall_status": payload["overall_status"],
                    "score": payload["score"],
                    "operational_state": payload["operational_state"],
                    "next_actions": payload["next_actions"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    if args.zero_on_red and payload.get("overall_status") == "red":
        return 0
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
