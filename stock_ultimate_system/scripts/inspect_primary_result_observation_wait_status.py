#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_observation_wait_status import build_primary_result_observation_wait_status


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect controlled wait status for the current primary result observation.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--candidate-index", type=int, default=0)
    parser.add_argument("--output", default="artifacts/primary_result_observation_wait_status_latest.json")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exit_code, payload = build_primary_result_observation_wait_status(
        exp_dir=args.exp_dir,
        candidate_index=args.candidate_index,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "result_id": payload["result_id"],
                    "ts_code": payload["ts_code"],
                    "window_start": payload["observation_window"]["started_at"],
                    "next_actions": payload["next_actions"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
