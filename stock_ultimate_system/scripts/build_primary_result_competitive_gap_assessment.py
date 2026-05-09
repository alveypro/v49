#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_competitive_gap_assessment import build_primary_result_competitive_gap_assessment


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a current /stock competitive gap assessment against an industry leader capability model.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--artifacts-dir", default="artifacts")
    parser.add_argument("--output", default="artifacts/primary_result_competitive_gap_assessment_latest.json")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    _exit_code, payload = build_primary_result_competitive_gap_assessment(
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
                    "overall_score": payload["overall_score"],
                    "positioning": payload["positioning"],
                    "critical_gaps": payload["critical_gaps"],
                    "priority_actions": payload["priority_actions"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
