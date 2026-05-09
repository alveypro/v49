#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_morning_operations_brief import build_primary_result_morning_operations_brief


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the /stock morning operations brief from the daily planner.")
    parser.add_argument("--planner-json", default="artifacts/primary_result_daily_planner_latest.json")
    parser.add_argument("--output", default="artifacts/primary_result_morning_operations_brief_latest.md")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exit_code, payload = build_primary_result_morning_operations_brief(
        planner_path=args.planner_json,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(json.dumps({"status": payload["status"], "output_path": payload["output_path"]}, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
