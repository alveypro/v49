#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_benchmark_plan_execution import run_primary_result_benchmark_plan_execution


def main() -> int:
    parser = argparse.ArgumentParser(description="Execute validation tests from a primary result benchmark plan.")
    parser.add_argument("--plan-json", required=True)
    parser.add_argument("--output", default="artifacts/primary_result_benchmark_plan_execution_latest.json")
    parser.add_argument("--timeout", type=int)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        exit_code, payload = run_primary_result_benchmark_plan_execution(
            plan_path=args.plan_json,
            output_path=args.output,
            cwd=PROJECT_ROOT,
            timeout=args.timeout,
        )
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(
                json.dumps(
                    {
                        "status": payload["status"],
                        "plan_id": payload["plan_id"],
                        "required_test_total": payload["required_test_total"],
                        "exit_code": payload["exit_code"],
                        "do_not_auto_apply": payload["do_not_auto_apply"],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
        return exit_code
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
