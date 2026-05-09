#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_benchmark_plan import PrimaryResultBenchmarkPlanRegistry


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an immutable benchmark plan from a needs_benchmark feedback review item.")
    parser.add_argument("--plans-dir", default="artifacts/primary_result_benchmark_plans")
    parser.add_argument("--review-item-json", required=True)
    parser.add_argument("--plan-id")
    args = parser.parse_args()

    registry = PrimaryResultBenchmarkPlanRegistry(plans_dir=args.plans_dir)
    try:
        plan = registry.create_plan(review_item_path=args.review_item_json, plan_id=args.plan_id)
        print(
            json.dumps(
                {
                    "status": "planned",
                    "plan_id": plan["plan_id"],
                    "review_id": plan["review_id"],
                    "required_test_total": len(plan["required_tests"]),
                    "requires_baseline_revalidation": plan["requires_baseline_revalidation"],
                    "plan_path": str(registry.history_dir / f"{plan['plan_id']}.json"),
                    "current_pointer_path": str(registry.current_path),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
