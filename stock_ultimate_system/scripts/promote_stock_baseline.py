#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.stock_baseline_registry import StockBaselineRegistry


def main() -> int:
    parser = argparse.ArgumentParser(description="Promote or rollback the official /stock baseline.")
    parser.add_argument("--artifacts-dir", default="artifacts/baselines")
    parser.add_argument("--policy-path", default="STOCK_PRIMARY_RESULT_BASELINE_POLICY.md")
    parser.add_argument("--baseline-id", help="Optional explicit baseline id for promotion.")
    parser.add_argument("--benchmark-report-json")
    parser.add_argument("--benchmark-diff-json")
    parser.add_argument("--release-gates-json")
    parser.add_argument("--evidence-bundle-json")
    parser.add_argument("--manifest-json")
    parser.add_argument("--release-decision-json")
    parser.add_argument("--rollback-baseline-id", help="Rollback current pointer to an immutable history snapshot.")
    args = parser.parse_args()

    registry = StockBaselineRegistry(
        baselines_dir=args.artifacts_dir,
        policy_path=args.policy_path,
    )

    try:
        if args.rollback_baseline_id:
            snapshot = registry.rollback(args.rollback_baseline_id)
            payload = {
                "action": "rollback",
                "status": "ok",
                "baseline_id": snapshot["baseline_id"],
                "run_id": snapshot["run_id"],
                "current_pointer_path": str(registry.current_path),
            }
        else:
            required_args = {
                "benchmark_report_json": args.benchmark_report_json,
                "benchmark_diff_json": args.benchmark_diff_json,
                "release_gates_json": args.release_gates_json,
                "evidence_bundle_json": args.evidence_bundle_json,
                "manifest_json": args.manifest_json,
                "release_decision_json": args.release_decision_json,
            }
            missing = sorted(name for name, value in required_args.items() if not value)
            if missing:
                raise ValueError(f"missing required args for promotion: {', '.join(missing)}")
            snapshot = registry.promote(
                baseline_id=args.baseline_id,
                benchmark_report_path=args.benchmark_report_json,
                benchmark_diff_path=args.benchmark_diff_json,
                release_gates_path=args.release_gates_json,
                evidence_bundle_path=args.evidence_bundle_json,
                manifest_path=args.manifest_json,
                release_decision_path=args.release_decision_json,
            )
            payload = {
                "action": "promote",
                "status": "ok",
                "baseline_id": snapshot["baseline_id"],
                "run_id": snapshot["run_id"],
                "snapshot_path": str(registry.history_dir / f"{snapshot['baseline_id']}.json"),
                "current_pointer_path": str(registry.current_path),
            }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "error",
                    "error": str(exc),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
