#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_production_readiness_ledger import PrimaryResultProductionReadinessLedger


def main() -> int:
    parser = argparse.ArgumentParser(description="Build immutable primary result production readiness evidence.")
    parser.add_argument("--readiness-dir", default="artifacts/primary_result_production_readiness")
    parser.add_argument("--release-decision-json", required=True)
    parser.add_argument("--baseline-current-json", required=True)
    parser.add_argument("--terminal-json", required=True)
    parser.add_argument("--performance-ledger-jsonl", required=True)
    parser.add_argument("--performance-summary-json", required=True)
    parser.add_argument("--readiness-id")
    args = parser.parse_args()

    ledger = PrimaryResultProductionReadinessLedger(readiness_dir=args.readiness_dir)
    try:
        readiness = ledger.create_readiness(
            release_decision_path=args.release_decision_json,
            baseline_current_path=args.baseline_current_json,
            terminal_path=args.terminal_json,
            performance_ledger_path=args.performance_ledger_jsonl,
            performance_summary_path=args.performance_summary_json,
            readiness_id=args.readiness_id,
        )
        print(
            json.dumps(
                {
                    "status": readiness["status"],
                    "readiness_id": readiness["readiness_id"],
                    "baseline_id": readiness["baseline_id"],
                    "decision_id": readiness["decision_id"],
                    "blocking_reasons": readiness["blocking_reasons"],
                    "readiness_path": str(ledger.history_dir / f"{readiness['readiness_id']}.json"),
                    "current_pointer_path": str(ledger.current_path),
                    "production_boundary": readiness["production_boundary"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0 if readiness["status"] == "ready" else 1
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
