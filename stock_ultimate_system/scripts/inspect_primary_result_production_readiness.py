#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_production_readiness_preflight import build_primary_result_production_readiness_preflight


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect primary result production readiness without writing history.")
    parser.add_argument("--release-decision-current-json", default="artifacts/primary_result_release_decisions/current.json")
    parser.add_argument("--baseline-current-json", default="artifacts/baselines/current.json")
    parser.add_argument("--terminal-json", default="data/experiments/primary_result_terminal_latest.json")
    parser.add_argument("--performance-ledger-jsonl", default="artifacts/primary_result_performance/ledger.jsonl")
    parser.add_argument("--performance-summary-json", default="artifacts/primary_result_performance/summary.json")
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    try:
        exit_code, payload = build_primary_result_production_readiness_preflight(
            release_decision_current_path=args.release_decision_current_json,
            baseline_current_path=args.baseline_current_json,
            terminal_path=args.terminal_json,
            performance_ledger_path=args.performance_ledger_jsonl,
            performance_summary_path=args.performance_summary_json,
            output_path=args.output,
        )
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "baseline_id": payload["baseline_id"],
                    "decision_id": payload["decision_id"],
                    "missing_artifacts": payload["missing_artifacts"],
                    "blocking_reason_total": len(payload["blocking_reasons"]),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
