#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_daily_closure_orchestrator import run_primary_result_daily_closure_orchestrator


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the local primary result daily closure chain.")
    parser.add_argument("--sqlite-db", required=True)
    parser.add_argument("--sqlite-table", default="daily_trading_data")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--candidate-index", type=int, default=0)
    parser.add_argument("--ts-code", required=True)
    parser.add_argument("--benchmark-ts-code", required=True)
    parser.add_argument("--window-start", required=True)
    parser.add_argument("--window-end", required=True)
    parser.add_argument("--price-history-csv", default="data/experiments/primary_result_price_history_latest.csv")
    parser.add_argument("--price-history-manifest-json", default="data/experiments/primary_result_price_history_manifest_latest.json")
    parser.add_argument("--performance-ledger-jsonl", default="artifacts/primary_result_performance/ledger.jsonl")
    parser.add_argument("--performance-summary-json", default="artifacts/primary_result_performance/summary.json")
    parser.add_argument("--output", default="data/experiments/primary_result_daily_closure_latest.json")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exit_code, payload = run_primary_result_daily_closure_orchestrator(
        sqlite_db_path=args.sqlite_db,
        sqlite_table=args.sqlite_table,
        exp_dir=args.exp_dir,
        candidate_index=args.candidate_index,
        ts_code=args.ts_code,
        benchmark_ts_code=args.benchmark_ts_code,
        window_start=args.window_start,
        window_end=args.window_end,
        price_history_csv=args.price_history_csv,
        price_history_manifest_json=args.price_history_manifest_json,
        performance_ledger_jsonl=args.performance_ledger_jsonl,
        performance_summary_json=args.performance_summary_json,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "terminal_outcome": payload["terminal_outcome"],
                    "stage_statuses": [
                        {"name": stage["name"], "status": stage["status"], "exit_code": stage["exit_code"]}
                        for stage in payload["stages"]
                    ],
                    "blocking_reason_total": len(payload["blocking_reasons"]),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
