#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_candidate_basket_observation import (
    PrimaryResultCandidateBasketPerformanceLedger,
    build_primary_result_candidate_basket_observation,
    current_basket_snapshot_path,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Observe a registered candidate basket against local price history.")
    parser.add_argument("--basket-snapshot")
    parser.add_argument("--baskets-dir", default="artifacts/primary_result_candidate_baskets")
    parser.add_argument("--price-history-csv", required=True)
    parser.add_argument("--benchmark-ts-code", required=True)
    parser.add_argument("--window-start", required=True)
    parser.add_argument("--window-end", required=True)
    parser.add_argument("--output", default="artifacts/primary_result_candidate_baskets/observation_latest.json")
    parser.add_argument("--ledger-jsonl", default="artifacts/primary_result_candidate_baskets/performance_ledger.jsonl")
    parser.add_argument("--summary-json", default="artifacts/primary_result_candidate_baskets/performance_summary.json")
    parser.add_argument("--register-ledger", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    snapshot_path = Path(args.basket_snapshot) if args.basket_snapshot else current_basket_snapshot_path(baskets_dir=args.baskets_dir)
    exit_code, observation = build_primary_result_candidate_basket_observation(
        basket_snapshot_path=snapshot_path,
        price_history_path=args.price_history_csv,
        benchmark_ts_code=args.benchmark_ts_code,
        window_start=args.window_start,
        window_end=args.window_end,
        output_path=args.output,
    )
    ledger_entry = None
    if args.register_ledger:
        ledger = PrimaryResultCandidateBasketPerformanceLedger(
            ledger_path=args.ledger_jsonl,
            summary_path=args.summary_json,
        )
        ledger_entry = ledger.append_observation(observation_path=args.output)
    payload = {
        "status": observation["status"],
        "basket_id": observation["basket_id"],
        "metrics": observation["metrics"],
        "ledger_registered": ledger_entry is not None,
        "ledger_entry": ledger_entry,
    }
    if args.json:
        print(json.dumps({"observation": observation, **payload}, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
