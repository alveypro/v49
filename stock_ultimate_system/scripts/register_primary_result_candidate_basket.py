#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_candidate_basket import (
    CONDITIONAL_MAX_INDUSTRY_WEIGHT,
    DEFAULT_MAX_HIGH_RISK_WEIGHT,
    DEFAULT_MAX_SINGLE_WEIGHT,
    TARGET_MAX_INDUSTRY_WEIGHT,
    PrimaryResultCandidateBasketRegistry,
    build_primary_result_candidate_basket_snapshot,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Register the current candidate TopN as a governed basket artifact.")
    parser.add_argument("--candidates-csv", default="data/experiments/candidates_top_latest.csv")
    parser.add_argument("--summary-json", default="data/experiments/candidates_basket_summary_latest.json")
    parser.add_argument("--validation-json")
    parser.add_argument("--baskets-dir", default="artifacts/primary_result_candidate_baskets")
    parser.add_argument("--basket-id")
    parser.add_argument("--run-id", default="candidate-basket")
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--max-single-weight", type=float, default=DEFAULT_MAX_SINGLE_WEIGHT)
    parser.add_argument("--max-high-risk-weight", type=float, default=DEFAULT_MAX_HIGH_RISK_WEIGHT)
    parser.add_argument("--target-max-industry-weight", type=float, default=TARGET_MAX_INDUSTRY_WEIGHT)
    parser.add_argument("--max-industry-weight", type=float, default=CONDITIONAL_MAX_INDUSTRY_WEIGHT)
    parser.add_argument("--min-items", type=int, default=1)
    parser.add_argument("--formal-release", action="store_true")
    parser.add_argument("--min-validation-rebalance-dates", type=int, default=0)
    parser.add_argument("--snapshot-output")
    parser.add_argument("--register-blocked", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exit_code, snapshot = build_primary_result_candidate_basket_snapshot(
        candidates_csv_path=args.candidates_csv,
        summary_json_path=args.summary_json,
        validation_json_path=args.validation_json,
        basket_id=args.basket_id,
        run_id=args.run_id,
        top_n=args.top_n,
        max_single_weight=args.max_single_weight,
        max_high_risk_weight=args.max_high_risk_weight,
        target_max_industry_weight=args.target_max_industry_weight,
        max_industry_weight=args.max_industry_weight,
        min_items=args.min_items,
        formal_release=args.formal_release,
        min_validation_rebalance_dates=args.min_validation_rebalance_dates,
        output_path=args.snapshot_output,
    )
    pointer = None
    if exit_code == 0 or args.register_blocked:
        registry = PrimaryResultCandidateBasketRegistry(baskets_dir=args.baskets_dir)
        pointer = registry.register_snapshot(snapshot)

    payload = {
        "status": snapshot["status"],
        "basket_id": snapshot["basket_id"],
        "registered": pointer is not None,
        "current_pointer": pointer,
        "blocking_reasons": snapshot["blocking_reasons"],
        "conditional_reasons": snapshot.get("conditional_reasons", []),
        "risk_budget": snapshot["risk_budget"],
    }
    if args.json:
        print(json.dumps({"snapshot": snapshot, **payload}, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if pointer is not None and snapshot["status"] in {"approved", "conditional"} else exit_code


if __name__ == "__main__":
    raise SystemExit(main())
