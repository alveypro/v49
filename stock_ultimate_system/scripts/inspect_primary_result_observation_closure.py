#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_observation_closure_preflight import build_primary_result_observation_closure_preflight


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect whether the current primary result observation can be closed.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--candidate-index", type=int, default=0)
    parser.add_argument("--price-history-csv", default="data/experiments/primary_result_price_history_latest.csv")
    parser.add_argument("--price-history-manifest-json", default="data/experiments/primary_result_price_history_manifest_latest.json")
    parser.add_argument("--benchmark-ts-code", default="BENCHMARK")
    parser.add_argument("--window-start")
    parser.add_argument("--window-end", required=True)
    parser.add_argument("--min-success-return", type=float, default=0.0)
    parser.add_argument("--max-drawdown-floor", type=float, default=-0.08)
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    try:
        exit_code, payload = build_primary_result_observation_closure_preflight(
            exp_dir=args.exp_dir,
            candidate_index=args.candidate_index,
            price_history_path=args.price_history_csv,
            price_history_manifest_path=args.price_history_manifest_json,
            benchmark_ts_code=args.benchmark_ts_code,
            window_start=args.window_start,
            window_end=args.window_end,
            min_success_return=args.min_success_return,
            max_drawdown_floor=args.max_drawdown_floor,
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
                    "closure_outcome": payload["closure_outcome"],
                    "result_id": payload["result_id"],
                    "ts_code": payload["ts_code"],
                    "blocking_reason_total": len(payload["blocking_reasons"]),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
