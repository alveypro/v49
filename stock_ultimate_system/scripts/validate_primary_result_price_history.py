#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_price_history_artifact import build_primary_result_price_history_artifact


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate local primary result price history CSV evidence.")
    parser.add_argument("--price-history-csv", default="data/experiments/primary_result_price_history_latest.csv")
    parser.add_argument("--ts-code", required=True)
    parser.add_argument("--benchmark-ts-code", default="BENCHMARK")
    parser.add_argument("--window-start", required=True)
    parser.add_argument("--window-end", required=True)
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exit_code, payload = build_primary_result_price_history_artifact(
        price_history_path=args.price_history_csv,
        ts_code=args.ts_code,
        benchmark_ts_code=args.benchmark_ts_code,
        window_start=args.window_start,
        window_end=args.window_end,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "ts_code": payload["ts_code"],
                    "benchmark_ts_code": payload["benchmark_ts_code"],
                    "row_counts": payload["row_counts"],
                    "blocking_reason_total": len(payload["blocking_reasons"]),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
