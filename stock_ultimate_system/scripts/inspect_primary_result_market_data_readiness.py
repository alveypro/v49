#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_market_data_readiness import build_primary_result_market_data_readiness


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect whether primary result market data is ready for observation closure.")
    parser.add_argument("--sqlite-db", required=True)
    parser.add_argument("--sqlite-table", default="daily_trading_data")
    parser.add_argument("--ts-code", required=True)
    parser.add_argument("--benchmark-ts-code", required=True)
    parser.add_argument("--window-start", required=True)
    parser.add_argument("--window-end", required=True)
    parser.add_argument("--min-window-rows", type=int, default=2)
    parser.add_argument("--min-target-amount", type=float, default=300000.0)
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exit_code, payload = build_primary_result_market_data_readiness(
        sqlite_db_path=args.sqlite_db,
        sqlite_table=args.sqlite_table,
        ts_code=args.ts_code,
        benchmark_ts_code=args.benchmark_ts_code,
        window_start=args.window_start,
        window_end=args.window_end,
        min_window_rows=args.min_window_rows,
        min_target_amount=args.min_target_amount,
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
                    "target_coverage": payload["target_coverage"],
                    "benchmark_coverage": payload["benchmark_coverage"],
                    "blocking_reason_total": len(payload["blocking_reasons"]),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
