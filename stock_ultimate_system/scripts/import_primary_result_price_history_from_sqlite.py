#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_price_history_sqlite_ingest import import_primary_result_price_history_from_sqlite


def main() -> int:
    parser = argparse.ArgumentParser(description="Import primary result price history from a read-only SQLite database.")
    parser.add_argument("--sqlite-db", required=True)
    parser.add_argument("--sqlite-table", default="daily_trading_data")
    parser.add_argument("--output-csv", default="data/experiments/primary_result_price_history_latest.csv")
    parser.add_argument("--manifest-output", default="data/experiments/primary_result_price_history_sqlite_ingest_latest.json")
    parser.add_argument("--ts-code", required=True)
    parser.add_argument("--benchmark-ts-code", default="BENCHMARK")
    parser.add_argument("--window-start", required=True)
    parser.add_argument("--window-end", required=True)
    parser.add_argument("--source-label", default="permanent_stock_database")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exit_code, payload = import_primary_result_price_history_from_sqlite(
        sqlite_db_path=args.sqlite_db,
        sqlite_table=args.sqlite_table,
        output_csv_path=args.output_csv,
        manifest_output_path=args.manifest_output,
        ts_code=args.ts_code,
        benchmark_ts_code=args.benchmark_ts_code,
        window_start=args.window_start,
        window_end=args.window_end,
        source_label=args.source_label,
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
                    "sqlite_table": payload["sqlite_table"],
                    "output_csv_path": payload["output_csv_path"],
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
