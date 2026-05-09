#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.stock_primary_result_benchmark_diff import (
    build_stock_primary_result_benchmark_diff,
    render_stock_primary_result_benchmark_diff_json,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare two stock primary result benchmark reports.")
    parser.add_argument("base_report")
    parser.add_argument("target_report")
    parser.add_argument("--output", help="Optional JSON output file.")
    args = parser.parse_args()

    diff = build_stock_primary_result_benchmark_diff(args.base_report, args.target_report)
    payload = render_stock_primary_result_benchmark_diff_json(diff)
    print(payload)
    if args.output:
        Path(args.output).write_text(payload + "\n", encoding="utf-8")
    return 1 if diff.has_blocking_regression else 0


if __name__ == "__main__":
    raise SystemExit(main())
