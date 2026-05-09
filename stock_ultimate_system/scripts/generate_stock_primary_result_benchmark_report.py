#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.stock_primary_result_benchmark_report import (
    build_stock_primary_result_benchmark_report,
    write_stock_primary_result_benchmark_report_artifacts,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate stock primary result benchmark report artifacts.")
    parser.add_argument(
        "--output-dir",
        default="artifacts",
        help="Directory for stock_primary_result_benchmark_report.json/.md",
    )
    args = parser.parse_args()

    report = build_stock_primary_result_benchmark_report()
    write_stock_primary_result_benchmark_report_artifacts(Path(args.output_dir), report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
