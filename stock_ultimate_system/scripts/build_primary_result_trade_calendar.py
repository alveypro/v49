#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_trade_calendar_artifact import build_primary_result_trade_calendar_artifact


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a local /stock primary result trade calendar artifact.")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--exchange", default="SSE")
    parser.add_argument("--output", default="artifacts/primary_result_trade_calendar_latest.json")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        payload = build_primary_result_trade_calendar_artifact(
            start_date=args.start_date,
            end_date=args.end_date,
            exchange=args.exchange,
            output_path=args.output,
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": "created",
                    "output": args.output,
                    "source": payload["source"],
                    "exchange": payload["exchange"],
                    "start_date": payload["start_date"],
                    "end_date": payload["end_date"],
                    "trade_date_total": payload["trade_date_total"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
