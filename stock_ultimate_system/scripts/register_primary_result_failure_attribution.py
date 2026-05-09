#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_failure_attribution_ledger import PrimaryResultFailureAttributionLedger


def main() -> int:
    parser = argparse.ArgumentParser(description="Append a passed primary result failure attribution into the formal ledger and refresh summary.")
    parser.add_argument("--attribution-json", default="data/experiments/primary_result_failure_attribution_latest.json")
    parser.add_argument("--ledger-jsonl", default="artifacts/primary_result_failure_attribution/ledger.jsonl")
    parser.add_argument("--summary-json", default="artifacts/primary_result_failure_attribution/summary.json")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    ledger = PrimaryResultFailureAttributionLedger(
        ledger_path=args.ledger_jsonl,
        summary_path=args.summary_json,
    )
    try:
        entry = ledger.append_attribution(attribution_path=args.attribution_json)
        response = {
            "status": "ok",
            "ledger_entry": entry,
            "ledger_path": str(ledger.ledger_path),
            "summary_path": str(ledger.summary_path),
        }
        if args.json:
            json.dump(response, sys.stdout, ensure_ascii=False, indent=2)
            sys.stdout.write("\n")
        return 0
    except Exception as exc:
        response = {"status": "error", "error": str(exc)}
        if args.json:
            json.dump(response, sys.stdout, ensure_ascii=False, indent=2)
            sys.stdout.write("\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
