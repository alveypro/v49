#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_performance_ledger import PrimaryResultPerformanceLedger


def main() -> int:
    parser = argparse.ArgumentParser(description="Append a closed primary result observation to the performance ledger.")
    parser.add_argument("--ledger-jsonl", default="artifacts/primary_result_performance/ledger.jsonl")
    parser.add_argument("--summary-json", default="artifacts/primary_result_performance/summary.json")
    parser.add_argument("--observation-json", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    ledger = PrimaryResultPerformanceLedger(ledger_path=args.ledger_jsonl, summary_path=args.summary_json)
    try:
        entry = ledger.append_observation(observation_path=args.observation_json)
        payload = {
            "status": "registered",
            "entry_id": entry["entry_id"],
            "result_id": entry["result_id"],
            "ts_code": entry["ts_code"],
            "outcome": entry["outcome"],
            "ledger_path": str(ledger.ledger_path),
            "summary_path": str(ledger.summary_path),
        }
        print(json.dumps(payload if not args.json else {"entry": entry, **payload}, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
