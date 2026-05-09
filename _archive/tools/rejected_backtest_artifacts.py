#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.rejected_backtest_artifact_ledger_service import (  # noqa: E402
    append_rejected_backtest_artifact,
    load_rejected_backtest_artifacts,
)


DEFAULT_LEDGER = "logs/openclaw/rejected_backtest_artifacts.jsonl"


def main() -> int:
    parser = argparse.ArgumentParser(description="Record or list rejected backtest artifacts.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    record = sub.add_parser("record", help="Append one rejected artifact ledger entry.")
    record.add_argument("--ledger", default=DEFAULT_LEDGER)
    record.add_argument("--artifact-path", required=True)
    record.add_argument("--strategy", required=True)
    record.add_argument("--reason", required=True)
    record.add_argument("--source-run-id", default="")
    record.add_argument("--operator-name", default="")
    record.add_argument("--note", default="")
    record.add_argument("--reused-as-runtime-default", action="store_true")

    listing = sub.add_parser("list", help="Print rejected artifact ledger as JSON.")
    listing.add_argument("--ledger", default=DEFAULT_LEDGER)

    args = parser.parse_args()
    if args.cmd == "record":
        entry = append_rejected_backtest_artifact(
            args.ledger,
            artifact_path=args.artifact_path,
            strategy=args.strategy,
            reason=args.reason,
            reused_as_runtime_default=bool(args.reused_as_runtime_default),
            source_run_id=args.source_run_id,
            operator_name=args.operator_name,
            note=args.note,
        )
        print(json.dumps(entry, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    if args.cmd == "list":
        print(json.dumps(load_rejected_backtest_artifacts(args.ledger), ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
