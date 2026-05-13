#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from openclaw.paths import db_path as default_db_path  # noqa: E402
from openclaw.services.strategy_competition_post_trade_reconciliation_service import (  # noqa: E402
    build_strategy_competition_post_trade_reconciliation,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile post-trade cash, position, costs, and exceptions for strategy competition Top5.")
    parser.add_argument("--db-path", default="", help="SQLite fact DB path.")
    parser.add_argument("--broker-execution-feedback-review-artifact", required=True)
    parser.add_argument("--post-trade-reconciliation-input-artifact", default="")
    parser.add_argument("--output-dir", default="logs/openclaw/strategy_competition_post_trade_reconciliation")
    parser.add_argument("--operator-name", default="strategy_competition_post_trade_reconciliation")
    args = parser.parse_args()

    conn = sqlite3.connect(str(args.db_path or default_db_path()), timeout=30)
    try:
        payload = build_strategy_competition_post_trade_reconciliation(
            conn,
            broker_execution_feedback_review_artifact_path=args.broker_execution_feedback_review_artifact,
            post_trade_reconciliation_input_artifact_path=args.post_trade_reconciliation_input_artifact,
            output_dir=args.output_dir,
            operator_name=args.operator_name,
        )
    finally:
        conn.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
