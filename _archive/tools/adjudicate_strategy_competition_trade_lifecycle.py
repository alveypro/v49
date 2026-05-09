#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from openclaw.paths import db_path as default_db_path  # noqa: E402
from openclaw.services.strategy_competition_trade_lifecycle_adjudication_service import (  # noqa: E402
    build_strategy_competition_trade_lifecycle_adjudication,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Adjudicate the full post-release trade lifecycle for strategy competition Top5.")
    parser.add_argument("--db-path", default="", help="SQLite fact DB path.")
    parser.add_argument("--human-release-approval-artifact", default="")
    parser.add_argument("--live-order-authority-artifact", default="")
    parser.add_argument("--broker-submission-guard-artifact", default="")
    parser.add_argument("--broker-submission-response-artifact", default="")
    parser.add_argument("--broker-execution-feedback-artifact", default="")
    parser.add_argument("--post-trade-reconciliation-artifact", default="")
    parser.add_argument("--output-dir", default="logs/openclaw/strategy_competition_trade_lifecycle_adjudication")
    parser.add_argument("--operator-name", default="strategy_competition_trade_lifecycle_adjudication")
    args = parser.parse_args()

    conn = sqlite3.connect(str(args.db_path or default_db_path()), timeout=30)
    try:
        payload = build_strategy_competition_trade_lifecycle_adjudication(
            conn,
            human_release_approval_artifact_path=args.human_release_approval_artifact,
            live_order_authority_artifact_path=args.live_order_authority_artifact,
            broker_submission_guard_artifact_path=args.broker_submission_guard_artifact,
            broker_submission_response_artifact_path=args.broker_submission_response_artifact,
            broker_execution_feedback_artifact_path=args.broker_execution_feedback_artifact,
            post_trade_reconciliation_artifact_path=args.post_trade_reconciliation_artifact,
            output_dir=args.output_dir,
            operator_name=args.operator_name,
        )
    finally:
        conn.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
