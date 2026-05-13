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
from openclaw.services.strategy_competition_post_rerun_broker_response_review_service import (  # noqa: E402
    build_strategy_competition_post_rerun_broker_response_review,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Review post-rerun broker response before execution feedback.")
    parser.add_argument("--db-path", default="", help="SQLite fact DB path.")
    parser.add_argument("--post-rerun-broker-guard-review-artifact", required=True)
    parser.add_argument("--broker-response-submission-artifact", default="")
    parser.add_argument("--broker-submission-response-evidence-artifact", default="")
    parser.add_argument("--output-dir", default="logs/openclaw/strategy_competition_post_rerun_broker_response_review")
    parser.add_argument("--operator-name", default="strategy_competition_post_rerun_broker_response_review")
    args = parser.parse_args()

    conn = sqlite3.connect(str(args.db_path or default_db_path()), timeout=30)
    try:
        payload = build_strategy_competition_post_rerun_broker_response_review(
            conn,
            post_rerun_broker_guard_review_artifact_path=args.post_rerun_broker_guard_review_artifact,
            broker_response_submission_artifact_path=args.broker_response_submission_artifact,
            broker_submission_response_evidence_artifact_path=args.broker_submission_response_evidence_artifact,
            output_dir=args.output_dir,
            operator_name=args.operator_name,
        )
    finally:
        conn.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
