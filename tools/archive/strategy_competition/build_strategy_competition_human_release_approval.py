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
from openclaw.services.strategy_competition_human_release_approval_service import (  # noqa: E402
    build_strategy_competition_human_release_approval,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the final human release approval gate for strategy competition Top5.")
    parser.add_argument("--db-path", default="", help="SQLite fact DB path.")
    parser.add_argument("--formal-validation-result-review-artifact", required=True)
    parser.add_argument("--release-chain-adjudication-artifact", required=True)
    parser.add_argument("--human-approval-decision-artifact", default="")
    parser.add_argument("--output-dir", default="logs/openclaw/strategy_competition_human_release_approval")
    parser.add_argument("--operator-name", default="strategy_competition_human_release_approval")
    args = parser.parse_args()

    conn = sqlite3.connect(str(args.db_path or default_db_path()), timeout=30)
    try:
        payload = build_strategy_competition_human_release_approval(
            conn,
            formal_validation_result_review_artifact_path=args.formal_validation_result_review_artifact,
            release_chain_adjudication_artifact_path=args.release_chain_adjudication_artifact,
            human_approval_decision_artifact_path=args.human_approval_decision_artifact,
            output_dir=args.output_dir,
            operator_name=args.operator_name,
        )
    finally:
        conn.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
