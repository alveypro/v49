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
from openclaw.services.strategy_competition_post_rerun_release_readiness_service import (  # noqa: E402
    build_strategy_competition_post_rerun_release_readiness,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Review post-rerun release readiness before live order authority checks.")
    parser.add_argument("--db-path", default="", help="SQLite fact DB path.")
    parser.add_argument("--rerun-court-rebuild-review-artifact", required=True)
    parser.add_argument("--release-readiness-submission-artifact", default="")
    parser.add_argument("--release-chain-adjudication-artifact", default="")
    parser.add_argument("--human-release-approval-artifact", default="")
    parser.add_argument("--output-dir", default="logs/openclaw/strategy_competition_post_rerun_release_readiness")
    parser.add_argument("--operator-name", default="strategy_competition_post_rerun_release_readiness")
    args = parser.parse_args()

    conn = sqlite3.connect(str(args.db_path or default_db_path()), timeout=30)
    try:
        payload = build_strategy_competition_post_rerun_release_readiness(
            conn,
            rerun_court_rebuild_review_artifact_path=args.rerun_court_rebuild_review_artifact,
            release_readiness_submission_artifact_path=args.release_readiness_submission_artifact,
            release_chain_adjudication_artifact_path=args.release_chain_adjudication_artifact,
            human_release_approval_artifact_path=args.human_release_approval_artifact,
            output_dir=args.output_dir,
            operator_name=args.operator_name,
        )
    finally:
        conn.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
