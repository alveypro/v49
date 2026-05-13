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
from openclaw.services.strategy_competition_rerun_court_rebuild_review_service import (  # noqa: E402
    build_strategy_competition_rerun_court_rebuild_review,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Review manifest and court-of-record rebuilds after accepted formal rerun results.")
    parser.add_argument("--db-path", default="", help="SQLite fact DB path.")
    parser.add_argument("--rerun-result-review-artifact", required=True)
    parser.add_argument("--court-rebuild-submission-artifact", default="")
    parser.add_argument("--rebuilt-manifest-artifact", default="")
    parser.add_argument("--rebuilt-release-chain-artifact", default="")
    parser.add_argument("--output-dir", default="logs/openclaw/strategy_competition_rerun_court_rebuild_review")
    parser.add_argument("--operator-name", default="strategy_competition_rerun_court_rebuild_review")
    args = parser.parse_args()

    conn = sqlite3.connect(str(args.db_path or default_db_path()), timeout=30)
    try:
        payload = build_strategy_competition_rerun_court_rebuild_review(
            conn,
            rerun_result_review_artifact_path=args.rerun_result_review_artifact,
            court_rebuild_submission_artifact_path=args.court_rebuild_submission_artifact,
            rebuilt_manifest_artifact_path=args.rebuilt_manifest_artifact,
            rebuilt_release_chain_artifact_path=args.rebuilt_release_chain_artifact,
            output_dir=args.output_dir,
            operator_name=args.operator_name,
        )
    finally:
        conn.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
