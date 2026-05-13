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
from openclaw.services.strategy_competition_release_chain_adjudication_service import (  # noqa: E402
    build_strategy_competition_release_chain_adjudication,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Adjudicate the full strategy competition Top5 release evidence chain.")
    parser.add_argument("--db-path", default="", help="SQLite fact DB path.")
    parser.add_argument("--competition-audit-artifact", required=True)
    parser.add_argument("--shadow-execution-artifact", default="")
    parser.add_argument("--independent-validation-artifact", default="")
    parser.add_argument("--operational-controls-artifact", default="")
    parser.add_argument("--evidence-submission-review-artifact", default="")
    parser.add_argument("--production-readiness-artifact", default="")
    parser.add_argument("--output-dir", default="logs/openclaw/strategy_competition_release_chain_adjudication")
    parser.add_argument("--operator-name", default="strategy_competition_release_chain_adjudication")
    args = parser.parse_args()

    conn = sqlite3.connect(str(args.db_path or default_db_path()), timeout=30)
    try:
        payload = build_strategy_competition_release_chain_adjudication(
            conn,
            competition_audit_artifact_path=args.competition_audit_artifact,
            shadow_execution_artifact_path=args.shadow_execution_artifact,
            independent_validation_artifact_path=args.independent_validation_artifact,
            operational_controls_artifact_path=args.operational_controls_artifact,
            evidence_submission_review_artifact_path=args.evidence_submission_review_artifact,
            production_readiness_artifact_path=args.production_readiness_artifact,
            output_dir=args.output_dir,
            operator_name=args.operator_name,
        )
    finally:
        conn.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
