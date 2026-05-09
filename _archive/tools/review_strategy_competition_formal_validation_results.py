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
from openclaw.services.strategy_competition_formal_validation_handoff_service import (  # noqa: E402
    build_strategy_competition_formal_validation_result_review,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Review formal validation outputs against a ready strategy competition handoff.")
    parser.add_argument("--db-path", default="", help="SQLite fact DB path.")
    parser.add_argument("--formal-validation-handoff-artifact", required=True)
    parser.add_argument("--shadow-execution-evidence-artifact", default="")
    parser.add_argument("--independent-validation-artifact", default="")
    parser.add_argument("--operational-controls-artifact", default="")
    parser.add_argument("--competition-audit-rerun-artifact", default="")
    parser.add_argument("--production-readiness-artifact", default="")
    parser.add_argument("--release-chain-adjudication-artifact", default="")
    parser.add_argument("--output-dir", default="logs/openclaw/strategy_competition_formal_validation_result_review")
    parser.add_argument("--operator-name", default="strategy_competition_formal_validation_result_review")
    args = parser.parse_args()

    result_paths = {
        "shadow_execution_evidence": args.shadow_execution_evidence_artifact,
        "independent_validation": args.independent_validation_artifact,
        "operational_controls": args.operational_controls_artifact,
        "competition_audit_rerun": args.competition_audit_rerun_artifact,
        "production_readiness": args.production_readiness_artifact,
        "release_chain_adjudication": args.release_chain_adjudication_artifact,
    }
    conn = sqlite3.connect(str(args.db_path or default_db_path()), timeout=30)
    try:
        payload = build_strategy_competition_formal_validation_result_review(
            conn,
            formal_validation_handoff_artifact_path=args.formal_validation_handoff_artifact,
            formal_result_artifact_paths=result_paths,
            output_dir=args.output_dir,
            operator_name=args.operator_name,
        )
    finally:
        conn.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
