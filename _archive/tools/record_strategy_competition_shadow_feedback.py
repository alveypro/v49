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
from openclaw.services.strategy_competition_shadow_feedback_service import (  # noqa: E402
    build_strategy_competition_shadow_execution_evidence,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Record shadow execution feedback for a competition shadow plan and emit evidence."
    )
    parser.add_argument("--db-path", default="", help="SQLite fact DB path.")
    parser.add_argument("--shadow-plan-artifact", required=True)
    parser.add_argument("--shadow-feedback-artifact", default="", help="Optional feedback JSON. Omit to summarize as blocked.")
    parser.add_argument("--minimum-source-tier", default="simulated", choices=["simulated", "quasi_live", "broker"])
    parser.add_argument("--output-dir", default="logs/openclaw/strategy_competition_shadow_execution_evidence")
    parser.add_argument("--operator-name", default="strategy_competition_shadow_feedback")
    args = parser.parse_args()

    conn = sqlite3.connect(str(args.db_path or default_db_path()), timeout=30)
    try:
        payload = build_strategy_competition_shadow_execution_evidence(
            conn,
            shadow_plan_artifact_path=args.shadow_plan_artifact,
            shadow_feedback_artifact_path=args.shadow_feedback_artifact,
            output_dir=args.output_dir,
            operator_name=args.operator_name,
            minimum_source_tier=args.minimum_source_tier,
        )
    finally:
        conn.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
