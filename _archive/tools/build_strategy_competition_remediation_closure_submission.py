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
from openclaw.services.strategy_competition_remediation_closure_submission_service import (  # noqa: E402
    build_strategy_competition_remediation_closure_submission,
)


def _parse_item_closure(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("closure mapping must be artifact=path")
    artifact, path = value.split("=", 1)
    artifact = artifact.strip()
    path = path.strip()
    if not artifact or not path:
        raise argparse.ArgumentTypeError("closure mapping must be artifact=path")
    return artifact, path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a remediation closure submission from validator artifacts.")
    parser.add_argument("--db-path", default="", help="SQLite fact DB path.")
    parser.add_argument("--work-order-artifact", required=True)
    parser.add_argument("--item-closure", action="append", default=[], metavar="ARTIFACT=PATH")
    parser.add_argument("--output-dir", default="logs/openclaw/strategy_competition_remediation_closure_submission")
    parser.add_argument("--operator-name", default="strategy_competition_remediation_closure_submission")
    args = parser.parse_args()

    closure_artifact_paths = dict(_parse_item_closure(item) for item in args.item_closure)
    conn = sqlite3.connect(str(args.db_path or default_db_path()), timeout=30)
    try:
        payload = build_strategy_competition_remediation_closure_submission(
            conn,
            work_order_artifact_path=args.work_order_artifact,
            closure_artifact_paths=closure_artifact_paths,
            output_dir=args.output_dir,
            operator_name=args.operator_name,
        )
    finally:
        conn.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
