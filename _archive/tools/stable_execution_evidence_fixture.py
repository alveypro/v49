#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.paths import db_path as default_db_path  # noqa: E402
from openclaw.services.stable_execution_evidence_fixture_service import (  # noqa: E402
    seed_stable_shadow_execution_evidence,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed stable shadow execution evidence fixture.")
    parser.add_argument("--db-path", default=str(default_db_path()), help="SQLite DB path.")
    parser.add_argument("--linked-run-id", required=True, help="Linked signal run id for lineage.")
    parser.add_argument(
        "--output-dir",
        default="logs/openclaw",
        help="Directory for generated fixture artifacts.",
    )
    parser.add_argument("--operator-name", default="stable_shadow_execution_fixture", help="Operator name for evidence records.")
    parser.add_argument("--json", action="store_true", help="Print full JSON payload.")
    args = parser.parse_args()

    conn = sqlite3.connect(str(args.db_path), timeout=30)
    try:
        payload = seed_stable_shadow_execution_evidence(
            conn,
            linked_run_id=args.linked_run_id,
            output_dir=args.output_dir,
            operator_name=args.operator_name,
        )
    finally:
        conn.close()

    summary = payload.get("execution_evidence") if isinstance(payload.get("execution_evidence"), dict) else {}
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(
            "stable_execution_evidence_fixture passed={passed} total_orders={orders} json={json_path}".format(
                passed=summary.get("passed"),
                orders=summary.get("total_orders"),
                json_path=(payload.get("artifacts") or {}).get("json"),
            )
        )
    return 0 if summary.get("passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
