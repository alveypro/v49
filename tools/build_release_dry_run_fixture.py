#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.release_dry_run_fixture_service import build_release_dry_run_fixture


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a minimal Airivo release dry-run fixture DB and report.")
    parser.add_argument("--db", required=True, help="Output SQLite fixture DB path. Must not be the production DB.")
    parser.add_argument("--report", required=True, help="Output markdown report path.")
    parser.add_argument("--payload", required=True, help="Output dry-run JSON payload path.")
    parser.add_argument("--code-root", default=str(ROOT), help="Repository root used for code version fingerprint.")
    parser.add_argument("--operator", default=os.environ.get("USER", "dry_run"), help="Operator name recorded in fixture facts.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite an existing fixture DB.")
    args = parser.parse_args()

    result = build_release_dry_run_fixture(
        db_path=args.db,
        code_root=args.code_root,
        report_path=args.report,
        payload_path=args.payload,
        operator_name=args.operator,
        overwrite=bool(args.overwrite),
    )
    print(json.dumps({k: v for k, v in result.items() if k != "payload"}, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if result["payload"].get("allow_release_gate") else 2


if __name__ == "__main__":
    raise SystemExit(main())
