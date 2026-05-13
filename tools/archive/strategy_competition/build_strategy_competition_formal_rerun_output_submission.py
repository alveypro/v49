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
from openclaw.services.strategy_competition_formal_rerun_output_submission_service import (  # noqa: E402
    build_strategy_competition_formal_rerun_output_submission,
)


def _parse_step_output(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("step output mapping must be step=path")
    step, path = value.split("=", 1)
    step = step.strip()
    path = path.strip()
    if not step or not path:
        raise argparse.ArgumentTypeError("step output mapping must be step=path")
    return step, path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a formal rerun output submission from step artifacts.")
    parser.add_argument("--db-path", default="", help="SQLite fact DB path.")
    parser.add_argument("--rerun-plan-artifact", required=True)
    parser.add_argument("--step-output", action="append", default=[], metavar="STEP=PATH")
    parser.add_argument("--output-dir", default="logs/openclaw/strategy_competition_formal_rerun_output_submission")
    parser.add_argument("--operator-name", default="strategy_competition_formal_rerun_output_submission")
    args = parser.parse_args()

    step_output_artifact_paths = dict(_parse_step_output(item) for item in args.step_output)
    conn = sqlite3.connect(str(args.db_path or default_db_path()), timeout=30)
    try:
        payload = build_strategy_competition_formal_rerun_output_submission(
            conn,
            rerun_plan_artifact_path=args.rerun_plan_artifact,
            step_output_artifact_paths=step_output_artifact_paths,
            output_dir=args.output_dir,
            operator_name=args.operator_name,
        )
    finally:
        conn.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
