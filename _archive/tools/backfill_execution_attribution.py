#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.paths import db_path as default_db_path  # noqa: E402
from openclaw.services.execution_attribution_backfill_service import (  # noqa: E402
    backfill_missing_execution_attribution,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill missing execution_attribution rows for stale orders.")
    parser.add_argument("--db-path", default=str(default_db_path()), help="SQLite DB path.")
    parser.add_argument(
        "--statuses",
        default="created,submitted",
        help="Comma-separated order statuses eligible for attribution backfill.",
    )
    parser.add_argument("--stale-minutes", type=int, default=30, help="Only backfill orders older than this age.")
    parser.add_argument("--max-orders", type=int, default=500, help="Maximum orders to process.")
    parser.add_argument("--apply", action="store_true", help="Persist backfilled attribution rows.")
    parser.add_argument("--output-dir", default="logs/openclaw", help="Artifact output directory.")
    parser.add_argument("--json", action="store_true", help="Print full JSON payload.")
    args = parser.parse_args()

    statuses = [item.strip().lower() for item in str(args.statuses or "").split(",") if item.strip()]
    conn = sqlite3.connect(str(args.db_path), timeout=30)
    try:
        payload = backfill_missing_execution_attribution(
            conn,
            statuses=statuses,
            stale_minutes=int(args.stale_minutes or 0),
            max_orders=int(args.max_orders or 0),
            apply_changes=bool(args.apply),
        )
    finally:
        conn.close()

    mode = "apply" if args.apply else "dry_run"
    out_dir = Path(args.output_dir)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"execution_attribution_backfill_{mode}_{ts}.json"
    payload["artifact_path"] = str(out_path)
    _write_json(out_path, payload)

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(
            "execution_attribution_backfill mode={mode} patched={patched} selected={selected} artifact={artifact}".format(
                mode=mode,
                patched=payload.get("patched_count"),
                selected=payload.get("selected_count"),
                artifact=payload.get("artifact_path"),
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
