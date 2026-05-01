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

from openclaw.services.release_dry_run_service import (
    render_release_dry_run_trend_markdown,
    run_release_dry_run_audit,
    summarize_release_dry_run_trend,
)


def _default_db_path(root: Path) -> Path:
    env_path = os.environ.get("OPENCLAW_DB_PATH") or os.environ.get("AIRIVO_DB_PATH") or ""
    return Path(env_path) if env_path else root / "data" / "openclaw.db"


def main() -> int:
    root = ROOT
    parser = argparse.ArgumentParser(description="Run a non-mutating Airivo release readiness dry-run audit.")
    parser.add_argument("--db", default=str(_default_db_path(root)), help="SQLite database path.")
    parser.add_argument("--code-root", default=str(root), help="Repository root used for code version fingerprint.")
    parser.add_argument("--output", default="", help="Optional JSON output path.")
    parser.add_argument("--operator", default=os.environ.get("USER", "system"), help="Operator name for the dry-run payload.")
    parser.add_argument("--json", action="store_true", help="Print full JSON payload instead of a concise summary.")
    parser.add_argument("--non-blocking", action="store_true", help="Always exit 0 while preserving readiness status in the payload.")
    parser.add_argument("--trend", action="append", default=[], help="Summarize one or more dry-run payload JSON files or directories.")
    parser.add_argument("--stable-threshold", type=int, default=2, help="Minimum latest consecutive validation failures before a hard-gate candidate.")
    parser.add_argument("--markdown-output", default="", help="Optional Markdown output path for trend summaries.")
    args = parser.parse_args()

    if args.trend:
        summary = summarize_release_dry_run_trend(
            payload_paths=args.trend,
            output_path=args.output,
            stable_threshold=args.stable_threshold,
        )
        if args.markdown_output:
            markdown_path = Path(args.markdown_output)
            markdown_path.parent.mkdir(parents=True, exist_ok=True)
            markdown_path.write_text(render_release_dry_run_trend_markdown(summary), encoding="utf-8")
        if args.json:
            print(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2))
        else:
            print(
                "release_dry_run_trend "
                f"payloads={summary['total_payloads']} "
                f"blocked={summary['blocked_payloads']} "
                f"hard_gate_candidates={len(summary['hard_gate_upgrade_candidates'])}"
            )
            if summary.get("stable_blocking_reasons"):
                print(
                    "stable_blocking_reasons="
                    + ",".join(item["reason"] for item in summary["stable_blocking_reasons"])
                )
            print(f"recommendation={summary['recommendation']}")
        return 0

    payload = run_release_dry_run_audit(
        db_path=args.db,
        code_root=args.code_root,
        output_path=args.output,
        operator_name=args.operator,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))
    else:
        print(f"release_dry_run decision={payload['decision']} allow_release_gate={payload['allow_release_gate']}")
        if payload.get("blocking_reasons"):
            print("blocking_reasons=" + ",".join(payload["blocking_reasons"]))
        print("satisfied_validations=" + ",".join(payload.get("satisfied_validations") or []))
        print("unsatisfied_validations=" + ",".join(payload.get("unsatisfied_validations") or []))
        rollback = payload.get("rollback_context") or {}
        reference = rollback.get("reference") if isinstance(rollback.get("reference"), dict) else {}
        print(f"rollback_reference={reference.get('release_id') or 'missing'}")
    if args.non_blocking:
        return 0
    return 0 if payload.get("allow_release_gate") else 2


if __name__ == "__main__":
    raise SystemExit(main())
