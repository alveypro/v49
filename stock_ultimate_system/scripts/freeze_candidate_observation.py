#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.candidate_quality.observation import (
    append_candidate_observation_ledger,
    build_candidate_observation_snapshot,
    freeze_candidate_observation_snapshot,
)
from src.utils.project_paths import resolve_experiments_path, resolve_project_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Freeze formal candidate observation snapshot and append ledger entries.")
    parser.add_argument("--exp-dir", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--ledger-jsonl", default=None)
    parser.add_argument("--candidates-csv", default=None)
    parser.add_argument("--lineage", default=None)
    parser.add_argument("--data-quality-gate", default=None)
    parser.add_argument("--realistic-backtest", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exp_dir = resolve_project_path(args.exp_dir) if args.exp_dir else resolve_experiments_path()
    output_dir = resolve_project_path(args.output_dir) if args.output_dir else exp_dir
    ledger_path = (
        resolve_project_path(args.ledger_jsonl)
        if args.ledger_jsonl
        else output_dir / "candidate_observation_ledger.jsonl"
    )
    snapshot = build_candidate_observation_snapshot(
        candidates_csv_path=resolve_project_path(args.candidates_csv) if args.candidates_csv else exp_dir / "candidates_top_latest.csv",
        lineage_path=resolve_project_path(args.lineage) if args.lineage else exp_dir / "candidate_lineage_latest.json",
        data_quality_gate_path=resolve_project_path(args.data_quality_gate)
        if args.data_quality_gate
        else exp_dir / "candidate_data_quality_gate_latest.json",
        realistic_backtest_path=resolve_project_path(args.realistic_backtest)
        if args.realistic_backtest
        else exp_dir / "realistic_backtest_latest.json",
    )
    snapshot_path, frozen = freeze_candidate_observation_snapshot(snapshot, output_dir=output_dir)
    ledger = append_candidate_observation_ledger(frozen, ledger_path=ledger_path)
    result = {
        "status": frozen.get("status"),
        "freeze_status": frozen.get("freeze_status"),
        "snapshot_path": snapshot_path,
        "snapshot_date": frozen.get("snapshot_date"),
        "candidate_count": frozen.get("candidate_count"),
        "ledger_path": ledger["ledger_path"],
        "ledger_appended_count": ledger["appended_count"],
        "ledger_skipped_duplicate_count": ledger["skipped_duplicate_count"],
        "blocking_reasons": frozen.get("blocking_reasons", []),
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(
            f"{result['status']} {result['freeze_status']}: snapshot={snapshot_path} "
            f"ledger_appended={result['ledger_appended_count']}"
        )
    return 0 if frozen.get("status") == "frozen" else 1


if __name__ == "__main__":
    raise SystemExit(main())
