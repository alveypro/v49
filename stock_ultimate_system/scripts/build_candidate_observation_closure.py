#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.candidate_quality.observation import (
    build_candidate_failure_attribution,
    build_candidate_observation_result,
    build_candidate_quality_20_sample_report,
    write_candidate_quality_payload,
)
from src.utils.project_paths import resolve_experiments_path, resolve_project_path


def _load_settings(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        return {}
    return yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}


def _first_benchmark(settings: dict[str, Any]) -> str:
    data = settings.get("data", {}) or {}
    raw = data.get("benchmark_indices", ["000001.SH"])
    if isinstance(raw, str):
        return raw.strip() or "000001.SH"
    if isinstance(raw, list):
        for item in raw:
            code = str(item or "").strip()
            if code:
                return code
    return "000001.SH"


def main() -> int:
    parser = argparse.ArgumentParser(description="Build candidate observation result, failure attribution, and 20-sample report.")
    parser.add_argument("--config", default="config/settings.yaml")
    parser.add_argument("--exp-dir", default=None)
    parser.add_argument("--snapshot", default=None)
    parser.add_argument("--ledger-jsonl", default=None)
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--table", default=None)
    parser.add_argument("--benchmark", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    settings = _load_settings(resolve_project_path(args.config))
    data_cfg = settings.get("data", {}) or {}
    exp_dir = resolve_project_path(args.exp_dir) if args.exp_dir else resolve_experiments_path()
    db_path = resolve_project_path(args.db_path or data_cfg.get("sqlite_db_path", "../_archive/permanent_stock_database.db"))
    table = str(args.table or data_cfg.get("sqlite_table", "daily_trading_data") or "daily_trading_data")
    snapshot = resolve_project_path(args.snapshot) if args.snapshot else exp_dir / "candidate_observation_snapshot_latest.json"
    ledger = resolve_project_path(args.ledger_jsonl) if args.ledger_jsonl else exp_dir / "candidate_observation_ledger.jsonl"
    benchmark = str(args.benchmark or _first_benchmark(settings))

    result = build_candidate_observation_result(
        snapshot_path=snapshot,
        ledger_path=ledger,
        sqlite_db_path=db_path,
        sqlite_table=table,
        benchmark_ts_code=benchmark,
    )
    attribution = build_candidate_failure_attribution(result)
    report = build_candidate_quality_20_sample_report(result)

    result_path = write_candidate_quality_payload(result, output_path=exp_dir / "candidate_observation_result_latest.json")
    attribution_path = write_candidate_quality_payload(attribution, output_path=exp_dir / "candidate_failure_attribution_latest.json")
    report_path = write_candidate_quality_payload(report, output_path=exp_dir / "candidate_quality_20_sample_report.json")
    payload = {
        "status": result.get("status"),
        "result_path": result_path,
        "failure_attribution_path": attribution_path,
        "sample_report_path": report_path,
        "completed_count": (result.get("summary", {}) or {}).get("completed_count", 0),
        "pending_count": (result.get("summary", {}) or {}).get("pending_count", 0),
        "blocked_count": (result.get("summary", {}) or {}).get("blocked_count", 0),
        "sample_report_status": report.get("status"),
        "sample_count": report.get("sample_count", 0),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            f"{payload['status']}: completed={payload['completed_count']} "
            f"pending={payload['pending_count']} blocked={payload['blocked_count']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
