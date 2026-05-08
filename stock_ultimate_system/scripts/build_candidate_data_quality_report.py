#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.candidate_quality.data_quality import (
    build_candidate_data_quality_gate,
    build_data_quality_report,
    write_candidate_data_quality_gate,
    write_data_quality_artifacts,
)
from src.utils.project_paths import resolve_experiments_path, resolve_project_path


def _load_settings(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        return {}
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _resolve_db_path(settings: dict[str, Any], explicit_db_path: str | None) -> Path:
    if explicit_db_path:
        return resolve_project_path(explicit_db_path)
    data_cfg = settings.get("data", {}) or {}
    configured = str(data_cfg.get("sqlite_db_path", "") or "").strip()
    return resolve_project_path(configured) if configured else resolve_project_path("data/permanent_stock_database.db")


def _resolve_table(settings: dict[str, Any], explicit_table: str | None) -> str:
    if explicit_table:
        return explicit_table
    data_cfg = settings.get("data", {}) or {}
    return str(data_cfg.get("sqlite_table", "daily_trading_data") or "daily_trading_data")


def _read_candidate_codes(candidate_csv: Path | None, output_dir: Path) -> list[str]:
    candidates = candidate_csv
    if candidates is None:
        formal = output_dir / "candidates_top_latest.csv"
        interim = output_dir / "candidates_top_interim_latest.csv"
        candidates = formal if formal.exists() else interim
    if candidates is None or not candidates.exists():
        return []
    frame = pd.read_csv(candidates)
    if "ts_code" not in frame.columns:
        return []
    return [str(code).strip() for code in frame["ts_code"].dropna().tolist() if str(code).strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Build candidate data quality report and candidate-level gate.")
    parser.add_argument("--config", default="config/settings.yaml")
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--table", default=None)
    parser.add_argument("--lookback-trade-days", type=int, default=60)
    parser.add_argument("--expected-latest-trade-date", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--candidate-csv", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    settings = _load_settings(resolve_project_path(args.config))
    output_dir = resolve_project_path(args.output_dir) if args.output_dir else resolve_experiments_path()
    db_path = _resolve_db_path(settings, args.db_path)
    table = _resolve_table(settings, args.table)

    report = build_data_quality_report(
        db_path=db_path,
        table=table,
        lookback_trade_days=args.lookback_trade_days,
        expected_latest_trade_date=args.expected_latest_trade_date,
    )
    report_paths = write_data_quality_artifacts(report, output_dir=output_dir)

    candidate_csv = resolve_project_path(args.candidate_csv) if args.candidate_csv else None
    candidate_codes = _read_candidate_codes(candidate_csv, output_dir)
    gate = build_candidate_data_quality_gate(report=report, candidate_codes=candidate_codes)
    gate_path = write_candidate_data_quality_gate(gate, output_dir=output_dir)

    result = {
        "status": report.get("status"),
        "report_path": report_paths["report_path"],
        "stock_csv_path": report_paths["stock_csv_path"],
        "gate_status": gate.get("status"),
        "gate_path": gate_path,
        "candidate_count": gate.get("candidate_count", 0),
        "blocked_count": gate.get("blocked_count", 0),
        "review_count": gate.get("review_count", 0),
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(
            f"report={result['status']} gate={result['gate_status']} "
            f"candidates={result['candidate_count']} blocked={result['blocked_count']} "
            f"path={result['report_path']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
