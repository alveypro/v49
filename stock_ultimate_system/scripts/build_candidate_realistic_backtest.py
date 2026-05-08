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

from src.candidate_quality.realistic_backtest import (
    RealisticBacktestConfig,
    build_capacity_constraint_report,
    build_realistic_backtest,
    build_transaction_cost_breakdown,
    write_capacity_constraint_report,
    write_realistic_backtest,
    write_transaction_cost_breakdown,
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Build realistic candidate backtest with A-share trading constraints.")
    parser.add_argument("--config", default="config/settings.yaml")
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--table", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--lineage", default=None)
    parser.add_argument("--hold-trade-days", type=int, default=5)
    parser.add_argument("--portfolio-notional", type=float, default=1_000_000.0)
    parser.add_argument("--commission-rate", type=float, default=0.0003)
    parser.add_argument("--slippage-rate", type=float, default=0.0005)
    parser.add_argument("--stamp-tax-rate", type=float, default=0.001)
    parser.add_argument("--impact-cost-coefficient", type=float, default=0.001)
    parser.add_argument("--capacity-warn-participation-rate", type=float, default=0.05)
    parser.add_argument("--capacity-block-participation-rate", type=float, default=0.10)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    settings = _load_settings(resolve_project_path(args.config))
    data_cfg = settings.get("data", {}) or {}
    table = str(args.table or data_cfg.get("sqlite_table", "daily_trading_data") or "daily_trading_data")
    output_dir = resolve_project_path(args.output_dir) if args.output_dir else resolve_experiments_path()
    lineage_path = resolve_project_path(args.lineage) if args.lineage else output_dir / "candidate_lineage_latest.json"
    lineage = json.loads(lineage_path.read_text(encoding="utf-8"))
    payload = build_realistic_backtest(
        db_path=_resolve_db_path(settings, args.db_path),
        table=table,
        lineage=lineage,
        config=RealisticBacktestConfig(
            hold_trade_days=max(int(args.hold_trade_days), 1),
            portfolio_notional=max(float(args.portfolio_notional), 0.0),
            commission_rate=max(float(args.commission_rate), 0.0),
            slippage_rate=max(float(args.slippage_rate), 0.0),
            stamp_tax_rate=max(float(args.stamp_tax_rate), 0.0),
            impact_cost_coefficient=max(float(args.impact_cost_coefficient), 0.0),
            capacity_warn_participation_rate=max(float(args.capacity_warn_participation_rate), 0.0),
            capacity_block_participation_rate=max(float(args.capacity_block_participation_rate), 0.0),
        ),
    )
    path = write_realistic_backtest(payload, output_dir=output_dir)
    cost_payload = build_transaction_cost_breakdown(payload)
    capacity_payload = build_capacity_constraint_report(payload)
    cost_path = write_transaction_cost_breakdown(cost_payload, output_dir=output_dir)
    capacity_path = write_capacity_constraint_report(capacity_payload, output_dir=output_dir)
    result = {
        "status": payload.get("status"),
        "path": path,
        "transaction_cost_breakdown_path": cost_path,
        "capacity_constraint_report_path": capacity_path,
        "transaction_cost_status": cost_payload.get("status"),
        "capacity_status": capacity_payload.get("status"),
        "candidate_count": (payload.get("summary", {}) or {}).get("candidate_count", 0),
        "passed_count": (payload.get("summary", {}) or {}).get("passed_count", 0),
        "review_count": (payload.get("summary", {}) or {}).get("review_count", 0),
        "blocked_count": (payload.get("summary", {}) or {}).get("blocked_count", 0),
        "blocking_reasons": payload.get("blocking_reasons", []),
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(
            f"{result['status']}: candidates={result['candidate_count']} "
            f"passed={result['passed_count']} blocked={result['blocked_count']} path={path}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
