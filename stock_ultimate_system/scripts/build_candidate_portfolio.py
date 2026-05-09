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

from src.candidate_quality.portfolio import (
    CandidatePortfolioConfig,
    build_candidate_portfolio,
    build_candidate_portfolio_quality,
    build_portfolio_capacity_report,
    write_candidate_portfolio_payload,
)
from src.utils.project_paths import resolve_experiments_path, resolve_project_path


def _load_settings(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        return {}
    return yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Build observation candidate portfolio and exposure report.")
    parser.add_argument("--config", default="config/settings.yaml")
    parser.add_argument("--exp-dir", default=None)
    parser.add_argument("--snapshot", default=None)
    parser.add_argument("--candidates-csv", default=None)
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--table", default=None)
    parser.add_argument("--max-single-weight", type=float, default=0.35)
    parser.add_argument("--target-max-industry-weight", type=float, default=0.50)
    parser.add_argument("--hard-max-industry-weight", type=float, default=0.65)
    parser.add_argument("--high-correlation-threshold", type=float, default=0.85)
    parser.add_argument("--correlation-lookback-trade-days", type=int, default=60)
    parser.add_argument("--min-correlation-points", type=int, default=20)
    parser.add_argument("--portfolio-notional", type=float, default=1_000_000.0)
    parser.add_argument("--min-amount", type=float, default=300_000.0)
    parser.add_argument("--capacity-warn-participation-rate", type=float, default=0.05)
    parser.add_argument("--capacity-block-participation-rate", type=float, default=0.10)
    parser.add_argument("--impact-cost-coefficient", type=float, default=0.001)
    parser.add_argument("--transaction-cost-report", default=None)
    parser.add_argument("--realistic-backtest", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    settings = _load_settings(resolve_project_path(args.config))
    data_cfg = settings.get("data", {}) or {}
    exp_dir = resolve_project_path(args.exp_dir) if args.exp_dir else resolve_experiments_path()
    snapshot = resolve_project_path(args.snapshot) if args.snapshot else exp_dir / "candidate_observation_snapshot_latest.json"
    candidates_csv = resolve_project_path(args.candidates_csv) if args.candidates_csv else exp_dir / "candidates_top_latest.csv"
    db_path = resolve_project_path(args.db_path or data_cfg.get("sqlite_db_path", "../_archive/permanent_stock_database.db"))
    table = str(args.table or data_cfg.get("sqlite_table", "daily_trading_data") or "daily_trading_data")
    config = CandidatePortfolioConfig(
        max_single_weight=args.max_single_weight,
        target_max_industry_weight=args.target_max_industry_weight,
        hard_max_industry_weight=args.hard_max_industry_weight,
        high_correlation_threshold=args.high_correlation_threshold,
        correlation_lookback_trade_days=args.correlation_lookback_trade_days,
        min_correlation_points=args.min_correlation_points,
        portfolio_notional=max(float(args.portfolio_notional), 0.0),
        min_amount=max(float(args.min_amount), 0.0),
        capacity_warn_participation_rate=max(float(args.capacity_warn_participation_rate), 0.0),
        capacity_block_participation_rate=max(float(args.capacity_block_participation_rate), 0.0),
        impact_cost_coefficient=max(float(args.impact_cost_coefficient), 0.0),
    )
    transaction_cost_path = (
        resolve_project_path(args.transaction_cost_report)
        if args.transaction_cost_report
        else exp_dir / "transaction_cost_breakdown_latest.json"
    )
    realistic_backtest_path = (
        resolve_project_path(args.realistic_backtest)
        if args.realistic_backtest
        else exp_dir / "realistic_backtest_latest.json"
    )

    portfolio, exposure = build_candidate_portfolio(
        snapshot_path=snapshot,
        candidates_csv_path=candidates_csv,
        sqlite_db_path=db_path,
        sqlite_table=table,
        config=config,
    )
    capacity = build_portfolio_capacity_report(
        portfolio=portfolio,
        sqlite_db_path=db_path,
        sqlite_table=table,
        config=config,
    )
    transaction_cost = (
        json.loads(transaction_cost_path.read_text(encoding="utf-8"))
        if transaction_cost_path.exists()
        else {"status": "missing"}
    )
    realistic_backtest = (
        json.loads(realistic_backtest_path.read_text(encoding="utf-8"))
        if realistic_backtest_path.exists()
        else {"status": "missing"}
    )
    quality = build_candidate_portfolio_quality(
        portfolio=portfolio,
        exposure_report=exposure,
        capacity_report=capacity,
        transaction_cost_report=transaction_cost,
        realistic_backtest=realistic_backtest,
        config=config,
    )
    portfolio_path = write_candidate_portfolio_payload(portfolio, output_path=exp_dir / "candidate_portfolio_latest.json")
    exposure_path = write_candidate_portfolio_payload(exposure, output_path=exp_dir / "portfolio_exposure_report_latest.json")
    capacity_path = write_candidate_portfolio_payload(capacity, output_path=exp_dir / "portfolio_capacity_report_latest.json")
    quality_path = write_candidate_portfolio_payload(quality, output_path=exp_dir / "candidate_portfolio_quality_latest.json")
    payload = {
        "status": portfolio.get("status"),
        "portfolio_path": portfolio_path,
        "exposure_report_path": exposure_path,
        "capacity_report_path": capacity_path,
        "quality_report_path": quality_path,
        "candidate_count": (portfolio.get("summary", {}) or {}).get("candidate_count", 0),
        "weight_sum": (portfolio.get("summary", {}) or {}).get("weight_sum", 0.0),
        "top_industry": (portfolio.get("summary", {}) or {}).get("top_industry", ""),
        "top_industry_weight": (portfolio.get("summary", {}) or {}).get("top_industry_weight", 0.0),
        "capacity_status": capacity.get("status"),
        "quality_status": quality.get("status"),
        "quality_score": quality.get("quality_score"),
        "review_reasons": portfolio.get("review_reasons", []),
        "blocking_reasons": portfolio.get("blocking_reasons", []),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            f"{payload['status']}: candidates={payload['candidate_count']} "
            f"weight_sum={payload['weight_sum']} top_industry_weight={payload['top_industry_weight']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
