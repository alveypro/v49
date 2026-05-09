#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.ensemble_alpha_predeclared_gate_walk_forward_service import (  # noqa: E402
    build_ensemble_alpha_predeclared_gate_walk_forward,
)
from openclaw.services.ensemble_alpha_sleeve_service import (  # noqa: E402
    build_ensemble_alpha_sleeve_fact_chain,
)


DEFAULT_STRATEGIES = ("v4", "v5", "v8", "v9", "combo", "v6", "v7")


def main() -> int:
    parser = argparse.ArgumentParser(description="Research-only predeclared gate walk-forward for rebuilt ensemble alpha.")
    parser.add_argument("--db-path", default="permanent_stock_database.db")
    parser.add_argument("--as-of-dates", required=True)
    parser.add_argument("--candidate", default="hard_event_alpha_candidate")
    parser.add_argument("--gate-name", default="risk_off_gate")
    parser.add_argument("--strategies", default=",".join(DEFAULT_STRATEGIES))
    parser.add_argument("--holding-days", type=int, default=5)
    parser.add_argument("--top-n-per-strategy", type=int, default=20)
    parser.add_argument("--min-sample-count", type=int, default=120)
    parser.add_argument("--min-retained-windows", type=int, default=4)
    parser.add_argument("--min-sample-retention", type=float, default=0.5)
    parser.add_argument("--calibration-as-of-dates", default="")
    parser.add_argument("--operator-name", default="ensemble_alpha_predeclared_gate_walk_forward")
    parser.add_argument("--output-dir", default="logs/openclaw/ensemble_alpha_predeclared_gate_walk_forward")
    args = parser.parse_args()

    as_of_dates = _csv(args.as_of_dates)
    calibration_as_of_dates = _csv(args.calibration_as_of_dates)
    strategies = _csv(args.strategies)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(args.db_path), timeout=30)
    try:
        fact_chains = [
            build_ensemble_alpha_sleeve_fact_chain(
                conn,
                as_of_date=as_of,
                strategies=strategies,
                holding_days=int(args.holding_days),
                top_n_per_strategy=int(args.top_n_per_strategy),
            )
            for as_of in as_of_dates
        ]
        walk_forward = build_ensemble_alpha_predeclared_gate_walk_forward(
            conn,
            fact_chains,
            candidate=str(args.candidate),
            horizon=int(args.holding_days),
            gate_name=str(args.gate_name),
            min_sample_count=int(args.min_sample_count),
            min_retained_windows=int(args.min_retained_windows),
            min_sample_retention=float(args.min_sample_retention),
            calibration_as_of_dates=calibration_as_of_dates,
        )
    finally:
        conn.close()

    payload: dict[str, Any] = {
        "run_version": "ensemble_alpha_predeclared_gate_walk_forward_tool.v1",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "operator_name": str(args.operator_name),
        "research_only": True,
        "db_path": str(args.db_path),
        "as_of_dates": as_of_dates,
        "calibration_as_of_dates": calibration_as_of_dates,
        "source_strategies": strategies,
        "walk_forward": walk_forward,
    }
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"ensemble_alpha_predeclared_gate_walk_forward_{stamp}.json"
    md_path = output_dir / f"ensemble_alpha_predeclared_gate_walk_forward_{stamp}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(_markdown(payload), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "markdown": str(md_path)}, ensure_ascii=False, indent=2))
    return 0


def _csv(raw: str) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def _markdown(payload: dict[str, Any]) -> str:
    walk_forward = payload.get("walk_forward") or {}
    validation = walk_forward.get("validation_review") or {}
    lines = [
        "# Ensemble Alpha Predeclared Gate Walk-Forward",
        "",
        f"- research_only: {payload.get('research_only')}",
        f"- candidate: {walk_forward.get('candidate')}",
        f"- gate_name: {(walk_forward.get('predeclared_gate') or {}).get('gate_name')}",
        f"- passed_predeclared_walk_forward_gate: {walk_forward.get('passed_predeclared_walk_forward_gate')}",
        f"- promotion_status: {walk_forward.get('promotion_status')}",
        f"- blocking_reasons: {', '.join(walk_forward.get('blocking_reasons') or []) or '(none)'}",
        "",
        "## Validation",
        "",
        f"- raw_sample_count: {validation.get('raw_sample_count')}",
        f"- sample_count: {validation.get('sample_count')}",
        f"- sample_retention: {validation.get('sample_retention')}",
        f"- retained_window_count: {validation.get('retained_window_count')}",
        f"- excluded_window_count: {validation.get('excluded_window_count')}",
        f"- positive_retained_window_count: {validation.get('positive_retained_window_count')}",
        f"- ic: {validation.get('ic')}",
        f"- rank_ic: {validation.get('rank_ic')}",
        "",
        "## Retained Windows",
        "",
    ]
    for review in validation.get("retained_window_reviews") or []:
        lines.append(
            f"- {review.get('as_of_date')}: regime={review.get('market_regime_label')}, "
            f"sample_count={review.get('sample_count')}, ic={review.get('ic')}, rank_ic={review.get('rank_ic')}"
        )
    lines.extend(["", "## Excluded Windows", ""])
    for review in validation.get("excluded_window_reviews") or []:
        lines.append(
            f"- {review.get('as_of_date')}: regime={review.get('market_regime_label')}, "
            f"sample_count={review.get('sample_count')}, avg_return_pct={review.get('avg_return_pct')}"
        )
    lines.extend(["", "## Hard Boundaries", ""])
    lines.extend(f"- {item}" for item in walk_forward.get("hard_boundaries") or [])
    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
