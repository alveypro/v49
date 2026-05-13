#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.ensemble_alpha_rebuild_lab_service import (  # noqa: E402
    build_ensemble_alpha_rebuild_multi_window_lab,
)
from openclaw.services.ensemble_alpha_sleeve_service import (  # noqa: E402
    build_ensemble_alpha_sleeve_fact_chain,
)


DEFAULT_STRATEGIES = ("v4", "v5", "v8", "v9", "combo", "v6", "v7")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run research-only multi-window alpha rebuild lab for ensemble_core candidates."
    )
    parser.add_argument("--db-path", default="permanent_stock_database.db")
    parser.add_argument("--as-of-dates", required=True, help="Comma-separated PIT as-of dates.")
    parser.add_argument("--strategies", default=",".join(DEFAULT_STRATEGIES))
    parser.add_argument("--holding-days", type=int, default=5)
    parser.add_argument("--top-n-per-strategy", type=int, default=20)
    parser.add_argument("--min-samples", type=int, default=30)
    parser.add_argument("--min-research-windows", type=int, default=5)
    parser.add_argument("--operator-name", default="ensemble_alpha_rebuild_multi_window_lab")
    parser.add_argument("--output-dir", default="logs/openclaw/ensemble_alpha_rebuild_multi_window_lab")
    args = parser.parse_args()

    as_of_dates = _csv(args.as_of_dates)
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
    finally:
        conn.close()

    lab = build_ensemble_alpha_rebuild_multi_window_lab(
        fact_chains,
        min_samples=int(args.min_samples),
        min_research_windows=int(args.min_research_windows),
    )
    payload: dict[str, Any] = {
        "run_version": "ensemble_alpha_rebuild_multi_window_lab_tool.v1",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "operator_name": str(args.operator_name),
        "research_only": True,
        "db_path": str(args.db_path),
        "as_of_dates": as_of_dates,
        "source_strategies": strategies,
        "fact_chain_summaries": [_fact_chain_summary(chain) for chain in fact_chains],
        "multi_window_lab": lab,
        "hard_boundaries": [
            "do_not_promote_ensemble_core_from_alpha_rebuild_lab_only",
            "do_not_feed_rebuilt_candidates_into_shadow_portfolio_without_sleeve_policy_acceptance",
            "do_not_package_single_window_positive_ic_as_tradeable_alpha",
        ],
    }
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"ensemble_alpha_rebuild_multi_window_lab_{stamp}.json"
    md_path = output_dir / f"ensemble_alpha_rebuild_multi_window_lab_{stamp}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(_markdown(payload), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "markdown": str(md_path)}, ensure_ascii=False, indent=2))
    return 0


def _csv(raw: str) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def _fact_chain_summary(chain: dict[str, Any]) -> dict[str, Any]:
    return {
        "as_of_date": chain.get("as_of_date"),
        "run_count": chain.get("run_count"),
        "signal_count": chain.get("signal_count"),
        "replayable_signal_count": chain.get("replayable_signal_count"),
        "blocking_reasons": chain.get("blocking_reasons") or [],
        "sleeve_use_policy": chain.get("sleeve_use_policy") or {},
    }


def _markdown(payload: dict[str, Any]) -> str:
    lab = payload.get("multi_window_lab") or {}
    lines = [
        "# Ensemble Core Alpha Rebuild Multi-Window Lab",
        "",
        f"- research_only: {payload.get('research_only')}",
        f"- as_of_dates: {', '.join(payload.get('as_of_dates') or [])}",
        f"- research_window_count: {lab.get('research_window_count')}",
        f"- candidate_alpha_sleeves: {', '.join(lab.get('candidate_alpha_sleeves') or []) or '(none)'}",
        f"- blocking_reasons: {', '.join(lab.get('blocking_reasons') or []) or '(none)'}",
        "",
        "## Candidate Reviews",
        "",
    ]
    for name, review in sorted((lab.get("candidate_reviews") or {}).items()):
        multi = review.get("multi_horizon_attribution") or {}
        h5 = (multi.get("horizons") or {}).get("5") or {}
        lines.extend(
            [
                f"### {name}",
                "",
                f"- recommended_use: {review.get('recommended_use')}",
                f"- active_signal_count: {review.get('active_signal_count')}",
                f"- window_positive_count: {review.get('window_positive_count')}",
                f"- 5d_ic: {h5.get('ic')}",
                f"- 5d_rank_ic: {h5.get('rank_ic')}",
                f"- blocking_reasons: {', '.join(review.get('blocking_reasons') or []) or '(none)'}",
                "",
            ]
        )
    lines.extend(["## Hard Boundaries", ""])
    lines.extend(f"- {item}" for item in payload.get("hard_boundaries") or [])
    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
