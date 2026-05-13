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

from openclaw.services.ensemble_alpha_gate_contrast_service import (  # noqa: E402
    build_ensemble_alpha_gate_contrast,
)
from openclaw.services.ensemble_alpha_sleeve_service import (  # noqa: E402
    build_ensemble_alpha_sleeve_fact_chain,
)


DEFAULT_STRATEGIES = ("v4", "v5", "v8", "v9", "combo", "v6", "v7")


def main() -> int:
    parser = argparse.ArgumentParser(description="Research-only gate contrast for rebuilt ensemble alpha candidates.")
    parser.add_argument("--db-path", default="permanent_stock_database.db")
    parser.add_argument("--as-of-dates", required=True)
    parser.add_argument("--candidate", default="hard_event_alpha_candidate")
    parser.add_argument("--strategies", default=",".join(DEFAULT_STRATEGIES))
    parser.add_argument("--blocked-sources", default="v6,v8,v9")
    parser.add_argument("--holding-days", type=int, default=5)
    parser.add_argument("--top-n-per-strategy", type=int, default=20)
    parser.add_argument("--operator-name", default="ensemble_alpha_gate_contrast")
    parser.add_argument("--output-dir", default="logs/openclaw/ensemble_alpha_gate_contrast")
    args = parser.parse_args()

    as_of_dates = _csv(args.as_of_dates)
    strategies = _csv(args.strategies)
    blocked_sources = _csv(args.blocked_sources)
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
        contrast = build_ensemble_alpha_gate_contrast(
            conn,
            fact_chains,
            candidate=str(args.candidate),
            horizon=int(args.holding_days),
            blocked_sources=blocked_sources,
        )
    finally:
        conn.close()

    payload: dict[str, Any] = {
        "run_version": "ensemble_alpha_gate_contrast_tool.v1",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "operator_name": str(args.operator_name),
        "research_only": True,
        "db_path": str(args.db_path),
        "as_of_dates": as_of_dates,
        "source_strategies": strategies,
        "contrast": contrast,
    }
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"ensemble_alpha_gate_contrast_{stamp}.json"
    md_path = output_dir / f"ensemble_alpha_gate_contrast_{stamp}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(_markdown(payload), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "markdown": str(md_path)}, ensure_ascii=False, indent=2))
    return 0


def _csv(raw: str) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def _markdown(payload: dict[str, Any]) -> str:
    contrast = payload.get("contrast") or {}
    lines = [
        "# Ensemble Alpha Gate Contrast",
        "",
        f"- research_only: {payload.get('research_only')}",
        f"- candidate: {contrast.get('candidate')}",
        f"- best_research_scenario: {contrast.get('best_research_scenario')}",
        f"- passed_research_gate: {contrast.get('passed_research_gate')}",
        f"- blocking_reasons: {', '.join(contrast.get('blocking_reasons') or []) or '(none)'}",
        "",
        "## Scenarios",
        "",
    ]
    for name, review in sorted((contrast.get("scenario_reviews") or {}).items()):
        lines.extend(
            [
                f"### {name}",
                "",
                f"- sample_count: {review.get('sample_count')}",
                f"- sample_retention: {review.get('sample_retention')}",
                f"- window_count: {review.get('window_count')}",
                f"- positive_window_count: {review.get('positive_window_count')}",
                f"- ic: {review.get('ic')}",
                f"- rank_ic: {review.get('rank_ic')}",
                f"- passed_research_gate: {review.get('passed_research_gate')}",
                "",
            ]
        )
    lines.extend(["## Hard Boundaries", ""])
    lines.extend(f"- {item}" for item in contrast.get("hard_boundaries") or [])
    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
