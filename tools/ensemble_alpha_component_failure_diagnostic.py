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

from openclaw.services.ensemble_alpha_component_failure_diagnostic_service import (  # noqa: E402
    build_ensemble_alpha_component_failure_diagnostic,
)
from openclaw.services.ensemble_alpha_sleeve_service import (  # noqa: E402
    build_ensemble_alpha_sleeve_fact_chain,
)


DEFAULT_STRATEGIES = ("v4", "v5", "v8", "v9", "combo", "v6", "v7")


def main() -> int:
    parser = argparse.ArgumentParser(description="Research-only component failure diagnostic for rebuilt ensemble alpha.")
    parser.add_argument("--db-path", default="permanent_stock_database.db")
    parser.add_argument("--as-of-date", required=True)
    parser.add_argument("--candidate", default="hard_event_alpha_candidate")
    parser.add_argument("--strategies", default=",".join(DEFAULT_STRATEGIES))
    parser.add_argument("--holding-days", type=int, default=5)
    parser.add_argument("--top-n-per-strategy", type=int, default=20)
    parser.add_argument("--operator-name", default="ensemble_alpha_component_failure_diagnostic")
    parser.add_argument("--output-dir", default="logs/openclaw/ensemble_alpha_component_failure_diagnostic")
    args = parser.parse_args()

    strategies = _csv(args.strategies)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(args.db_path), timeout=30)
    try:
        fact_chain = build_ensemble_alpha_sleeve_fact_chain(
            conn,
            as_of_date=str(args.as_of_date),
            strategies=strategies,
            holding_days=int(args.holding_days),
            top_n_per_strategy=int(args.top_n_per_strategy),
        )
        diagnostic = build_ensemble_alpha_component_failure_diagnostic(
            fact_chain,
            candidate=str(args.candidate),
            horizon=int(args.holding_days),
        )
    finally:
        conn.close()

    payload: dict[str, Any] = {
        "run_version": "ensemble_alpha_component_failure_diagnostic_tool.v1",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "operator_name": str(args.operator_name),
        "research_only": True,
        "db_path": str(args.db_path),
        "source_strategies": strategies,
        "diagnostic": diagnostic,
    }
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"ensemble_alpha_component_failure_diagnostic_{stamp}.json"
    md_path = output_dir / f"ensemble_alpha_component_failure_diagnostic_{stamp}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(_markdown(payload), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "markdown": str(md_path)}, ensure_ascii=False, indent=2))
    return 0


def _csv(raw: str) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def _markdown(payload: dict[str, Any]) -> str:
    diagnostic = payload.get("diagnostic") or {}
    lines = [
        "# Ensemble Alpha Component Failure Diagnostic",
        "",
        f"- research_only: {payload.get('research_only')}",
        f"- as_of_date: {diagnostic.get('as_of_date')}",
        f"- candidate: {diagnostic.get('candidate')}",
        f"- candidate_ic: {diagnostic.get('candidate_ic')}",
        f"- candidate_rank_ic: {diagnostic.get('candidate_rank_ic')}",
        f"- failure_hypotheses: {', '.join(diagnostic.get('failure_hypotheses') or []) or '(none)'}",
        "",
        "## Buckets",
        "",
        f"- top_score_bucket: {diagnostic.get('top_score_bucket')}",
        f"- bottom_score_bucket: {diagnostic.get('bottom_score_bucket')}",
        "",
        "## Components",
        "",
    ]
    for name, review in sorted((diagnostic.get("component_reviews") or {}).items()):
        lines.append(
            f"- {name}: ic={review.get('ic')}, rank_ic={review.get('rank_ic')}, "
            f"top_avg={review.get('top_score_bucket_avg')}, bottom_avg={review.get('bottom_score_bucket_avg')}"
        )
    lines.extend(["", "## Source Strategies", ""])
    for review in diagnostic.get("source_strategy_reviews") or []:
        lines.append(
            f"- {review.get('strategy')}: sample_count={review.get('sample_count')}, "
            f"avg_score={review.get('avg_candidate_score')}, avg_return={review.get('avg_return_pct')}"
        )
    lines.extend(["", "## Hard Boundaries", ""])
    lines.extend(f"- {item}" for item in diagnostic.get("hard_boundaries") or [])
    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
