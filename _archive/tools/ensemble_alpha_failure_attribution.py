#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.ensemble_alpha_failure_attribution_service import (  # noqa: E402
    build_ensemble_alpha_failure_attribution,
)
from openclaw.services.ensemble_alpha_sleeve_service import (  # noqa: E402
    build_ensemble_alpha_sleeve_fact_chain,
)


DEFAULT_STRATEGIES = ("v4", "v5", "v8", "v9", "combo", "v6", "v7")


def main() -> int:
    parser = argparse.ArgumentParser(description="Explain failed windows for rebuilt ensemble alpha candidates.")
    parser.add_argument("--db-path", default="permanent_stock_database.db")
    parser.add_argument("--as-of-dates", required=True)
    parser.add_argument("--candidate", default="hard_event_alpha_candidate")
    parser.add_argument("--strategies", default=",".join(DEFAULT_STRATEGIES))
    parser.add_argument("--holding-days", type=int, default=5)
    parser.add_argument("--top-n-per-strategy", type=int, default=20)
    parser.add_argument("--operator-name", default="ensemble_alpha_failure_attribution")
    parser.add_argument("--output-dir", default="logs/openclaw/ensemble_alpha_failure_attribution")
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
        attribution = build_ensemble_alpha_failure_attribution(
            conn,
            fact_chains,
            candidate=str(args.candidate),
            horizon=int(args.holding_days),
        )
    finally:
        conn.close()

    payload: dict[str, Any] = {
        "run_version": "ensemble_alpha_failure_attribution_tool.v1",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "operator_name": str(args.operator_name),
        "research_only": True,
        "db_path": str(args.db_path),
        "as_of_dates": as_of_dates,
        "source_strategies": strategies,
        "attribution": attribution,
    }
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"ensemble_alpha_failure_attribution_{stamp}.json"
    md_path = output_dir / f"ensemble_alpha_failure_attribution_{stamp}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(_markdown(payload), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "markdown": str(md_path)}, ensure_ascii=False, indent=2))
    return 0


def _csv(raw: str) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def _markdown(payload: dict[str, Any]) -> str:
    attribution = payload.get("attribution") or {}
    focus = attribution.get("focus_failed_window") or ""
    focus_diag = next(
        (item for item in attribution.get("window_diagnostics") or [] if item.get("as_of_date") == focus),
        {},
    )
    lines = [
        "# Ensemble Alpha Failure Attribution",
        "",
        f"- research_only: {payload.get('research_only')}",
        f"- candidate: {attribution.get('candidate')}",
        f"- failed_windows: {', '.join(attribution.get('failed_windows') or []) or '(none)'}",
        f"- successful_windows: {', '.join(attribution.get('successful_windows') or []) or '(none)'}",
        f"- focus_failed_window: {focus or '(none)'}",
        f"- inferred_failure_drivers: {', '.join(attribution.get('inferred_failure_drivers') or []) or '(none)'}",
        "",
        "## Focus Window",
        "",
        f"- candidate_ic: {focus_diag.get('candidate_ic')}",
        f"- candidate_rank_ic: {focus_diag.get('candidate_rank_ic')}",
        f"- top_score_bucket_return: {focus_diag.get('top_score_bucket_return')}",
        f"- bottom_score_bucket_return: {focus_diag.get('bottom_score_bucket_return')}",
        f"- market_regime: {(focus_diag.get('market_regime') or {}).get('label')}",
        "",
        "## Risk-Off Exhaustion Profile",
        "",
    ]
    profile = focus_diag.get("risk_off_exhaustion_profile") if isinstance(focus_diag.get("risk_off_exhaustion_profile"), dict) else {}
    lines.extend(
        [
            f"- active: {profile.get('active')}",
            f"- exhausted_signal_count: {profile.get('exhausted_signal_count')}",
            f"- exhausted_negative_count: {profile.get('exhausted_negative_count')}",
            f"- exhausted_negative_ratio: {profile.get('exhausted_negative_ratio')}",
            f"- exhausted_avg_forward_return_pct: {profile.get('exhausted_avg_forward_return_pct')}",
            f"- non_exhausted_avg_forward_return_pct: {profile.get('non_exhausted_avg_forward_return_pct')}",
            f"- predeclared_structure: {profile.get('predeclared_structure')}",
            "",
            "### Exhausted Negative Examples",
            "",
        ]
    )
    for item in profile.get("exhausted_top_negative_examples") or []:
        lines.append(
            f"- {item.get('ts_code')} {item.get('strategy')}: "
            f"score={item.get('candidate_score')}, return={item.get('forward_return_pct')}"
        )
    lines.extend(
        [
            "",
        "## Hard Boundaries",
        "",
        ]
    )
    lines.extend(f"- {item}" for item in attribution.get("hard_boundaries") or [])
    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
