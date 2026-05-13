#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
import sys

sys.path.insert(0, str(ROOT))

from tools.check_top5_execution_observation_completeness import (  # noqa: E402
    CLOSING_STATUSES,
    OPEN_STATUSES,
    _key,
    _load_jsonl,
    build_report,
)


ARTIFACT_VERSION = "top5_execution_evidence_summary.v1"


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _risk_level(report: dict[str, Any], *, min_closure_rate: float) -> tuple[str, list[str], list[str]]:
    total = int(report.get("planned_signal_count") or 0)
    open_count = int(report.get("open_observation_count") or 0)
    quality_gap_count = int(report.get("quality_gap_count") or 0)
    parse_error_count = int(report.get("parse_error_count") or 0)
    closure_rate = float(report.get("closure_rate") or 0.0)
    triggered: list[str] = []
    actions: list[str] = []
    if parse_error_count > 0:
        triggered.append("ledger_parse_errors")
        actions.append("Stop promotion; repair the JSONL ledger before using execution evidence.")
        return "red", triggered, actions
    if total == 0:
        triggered.append("no_planned_execution_evidence")
        actions.append("Do not claim live evidence; verify planned observation recording.")
        return "red", triggered, actions
    if quality_gap_count > 0:
        triggered.append("quality_gaps_present")
        actions.append("Fix missing price, quantity, slippage, or attribution fields.")
    if open_count > 0:
        triggered.append("open_observations_present")
        actions.append("Close each open planned/submitted row as filled, not_filled, stopped, take_profit, or manual_skip.")
    if closure_rate < min_closure_rate:
        triggered.append(f"closure_rate_below_min:{closure_rate:.2%}<{min_closure_rate:.2%}")
        actions.append("Keep AIRIVO_REQUIRE_EXECUTION_CLOSURE=0; execution evidence is not mature enough for a hard gate.")
    if quality_gap_count > 0 or closure_rate < 0.5:
        return "red", triggered, actions
    if open_count > 5 or closure_rate < min_closure_rate:
        return "orange", triggered, actions
    if open_count > 0:
        return "yellow", triggered, actions
    return "green", triggered, ["Execution evidence is closed for the observed Top5 candidates."]


def build_summary(ledger: Path, *, min_closure_rate: float) -> dict[str, Any]:
    rows = _load_jsonl(ledger)
    completeness = build_report(rows)
    valid = [row for row in rows if not row.get("_parse_error")]
    planned_keys = {_key(row) for row in valid if str(row.get("status") or "") in OPEN_STATUSES | CLOSING_STATUSES}
    closing_keys = {_key(row) for row in valid if str(row.get("status") or "") in CLOSING_STATUSES}
    status_counts = Counter(str(row.get("status") or "") for row in valid)
    planned_signal_count = len(planned_keys)
    closed_signal_count = len(closing_keys)
    closure_rate = (closed_signal_count / planned_signal_count) if planned_signal_count else 0.0
    completeness["planned_signal_count"] = planned_signal_count
    completeness["closed_signal_count"] = closed_signal_count
    completeness["closure_rate"] = closure_rate
    risk_level, triggered_rules, recommended_actions = _risk_level(completeness, min_closure_rate=min_closure_rate)
    return {
        "artifact_version": ARTIFACT_VERSION,
        "created_at": _now_text(),
        "ledger": str(ledger),
        "risk_level": risk_level,
        "triggered_rules": triggered_rules,
        "recommended_actions": recommended_actions,
        "min_closure_rate": min_closure_rate,
        "planned_signal_count": planned_signal_count,
        "closed_signal_count": closed_signal_count,
        "closure_rate": round(closure_rate, 6),
        "status_counts": dict(sorted(status_counts.items())),
        "open_observation_count": completeness.get("open_observation_count"),
        "quality_gap_count": completeness.get("quality_gap_count"),
        "parse_error_count": completeness.get("parse_error_count"),
        "latest_trade_date_compact": completeness.get("latest_trade_date_compact"),
        "open_observations": completeness.get("open_observations", [])[:20],
        "quality_gaps": completeness.get("quality_gaps", [])[:20],
    }


def _markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Top5 Execution Evidence Summary",
        "",
        f"- Created at: {summary.get('created_at')}",
        f"- Risk level: {summary.get('risk_level')}",
        f"- Latest trade date: {summary.get('latest_trade_date_compact') or 'n/a'}",
        f"- Planned signals: {summary.get('planned_signal_count')}",
        f"- Closed signals: {summary.get('closed_signal_count')}",
        f"- Closure rate: {float(summary.get('closure_rate') or 0.0):.2%}",
        f"- Open observations: {summary.get('open_observation_count')}",
        f"- Quality gaps: {summary.get('quality_gap_count')}",
        "",
        "## Triggered Rules",
    ]
    triggered = summary.get("triggered_rules") or []
    lines.extend([f"- {item}" for item in triggered] or ["- none"])
    lines.extend(["", "## Recommended Actions"])
    actions = summary.get("recommended_actions") or []
    lines.extend([f"- {item}" for item in actions] or ["- none"])
    lines.extend(["", "## Open Observations"])
    open_rows = summary.get("open_observations") or []
    if not open_rows:
        lines.append("- none")
    else:
        for row in open_rows:
            lines.append(
                f"- {row.get('trade_date_compact')} {row.get('competition_run_id')} "
                f"{row.get('ts_code')} {row.get('name')} rank={row.get('rank')} status={row.get('status')}"
            )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a daily Top5 execution evidence summary with risk level.")
    parser.add_argument("--ledger", default="logs/openclaw/top5_execution_observations.jsonl")
    parser.add_argument("--output-json", default="exports/top5_execution_evidence_summary.json")
    parser.add_argument("--output-md", default="exports/top5_execution_evidence_summary.md")
    parser.add_argument("--min-closure-rate", type=float, default=0.95)
    args = parser.parse_args()

    summary = build_summary(Path(args.ledger), min_closure_rate=float(args.min_closure_rate))
    out_json = Path(args.output_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.output_md:
        out_md = Path(args.output_md)
        out_md.parent.mkdir(parents=True, exist_ok=True)
        out_md.write_text(_markdown(summary), encoding="utf-8")
    print(json.dumps({"output_json": str(out_json), "risk_level": summary["risk_level"], "closure_rate": summary["closure_rate"]}, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
