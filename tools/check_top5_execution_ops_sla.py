#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
import sys

sys.path.insert(0, str(ROOT))

from tools.check_top5_execution_observation_completeness import (  # noqa: E402
    _load_jsonl,
    build_report,
)


ARTIFACT_VERSION = "top5_execution_ops_sla.v1"


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _risk_level(*, stale_count: int, open_count: int, quality_gap_count: int, parse_error_count: int) -> tuple[str, list[str], list[str]]:
    rules: list[str] = []
    actions: list[str] = []
    if parse_error_count > 0:
        rules.append("ledger_parse_errors_present")
        actions.append("Repair ledger parse errors before continuing daily execution operations.")
        return "red", rules, actions
    if quality_gap_count > 0:
        rules.append("execution_quality_gaps_present")
        actions.append("Fix invalid imported execution records before claiming evidence quality.")
    if stale_count > 0:
        rules.append("stale_open_observations_present")
        actions.append("Close historical open observations before treating new Top5 as operationally executable.")
    if open_count > 0:
        rules.append("open_observations_present")
        actions.append("Complete the daily open-observation CSV and run close_top5_execution_day.py.")
    if stale_count > 0 or quality_gap_count > 0:
        return "red", rules, actions
    if open_count > 0:
        return "orange", rules, actions
    return "green", rules, ["Execution operations are closed for the current ledger."]


def build_sla_report(ledger: Path) -> dict[str, Any]:
    report = build_report(_load_jsonl(ledger))
    latest = str(report.get("latest_trade_date_compact") or "")
    open_rows = report.get("open_observations") if isinstance(report.get("open_observations"), list) else []
    stale = [
        row
        for row in open_rows
        if latest and str(row.get("trade_date_compact") or "") and str(row.get("trade_date_compact") or "") < latest
    ]
    current = [
        row
        for row in open_rows
        if latest and str(row.get("trade_date_compact") or "") == latest
    ]
    risk, rules, actions = _risk_level(
        stale_count=len(stale),
        open_count=int(report.get("open_observation_count") or 0),
        quality_gap_count=int(report.get("quality_gap_count") or 0),
        parse_error_count=int(report.get("parse_error_count") or 0),
    )
    return {
        "artifact_version": ARTIFACT_VERSION,
        "created_at": _now_text(),
        "ledger": str(ledger),
        "risk_level": risk,
        "triggered_rules": rules,
        "recommended_actions": actions,
        "latest_trade_date_compact": latest,
        "open_observation_count": int(report.get("open_observation_count") or 0),
        "stale_open_observation_count": len(stale),
        "current_trade_date_open_count": len(current),
        "quality_gap_count": int(report.get("quality_gap_count") or 0),
        "parse_error_count": int(report.get("parse_error_count") or 0),
        "stale_open_observations": stale[:50],
        "current_trade_date_open_observations": current[:50],
    }


def _markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Top5 Execution Operations SLA",
        "",
        f"- Created at: {report.get('created_at')}",
        f"- Risk level: {report.get('risk_level')}",
        f"- Latest trade date: {report.get('latest_trade_date_compact') or 'n/a'}",
        f"- Open observations: {report.get('open_observation_count')}",
        f"- Stale open observations: {report.get('stale_open_observation_count')}",
        f"- Current trade-date open observations: {report.get('current_trade_date_open_count')}",
        "",
        "## Triggered Rules",
    ]
    lines.extend([f"- {item}" for item in (report.get("triggered_rules") or [])] or ["- none"])
    lines.extend(["", "## Recommended Actions"])
    lines.extend([f"- {item}" for item in (report.get("recommended_actions") or [])] or ["- none"])
    stale = report.get("stale_open_observations") or []
    lines.extend(["", "## Stale Open Observations"])
    if not stale:
        lines.append("- none")
    else:
        for row in stale:
            lines.append(
                f"- {row.get('trade_date_compact')} {row.get('competition_run_id')} "
                f"{row.get('ts_code')} {row.get('name')} rank={row.get('rank')} status={row.get('status')}"
            )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Check Top5 execution operations SLA for stale open observations.")
    parser.add_argument("--ledger", default="logs/openclaw/top5_execution_observations.jsonl")
    parser.add_argument("--output-json", default="exports/top5_execution_ops_sla.json")
    parser.add_argument("--output-md", default="exports/top5_execution_ops_sla.md")
    parser.add_argument("--fail-on-stale", action="store_true")
    args = parser.parse_args()

    report = build_sla_report(Path(args.ledger))
    blocking: list[str] = []
    if args.fail_on_stale and int(report.get("stale_open_observation_count") or 0) > 0:
        blocking.append("stale_open_observations_present")
    if int(report.get("parse_error_count") or 0) > 0:
        blocking.append("ledger_parse_errors_present")
    report["blocking_reasons"] = blocking
    report["passed"] = not blocking

    out_json = Path(args.output_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.output_md:
        out_md = Path(args.output_md)
        out_md.parent.mkdir(parents=True, exist_ok=True)
        out_md.write_text(_markdown(report), encoding="utf-8")
    print(json.dumps({"output_json": str(out_json), "risk_level": report["risk_level"], "passed": report["passed"]}, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
