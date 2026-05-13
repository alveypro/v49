#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path
from typing import Any


ARTIFACT_VERSION = "top5_execution_observation_completeness.v1"
DEFAULT_LEDGER = Path("logs/openclaw/top5_execution_observations.jsonl")
OPEN_STATUSES = {"planned", "submitted"}
ENTRY_STATUSES = {"filled", "partial_fill"}
EXIT_STATUSES = {"stopped", "take_profit"}
NON_FILL_STATUSES = {"not_filled", "cancelled", "manual_skip"}
CLOSING_STATUSES = ENTRY_STATUSES | EXIT_STATUSES | NON_FILL_STATUSES


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except Exception as exc:
                rows.append({"_line_no": line_no, "_parse_error": str(exc)})
                continue
            if isinstance(payload, dict):
                payload["_line_no"] = line_no
                rows.append(payload)
    return rows


def _key(row: dict[str, Any]) -> tuple[str, str]:
    return (str(row.get("competition_run_id") or ""), str(row.get("ts_code") or ""))


def _compact(row: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "_line_no",
        "recorded_at",
        "trade_date_compact",
        "competition_run_id",
        "ts_code",
        "name",
        "rank",
        "planned_entry_price",
        "planned_exit_price",
        "planned_stop_price",
        "status",
        "actual_entry_price",
        "actual_exit_price",
        "filled_qty",
        "slippage_bp",
        "miss_reason",
        "failure_attribution",
    )
    return {field: row.get(field) for field in fields if row.get(field) not in (None, "")}


def _quality_gaps(row: dict[str, Any]) -> list[str]:
    status = str(row.get("status") or "")
    gaps: list[str] = []
    if status in ENTRY_STATUSES:
        if row.get("actual_entry_price") in (None, ""):
            gaps.append("actual_entry_price_missing")
        if row.get("filled_qty") in (None, ""):
            gaps.append("filled_qty_missing")
        if row.get("slippage_bp") in (None, ""):
            gaps.append("slippage_bp_missing")
    if status in EXIT_STATUSES:
        if row.get("actual_exit_price") in (None, ""):
            gaps.append("actual_exit_price_missing")
        if status == "stopped" and row.get("stop_triggered") is not True:
            gaps.append("stop_trigger_not_marked")
        if status == "take_profit" and row.get("take_profit_triggered") is not True:
            gaps.append("take_profit_trigger_not_marked")
    if status in NON_FILL_STATUSES:
        if not str(row.get("miss_reason") or row.get("failure_attribution") or "").strip():
            gaps.append("miss_reason_or_failure_attribution_missing")
    return gaps


def build_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    parse_errors = [_compact(row) | {"parse_error": row.get("_parse_error")} for row in rows if row.get("_parse_error")]
    valid = [row for row in rows if not row.get("_parse_error")]
    closing_keys = {_key(row) for row in valid if str(row.get("status") or "") in CLOSING_STATUSES}
    open_rows = [
        row
        for row in valid
        if str(row.get("status") or "") in OPEN_STATUSES and _key(row) not in closing_keys
    ]
    quality_rows = []
    for row in valid:
        gaps = _quality_gaps(row)
        if gaps:
            quality_rows.append(_compact(row) | {"quality_gaps": gaps})
    return {
        "artifact_version": ARTIFACT_VERSION,
        "created_at": _now_text(),
        "total_records": len(valid),
        "parse_error_count": len(parse_errors),
        "open_observation_count": len(open_rows),
        "quality_gap_count": len(quality_rows),
        "latest_trade_date_compact": max((str(row.get("trade_date_compact") or "") for row in valid), default=""),
        "open_observations": [_compact(row) for row in open_rows[:50]],
        "quality_gaps": quality_rows[:50],
        "parse_errors": parse_errors[:20],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Check Top5 execution observation ledger completeness.")
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--output-json", default="")
    parser.add_argument("--open-output-csv", default="")
    parser.add_argument("--fail-on-open", action="store_true")
    parser.add_argument("--fail-on-quality", action="store_true")
    args = parser.parse_args()

    ledger = Path(args.ledger)
    report = build_report(_load_jsonl(ledger))
    report["ledger"] = str(ledger)
    report["passed"] = True
    blocking: list[str] = []
    if args.fail_on_open and int(report["open_observation_count"]) > 0:
        blocking.append("open_observations_present")
    if args.fail_on_quality and int(report["quality_gap_count"]) > 0:
        blocking.append("quality_gaps_present")
    if int(report["parse_error_count"]) > 0:
        blocking.append("ledger_parse_errors_present")
    report["blocking_reasons"] = blocking
    report["passed"] = not blocking

    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output_json:
        Path(args.output_json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output_json).write_text(text, encoding="utf-8")
    if args.open_output_csv:
        csv_path = Path(args.open_output_csv)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        fields = [
            "trade_date_compact",
            "competition_run_id",
            "ts_code",
            "name",
            "rank",
            "planned_entry_price",
            "planned_exit_price",
            "planned_stop_price",
            "status_to_record",
            "actual_entry_price",
            "actual_exit_price",
            "filled_qty",
            "slippage_bp",
            "miss_reason",
            "failure_attribution",
            "notes",
        ]
        open_rows = report.get("open_observations") if isinstance(report.get("open_observations"), list) else []
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for row in open_rows:
                if not isinstance(row, dict):
                    continue
                writer.writerow(
                    {
                        "trade_date_compact": row.get("trade_date_compact", ""),
                        "competition_run_id": row.get("competition_run_id", ""),
                        "ts_code": row.get("ts_code", ""),
                        "name": row.get("name", ""),
                        "rank": row.get("rank", ""),
                        "planned_entry_price": row.get("planned_entry_price", ""),
                        "planned_exit_price": row.get("planned_exit_price", ""),
                        "planned_stop_price": row.get("planned_stop_price", ""),
                        "status_to_record": "",
                        "actual_entry_price": "",
                        "actual_exit_price": "",
                        "filled_qty": "",
                        "slippage_bp": "",
                        "miss_reason": "",
                        "failure_attribution": "",
                        "notes": "",
                    }
                )
    print(text, end="")
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
