#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import time
from pathlib import Path
from typing import Any


ARTIFACT_VERSION = "top5_execution_observation.v1"
DEFAULT_LEDGER = Path("logs/openclaw/top5_execution_observations.jsonl")
VALID_CLOSING_STATUSES = {
    "filled",
    "partial_fill",
    "not_filled",
    "cancelled",
    "stopped",
    "take_profit",
    "manual_skip",
}
ENTRY_STATUSES = {"filled", "partial_fill"}
EXIT_STATUSES = {"stopped", "take_profit"}
NON_FILL_STATUSES = {"not_filled", "cancelled", "manual_skip"}


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _stamp() -> str:
    return f"{time.strftime('%Y%m%d_%H%M%S')}_{time.time_ns() % 1_000_000_000:09d}"


def _sha256_file(path: Path) -> str:
    if not path.is_file():
        return ""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except Exception:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _ledger_stats(path: Path) -> dict[str, Any]:
    rows = _load_jsonl(path)
    return {
        "path": str(path),
        "exists": path.is_file(),
        "line_count": len(rows),
        "sha256": _sha256_file(path),
    }


def _optional_float(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    return float(text)


def _optional_int(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    return int(float(text))


def _key(row: dict[str, Any], *, status: str | None = None) -> tuple[str, str, str]:
    return (
        str(row.get("competition_run_id") or "").strip(),
        str(row.get("ts_code") or "").strip(),
        str(status if status is not None else row.get("status") or "").strip(),
    )


def _required_gaps(row: dict[str, str], status: str) -> list[str]:
    gaps: list[str] = []
    if status in ENTRY_STATUSES:
        for field in ("actual_entry_price", "filled_qty", "slippage_bp"):
            if not str(row.get(field) or "").strip():
                gaps.append(f"{field}_missing")
    if status in EXIT_STATUSES:
        if not str(row.get("actual_exit_price") or "").strip():
            gaps.append("actual_exit_price_missing")
        if not str(row.get("failure_attribution") or row.get("notes") or "").strip():
            gaps.append("failure_attribution_or_notes_missing")
    if status in NON_FILL_STATUSES:
        if not str(row.get("miss_reason") or row.get("failure_attribution") or "").strip():
            gaps.append("miss_reason_or_failure_attribution_missing")
    return gaps


def _build_record(row: dict[str, str], planned: dict[str, Any], *, operator: str) -> dict[str, Any]:
    status = str(row.get("status_to_record") or "").strip()
    record = dict(planned)
    record.update(
        {
            "artifact_version": ARTIFACT_VERSION,
            "recorded_at": _now_text(),
            "operator": operator,
            "status": status,
            "actual_entry_price": _optional_float(row.get("actual_entry_price")),
            "actual_exit_price": _optional_float(row.get("actual_exit_price")),
            "filled_qty": _optional_int(row.get("filled_qty")),
            "slippage_bp": _optional_float(row.get("slippage_bp")),
            "miss_reason": str(row.get("miss_reason") or "").strip(),
            "failure_attribution": str(row.get("failure_attribution") or "").strip(),
            "notes": str(row.get("notes") or "").strip(),
            "stop_triggered": status == "stopped",
            "take_profit_triggered": status == "take_profit",
        }
    )
    return record


def _write_reject_csv(path: Path, skipped: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["line_no", "ts_code", "status", "reason", "gaps"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for item in skipped:
            writer.writerow(
                {
                    "line_no": item.get("line_no", ""),
                    "ts_code": item.get("ts_code", ""),
                    "status": item.get("status", ""),
                    "reason": item.get("reason", ""),
                    "gaps": ",".join(str(x) for x in item.get("gaps", []) or []),
                }
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Import filled Top5 execution observation updates from operations CSV.")
    parser.add_argument("--input-csv", default="exports/top5_execution_open_observations.csv")
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--operator", default="daily_ops")
    parser.add_argument("--output-dir", default="logs/openclaw/top5_execution_imports")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-duplicates", action="store_true")
    args = parser.parse_args()

    input_csv = Path(args.input_csv)
    ledger = Path(args.ledger)
    batch_id = f"top5_exec_import_{_stamp()}"
    before_stats = _ledger_stats(ledger)
    planned_rows = _load_jsonl(ledger)
    planned_by_key = {
        (str(row.get("competition_run_id") or ""), str(row.get("ts_code") or "")): row
        for row in planned_rows
        if str(row.get("status") or "") in {"planned", "submitted"}
    }
    existing = {_key(row) for row in planned_rows}
    imported: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    with input_csv.open(encoding="utf-8-sig", newline="") as f:
        for line_no, row in enumerate(csv.DictReader(f), start=2):
            status = str(row.get("status_to_record") or "").strip()
            if not status:
                skipped.append({"line_no": line_no, "reason": "status_to_record_empty", "ts_code": row.get("ts_code"), "status": status})
                continue
            if status not in VALID_CLOSING_STATUSES:
                skipped.append({"line_no": line_no, "reason": f"invalid_status:{status}", "ts_code": row.get("ts_code"), "status": status})
                continue
            key2 = (str(row.get("competition_run_id") or "").strip(), str(row.get("ts_code") or "").strip())
            planned = planned_by_key.get(key2)
            if not planned:
                skipped.append({"line_no": line_no, "reason": "matching_planned_observation_missing", "ts_code": row.get("ts_code"), "status": status})
                continue
            gaps = _required_gaps(row, status)
            if gaps:
                skipped.append({"line_no": line_no, "reason": "required_fields_missing", "gaps": gaps, "ts_code": row.get("ts_code"), "status": status})
                continue
            record = _build_record(row, planned, operator=str(args.operator or "daily_ops"))
            record["import_batch_id"] = batch_id
            record["source_update_csv"] = str(input_csv)
            record["source_update_csv_sha256"] = _sha256_file(input_csv)
            if not args.allow_duplicates and _key(record) in existing:
                skipped.append({"line_no": line_no, "reason": "duplicate_observation", "ts_code": row.get("ts_code"), "status": status})
                continue
            imported.append(record)
            existing.add(_key(record))

    if imported and not args.dry_run:
        ledger.parent.mkdir(parents=True, exist_ok=True)
        with ledger.open("a", encoding="utf-8") as f:
            for record in imported:
                f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    reject_csv = output_dir / f"{batch_id}_rejects.csv"
    _write_reject_csv(reject_csv, skipped)
    after_stats = _ledger_stats(ledger)
    manifest = {
        "artifact_version": "top5_execution_observation_import.v1",
        "batch_id": batch_id,
        "created_at": _now_text(),
        "operator": str(args.operator or "daily_ops"),
        "dry_run": bool(args.dry_run),
        "input_csv": str(input_csv),
        "input_csv_sha256": _sha256_file(input_csv),
        "ledger_before": before_stats,
        "ledger_after": after_stats,
        "imported_count": len(imported),
        "skipped_count": len(skipped),
        "imported_keys": [
            {"competition_run_id": key[0], "ts_code": key[1], "status": key[2]}
            for key in [_key(record) for record in imported]
        ],
        "reject_csv": str(reject_csv),
        "skipped": skipped[:200],
    }
    manifest_path = output_dir / f"{batch_id}.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "batch_id": batch_id,
                "input_csv": str(input_csv),
                "ledger": str(ledger),
                "dry_run": bool(args.dry_run),
                "imported_count": len(imported),
                "manifest": str(manifest_path),
                "reject_csv": str(reject_csv),
                "skipped_count": len(skipped),
                "skipped": skipped[:50],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if imported or skipped else 0


if __name__ == "__main__":
    raise SystemExit(main())
