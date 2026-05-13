#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path
from typing import Any


ARTIFACT_VERSION = "top5_execution_observation.v1"
DEFAULT_LEDGER = Path("logs/openclaw/top5_execution_observations.jsonl")
VALID_STATUSES = {
    "planned",
    "submitted",
    "filled",
    "partial_fill",
    "not_filled",
    "cancelled",
    "stopped",
    "take_profit",
    "manual_skip",
}


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _load_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("manifest must be a JSON object")
    return payload


def _load_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _optional_float(value: str) -> float | None:
    text = str(value or "").strip()
    if text == "":
        return None
    return float(text)


def _optional_int(value: str) -> int | None:
    text = str(value or "").strip()
    if text == "":
        return None
    return int(float(text))


def _build_records(args: argparse.Namespace) -> list[dict[str, Any]]:
    manifest = _load_manifest(Path(args.manifest))
    csv_path = Path(str(manifest.get("csv") or ""))
    if not csv_path.is_file():
        raise FileNotFoundError(f"top5 csv missing: {csv_path}")
    rows = _load_csv_rows(csv_path)
    status = str(args.status or "planned").strip()
    if status not in VALID_STATUSES:
        raise ValueError(f"unsupported status: {status}")
    records: list[dict[str, Any]] = []
    selected = str(args.ts_code or "").strip()
    for row in rows:
        ts_code = str(row.get("股票代码") or row.get("ts_code") or "").strip()
        if selected and ts_code != selected:
            continue
        records.append(
            {
                "artifact_version": ARTIFACT_VERSION,
                "recorded_at": _now_text(),
                "operator": str(args.operator or ""),
                "competition_run_id": str(manifest.get("competition_run_id") or ""),
                "manifest_written_at": str(manifest.get("written_at") or ""),
                "trade_date_compact": str(manifest.get("trade_date_compact") or ""),
                "source_manifest": str(Path(args.manifest).resolve()),
                "source_csv": str(csv_path),
                "strategy_scope": "v9_canary_top5",
                "ts_code": ts_code,
                "name": str(row.get("股票名称") or ""),
                "rank": _optional_int(str(row.get("序号") or "")),
                "target_weight": _optional_float(str(row.get("目标权重") or "")),
                "planned_entry_price": _optional_float(str(row.get("参考买入价") or "")),
                "planned_exit_price": _optional_float(str(row.get("参考卖出价") or "")),
                "planned_stop_price": _optional_float(str(row.get("止损价") or "")),
                "status": status,
                "actual_entry_price": _optional_float(args.actual_entry_price),
                "actual_exit_price": _optional_float(args.actual_exit_price),
                "filled_qty": _optional_int(args.filled_qty),
                "slippage_bp": _optional_float(args.slippage_bp),
                "miss_reason": str(args.miss_reason or ""),
                "stop_triggered": bool(args.stop_triggered),
                "take_profit_triggered": bool(args.take_profit_triggered),
                "failure_attribution": str(args.failure_attribution or ""),
                "notes": str(args.notes or ""),
            }
        )
    if not records:
        raise ValueError("no matching top5 rows to record")
    return records


def _record_key(record: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(record.get("competition_run_id") or ""),
        str(record.get("ts_code") or ""),
        str(record.get("status") or ""),
    )


def _existing_keys(path: Path) -> set[tuple[str, str, str]]:
    if not path.is_file():
        return set()
    keys: set[tuple[str, str, str]] = set()
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
                keys.add(_record_key(payload))
    return keys


def main() -> int:
    parser = argparse.ArgumentParser(description="Append quasi-live execution observations for latest Top5 candidates.")
    parser.add_argument("--manifest", default="exports/top5_trader_brief_latest_manifest.json")
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--operator", default="")
    parser.add_argument("--ts-code", default="", help="Record one symbol. Default records all Top5 rows.")
    parser.add_argument("--status", default="planned", choices=sorted(VALID_STATUSES))
    parser.add_argument("--actual-entry-price", default="")
    parser.add_argument("--actual-exit-price", default="")
    parser.add_argument("--filled-qty", default="")
    parser.add_argument("--slippage-bp", default="")
    parser.add_argument("--miss-reason", default="")
    parser.add_argument("--failure-attribution", default="")
    parser.add_argument("--notes", default="")
    parser.add_argument("--stop-triggered", action="store_true")
    parser.add_argument("--take-profit-triggered", action="store_true")
    parser.add_argument("--allow-duplicates", action="store_true")
    args = parser.parse_args()

    records = _build_records(args)
    ledger = Path(args.ledger)
    ledger.parent.mkdir(parents=True, exist_ok=True)
    if not args.allow_duplicates:
        existing = _existing_keys(ledger)
        records = [record for record in records if _record_key(record) not in existing]
    with ledger.open("a", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    print(
        json.dumps(
            {"ledger": str(ledger), "records_appended": len(records), "status": args.status},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
