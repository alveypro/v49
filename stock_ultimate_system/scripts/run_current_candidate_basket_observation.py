#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_candidate_basket_observation import (
    PrimaryResultCandidateBasketPerformanceLedger,
    build_primary_result_candidate_basket_observation,
    current_basket_snapshot_path,
)
from src.primary_result_candidate_basket_feedback import build_primary_result_candidate_basket_feedback
from src.primary_result_observation_metrics import _date_key
from src.utils.project_paths import resolve_project_path


def _load_yaml(path: Path) -> dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"config file missing: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"config file must contain an object: {path}")
    return payload


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _first_benchmark(settings: dict[str, object]) -> str:
    data = settings.get("data", {})
    if not isinstance(data, dict):
        return "000001.SH"
    raw = data.get("benchmark_indices", ["000001.SH"])
    if isinstance(raw, str):
        return raw.strip() or "000001.SH"
    if isinstance(raw, list):
        for item in raw:
            code = str(item or "").strip()
            if code:
                return code
    return "000001.SH"


def _data_value(settings: dict[str, object], key: str, default: str) -> str:
    data = settings.get("data", {})
    if not isinstance(data, dict):
        return default
    value = str(data.get(key, "") or "").strip()
    return value or default


def _observation_window_start(exp_dir: Path) -> str:
    path = exp_dir / "primary_result_observation_latest.json"
    if not path.exists():
        raise FileNotFoundError(f"open primary result observation missing: {path}")
    payload = _read_json(path)
    window = payload.get("observation_window")
    if not isinstance(window, dict):
        raise ValueError("primary result observation missing observation_window")
    started_at = str(window.get("started_at") or "").strip()
    if not started_at:
        raise ValueError("primary result observation window start is missing")
    return started_at


def _today_shanghai() -> str:
    return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")


def _extract_price_history_from_sqlite(
    *,
    sqlite_db_path: Path,
    sqlite_table: str,
    ts_codes: list[str],
    window_start: str,
    window_end: str,
    output_csv: Path,
) -> dict[str, object]:
    start = _date_key(window_start).replace("-", "")
    end = _date_key(window_end).replace("-", "")
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    placeholders = ",".join("?" for _ in ts_codes)
    with sqlite3.connect(f"file:{sqlite_db_path}?mode=ro", uri=True) as conn:
        rows = conn.execute(
            f"""
            SELECT ts_code, trade_date, close_price
            FROM {sqlite_table}
            WHERE ts_code IN ({placeholders})
              AND trade_date >= ?
              AND trade_date <= ?
              AND close_price IS NOT NULL
              AND close_price > 0
            ORDER BY ts_code, trade_date
            """,
            (*ts_codes, start, end),
        ).fetchall()
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["ts_code", "trade_date", "close"])
        writer.writerows(rows)
    counts = {code: 0 for code in ts_codes}
    latest = {code: None for code in ts_codes}
    for code, trade_date, _close in rows:
        counts[str(code)] = counts.get(str(code), 0) + 1
        latest[str(code)] = str(trade_date)
    return {
        "output_csv": str(output_csv),
        "row_count": len(rows),
        "row_counts_by_code": counts,
        "latest_trade_date_by_code": latest,
    }


def run_current_candidate_basket_observation(
    *,
    config_path: str | Path = "config/settings.yaml",
    exp_dir: str | Path = "data/experiments",
    baskets_dir: str | Path = "artifacts/primary_result_candidate_baskets",
    window_end: str | None = None,
    price_history_csv: str | Path = "data/experiments/current_candidate_basket_price_history_latest.csv",
    output_path: str | Path = "artifacts/primary_result_candidate_baskets/observation_latest.json",
    ledger_jsonl: str | Path = "artifacts/primary_result_candidate_baskets/performance_ledger.jsonl",
    summary_json: str | Path = "artifacts/primary_result_candidate_baskets/performance_summary.json",
    feedback_json: str | Path | None = None,
) -> tuple[int, dict[str, object]]:
    resolved_config = resolve_project_path(config_path)
    resolved_exp_dir = resolve_project_path(exp_dir)
    settings = _load_yaml(resolved_config)
    benchmark = _first_benchmark(settings)
    snapshot_path = current_basket_snapshot_path(baskets_dir=baskets_dir)
    snapshot = _read_json(snapshot_path)
    items = [item for item in snapshot.get("items", []) if isinstance(item, dict)]
    ts_codes = []
    for item in items:
        code = str(item.get("ts_code") or "").strip()
        if code and code not in ts_codes:
            ts_codes.append(code)
    if benchmark not in ts_codes:
        ts_codes.append(benchmark)
    resolved_window_start = _observation_window_start(resolved_exp_dir)
    resolved_window_end = window_end or _today_shanghai()
    resolved_output_path = resolve_project_path(output_path)
    resolved_feedback_json = (
        resolve_project_path(feedback_json)
        if feedback_json is not None
        else resolved_output_path.parent / "feedback_latest.json"
    )
    if _date_key(resolved_window_end) < _date_key(resolved_window_start):
        payload = {
            "runner": "current_candidate_basket_observation",
            "status": "pending_window",
            "basket_id": snapshot.get("basket_id"),
            "benchmark_ts_code": benchmark,
            "window_start": resolved_window_start,
            "window_end": resolved_window_end,
            "blocking_reasons": [],
            "next_actions": [
                "wait until the primary result observation window starts before closing candidate basket observation"
            ],
            "production_boundary": (
                "current candidate basket observation runner only reads local SQLite and registered basket artifacts; "
                "pending-window reports do not write performance ledger entries"
            ),
        }
        _write_json(resolved_output_path, payload)
        return 0, payload
    resolved_price_history = resolve_project_path(price_history_csv)
    resolved_sqlite_db_path = resolve_project_path(
        _data_value(settings, "sqlite_db_path", "/opt/openclaw/permanent_stock_database.db")
    )
    extract = _extract_price_history_from_sqlite(
        sqlite_db_path=resolved_sqlite_db_path,
        sqlite_table=_data_value(settings, "sqlite_table", "daily_trading_data"),
        ts_codes=ts_codes,
        window_start=resolved_window_start,
        window_end=resolved_window_end,
        output_csv=resolved_price_history,
    )
    insufficient = {
        code: count
        for code, count in dict(extract["row_counts_by_code"]).items()
        if int(count) < 2
    }
    if insufficient:
        payload = {
            "runner": "current_candidate_basket_observation",
            "status": "blocked",
            "basket_id": snapshot.get("basket_id"),
            "benchmark_ts_code": benchmark,
            "window_start": resolved_window_start,
            "window_end": resolved_window_end,
            "price_history_extract": extract,
            "blocking_reasons": [
                f"insufficient price rows for {code}: {count}" for code, count in sorted(insufficient.items())
            ],
            "production_boundary": (
                "current candidate basket observation runner only reads local SQLite and registered basket artifacts; "
                "blocked reports do not write performance ledger entries"
            ),
        }
        _write_json(resolved_output_path, payload)
        return 1, payload

    obs_code, observation = build_primary_result_candidate_basket_observation(
        basket_snapshot_path=snapshot_path,
        price_history_path=resolved_price_history,
        benchmark_ts_code=benchmark,
        window_start=resolved_window_start,
        window_end=resolved_window_end,
        output_path=resolved_output_path,
    )
    ledger = PrimaryResultCandidateBasketPerformanceLedger(
        ledger_path=ledger_jsonl,
        summary_path=summary_json,
    )
    try:
        entry = ledger.append_observation(observation_path=output_path)
        ledger_status = "registered"
        ledger_error = None
    except FileExistsError as exc:
        entry = None
        ledger_status = "duplicate"
        ledger_error = str(exc)
    _, feedback = build_primary_result_candidate_basket_feedback(
        observation_path=resolved_output_path,
        performance_summary_path=summary_json,
        output_path=resolved_feedback_json,
    )
    payload = {
        "runner": "current_candidate_basket_observation",
        "status": observation["status"],
        "basket_id": observation["basket_id"],
        "benchmark_ts_code": benchmark,
        "window_start": resolved_window_start,
        "window_end": resolved_window_end,
        "metrics": observation["metrics"],
        "price_history_extract": extract,
        "ledger_status": ledger_status,
        "ledger_entry": entry,
        "ledger_error": ledger_error,
        "feedback_status": feedback["status"],
        "feedback_level": feedback["feedback_level"],
        "feedback_change_total": feedback["change_total"],
        "feedback_output_path": str(resolved_feedback_json),
    }
    _write_json(resolved_output_path, {**observation, "runner_report": payload})
    return obs_code, payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Run observation for the current registered candidate basket.")
    parser.add_argument("--config", default="config/settings.yaml")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--baskets-dir", default="artifacts/primary_result_candidate_baskets")
    parser.add_argument("--window-end")
    parser.add_argument("--price-history-csv", default="data/experiments/current_candidate_basket_price_history_latest.csv")
    parser.add_argument("--output", default="artifacts/primary_result_candidate_baskets/observation_latest.json")
    parser.add_argument("--ledger-jsonl", default="artifacts/primary_result_candidate_baskets/performance_ledger.jsonl")
    parser.add_argument("--summary-json", default="artifacts/primary_result_candidate_baskets/performance_summary.json")
    parser.add_argument("--feedback-json")
    parser.add_argument("--zero-on-blocked", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        exit_code, payload = run_current_candidate_basket_observation(
            config_path=args.config,
            exp_dir=args.exp_dir,
            baskets_dir=args.baskets_dir,
            window_end=args.window_end,
            price_history_csv=args.price_history_csv,
            output_path=args.output,
            ledger_jsonl=args.ledger_jsonl,
            summary_json=args.summary_json,
            feedback_json=args.feedback_json,
        )
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(payload, ensure_ascii=False, indent=2) if args.json else json.dumps({
        "status": payload.get("status"),
        "basket_id": payload.get("basket_id"),
        "blocking_reason_total": len(payload.get("blocking_reasons", []) or []),
    }, ensure_ascii=False, indent=2))
    if args.zero_on_blocked and payload.get("status") == "blocked":
        return 0
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
