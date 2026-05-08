from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CANDIDATE_OBSERVATION_SNAPSHOT_VERSION = "candidate_observation_snapshot.v1"
CANDIDATE_OBSERVATION_LEDGER_VERSION = "candidate_observation_ledger.v1"
CANDIDATE_OBSERVATION_RESULT_VERSION = "candidate_observation_result.v1"
CANDIDATE_FAILURE_ATTRIBUTION_VERSION = "candidate_failure_attribution.v1"
CANDIDATE_QUALITY_20_SAMPLE_REPORT_VERSION = "candidate_quality_20_sample_report.v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _read_csv_rows(path: str | Path) -> list[dict[str, str]]:
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _date_key(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text[:10].replace("-", "")


def _sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _existing_ledger_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    ids: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict) and payload.get("observation_id"):
            ids.add(str(payload["observation_id"]))
    return ids


def _load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    resolved = Path(path)
    if not resolved.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in resolved.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"expected object jsonl entry: {resolved}")
        rows.append(payload)
    return rows


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _fetch_trade_dates(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"SELECT DISTINCT trade_date FROM {table} ORDER BY trade_date").fetchall()
    return [str(row[0]) for row in rows if row and row[0]]


def _fetch_close_rows(
    conn: sqlite3.Connection,
    table: str,
    ts_codes: list[str],
    trade_dates: list[str],
) -> dict[tuple[str, str], float]:
    if not ts_codes or not trade_dates:
        return {}
    code_placeholders = ",".join("?" for _ in ts_codes)
    date_placeholders = ",".join("?" for _ in trade_dates)
    rows = conn.execute(
        f"""
        SELECT ts_code, trade_date, close_price
        FROM {table}
        WHERE ts_code IN ({code_placeholders})
          AND trade_date IN ({date_placeholders})
          AND close_price IS NOT NULL
          AND close_price > 0
        """,
        [*ts_codes, *trade_dates],
    ).fetchall()
    return {(str(code), str(date)): float(close) for code, date, close in rows}


def _max_drawdown_from_closes(closes: list[float]) -> float:
    if not closes:
        return 0.0
    peak = closes[0]
    max_drawdown = 0.0
    for close in closes:
        if close > peak:
            peak = close
        if peak <= 0:
            continue
        drawdown = close / peak - 1.0
        if drawdown < max_drawdown:
            max_drawdown = drawdown
    return round(max_drawdown, 6)


def build_candidate_observation_snapshot(
    *,
    candidates_csv_path: str | Path,
    lineage_path: str | Path,
    data_quality_gate_path: str | Path,
    realistic_backtest_path: str | Path | None = None,
) -> dict[str, Any]:
    lineage = _read_json(lineage_path)
    gate = _read_json(data_quality_gate_path)
    backtest = _read_json(realistic_backtest_path) if realistic_backtest_path and Path(realistic_backtest_path).exists() else {}
    rows = _read_csv_rows(candidates_csv_path)
    data_as_of = _date_key(lineage.get("data_as_of"))
    if not data_as_of:
        raise ValueError("lineage missing data_as_of")
    lineage_by_code = {str(item.get("ts_code") or ""): item for item in lineage.get("candidates", []) or []}
    gate_by_code = {str(item.get("ts_code") or ""): item for item in gate.get("candidates", []) or []}
    backtest_by_code = {str(item.get("ts_code") or ""): item for item in backtest.get("candidates", []) or []}
    items: list[dict[str, Any]] = []
    for row in rows:
        code = str(row.get("ts_code") or "").strip()
        if not code:
            continue
        lineage_item = lineage_by_code.get(code, {})
        gate_item = gate_by_code.get(code, {})
        backtest_item = backtest_by_code.get(code, {})
        observation_id = f"{data_as_of}:{code}"
        items.append(
            {
                "observation_id": observation_id,
                "ts_code": code,
                "rank": int(float(row.get("rank") or 0)),
                "stock_name": row.get("stock_name", ""),
                "industry": row.get("industry", ""),
                "selected_at": data_as_of,
                "observation_status": "observing",
                "observation_horizons": [5, 20, 60],
                "selection_reason": row.get("reason", ""),
                "signal": row.get("signal", ""),
                "final_score": float(row.get("final_score") or 0.0),
                "data_quality_level": row.get("data_quality_level") or gate_item.get("quality_level", ""),
                "data_quality_score": float(row.get("data_quality_score") or gate_item.get("quality_score") or 0.0),
                "lineage_hash": lineage_item.get("lineage_hash"),
                "lineage_run_id": lineage.get("run_id"),
                "realistic_backtest_status": backtest_item.get("status", backtest.get("status")),
                "realistic_backtest_blocking_reasons": backtest_item.get("blocking_reasons", []),
            }
        )
    status = "blocked" if not items else "frozen"
    blocking_reasons = ["no_formal_candidates"] if not items else []
    if gate.get("status") == "failed" and gate.get("blocked_count", 0):
        blocking_reasons.append("data_quality_gate_has_blocked_candidates")
    return {
        "schema_version": CANDIDATE_OBSERVATION_SNAPSHOT_VERSION,
        "status": status,
        "generated_at": _utc_now(),
        "snapshot_date": data_as_of,
        "lineage_run_id": lineage.get("run_id"),
        "candidate_count": len(items),
        "source_files": {
            "candidates_csv": str(Path(candidates_csv_path)),
            "candidate_lineage": str(Path(lineage_path)),
            "candidate_data_quality_gate": str(Path(data_quality_gate_path)),
            "realistic_backtest": str(Path(realistic_backtest_path)) if realistic_backtest_path else "",
        },
        "source_hashes": {
            "candidates_csv": _sha256_file(candidates_csv_path),
            "candidate_lineage": _sha256_file(lineage_path),
            "candidate_data_quality_gate": _sha256_file(data_quality_gate_path),
            "realistic_backtest": _sha256_file(realistic_backtest_path)
            if realistic_backtest_path and Path(realistic_backtest_path).exists()
            else "",
        },
        "blocking_reasons": blocking_reasons,
        "items": items,
    }


def freeze_candidate_observation_snapshot(
    snapshot: dict[str, Any],
    *,
    output_dir: str | Path,
) -> tuple[str, dict[str, Any]]:
    resolved_output_dir = Path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_date = _date_key(snapshot.get("snapshot_date"))
    if not snapshot_date:
        raise ValueError("snapshot missing snapshot_date")
    path = resolved_output_dir / f"candidate_observation_snapshot_{snapshot_date}.json"
    latest_path = resolved_output_dir / "candidate_observation_snapshot_latest.json"
    if path.exists():
        existing = _read_json(path)
        latest_path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(path), {**existing, "freeze_status": "already_frozen"}
    payload = {**snapshot, "freeze_status": "frozen"}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path), payload


def append_candidate_observation_ledger(
    snapshot: dict[str, Any],
    *,
    ledger_path: str | Path,
) -> dict[str, Any]:
    resolved_ledger = Path(ledger_path)
    resolved_ledger.parent.mkdir(parents=True, exist_ok=True)
    existing_ids = _existing_ledger_ids(resolved_ledger)
    appended = 0
    skipped = 0
    with resolved_ledger.open("a", encoding="utf-8") as handle:
        for item in snapshot.get("items", []) or []:
            observation_id = str(item.get("observation_id") or "")
            if not observation_id or observation_id in existing_ids:
                skipped += 1
                continue
            entry = {
                "ledger_version": CANDIDATE_OBSERVATION_LEDGER_VERSION,
                "observation_id": observation_id,
                "snapshot_date": snapshot.get("snapshot_date"),
                "ts_code": item.get("ts_code"),
                "rank": item.get("rank"),
                "selected_at": item.get("selected_at"),
                "observation_status": "open",
                "observation_horizons": item.get("observation_horizons", [5, 20, 60]),
                "selection_reason": item.get("selection_reason", ""),
                "lineage_run_id": snapshot.get("lineage_run_id"),
                "lineage_hash": item.get("lineage_hash"),
                "returns": {"5d": None, "20d": None, "60d": None},
                "max_drawdown": None,
                "hit_status": "pending",
                "failure_reason": None,
                "created_at": _utc_now(),
            }
            handle.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
            existing_ids.add(observation_id)
            appended += 1
    return {
        "ledger_path": str(resolved_ledger),
        "appended_count": appended,
        "skipped_duplicate_count": skipped,
        "total_known_observations": len(existing_ids),
    }


def build_candidate_observation_result(
    *,
    snapshot_path: str | Path,
    ledger_path: str | Path,
    sqlite_db_path: str | Path,
    sqlite_table: str = "daily_trading_data",
    benchmark_ts_code: str = "000001.SH",
    horizons: list[int] | None = None,
) -> dict[str, Any]:
    horizons = horizons or [5, 20, 60]
    snapshot = _read_json(snapshot_path)
    ledger_rows = _load_jsonl(ledger_path)
    by_id = {str(row.get("observation_id") or ""): row for row in ledger_rows}
    db_path = Path(sqlite_db_path)
    if not db_path.exists():
        return {
            "schema_version": CANDIDATE_OBSERVATION_RESULT_VERSION,
            "status": "blocked",
            "generated_at": _utc_now(),
            "blocking_reasons": ["database_missing"],
            "candidates": [],
            "summary": {"completed_count": 0, "pending_count": 0, "blocked_count": len(snapshot.get("items", []) or [])},
        }
    items = list(snapshot.get("items", []) or [])
    codes = [str(item.get("ts_code") or "") for item in items if item.get("ts_code")]
    if benchmark_ts_code not in codes:
        codes.append(benchmark_ts_code)
    selected_at = _date_key(snapshot.get("snapshot_date"))
    with sqlite3.connect(str(db_path)) as conn:
        trade_dates = _fetch_trade_dates(conn, sqlite_table)
        selected_idx = trade_dates.index(selected_at) if selected_at in trade_dates else -1
        needed_dates = []
        if selected_idx >= 0:
            max_horizon = max(horizons)
            needed_dates = trade_dates[selected_idx : selected_idx + max_horizon + 1]
        close_by_key = _fetch_close_rows(conn, sqlite_table, codes, needed_dates)

    candidates: list[dict[str, Any]] = []
    for item in items:
        code = str(item.get("ts_code") or "")
        observation_id = str(item.get("observation_id") or "")
        ledger_entry = by_id.get(observation_id)
        reasons: list[str] = []
        returns: dict[str, float | None] = {}
        excess: dict[str, float | None] = {}
        hit: dict[str, str] = {}
        if not ledger_entry:
            reasons.append("missing_ledger_entry")
        if selected_idx < 0:
            reasons.append("snapshot_date_not_in_trade_calendar")
        start_close = close_by_key.get((code, selected_at))
        benchmark_start = close_by_key.get((benchmark_ts_code, selected_at))
        if start_close is None:
            reasons.append("missing_start_price")
        if benchmark_start is None:
            reasons.append("missing_benchmark_start_price")
        max_drawdown = None
        for horizon in horizons:
            key = f"{horizon}d"
            if selected_idx < 0 or selected_idx + horizon >= len(trade_dates):
                returns[key] = None
                excess[key] = None
                hit[key] = "pending"
                reasons.append(f"insufficient_{horizon}d_trade_dates")
                continue
            target_date = trade_dates[selected_idx + horizon]
            end_close = close_by_key.get((code, target_date))
            benchmark_end = close_by_key.get((benchmark_ts_code, target_date))
            if start_close is None or end_close is None:
                returns[key] = None
                excess[key] = None
                hit[key] = "blocked"
                reasons.append(f"missing_{horizon}d_price")
                continue
            observed_return = round(end_close / start_close - 1.0, 6)
            returns[key] = observed_return
            if benchmark_start is not None and benchmark_end is not None:
                benchmark_return = benchmark_end / benchmark_start - 1.0
                excess[key] = round(observed_return - benchmark_return, 6)
            else:
                excess[key] = None
                reasons.append(f"missing_{horizon}d_benchmark_price")
            hit[key] = "hit" if observed_return > 0 and (excess[key] is None or float(excess[key] or 0.0) >= 0) else "miss"
        if selected_idx >= 0 and start_close is not None:
            valid_closes = [
                close_by_key[(code, date)]
                for date in needed_dates
                if (code, date) in close_by_key
            ]
            max_drawdown = _max_drawdown_from_closes(valid_closes) if valid_closes else None
        completed = all(returns.get(f"{h}d") is not None for h in horizons)
        has_blocked_price = any(str(reason).startswith("missing_") for reason in reasons)
        status = "completed" if completed else "blocked" if has_blocked_price else "pending"
        candidates.append(
            {
                "observation_id": observation_id,
                "ts_code": code,
                "status": status,
                "selected_at": selected_at,
                "returns": returns,
                "excess_returns": excess,
                "max_drawdown": max_drawdown,
                "hit_status": hit,
                "blocking_reasons": sorted(set(reasons)),
            }
        )
    completed_rows = [row for row in candidates if row["status"] == "completed"]
    pending_rows = [row for row in candidates if row["status"] == "pending"]
    blocked_rows = [row for row in candidates if row["status"] == "blocked"]
    status = "passed" if completed_rows and not pending_rows and not blocked_rows else "blocked" if blocked_rows else "pending"
    return {
        "schema_version": CANDIDATE_OBSERVATION_RESULT_VERSION,
        "status": status,
        "generated_at": _utc_now(),
        "snapshot_path": str(Path(snapshot_path)),
        "ledger_path": str(Path(ledger_path)),
        "sqlite_db_path": str(db_path),
        "sqlite_table": sqlite_table,
        "benchmark_ts_code": benchmark_ts_code,
        "horizons": horizons,
        "summary": {
            "candidate_count": len(candidates),
            "completed_count": len(completed_rows),
            "pending_count": len(pending_rows),
            "blocked_count": len(blocked_rows),
        },
        "candidates": candidates,
    }


def build_candidate_failure_attribution(result: dict[str, Any]) -> dict[str, Any]:
    rows = []
    counts: dict[str, int] = {}
    for item in result.get("candidates", []) or []:
        status = str(item.get("status") or "")
        reasons = list(item.get("blocking_reasons", []) or [])
        category = None
        if status == "pending":
            category = "insufficient_observation_window"
        elif any("price" in reason or "trade_dates" in reason for reason in reasons):
            category = "data_or_calendar_insufficient"
        elif item.get("max_drawdown") is not None and float(item.get("max_drawdown") or 0.0) < -0.08:
            category = "risk_control_failure"
        else:
            returns = item.get("returns", {}) or {}
            excess = item.get("excess_returns", {}) or {}
            r5 = returns.get("5d")
            e5 = excess.get("5d")
            if r5 is not None and float(r5) < 0:
                category = "negative_absolute_return"
            elif e5 is not None and float(e5) < 0:
                category = "benchmark_underperformance"
        if category:
            counts[category] = counts.get(category, 0) + 1
            rows.append(
                {
                    "observation_id": item.get("observation_id"),
                    "ts_code": item.get("ts_code"),
                    "status": status,
                    "primary_failure_category": category,
                    "evidence": {
                        "blocking_reasons": reasons,
                        "returns": item.get("returns"),
                        "excess_returns": item.get("excess_returns"),
                        "max_drawdown": item.get("max_drawdown"),
                    },
                }
            )
    return {
        "schema_version": CANDIDATE_FAILURE_ATTRIBUTION_VERSION,
        "status": "passed" if rows or not result.get("candidates") else "blocked",
        "generated_at": _utc_now(),
        "attribution_count": len(rows),
        "category_counts": counts,
        "items": rows,
    }


def build_candidate_quality_20_sample_report(result: dict[str, Any], *, min_samples: int = 20) -> dict[str, Any]:
    completed = [item for item in result.get("candidates", []) or [] if item.get("status") == "completed"]
    sample_count = len(completed)
    if sample_count < min_samples:
        return {
            "schema_version": CANDIDATE_QUALITY_20_SAMPLE_REPORT_VERSION,
            "status": "blocked",
            "generated_at": _utc_now(),
            "sample_count": sample_count,
            "required_sample_count": min_samples,
            "blocking_reasons": ["insufficient_completed_samples"],
            "conclusion": "continue_observation",
        }
    returns_5d = [float((item.get("returns", {}) or {}).get("5d") or 0.0) for item in completed]
    excess_5d = [float((item.get("excess_returns", {}) or {}).get("5d") or 0.0) for item in completed]
    return {
        "schema_version": CANDIDATE_QUALITY_20_SAMPLE_REPORT_VERSION,
        "status": "passed",
        "generated_at": _utc_now(),
        "sample_count": sample_count,
        "required_sample_count": min_samples,
        "avg_return_5d": round(sum(returns_5d) / sample_count, 6),
        "avg_excess_return_5d": round(sum(excess_5d) / sample_count, 6),
        "hit_rate_5d": round(sum(1 for value in returns_5d if value > 0) / sample_count, 6),
        "conclusion": "sample_floor_met",
    }


def write_candidate_quality_payload(payload: dict[str, Any], *, output_path: str | Path) -> str:
    resolved = Path(output_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(resolved)
