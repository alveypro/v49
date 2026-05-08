from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from src.artifact_registry import sha256_file
from src.primary_result_candidate_basket import (
    PRIMARY_RESULT_CANDIDATE_BASKET_VERSION,
    PrimaryResultCandidateBasketRegistry,
)
from src.primary_result_observation_metrics import _date_key
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_CANDIDATE_BASKET_OBSERVATION_VERSION = "primary_result_candidate_basket_observation.v1"
PRIMARY_RESULT_CANDIDATE_BASKET_PERFORMANCE_LEDGER_VERSION = "primary_result_candidate_basket_performance_ledger.v1"
PRIMARY_RESULT_CANDIDATE_BASKET_PERFORMANCE_SUMMARY_VERSION = "primary_result_candidate_basket_performance_summary.v1"
REQUIRED_PRICE_COLUMNS = {"ts_code", "trade_date", "close"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_float(value: object, *, field_name: str) -> float:
    text = _normalize_text(value)
    if not text:
        raise ValueError(f"{field_name} is required")
    try:
        return float(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be numeric") from exc


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _load_price_rows(path: Path) -> dict[str, list[dict[str, object]]]:
    if not path.exists():
        raise FileNotFoundError(f"price history csv missing: {path}")
    grouped: dict[str, list[dict[str, object]]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = set(reader.fieldnames or [])
        missing = sorted(REQUIRED_PRICE_COLUMNS - fieldnames)
        if missing:
            raise ValueError(f"price history csv missing columns: {', '.join(missing)}")
        for row in reader:
            ts_code = _normalize_text(row.get("ts_code"))
            if not ts_code:
                continue
            grouped.setdefault(ts_code, []).append(
                {
                    "trade_date": _date_key(row.get("trade_date")),
                    "close": _normalize_float(row.get("close"), field_name="close"),
                }
            )
    for rows in grouped.values():
        rows.sort(key=lambda item: str(item["trade_date"]))
    return grouped


def _window_points(rows: list[dict[str, object]], *, window_start: str, window_end: str) -> list[dict[str, object]]:
    start = _date_key(window_start)
    end = _date_key(window_end)
    if end < start:
        raise ValueError("window_end must be greater than or equal to window_start")
    return [row for row in rows if start <= str(row["trade_date"]) <= end]


def _return_from_points(points: list[dict[str, object]]) -> float:
    if len(points) < 2:
        raise ValueError("price history must include at least two points in the observation window")
    start_close = float(points[0]["close"])
    end_close = float(points[-1]["close"])
    if start_close <= 0:
        raise ValueError("start close must be positive")
    return round(end_close / start_close - 1.0, 6)


def _max_drawdown(series: list[float]) -> float:
    if not series:
        return 0.0
    peak = series[0]
    max_drawdown = 0.0
    for value in series:
        if value > peak:
            peak = value
        if peak <= 0:
            raise ValueError("portfolio value peak must be positive")
        drawdown = value / peak - 1.0
        if drawdown < max_drawdown:
            max_drawdown = drawdown
    return round(max_drawdown, 6)


def _portfolio_value_series(
    items: list[dict[str, object]],
    grouped_prices: dict[str, list[dict[str, object]]],
    *,
    window_start: str,
    window_end: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    per_item: list[dict[str, object]] = []
    date_values: dict[str, float] = {}
    for item in items:
        ts_code = _normalize_text(item.get("ts_code"))
        weight = float(item.get("weight", 0.0) or 0.0)
        points = _window_points(grouped_prices.get(ts_code, []), window_start=window_start, window_end=window_end)
        item_return = _return_from_points(points)
        start_close = float(points[0]["close"])
        for point in points:
            date = str(point["trade_date"])
            normalized_value = float(point["close"]) / start_close
            date_values[date] = date_values.get(date, 0.0) + weight * normalized_value
        per_item.append(
            {
                "ts_code": ts_code,
                "stock_name": item.get("stock_name"),
                "industry": item.get("industry"),
                "weight": weight,
                "start_trade_date": points[0]["trade_date"],
                "end_trade_date": points[-1]["trade_date"],
                "start_close": start_close,
                "end_close": float(points[-1]["close"]),
                "observed_return": item_return,
                "weighted_contribution": round(weight * item_return, 6),
            }
        )
    value_series = [
        {"trade_date": date, "value": round(value, 8)}
        for date, value in sorted(date_values.items(), key=lambda pair: pair[0])
    ]
    return per_item, value_series


def _industry_contributions(per_item: list[dict[str, object]]) -> dict[str, float]:
    contributions: dict[str, float] = {}
    for item in per_item:
        industry = str(item.get("industry") or "unknown")
        contributions[industry] = round(contributions.get(industry, 0.0) + float(item["weighted_contribution"]), 6)
    return contributions


def build_primary_result_candidate_basket_observation(
    *,
    basket_snapshot_path: str | Path,
    price_history_path: str | Path,
    benchmark_ts_code: str,
    window_start: str,
    window_end: str,
    min_success_return: float = 0.0,
    max_drawdown_floor: float = -0.08,
    output_path: str | Path | None = None,
) -> tuple[int, dict[str, object]]:
    resolved_snapshot_path = resolve_project_path(basket_snapshot_path)
    resolved_price_history_path = resolve_project_path(price_history_path)
    snapshot = _read_json(resolved_snapshot_path)
    if snapshot.get("basket_version") != PRIMARY_RESULT_CANDIDATE_BASKET_VERSION:
        raise ValueError("candidate basket snapshot version is invalid")
    if snapshot.get("status") not in {"approved", "conditional"}:
        raise ValueError("candidate basket observation requires approved or conditional basket snapshot")
    items = snapshot.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError("candidate basket snapshot must include items")
    grouped_prices = _load_price_rows(resolved_price_history_path)
    per_item, value_series = _portfolio_value_series(
        [dict(item) for item in items if isinstance(item, dict)],
        grouped_prices,
        window_start=window_start,
        window_end=window_end,
    )
    if len(value_series) < 2:
        raise ValueError("basket price history must include at least two portfolio value points")
    basket_return = round(float(value_series[-1]["value"]) / float(value_series[0]["value"]) - 1.0, 6)
    max_drawdown = _max_drawdown([float(row["value"]) for row in value_series])
    benchmark_points = _window_points(grouped_prices.get(benchmark_ts_code, []), window_start=window_start, window_end=window_end)
    benchmark_return = _return_from_points(benchmark_points)
    excess_return = round(basket_return - benchmark_return, 6)
    success = basket_return >= min_success_return and max_drawdown >= max_drawdown_floor
    payload = {
        "observation_version": PRIMARY_RESULT_CANDIDATE_BASKET_OBSERVATION_VERSION,
        "generated_at": _utc_now_iso(),
        "status": "completed" if success else "failed",
        "basket_id": snapshot.get("basket_id"),
        "source_basket_snapshot_path": str(resolved_snapshot_path),
        "source_basket_snapshot_hash": sha256_file(resolved_snapshot_path),
        "source_price_history_path": str(resolved_price_history_path),
        "source_price_history_hash": sha256_file(resolved_price_history_path),
        "benchmark_ts_code": benchmark_ts_code,
        "observation_window": {"started_at": window_start, "ended_at": window_end, "status": "closed"},
        "metrics": {
            "basket_return": basket_return,
            "benchmark_return": benchmark_return,
            "excess_return": excess_return,
            "max_drawdown": max_drawdown,
            "item_total": len(per_item),
        },
        "completion_criteria": {
            "min_success_return": min_success_return,
            "max_drawdown_floor": max_drawdown_floor,
            "passed": success,
        },
        "value_series": value_series,
        "item_contributions": per_item,
        "industry_contributions": _industry_contributions(per_item),
        "production_boundary": (
            "candidate basket observation measures a registered basket artifact; it does not replace the /stock primary result, "
            "trade, promote baselines, or mutate the basket registry"
        ),
    }
    if output_path:
        _write_json(resolve_project_path(output_path), payload)
    return (0 if success else 1), payload


class PrimaryResultCandidateBasketPerformanceLedger:
    def __init__(
        self,
        *,
        ledger_path: str | Path = "artifacts/primary_result_candidate_baskets/performance_ledger.jsonl",
        summary_path: str | Path = "artifacts/primary_result_candidate_baskets/performance_summary.json",
    ) -> None:
        self.ledger_path = resolve_project_path(ledger_path)
        self.summary_path = resolve_project_path(summary_path)

    def list_entries(self) -> list[dict[str, object]]:
        if not self.ledger_path.exists():
            return []
        entries = []
        for line in self.ledger_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                entry = json.loads(line)
                if not isinstance(entry, dict):
                    raise ValueError(f"expected object jsonl entry: {self.ledger_path}")
                entries.append(entry)
        return entries

    def append_observation(self, *, observation_path: str | Path) -> dict[str, object]:
        resolved_observation_path = resolve_project_path(observation_path)
        observation = _read_json(resolved_observation_path)
        if observation.get("observation_version") != PRIMARY_RESULT_CANDIDATE_BASKET_OBSERVATION_VERSION:
            raise ValueError("candidate basket observation version is invalid")
        status = _normalize_text(observation.get("status")).lower()
        if status not in {"completed", "failed"}:
            raise ValueError("candidate basket performance ledger only accepts completed or failed observations")
        window = dict(observation.get("observation_window", {}) or {})
        metrics = dict(observation.get("metrics", {}) or {})
        entry = {
            "ledger_version": PRIMARY_RESULT_CANDIDATE_BASKET_PERFORMANCE_LEDGER_VERSION,
            "entry_id": f"{observation.get('basket_id')}:{window.get('ended_at')}:{status}",
            "recorded_at": _utc_now_iso(),
            "basket_id": observation.get("basket_id"),
            "outcome": "success" if status == "completed" else "failed",
            "window_started_at": window.get("started_at"),
            "window_ended_at": window.get("ended_at"),
            "basket_return": metrics.get("basket_return"),
            "benchmark_return": metrics.get("benchmark_return"),
            "excess_return": metrics.get("excess_return"),
            "max_drawdown": metrics.get("max_drawdown"),
            "source_observation_path": str(resolved_observation_path),
            "source_observation_hash": sha256_file(resolved_observation_path),
        }
        entries = self.list_entries()
        if any(existing.get("entry_id") == entry["entry_id"] for existing in entries):
            raise FileExistsError(f"candidate basket performance entry already exists: {entry['entry_id']}")
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        with self.ledger_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
        _write_json(self.summary_path, summarize_candidate_basket_performance(entries + [entry]))
        return entry


def summarize_candidate_basket_performance(entries: list[dict[str, object]]) -> dict[str, object]:
    total = len(entries)
    success_total = sum(1 for entry in entries if entry.get("outcome") == "success")
    basket_returns = [float(entry["basket_return"]) for entry in entries if entry.get("basket_return") is not None]
    excess_returns = [float(entry["excess_return"]) for entry in entries if entry.get("excess_return") is not None]
    drawdowns = [float(entry["max_drawdown"]) for entry in entries if entry.get("max_drawdown") is not None]
    return {
        "summary_version": PRIMARY_RESULT_CANDIDATE_BASKET_PERFORMANCE_SUMMARY_VERSION,
        "generated_at": _utc_now_iso(),
        "entry_total": total,
        "success_total": success_total,
        "failed_total": total - success_total,
        "success_rate": round(success_total / total, 6) if total else None,
        "average_basket_return": round(sum(basket_returns) / len(basket_returns), 6) if basket_returns else None,
        "average_excess_return": round(sum(excess_returns) / len(excess_returns), 6) if excess_returns else None,
        "worst_max_drawdown": min(drawdowns) if drawdowns else None,
        "latest_entry_id": entries[-1].get("entry_id") if entries else None,
    }


def current_basket_snapshot_path(*, baskets_dir: str | Path = "artifacts/primary_result_candidate_baskets") -> Path:
    pointer = PrimaryResultCandidateBasketRegistry(baskets_dir=baskets_dir).current()
    snapshot_path = _normalize_text(pointer.get("snapshot_path"))
    if not snapshot_path:
        raise FileNotFoundError("candidate basket current pointer is missing")
    return resolve_project_path(snapshot_path)
