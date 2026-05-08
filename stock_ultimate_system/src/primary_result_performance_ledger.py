from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from src.artifact_registry import sha256_file
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_PERFORMANCE_LEDGER_VERSION = "primary_result_performance_ledger.v1"
PRIMARY_RESULT_PERFORMANCE_SUMMARY_VERSION = "primary_result_performance_summary.v1"
TERMINAL_OBSERVATION_STATUSES = {"completed", "failed"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_float(value: object, *, field_name: str) -> float:
    if value is None:
        raise ValueError(f"{field_name} is required")
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    try:
        return float(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be numeric") from exc


def _load_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"expected object jsonl entry: {path}")
        rows.append(payload)
    return rows


@dataclass(frozen=True)
class PrimaryResultPerformanceEntry:
    entry_id: str
    recorded_at: str
    result_id: str
    ts_code: str
    stock_name: str | None
    observation_status: str
    outcome: str
    window_started_at: str
    window_ended_at: str
    observed_return: float
    benchmark_return: float | None
    excess_return: float | None
    max_drawdown: float
    completion_criteria_passed: bool
    source_observation_path: str
    source_observation_hash: str
    ledger_version: str = PRIMARY_RESULT_PERFORMANCE_LEDGER_VERSION

    def as_dict(self) -> dict[str, object]:
        return {
            "ledger_version": self.ledger_version,
            "entry_id": self.entry_id,
            "recorded_at": self.recorded_at,
            "result_id": self.result_id,
            "ts_code": self.ts_code,
            "stock_name": self.stock_name,
            "observation_status": self.observation_status,
            "outcome": self.outcome,
            "window_started_at": self.window_started_at,
            "window_ended_at": self.window_ended_at,
            "observed_return": self.observed_return,
            "benchmark_return": self.benchmark_return,
            "excess_return": self.excess_return,
            "max_drawdown": self.max_drawdown,
            "completion_criteria_passed": self.completion_criteria_passed,
            "source_observation_path": self.source_observation_path,
            "source_observation_hash": self.source_observation_hash,
        }


class PrimaryResultPerformanceLedger:
    def __init__(
        self,
        *,
        ledger_path: str | Path = "artifacts/primary_result_performance/ledger.jsonl",
        summary_path: str | Path = "artifacts/primary_result_performance/summary.json",
    ) -> None:
        self.ledger_path = resolve_project_path(ledger_path)
        self.summary_path = resolve_project_path(summary_path)

    def list_entries(self) -> list[dict[str, object]]:
        return _load_jsonl(self.ledger_path)

    def append_observation(
        self,
        *,
        observation_path: str | Path,
        recorded_at: str | None = None,
    ) -> dict[str, object]:
        resolved_observation_path = resolve_project_path(observation_path)
        observation = self._validate_observation(resolved_observation_path)
        entry = self._build_entry(
            observation=observation,
            observation_path=resolved_observation_path,
            recorded_at=recorded_at or _utc_now_iso(),
        )
        entries = self.list_entries()
        if any(existing.get("entry_id") == entry.entry_id for existing in entries):
            raise FileExistsError(f"primary result performance ledger entry already exists: {entry.entry_id}")

        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        with self.ledger_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry.as_dict(), ensure_ascii=False, sort_keys=True) + "\n")
        summary = summarize_primary_result_performance(entries + [entry.as_dict()])
        _write_json(self.summary_path, summary)
        return entry.as_dict()

    def _validate_observation(self, observation_path: Path) -> dict[str, object]:
        if not observation_path.exists():
            raise FileNotFoundError(f"primary result observation artifact missing: {observation_path}")
        observation = _read_json(observation_path)
        if observation.get("observation_version") != "primary_result_observation.v1":
            raise ValueError("primary result observation version is invalid")
        observation_status = _normalize_text(observation.get("observation_status")).lower()
        if observation_status not in TERMINAL_OBSERVATION_STATUSES:
            raise ValueError("primary result performance ledger only accepts completed or failed observations")
        window = observation.get("observation_window")
        if not isinstance(window, dict):
            raise ValueError("primary result observation missing observation_window")
        if not window.get("started_at") or not window.get("ended_at"):
            raise ValueError("primary result observation window must be closed")
        metrics = observation.get("observation_metrics")
        if not isinstance(metrics, dict):
            raise ValueError("primary result observation missing observation_metrics")
        _normalize_float(metrics.get("observed_return"), field_name="observed_return")
        _normalize_float(metrics.get("max_drawdown"), field_name="max_drawdown")
        return observation

    def _build_entry(
        self,
        *,
        observation: dict[str, object],
        observation_path: Path,
        recorded_at: str,
    ) -> PrimaryResultPerformanceEntry:
        result_id = _normalize_text(observation.get("result_id"))
        ts_code = _normalize_text(observation.get("ts_code"))
        if not result_id:
            raise ValueError("primary result observation missing result_id")
        if not ts_code:
            raise ValueError("primary result observation missing ts_code")
        window = dict(observation.get("observation_window", {}) or {})
        metrics = dict(observation.get("observation_metrics", {}) or {})
        criteria = dict(observation.get("completion_criteria", {}) or {})
        observation_status = _normalize_text(observation.get("observation_status")).lower()
        observed_return = _normalize_float(metrics.get("observed_return"), field_name="observed_return")
        max_drawdown = _normalize_float(metrics.get("max_drawdown"), field_name="max_drawdown")
        benchmark_return = metrics.get("benchmark_return")
        excess_return = metrics.get("excess_return")
        criteria_passed = bool(criteria.get("passed"))
        outcome = "success" if observation_status == "completed" and criteria_passed else "failed"
        window_ended_at = _normalize_text(window.get("ended_at"))
        entry_id = f"{result_id}:{window_ended_at}:{observation_status}"
        return PrimaryResultPerformanceEntry(
            entry_id=entry_id,
            recorded_at=recorded_at,
            result_id=result_id,
            ts_code=ts_code,
            stock_name=_normalize_text(observation.get("stock_name")) or None,
            observation_status=observation_status,
            outcome=outcome,
            window_started_at=_normalize_text(window.get("started_at")),
            window_ended_at=window_ended_at,
            observed_return=observed_return,
            benchmark_return=float(benchmark_return) if benchmark_return is not None else None,
            excess_return=float(excess_return) if excess_return is not None else None,
            max_drawdown=max_drawdown,
            completion_criteria_passed=criteria_passed,
            source_observation_path=str(observation_path),
            source_observation_hash=sha256_file(observation_path),
        )


def summarize_primary_result_performance(entries: list[dict[str, object]]) -> dict[str, object]:
    total = len(entries)
    success_total = sum(1 for entry in entries if entry.get("outcome") == "success")
    failed_total = sum(1 for entry in entries if entry.get("outcome") == "failed")
    observed_returns = [float(entry["observed_return"]) for entry in entries if entry.get("observed_return") is not None]
    excess_returns = [float(entry["excess_return"]) for entry in entries if entry.get("excess_return") is not None]
    drawdowns = [float(entry["max_drawdown"]) for entry in entries if entry.get("max_drawdown") is not None]
    return {
        "summary_version": PRIMARY_RESULT_PERFORMANCE_SUMMARY_VERSION,
        "generated_at": _utc_now_iso(),
        "entry_total": total,
        "success_total": success_total,
        "failed_total": failed_total,
        "success_rate": round(success_total / total, 6) if total else None,
        "average_observed_return": round(sum(observed_returns) / len(observed_returns), 6) if observed_returns else None,
        "average_excess_return": round(sum(excess_returns) / len(excess_returns), 6) if excess_returns else None,
        "worst_max_drawdown": min(drawdowns) if drawdowns else None,
        "latest_entry_id": entries[-1].get("entry_id") if entries else None,
    }
