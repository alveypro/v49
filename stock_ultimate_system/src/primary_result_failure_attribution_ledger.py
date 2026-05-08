from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.artifact_registry import sha256_file
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_FAILURE_ATTRIBUTION_LEDGER_VERSION = "primary_result_failure_attribution_ledger.v1"
SUPPORTED_ATTRIBUTION_VERSION = "primary_result_failure_attribution.v1"


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


def _append_jsonl(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


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


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _category_names(attribution: dict[str, object]) -> list[str]:
    items = attribution.get("contributing_categories")
    if not isinstance(items, list):
        return []
    names: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        name = _normalize_text(item.get("category"))
        if name:
            names.append(name)
    return names


def _category_case_count(entries: list[dict[str, object]], category: str) -> int:
    total = 0
    for entry in entries:
        names = entry.get("contributing_category_names") or []
        if isinstance(names, list) and category in names:
            total += 1
    return total


class PrimaryResultFailureAttributionLedger:
    def __init__(
        self,
        *,
        ledger_path: str | Path = "artifacts/primary_result_failure_attribution/ledger.jsonl",
        summary_path: str | Path = "artifacts/primary_result_failure_attribution/summary.json",
    ) -> None:
        self.ledger_path = resolve_project_path(ledger_path)
        self.summary_path = resolve_project_path(summary_path)
        self.ensure_layout()

    def ensure_layout(self) -> None:
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        self.summary_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.summary_path.exists():
            self._write_summary()

    def append_attribution(self, *, attribution_path: str | Path) -> dict[str, object]:
        resolved_path = resolve_project_path(attribution_path)
        if not resolved_path.exists():
            raise FileNotFoundError(f"primary result failure attribution artifact missing: {resolved_path}")
        attribution = _read_json(resolved_path)
        if attribution.get("attribution_version") != SUPPORTED_ATTRIBUTION_VERSION:
            raise ValueError("primary result failure attribution version is invalid")
        if attribution.get("status") != "passed":
            raise ValueError("primary result failure attribution must be passed")

        entry = {
            "ledger_version": PRIMARY_RESULT_FAILURE_ATTRIBUTION_LEDGER_VERSION,
            "recorded_at": _utc_now_iso(),
            "generated_at": attribution.get("generated_at"),
            "result_id": attribution.get("result_id"),
            "ts_code": attribution.get("ts_code"),
            "stock_name": attribution.get("stock_name"),
            "observation_status": attribution.get("observation_status"),
            "outcome": attribution.get("outcome"),
            "attribution_required": bool(attribution.get("attribution_required")),
            "primary_failure_category": attribution.get("primary_failure_category"),
            "contributing_category_names": _category_names(attribution),
            "source_attribution_path": str(resolved_path),
            "source_attribution_hash": sha256_file(resolved_path),
        }
        _append_jsonl(self.ledger_path, entry)
        self._write_summary()
        return entry

    def list_entries(self) -> list[dict[str, object]]:
        return _load_jsonl(self.ledger_path)

    def _write_summary(self) -> None:
        entries = self.list_entries()
        category_counts: dict[str, int] = {}
        primary_counts: dict[str, int] = {}
        for entry in entries:
            primary = _normalize_text(entry.get("primary_failure_category"))
            if primary:
                primary_counts[primary] = primary_counts.get(primary, 0) + 1
            for name in entry.get("contributing_category_names") or []:
                if not isinstance(name, str) or not name:
                    continue
                category_counts[name] = category_counts.get(name, 0) + 1

        summary = {
            "summary_version": PRIMARY_RESULT_FAILURE_ATTRIBUTION_LEDGER_VERSION,
            "generated_at": _utc_now_iso(),
            "status": "passed",
            "entry_total": len(entries),
            "attribution_required_total": sum(1 for entry in entries if bool(entry.get("attribution_required"))),
            "failed_total": sum(1 for entry in entries if entry.get("outcome") == "failed"),
            "success_total": sum(1 for entry in entries if entry.get("outcome") == "success"),
            "primary_failure_category_counts": primary_counts,
            "contributing_category_counts": category_counts,
            "false_positive_cases": sum(1 for entry in entries if entry.get("outcome") == "failed"),
            "missed_winner_cases": _category_case_count(entries, "weak_success"),
            "rank_too_low_cases": _category_case_count(entries, "weak_source_signal"),
            "risk_gate_blocked_but_later_strong_cases": _category_case_count(entries, "source_risk_mismatch"),
            "evidence_insufficient_cases": category_counts.get("data_quality_failure", 0),
            "regime_mismatch_cases": category_counts.get("market_drag", 0),
            "risk_control_failure_cases": category_counts.get("risk_control_failure", 0),
            "benchmark_underperformance_cases": category_counts.get("benchmark_underperformance", 0),
            "negative_absolute_return_cases": category_counts.get("negative_absolute_return", 0),
            "source_risk_mismatch_cases": category_counts.get("source_risk_mismatch", 0),
            "weak_source_signal_cases": category_counts.get("weak_source_signal", 0),
            "weak_success_cases": category_counts.get("weak_success", 0),
            "unclassified_failure_cases": category_counts.get("unclassified_failure", 0),
            "supported_taxonomy_fields": [
                "false_positive_cases",
                "missed_winner_cases",
                "rank_too_low_cases",
                "risk_gate_blocked_but_later_strong_cases",
                "evidence_insufficient_cases",
                "regime_mismatch_cases",
                "risk_control_failure_cases",
                "benchmark_underperformance_cases",
                "negative_absolute_return_cases",
                "source_risk_mismatch_cases",
                "weak_source_signal_cases",
                "weak_success_cases",
                "unclassified_failure_cases",
            ],
            "production_boundary": (
                "failure attribution ledger and summary only accumulate governed attribution outcomes; "
                "they do not mutate strategy rules, promote baselines, or override candidate quality evaluation"
            ),
        }
        _write_json(self.summary_path, summary)
