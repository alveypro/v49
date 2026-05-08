from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.artifact_source_guard import assert_mapping_has_no_temp_sources
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_PERFORMANCE_EVIDENCE_VERSION = "primary_result_performance_evidence.v1"
DEFAULT_EVIDENCE_FLOORS = (20, 60, 120)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    entries: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"expected object jsonl entry: {path}")
        assert_mapping_has_no_temp_sources(payload, field_names=("source_observation_path",))
        entries.append(payload)
    return entries


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _float_values(entries: list[dict[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for entry in entries:
        value = entry.get(key)
        if value is None:
            continue
        values.append(float(value))
    return values


def _rate(entries: list[dict[str, Any]], outcome: str) -> float | None:
    if not entries:
        return None
    return round(sum(1 for entry in entries if entry.get("outcome") == outcome) / len(entries), 6)


def _avg(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 6) if values else None


def _metric_summary(entries: list[dict[str, Any]], *, return_key: str) -> dict[str, Any]:
    returns = _float_values(entries, return_key)
    excess_returns = _float_values(entries, "excess_return")
    drawdowns = _float_values(entries, "max_drawdown")
    return {
        "entry_total": len(entries),
        "success_total": sum(1 for entry in entries if entry.get("outcome") == "success"),
        "failed_total": sum(1 for entry in entries if entry.get("outcome") == "failed"),
        "success_rate": _rate(entries, "success"),
        "average_return": _avg(returns),
        "average_excess_return": _avg(excess_returns),
        "worst_max_drawdown": min(drawdowns) if drawdowns else None,
        "latest_entry_id": entries[-1].get("entry_id") if entries else None,
    }


def _window_status(
    *,
    entries: list[dict[str, Any]],
    floor: int,
    return_key: str,
    min_success_rate: float,
    min_average_excess_return: float,
    max_drawdown_floor: float,
) -> dict[str, Any]:
    recent = entries[-floor:] if len(entries) >= floor else entries
    summary = _metric_summary(recent, return_key=return_key)
    sufficient = len(entries) >= floor
    checks = [
        {
            "name": "sample_floor_met",
            "passed": sufficient,
            "detail": f"requires at least {floor} ledger entries",
            "details": {"required": floor, "actual": len(entries)},
        },
        {
            "name": "success_rate_floor_met",
            "passed": summary["success_rate"] is not None and float(summary["success_rate"]) >= min_success_rate,
            "detail": "success rate must meet evidence floor",
            "details": {"required": min_success_rate, "actual": summary["success_rate"]},
        },
        {
            "name": "average_excess_return_floor_met",
            "passed": summary["average_excess_return"] is not None
            and float(summary["average_excess_return"]) >= min_average_excess_return,
            "detail": "average excess return must meet evidence floor",
            "details": {"required": min_average_excess_return, "actual": summary["average_excess_return"]},
        },
        {
            "name": "drawdown_floor_met",
            "passed": summary["worst_max_drawdown"] is not None and float(summary["worst_max_drawdown"]) >= max_drawdown_floor,
            "detail": "worst max drawdown must stay above floor",
            "details": {"required": max_drawdown_floor, "actual": summary["worst_max_drawdown"]},
        },
    ]
    failed_checks = [check for check in checks if check["passed"] is not True]
    if not sufficient:
        status = "accumulating"
    elif failed_checks:
        status = "failed"
    else:
        status = "passed"
    return {
        "floor": floor,
        "status": status,
        "summary": summary,
        "checks": checks,
        "blocking_reasons": [str(check["detail"]) for check in failed_checks],
    }


def _evaluate_stream(
    *,
    stream_id: str,
    entries: list[dict[str, Any]],
    floors: tuple[int, ...],
    return_key: str,
    min_success_rate: float,
    min_average_excess_return: float,
    max_drawdown_floor: float,
) -> dict[str, Any]:
    entries = sorted(entries, key=lambda entry: str(entry.get("window_ended_at") or entry.get("recorded_at") or ""))
    windows = [
        _window_status(
            entries=entries,
            floor=floor,
            return_key=return_key,
            min_success_rate=min_success_rate,
            min_average_excess_return=min_average_excess_return,
            max_drawdown_floor=max_drawdown_floor,
        )
        for floor in floors
    ]
    first_floor = windows[0]
    if first_floor["status"] == "passed":
        status = "evidence_ready"
    elif first_floor["status"] == "failed":
        status = "evidence_failed"
    else:
        status = "accumulating"
    return {
        "stream_id": stream_id,
        "status": status,
        "entry_total": len(entries),
        "all_time_summary": _metric_summary(entries, return_key=return_key),
        "windows": windows,
    }


def build_primary_result_performance_evidence(
    *,
    primary_ledger_jsonl: str | Path = "artifacts/primary_result_performance/ledger.jsonl",
    basket_ledger_jsonl: str | Path = "artifacts/primary_result_candidate_baskets/performance_ledger.jsonl",
    output_path: str | Path | None = None,
    evidence_floors: tuple[int, ...] = DEFAULT_EVIDENCE_FLOORS,
    min_success_rate: float = 0.5,
    min_average_excess_return: float = 0.0,
    max_drawdown_floor: float = -0.08,
) -> tuple[int, dict[str, Any]]:
    floors = tuple(sorted({int(floor) for floor in evidence_floors if int(floor) > 0}))
    if not floors:
        raise ValueError("at least one positive evidence floor is required")
    primary_path = resolve_project_path(primary_ledger_jsonl)
    basket_path = resolve_project_path(basket_ledger_jsonl)
    primary_entries = _read_jsonl(primary_path)
    basket_entries = _read_jsonl(basket_path)

    primary = _evaluate_stream(
        stream_id="primary_result",
        entries=primary_entries,
        floors=floors,
        return_key="observed_return",
        min_success_rate=min_success_rate,
        min_average_excess_return=min_average_excess_return,
        max_drawdown_floor=max_drawdown_floor,
    )
    basket = _evaluate_stream(
        stream_id="candidate_basket",
        entries=basket_entries,
        floors=floors,
        return_key="basket_return",
        min_success_rate=min_success_rate,
        min_average_excess_return=min_average_excess_return,
        max_drawdown_floor=max_drawdown_floor,
    )
    stream_statuses = {primary["status"], basket["status"]}
    if "evidence_failed" in stream_statuses:
        status = "failed"
    elif stream_statuses == {"evidence_ready"}:
        status = "ready"
    else:
        status = "accumulating"
    payload = {
        "evidence_version": PRIMARY_RESULT_PERFORMANCE_EVIDENCE_VERSION,
        "generated_at": _utc_now_iso(),
        "status": status,
        "evidence_floors": list(floors),
        "criteria": {
            "min_success_rate": min_success_rate,
            "min_average_excess_return": min_average_excess_return,
            "max_drawdown_floor": max_drawdown_floor,
        },
        "streams": [primary, basket],
        "blocking_reasons": [
            reason
            for stream in (primary, basket)
            for window in stream["windows"]
            if window["floor"] == floors[0]
            for reason in window["blocking_reasons"]
        ],
        "next_actions": _next_actions(primary=primary, basket=basket, first_floor=floors[0]),
        "evidence_paths": {
            "primary_ledger_jsonl": str(primary_path),
            "basket_ledger_jsonl": str(basket_path),
        },
        "production_boundary": (
            "performance evidence report only evaluates local ledger samples; it does not promote baselines, change strategy rules, "
            "trade, deploy, or infer long-term alpha before evidence floors are met"
        ),
    }
    if output_path:
        _write_json(resolve_project_path(output_path), payload)
    return (0 if status != "failed" else 1), payload


def _next_actions(*, primary: dict[str, Any], basket: dict[str, Any], first_floor: int) -> list[str]:
    actions: list[str] = []
    for stream in (primary, basket):
        missing = max(0, first_floor - int(stream["entry_total"]))
        if missing:
            actions.append(f"{stream['stream_id']} needs {missing} more ledger entries to reach the {first_floor}-sample evidence floor")
        elif stream["status"] == "evidence_failed":
            actions.append(f"{stream['stream_id']} failed the {first_floor}-sample evidence floor; route to review before any promotion")
    if not actions:
        actions.append("first evidence floor is ready for governed promotion review")
    return actions
