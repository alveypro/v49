from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.artifact_registry import sha256_file
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_FAILURE_ATTRIBUTION_VERSION = "primary_result_failure_attribution.v1"
CLOSED_OBSERVATION_STATUSES = {"completed", "failed"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _read_jsonl(path: Path) -> list[dict[str, object]]:
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


def _normalize_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _failed_blocking_checks(observation: dict[str, object]) -> list[dict[str, object]]:
    checks = observation.get("checks")
    if not isinstance(checks, list):
        return []
    failed: list[dict[str, object]] = []
    for check in checks:
        if not isinstance(check, dict):
            continue
        if check.get("severity") == "blocking" and check.get("passed") is False:
            failed.append(check)
    return failed


def _match_ledger_entry(ledger_entries: list[dict[str, object]], *, result_id: str, window_ended_at: str) -> dict[str, object] | None:
    for entry in reversed(ledger_entries):
        if entry.get("result_id") == result_id and entry.get("window_ended_at") == window_ended_at:
            return entry
    return None


def _category(category: str, severity: str, evidence: dict[str, object]) -> dict[str, object]:
    return {
        "category": category,
        "severity": severity,
        "evidence": evidence,
    }


def _actions_for_categories(categories: list[dict[str, object]]) -> list[str]:
    names = {str(item.get("category")) for item in categories}
    actions: list[str] = []
    if "data_quality_failure" in names:
        actions.append("repair observation data quality before using the result for model learning")
    if "risk_control_failure" in names:
        actions.append("tighten drawdown controls or reduce position sizing for similar setups")
    if "benchmark_underperformance" in names:
        actions.append("review selection factors because the result underperformed its benchmark")
    if "negative_absolute_return" in names:
        actions.append("review entry timing and stop policy for negative absolute return cases")
    if "market_drag" in names:
        actions.append("separate market-regime impact from stock-specific selection failure")
    if "source_risk_mismatch" in names:
        actions.append("require manual review or lower allocation when source risk is high")
    if "weak_success" in names:
        actions.append("do not promote weak success cases into high-confidence examples without review")
    if not actions:
        actions.append("no failure attribution required for this closed observation")
    return actions


def build_primary_result_failure_attribution(
    observation_payload: dict[str, object],
    *,
    source_observation_path: str | Path | None = None,
    ledger_entries: list[dict[str, object]] | None = None,
    min_success_return: float = 0.0,
    min_excess_return: float = 0.0,
    max_drawdown_floor: float = -0.08,
    generated_at: str | None = None,
) -> dict[str, object]:
    if observation_payload.get("observation_version") != "primary_result_observation.v1":
        raise ValueError("primary result observation version is invalid")
    observation_status = _normalize_text(observation_payload.get("observation_status")).lower()
    if observation_status not in CLOSED_OBSERVATION_STATUSES:
        raise ValueError("failure attribution requires a closed observation")

    result_id = _normalize_text(observation_payload.get("result_id"))
    ts_code = _normalize_text(observation_payload.get("ts_code"))
    stock_name = _normalize_text(observation_payload.get("stock_name")) or None
    if not result_id:
        raise ValueError("primary result observation missing result_id")
    if not ts_code:
        raise ValueError("primary result observation missing ts_code")

    metrics = observation_payload.get("observation_metrics")
    if not isinstance(metrics, dict):
        raise ValueError("primary result observation missing observation_metrics")
    criteria = observation_payload.get("completion_criteria")
    criteria = criteria if isinstance(criteria, dict) else {}
    window = observation_payload.get("observation_window")
    if not isinstance(window, dict):
        raise ValueError("primary result observation missing observation_window")
    window_ended_at = _normalize_text(window.get("ended_at"))
    if not window_ended_at:
        raise ValueError("primary result observation window must be closed")

    observed_return = _normalize_float(metrics.get("observed_return"))
    benchmark_return = _normalize_float(metrics.get("benchmark_return"))
    excess_return = _normalize_float(metrics.get("excess_return"))
    max_drawdown = _normalize_float(metrics.get("max_drawdown"))
    if observed_return is None:
        raise ValueError("primary result observation missing observed_return")
    if max_drawdown is None:
        raise ValueError("primary result observation missing max_drawdown")

    primary_payload = observation_payload.get("primary_result_payload")
    primary_payload = primary_payload if isinstance(primary_payload, dict) else {}
    risk_level = _normalize_text(primary_payload.get("risk_level")).lower() or None
    signal_level = _normalize_text(primary_payload.get("signal_level")).lower() or None
    criteria_passed = bool(criteria.get("passed"))
    outcome = "success" if observation_status == "completed" and criteria_passed else "failed"
    weak_success = outcome == "success" and excess_return is not None and excess_return < min_excess_return
    attribution_required = outcome == "failed" or weak_success

    categories: list[dict[str, object]] = []
    failed_checks = _failed_blocking_checks(observation_payload)
    if failed_checks:
        categories.append(_category("data_quality_failure", "critical", {"failed_blocking_check_total": len(failed_checks)}))
    if max_drawdown < max_drawdown_floor:
        categories.append(
            _category(
                "risk_control_failure",
                "high",
                {"max_drawdown": max_drawdown, "max_drawdown_floor": max_drawdown_floor},
            )
        )
    if excess_return is not None and excess_return < min_excess_return:
        categories.append(
            _category(
                "benchmark_underperformance",
                "high" if outcome == "failed" else "medium",
                {"excess_return": excess_return, "min_excess_return": min_excess_return},
            )
        )
    if observed_return < min_success_return:
        categories.append(
            _category(
                "negative_absolute_return",
                "high",
                {"observed_return": observed_return, "min_success_return": min_success_return},
            )
        )
    if (
        benchmark_return is not None
        and benchmark_return < 0
        and observed_return < min_success_return
        and (excess_return is None or excess_return >= min_excess_return)
    ):
        categories.append(
            _category(
                "market_drag",
                "medium",
                {"observed_return": observed_return, "benchmark_return": benchmark_return, "excess_return": excess_return},
            )
        )
    if risk_level in {"high", "critical"} and outcome == "failed":
        categories.append(_category("source_risk_mismatch", "medium", {"risk_level": risk_level}))
    if signal_level in {"low", "none"} and outcome == "failed":
        categories.append(_category("weak_source_signal", "medium", {"signal_level": signal_level}))
    if weak_success:
        categories.append(_category("weak_success", "medium", {"excess_return": excess_return}))
    if attribution_required and not categories:
        categories.append(_category("unclassified_failure", "medium", {}))

    priority = [
        "data_quality_failure",
        "risk_control_failure",
        "benchmark_underperformance",
        "negative_absolute_return",
        "market_drag",
        "source_risk_mismatch",
        "weak_source_signal",
        "weak_success",
        "unclassified_failure",
    ]
    primary_failure_category = None
    for name in priority:
        if any(item.get("category") == name for item in categories):
            primary_failure_category = name
            break

    ledger_entry = _match_ledger_entry(ledger_entries or [], result_id=result_id, window_ended_at=window_ended_at)
    source_path = resolve_project_path(source_observation_path) if source_observation_path is not None else None
    return {
        "attribution_version": PRIMARY_RESULT_FAILURE_ATTRIBUTION_VERSION,
        "generated_at": generated_at or _utc_now_iso(),
        "status": "passed",
        "result_id": result_id,
        "ts_code": ts_code,
        "stock_name": stock_name,
        "observation_status": observation_status,
        "outcome": outcome,
        "attribution_required": attribution_required,
        "primary_failure_category": primary_failure_category,
        "contributing_categories": categories,
        "recommended_actions": _actions_for_categories(categories),
        "metrics": {
            "observed_return": observed_return,
            "benchmark_return": benchmark_return,
            "excess_return": excess_return,
            "max_drawdown": max_drawdown,
        },
        "source_context": {
            "risk_level": risk_level,
            "signal_level": signal_level,
            "completion_criteria_passed": criteria_passed,
            "failed_blocking_checks": failed_checks,
        },
        "ledger_entry": ledger_entry,
        "source_observation_path": str(source_path) if source_path is not None else None,
        "source_observation_hash": sha256_file(source_path) if source_path is not None else None,
    }


def build_primary_result_failure_attribution_from_paths(
    *,
    observation_path: str | Path,
    ledger_jsonl_path: str | Path | None = None,
    min_success_return: float = 0.0,
    min_excess_return: float = 0.0,
    max_drawdown_floor: float = -0.08,
) -> dict[str, object]:
    resolved_observation_path = resolve_project_path(observation_path)
    if not resolved_observation_path.exists():
        raise FileNotFoundError(f"primary result observation artifact missing: {resolved_observation_path}")
    ledger_entries = _read_jsonl(resolve_project_path(ledger_jsonl_path)) if ledger_jsonl_path is not None else []
    return build_primary_result_failure_attribution(
        _read_json(resolved_observation_path),
        source_observation_path=resolved_observation_path,
        ledger_entries=ledger_entries,
        min_success_return=min_success_return,
        min_excess_return=min_excess_return,
        max_drawdown_floor=max_drawdown_floor,
    )
