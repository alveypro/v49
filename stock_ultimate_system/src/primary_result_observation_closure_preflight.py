from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.artifact_registry import sha256_file
from src.primary_result_observation_metrics import calculate_primary_result_observation_metrics
from src.unified_result_builder import build_primary_result_api_payload
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_OBSERVATION_CLOSURE_PREFLIGHT_VERSION = "primary_result_observation_closure_preflight.v1"


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


def _read_optional_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        return _read_json(path)
    except Exception:
        return {}


def _is_missing_primary_value(value: object) -> bool:
    text = _normalize_text(value)
    return not text or text in {"-", "primary:unavailable", "对象信息暂缺", "对象暂缺", "名称暂缺"}


def _check(name: str, passed: bool, detail: str) -> dict[str, object]:
    return {"name": name, "passed": passed, "detail": detail}


def _default_window_start(exp_dir: Path) -> str | None:
    observation_path = exp_dir / "primary_result_observation_latest.json"
    if not observation_path.exists():
        return None
    observation = _read_json(observation_path)
    window = observation.get("observation_window")
    if not isinstance(window, dict):
        return None
    started_at = window.get("started_at")
    return str(started_at) if started_at else None


def build_primary_result_observation_closure_preflight(
    *,
    exp_dir: str | Path = "data/experiments",
    candidate_index: int = 0,
    price_history_path: str | Path = "data/experiments/primary_result_price_history_latest.csv",
    price_history_manifest_path: str | Path = "data/experiments/primary_result_price_history_manifest_latest.json",
    benchmark_ts_code: str = "BENCHMARK",
    window_start: str | None = None,
    window_end: str | None = None,
    min_success_return: float = 0.0,
    max_drawdown_floor: float = -0.08,
    output_path: str | Path | None = None,
) -> tuple[int, dict[str, object]]:
    resolved_exp_dir = resolve_project_path(exp_dir)
    resolved_price_history_path = resolve_project_path(price_history_path)
    resolved_price_history_manifest_path = resolve_project_path(price_history_manifest_path)
    primary_result = build_primary_result_api_payload(
        resolved_exp_dir,
        candidate_index=candidate_index,
        require_current_pointer=True,
    )
    observation_path = resolved_exp_dir / "primary_result_observation_latest.json"
    observation = _read_json(observation_path) if observation_path.exists() else {}
    if (
        observation
        and (
            primary_result.get("disabled") is True
            or _is_missing_primary_value(primary_result.get("result_id"))
            or _is_missing_primary_value(primary_result.get("ts_code"))
            or _is_missing_primary_value(primary_result.get("execution_status"))
            or _is_missing_primary_value(primary_result.get("rollback_status"))
        )
    ):
        primary_result = build_primary_result_api_payload(
            resolved_exp_dir,
            candidate_index=candidate_index,
            require_current_pointer=False,
        )
        execution = _read_optional_json(resolved_exp_dir / "primary_result_execution_latest.json")
        rollback = _read_optional_json(resolved_exp_dir / "primary_result_rollback_latest.json")
        for key in ("result_id", "ts_code"):
            if _is_missing_primary_value(primary_result.get(key)):
                primary_result[key] = observation.get(key) or execution.get(key) or rollback.get(key)
        if _is_missing_primary_value(primary_result.get("execution_status")):
            primary_result["execution_status"] = execution.get("execution_status")
        if _is_missing_primary_value(primary_result.get("rollback_status")):
            primary_result["rollback_status"] = rollback.get("rollback_status")
    resolved_window_start = window_start or _default_window_start(resolved_exp_dir)

    ts_code = _normalize_text(primary_result.get("ts_code"))
    execution_status = _normalize_text(primary_result.get("execution_status")).lower()
    rollback_status = _normalize_text(primary_result.get("rollback_status")).lower()
    terminal_outcome = _normalize_text(primary_result.get("terminal_outcome")).lower()
    observation_status = _normalize_text(observation.get("observation_status") or primary_result.get("observation_status")).lower()
    price_history_manifest = _read_json(resolved_price_history_manifest_path) if resolved_price_history_manifest_path.exists() else {}
    price_history_hash = sha256_file(resolved_price_history_path) if resolved_price_history_path.exists() else None

    checks = [
        _check("primary_result_identity_present", bool(primary_result.get("result_id") and ts_code), "primary result identity must exist"),
        _check("execution_ready", execution_status in {"ready", "running", "completed"}, "execution must be ready/running/completed"),
        _check("rollback_clear", rollback_status in {"not_required", "completed"}, "rollback must be clear"),
        _check("terminal_not_recorded", not terminal_outcome, "terminal outcome must not already be recorded"),
        _check("observation_artifact_exists", observation_path.exists(), "open observation artifact must exist"),
        _check("observation_is_observing", observation_status == "observing", "observation status must be observing before closure"),
        _check("window_start_present", bool(resolved_window_start), "observation window start is required"),
        _check("window_end_present", bool(window_end), "window_end is required"),
        _check("price_history_exists", resolved_price_history_path.exists(), "local price history CSV must exist"),
        _check(
            "price_history_manifest_exists",
            resolved_price_history_manifest_path.exists(),
            "validated price history manifest must exist",
        ),
        _check(
            "price_history_manifest_valid",
            price_history_manifest.get("artifact_version") == "primary_result_price_history_artifact.v1"
            and price_history_manifest.get("status") == "valid",
            "price history manifest must be valid",
        ),
        _check(
            "price_history_manifest_hash_matches",
            bool(price_history_hash)
            and price_history_manifest.get("source_price_history_hash") == price_history_hash
            and price_history_manifest.get("source_price_history_path") == str(resolved_price_history_path),
            "price history manifest must bind to the same CSV hash and path",
        ),
    ]

    metrics: dict[str, object] | None = None
    metrics_error: str | None = None
    if all(check["passed"] is True for check in checks):
        try:
            metrics = calculate_primary_result_observation_metrics(
                price_history_path=resolved_price_history_path,
                ts_code=ts_code,
                benchmark_ts_code=benchmark_ts_code,
                window_start=str(resolved_window_start),
                window_end=str(window_end),
            )
            checks.append(_check("metrics_calculable", True, "observation metrics can be calculated"))
        except Exception as exc:
            metrics_error = str(exc)
            checks.append(_check("metrics_calculable", False, metrics_error))
    else:
        checks.append(_check("metrics_calculable", False, "prerequisite checks failed"))

    closure_outcome = None
    if metrics is not None:
        observed_return = float(metrics["observed_return"])
        max_drawdown = float(metrics["max_drawdown"])
        return_passed = observed_return >= min_success_return
        drawdown_passed = max_drawdown >= max_drawdown_floor
        checks.extend(
            [
                _check("observed_return_meets_success_floor", return_passed, "observed return must meet success floor"),
                _check("max_drawdown_within_floor", drawdown_passed, "max drawdown must stay within floor"),
            ]
        )
        closure_outcome = "completed" if return_passed and drawdown_passed else "failed"
    else:
        checks.extend(
            [
                _check("observed_return_meets_success_floor", False, "metrics unavailable"),
                _check("max_drawdown_within_floor", False, "metrics unavailable"),
            ]
        )

    blocking_checks = [check for check in checks if check["passed"] is not True]
    if blocking_checks:
        status = "blocked"
    elif closure_outcome == "completed":
        status = "ready_for_terminal_success"
    else:
        status = "closable_failed"

    payload = {
        "preflight_version": PRIMARY_RESULT_OBSERVATION_CLOSURE_PREFLIGHT_VERSION,
        "generated_at": _utc_now_iso(),
        "status": status,
        "closure_outcome": closure_outcome,
        "result_id": primary_result.get("result_id"),
        "ts_code": ts_code,
        "benchmark_ts_code": benchmark_ts_code,
        "window_start": resolved_window_start,
        "window_end": window_end,
        "price_history_manifest_path": str(resolved_price_history_manifest_path),
        "price_history_hash": price_history_hash,
        "metrics": metrics,
        "metrics_error": metrics_error,
        "checks": checks,
        "blocking_reasons": [str(check["detail"]) for check in blocking_checks],
        "production_boundary": (
            "observation closure preflight is diagnostics only; it does not close observation, record terminal outcome, "
            "write performance ledger, trade, or change strategy state"
        ),
    }
    if output_path:
        _write_json(resolve_project_path(output_path), payload)
    return (0 if status == "ready_for_terminal_success" else 1), payload
