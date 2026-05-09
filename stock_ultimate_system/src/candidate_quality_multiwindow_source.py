from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.utils.project_paths import resolve_experiments_path


CANDIDATE_QUALITY_MULTIWINDOW_SOURCE_VERSION = "candidate_quality_multiwindow_source.v1"
REQUIRED_BUCKETS = ("top1", "top3", "top5", "top10")
REQUIRED_WINDOWS = ("60d", "120d")
MIN_REQUIRED_SAMPLES = {"60d": 3, "120d": 5}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _empty_windows() -> dict[str, dict[str, dict[str, float | None]]]:
    return {
        window: {
            bucket: {
                "avg_return": None,
                "avg_excess_return": None,
                "win_rate": None,
            }
            for bucket in REQUIRED_BUCKETS
        }
        for window in REQUIRED_WINDOWS
    }


def _parse_validation_date_from_name(path: Path) -> date | None:
    stem = path.stem
    prefix = "candidates_basket_validation_"
    if not stem.startswith(prefix):
        return None
    suffix = stem[len(prefix):]
    token = suffix.split("_", 1)[0]
    if len(token) != 8 or not token.isdigit():
        return None
    return date(int(token[:4]), int(token[4:6]), int(token[6:8]))


def _extract_bucket_metrics(payload: dict[str, Any], bucket: str) -> dict[str, float | None]:
    summary = payload.get("summary") or {}
    if not isinstance(summary, dict):
        summary = {}
    variants = payload.get("variants") or {}
    if not isinstance(variants, dict):
        variants = {}
    bucket_payload = variants.get(bucket) or {}
    if not isinstance(bucket_payload, dict):
        bucket_payload = {}

    if bucket == "top1":
        return {
            "avg_return": _safe_float(bucket_payload.get("avg_return_5d") or summary.get("avg_top1_return_5d")),
            "avg_excess_return": _safe_float(bucket_payload.get("avg_excess_return_5d")),
            "win_rate": _safe_float(bucket_payload.get("win_rate_5d")),
        }
    if bucket == "top5":
        return {
            "avg_return": _safe_float(summary.get("avg_basket_return_5d")),
            "avg_excess_return": _safe_float(summary.get("avg_excess_return_5d")),
            "win_rate": _safe_float(summary.get("basket_win_rate_5d")),
        }
    return {
        "avg_return": _safe_float(bucket_payload.get("avg_return_5d")),
        "avg_excess_return": _safe_float(bucket_payload.get("avg_excess_return_5d")),
        "win_rate": _safe_float(bucket_payload.get("win_rate_5d")),
    }


def _mean(values: list[float | None]) -> float | None:
    present = [value for value in values if value is not None]
    if not present:
        return None
    return round(sum(present) / len(present), 6)


def _aggregate_history_window(
    validation_paths: list[Path],
    *,
    latest_validation_date: date,
    lookback_days: int,
) -> tuple[dict[str, dict[str, float | None]], int]:
    window_start = latest_validation_date - timedelta(days=lookback_days - 1)
    selected_paths_by_date: dict[date, Path] = {}
    for path in validation_paths:
        validation_date = _parse_validation_date_from_name(path)
        if validation_date is None or validation_date < window_start or validation_date > latest_validation_date:
            continue
        existing = selected_paths_by_date.get(validation_date)
        if existing is None or path.name > existing.name:
            selected_paths_by_date[validation_date] = path

    selected_payloads = [_read_json(path) for _, path in sorted(selected_paths_by_date.items())]

    aggregated: dict[str, dict[str, float | None]] = {}
    for bucket in REQUIRED_BUCKETS:
        metrics_by_sample = [_extract_bucket_metrics(payload, bucket) for payload in selected_payloads]
        aggregated[bucket] = {
            "avg_return": _mean([item.get("avg_return") for item in metrics_by_sample]),
            "avg_excess_return": _mean([item.get("avg_excess_return") for item in metrics_by_sample]),
            "win_rate": _mean([item.get("win_rate") for item in metrics_by_sample]),
        }
    return aggregated, len(selected_payloads)


def _sample_density_status(sample_total: int, *, minimum_required: int) -> str:
    return "passed" if sample_total >= minimum_required else "blocked"


def _load_window_payload(path: Path, expected_window: str) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = _read_json(path)
    declared_window = str(payload.get("window") or expected_window).strip()
    if declared_window != expected_window:
        raise ValueError(f"window payload mismatch for {path}: expected {expected_window}, got {declared_window}")
    return payload


def build_candidate_quality_multiwindow_source(
    *,
    exp_dir: str | Path = "data/experiments",
    source_60d_path: str | Path | None = None,
    source_120d_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved_exp_dir = resolve_experiments_path(exp_dir)
    resolved_60d = (
        resolve_experiments_path(source_60d_path)
        if source_60d_path is not None
        else resolved_exp_dir / "candidate_quality_window_60d.json"
    )
    resolved_120d = (
        resolve_experiments_path(source_120d_path)
        if source_120d_path is not None
        else resolved_exp_dir / "candidate_quality_window_120d.json"
    )

    blocking_reasons: list[str] = []
    windows = _empty_windows()
    source_paths = {"60d": str(resolved_60d), "120d": str(resolved_120d)}
    source_mode = "explicit_window_files" if source_60d_path is not None or source_120d_path is not None else "validation_history"
    sample_totals = {"60d": 0, "120d": 0}
    sample_density: dict[str, dict[str, int | str]] = {
        window: {
            "sample_total": 0,
            "minimum_required": MIN_REQUIRED_SAMPLES[window],
            "status": "blocked",
        }
        for window in REQUIRED_WINDOWS
    }

    if source_mode == "explicit_window_files":
        for window, path in (("60d", resolved_60d), ("120d", resolved_120d)):
            payload = _load_window_payload(path, window)
            if payload is None:
                blocking_reasons.append(f"{window}_source_missing")
                continue
            sample_total = int(payload.get("sample_total") or 0)
            sample_totals[window] = sample_total
            sample_density[window] = {
                "sample_total": sample_total,
                "minimum_required": MIN_REQUIRED_SAMPLES[window],
                "status": _sample_density_status(sample_total, minimum_required=MIN_REQUIRED_SAMPLES[window]),
            }
            if sample_total < MIN_REQUIRED_SAMPLES[window]:
                blocking_reasons.append(f"{window}_sample_density_insufficient")
            buckets = payload.get("buckets") or {}
            if not isinstance(buckets, dict):
                buckets = {}
            for bucket in REQUIRED_BUCKETS:
                bucket_payload = buckets.get(bucket) or {}
                if not isinstance(bucket_payload, dict):
                    bucket_payload = {}
                windows[window][bucket] = {
                    "avg_return": _safe_float(bucket_payload.get("avg_return")),
                    "avg_excess_return": _safe_float(bucket_payload.get("avg_excess_return")),
                    "win_rate": _safe_float(bucket_payload.get("win_rate")),
                }
                if windows[window][bucket]["avg_return"] is None:
                    blocking_reasons.append(f"missing_{window}_{bucket}_avg_return")
                if windows[window][bucket]["avg_excess_return"] is None:
                    blocking_reasons.append(f"missing_{window}_{bucket}_avg_excess_return")
    else:
        validation_paths = sorted(resolved_exp_dir.glob("candidates_basket_validation_*.json"))
        dated_paths = [(path, _parse_validation_date_from_name(path)) for path in validation_paths]
        dated_paths = [(path, value) for path, value in dated_paths if value is not None]
        if not dated_paths:
            blocking_reasons.append("validation_history_missing")
        else:
            latest_validation_date = max(value for _, value in dated_paths)
            ordered_paths = [path for path, _ in dated_paths]
            for window, lookback_days in (("60d", 60), ("120d", 120)):
                aggregated, sample_total = _aggregate_history_window(
                    ordered_paths,
                    latest_validation_date=latest_validation_date,
                    lookback_days=lookback_days,
                )
                windows[window] = aggregated
                sample_totals[window] = sample_total
                sample_density[window] = {
                    "sample_total": sample_total,
                    "minimum_required": MIN_REQUIRED_SAMPLES[window],
                    "status": _sample_density_status(sample_total, minimum_required=MIN_REQUIRED_SAMPLES[window]),
                }
                if sample_total == 0:
                    blocking_reasons.append(f"{window}_validation_history_missing")
                elif sample_total < MIN_REQUIRED_SAMPLES[window]:
                    blocking_reasons.append(f"{window}_sample_density_insufficient")
                for bucket in REQUIRED_BUCKETS:
                    if windows[window][bucket]["avg_return"] is None:
                        blocking_reasons.append(f"missing_{window}_{bucket}_avg_return")
                    if windows[window][bucket]["avg_excess_return"] is None:
                        blocking_reasons.append(f"missing_{window}_{bucket}_avg_excess_return")

    return {
        "artifact_version": CANDIDATE_QUALITY_MULTIWINDOW_SOURCE_VERSION,
        "generated_at": _utc_now_iso(),
        "status": "passed" if not blocking_reasons else "blocked",
        "blocking_reasons": blocking_reasons,
        "source_paths": source_paths,
        "source_mode": source_mode,
        "sample_totals": sample_totals,
        "sample_density": sample_density,
        "windows": windows,
        "production_boundary": (
            "candidate quality multiwindow source only aggregates formally supplied or formally recorded validation history inputs; "
            "it does not infer absent long-window performance, promote baselines, or override candidate quality evaluation"
        ),
    }


def write_candidate_quality_multiwindow_source_artifact(
    payload: dict[str, Any],
    *,
    output_dir: str | Path = "data/experiments",
) -> str:
    resolved_output_dir = resolve_experiments_path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    output_path = resolved_output_dir / "candidate_quality_multiwindow_source.json"
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return str(output_path)
