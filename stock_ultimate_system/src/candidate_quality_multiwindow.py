from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.project_paths import resolve_experiments_path


CANDIDATE_QUALITY_MULTIWINDOW_VERSION = "candidate_quality_multiwindow.v1"
REQUIRED_BUCKETS = ("top1", "top3", "top5", "top10")
REQUIRED_WINDOWS = ("60d", "120d")


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


def build_candidate_quality_multiwindow(
    *,
    exp_dir: str | Path = "data/experiments",
    source_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved_exp_dir = resolve_experiments_path(exp_dir)
    resolved_source_path = (
        resolve_experiments_path(source_path)
        if source_path is not None
        else resolved_exp_dir / "candidate_quality_multiwindow_source.json"
    )

    windows = _empty_windows()
    blocking_reasons: list[str] = []
    source_payload: dict[str, Any] | None = None
    sample_totals = {"60d": 0, "120d": 0}
    sample_density = {
        "60d": {"sample_total": 0, "minimum_required": 0, "status": "blocked"},
        "120d": {"sample_total": 0, "minimum_required": 0, "status": "blocked"},
    }
    if resolved_source_path.exists():
        source_payload = _read_json(resolved_source_path)
        raw_sample_totals = source_payload.get("sample_totals") or {}
        if isinstance(raw_sample_totals, dict):
            for window in REQUIRED_WINDOWS:
                try:
                    sample_totals[window] = int(raw_sample_totals.get(window) or 0)
                except (TypeError, ValueError):
                    sample_totals[window] = 0
        raw_sample_density = source_payload.get("sample_density") or {}
        if isinstance(raw_sample_density, dict):
            for window in REQUIRED_WINDOWS:
                density = raw_sample_density.get(window) or {}
                if not isinstance(density, dict):
                    density = {}
                sample_density[window] = {
                    "sample_total": int(density.get("sample_total") or sample_totals[window] or 0),
                    "minimum_required": int(density.get("minimum_required") or 0),
                    "status": str(density.get("status") or "blocked"),
                }
        raw_windows = source_payload.get("windows") or {}
        if not isinstance(raw_windows, dict):
            raw_windows = {}
        for window in REQUIRED_WINDOWS:
            raw_window = raw_windows.get(window) or {}
            if not isinstance(raw_window, dict):
                raw_window = {}
            for bucket in REQUIRED_BUCKETS:
                raw_bucket = raw_window.get(bucket) or {}
                if not isinstance(raw_bucket, dict):
                    raw_bucket = {}
                windows[window][bucket] = {
                    "avg_return": _safe_float(raw_bucket.get("avg_return")),
                    "avg_excess_return": _safe_float(raw_bucket.get("avg_excess_return")),
                    "win_rate": _safe_float(raw_bucket.get("win_rate")),
                }
    else:
        blocking_reasons.append("multiwindow_source_missing")

    for window in REQUIRED_WINDOWS:
        if sample_density[window]["status"] != "passed":
            blocking_reasons.append(f"{window}_sample_density_insufficient")

    for window in REQUIRED_WINDOWS:
        for bucket in REQUIRED_BUCKETS:
            metrics = windows[window][bucket]
            if metrics["avg_return"] is None:
                blocking_reasons.append(f"missing_{window}_{bucket}_avg_return")
            if metrics["avg_excess_return"] is None:
                blocking_reasons.append(f"missing_{window}_{bucket}_avg_excess_return")

    return {
        "artifact_version": CANDIDATE_QUALITY_MULTIWINDOW_VERSION,
        "generated_at": _utc_now_iso(),
        "status": "passed" if not blocking_reasons else "blocked",
        "blocking_reasons": blocking_reasons,
        "source_path": str(resolved_source_path),
        "sample_totals": sample_totals,
        "sample_density": sample_density,
        "windows": windows,
        "production_boundary": (
            "candidate quality multiwindow artifact only summarizes supplied validation windows; "
            "it does not infer missing long-window metrics, promote baselines, or mutate formal candidate results"
        ),
        "source_payload_present": source_payload is not None,
    }


def write_candidate_quality_multiwindow_artifact(
    payload: dict[str, Any],
    *,
    output_dir: str | Path = "data/experiments",
) -> str:
    resolved_output_dir = resolve_experiments_path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    output_path = resolved_output_dir / "candidate_quality_multiwindow_latest.json"
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return str(output_path)
