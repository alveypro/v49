from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.project_paths import resolve_experiments_path


CANDIDATE_QUALITY_DENSITY_PROGRESS_VERSION = "candidate_quality_density_progress.v1"
TRACKED_WINDOWS = ("60d", "120d")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _load_optional_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return _read_json(path)


def _parse_validation_date_from_name(path: Path) -> date | None:
    stem = path.stem
    prefix = "candidates_basket_validation_"
    if not stem.startswith(prefix):
        return None
    token = stem[len(prefix):].split("_", 1)[0]
    if len(token) != 8 or not token.isdigit():
        return None
    return date(int(token[:4]), int(token[4:6]), int(token[6:8]))


def _window_coverage(validation_dates: list[date], sample_total: int) -> dict[str, Any]:
    if sample_total <= 0 or not validation_dates:
        return {
            "latest_validation_date": None,
            "earliest_validation_date": None,
        }
    selected = sorted(validation_dates)[-sample_total:]
    return {
        "latest_validation_date": selected[-1].isoformat(),
        "earliest_validation_date": selected[0].isoformat(),
    }


def _progress_entry(
    *,
    sample_total: int,
    minimum_required: int,
    status: str,
    validation_dates: list[date],
) -> dict[str, Any]:
    denominator = max(minimum_required, 1)
    progress_ratio = round(min(sample_total / denominator, 1.0), 4)
    remaining = max(minimum_required - sample_total, 0)
    coverage = _window_coverage(validation_dates, sample_total)
    return {
        "sample_total": sample_total,
        "minimum_required": minimum_required,
        "remaining_samples_needed": remaining,
        "progress_ratio": progress_ratio,
        "status": status,
        "latest_validation_date": coverage["latest_validation_date"],
        "earliest_validation_date": coverage["earliest_validation_date"],
    }


def build_candidate_quality_density_progress(
    *,
    exp_dir: str | Path = "data/experiments",
) -> dict[str, Any]:
    resolved_exp_dir = resolve_experiments_path(exp_dir)
    summary = _load_optional_json(resolved_exp_dir / "candidate_quality_summary.json")
    multiwindow_source = _load_optional_json(resolved_exp_dir / "candidate_quality_multiwindow_source.json")

    density_payload = summary.get("multiwindow_sample_density") or {}
    if not isinstance(density_payload, dict):
        density_payload = {}

    validation_dates = [
        parsed
        for parsed in (
            _parse_validation_date_from_name(path)
            for path in sorted(resolved_exp_dir.glob("candidates_basket_validation_*.json"))
        )
        if parsed is not None
    ]

    progress: dict[str, dict[str, Any]] = {}
    blocking_reasons: list[str] = []
    for window in TRACKED_WINDOWS:
        raw = density_payload.get(window) or {}
        if not isinstance(raw, dict):
            raw = {}
        sample_total = int(raw.get("sample_total") or 0)
        minimum_required = int(raw.get("minimum_required") or 0)
        status = str(raw.get("status") or "blocked")
        progress[window] = _progress_entry(
            sample_total=sample_total,
            minimum_required=minimum_required,
            status=status,
            validation_dates=validation_dates,
        )
        if status != "passed":
            blocking_reasons.append(f"{window}_sample_density_insufficient")

    payload = {
        "artifact_version": CANDIDATE_QUALITY_DENSITY_PROGRESS_VERSION,
        "generated_at": _utc_now_iso(),
        "status": "passed" if not blocking_reasons else "blocked",
        "blocking_reasons": blocking_reasons,
        "source_scope": "stock",
        "progress": progress,
        "source_paths": {
            "candidate_quality_summary": str(resolved_exp_dir / "candidate_quality_summary.json"),
            "candidate_quality_multiwindow_source": str(resolved_exp_dir / "candidate_quality_multiwindow_source.json"),
        },
        "source_mode": multiwindow_source.get("source_mode"),
        "validation_history_total": len(validation_dates),
        "production_boundary": (
            "candidate quality density progress only tracks long-window sample thickness and validation history coverage; "
            "it does not score candidates, promote baselines, or override candidate quality evaluation"
        ),
    }
    return payload


def write_candidate_quality_density_progress_artifact(
    payload: dict[str, Any],
    *,
    output_dir: str | Path = "data/experiments",
) -> str:
    resolved_output_dir = resolve_experiments_path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    output_path = resolved_output_dir / "candidate_quality_density_progress.json"
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return str(output_path)
