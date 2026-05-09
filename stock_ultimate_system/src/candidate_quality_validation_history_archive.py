from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.artifact_registry import sha256_file
from src.utils.project_paths import resolve_experiments_path


CANDIDATE_QUALITY_VALIDATION_HISTORY_ARCHIVE_VERSION = "candidate_quality_validation_history_archive.v1"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def build_candidate_quality_validation_history_archive(
    *,
    exp_dir: str | Path = "data/experiments",
) -> tuple[int, dict[str, Any]]:
    resolved_exp_dir = resolve_experiments_path(exp_dir)
    latest_path = resolved_exp_dir / "candidates_basket_validation_latest.json"
    if not latest_path.exists():
        payload = {
            "archive_version": CANDIDATE_QUALITY_VALIDATION_HISTORY_ARCHIVE_VERSION,
            "generated_at": _utc_now_iso(),
            "status": "blocked",
            "blocking_reasons": ["candidate_quality_validation_latest_missing"],
            "output_path": None,
            "source_path": str(latest_path),
            "production_boundary": (
                "validation history archive only snapshots the current formal candidate validation artifact into dated history; "
                "it does not infer missing validation results or modify candidate quality evaluation"
            ),
        }
        return 0, payload

    latest_payload = _read_json(latest_path)
    summary = latest_payload.get("summary") or {}
    if not isinstance(summary, dict):
        summary = {}
    rebalance_dates = int(summary.get("rebalance_dates") or 0)
    if rebalance_dates <= 0:
        payload = {
            "archive_version": CANDIDATE_QUALITY_VALIDATION_HISTORY_ARCHIVE_VERSION,
            "generated_at": _utc_now_iso(),
            "status": "blocked",
            "blocking_reasons": ["candidate_quality_validation_rebalance_dates_missing_or_zero"],
            "output_path": None,
            "source_path": str(latest_path),
            "rebalance_dates": rebalance_dates,
            "production_boundary": (
                "validation history archive only snapshots current formal validation results with positive rebalance coverage; "
                "it does not fabricate historical evidence from empty or placeholder latest artifacts"
            ),
        }
        return 0, payload

    stamp = _utc_now().strftime("%Y%m%d_%H%M%S")
    output_path = resolved_exp_dir / f"candidates_basket_validation_{stamp}.json"
    _write_json(output_path, latest_payload)

    payload = {
        "archive_version": CANDIDATE_QUALITY_VALIDATION_HISTORY_ARCHIVE_VERSION,
        "generated_at": _utc_now_iso(),
        "status": "passed",
        "blocking_reasons": [],
        "output_path": str(output_path),
        "source_path": str(latest_path),
        "source_sha256": sha256_file(latest_path),
        "archived_sha256": sha256_file(output_path),
        "rebalance_dates": rebalance_dates,
        "production_boundary": (
            "validation history archive only snapshots the current formal candidate validation artifact into dated history; "
            "it does not infer missing validation results or modify candidate quality evaluation"
        ),
    }
    return 0, payload
