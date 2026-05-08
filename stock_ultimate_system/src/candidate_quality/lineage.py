from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


CANDIDATE_LINEAGE_VERSION = "candidate_lineage.v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _file_entry(path: str | Path, role: str) -> dict[str, Any]:
    resolved = Path(path)
    entry: dict[str, Any] = {
        "role": role,
        "path": str(resolved),
        "exists": resolved.exists(),
    }
    if resolved.exists():
        stat = resolved.stat()
        entry["size_bytes"] = int(stat.st_size)
        entry["modified_at"] = datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat()
    return entry


def _row_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _infer_data_as_of(data_quality_report: dict[str, Any] | None) -> str:
    if not data_quality_report:
        return ""
    for key in ("expected_latest_trade_date", "trade_date_end", "data_as_of"):
        value = str(data_quality_report.get(key, "") or "").strip()
        if value:
            return value
    return ""


def build_candidate_lineage(
    *,
    candidate_frame: pd.DataFrame,
    run_id: str,
    output_paths: dict[str, str],
    data_quality_report: dict[str, Any] | None = None,
    data_quality_gate: dict[str, Any] | None = None,
    generation_meta: dict[str, Any] | None = None,
    guardrail: dict[str, Any] | None = None,
    validation_result: dict[str, Any] | None = None,
    latest_tag: str = "latest",
) -> dict[str, Any]:
    generation_meta = generation_meta or {}
    guardrail = guardrail or {}
    validation_result = validation_result or {}
    data_as_of = _infer_data_as_of(data_quality_report)
    candidate_source = "formal" if latest_tag == "latest" else "interim"
    model_version = str(generation_meta.get("champion_version", "") or "runtime_candidate_models")
    source_files = [
        _file_entry(output_paths["latest_csv"], "candidate_csv"),
        _file_entry(output_paths["latest_md"], "candidate_markdown"),
        _file_entry(output_paths["latest_summary"], "basket_summary"),
        _file_entry(output_paths["latest_validation"], "validation_summary"),
        _file_entry(output_paths["latest_audit"], "candidate_audit"),
        _file_entry(output_paths["latest_data_quality_gate"], "data_quality_gate"),
    ]
    report_path = str(output_paths.get("data_quality_report", "") or "").strip()
    if report_path:
        source_files.append(_file_entry(report_path, "data_quality_report"))

    candidates: list[dict[str, Any]] = []
    quality_by_code = {
        str(item.get("ts_code") or ""): item for item in (data_quality_gate or {}).get("candidates", []) or []
    }
    for _, row in candidate_frame.iterrows():
        ts_code = str(row.get("ts_code", "") or "").strip()
        quality = quality_by_code.get(ts_code, {})
        rank = int(row.get("rank", 0) or 0)
        candidate_payload = {
            "ts_code": ts_code,
            "rank": rank,
            "stock_name": str(row.get("stock_name", "") or ""),
            "industry": str(row.get("industry", "") or ""),
            "run_id": run_id,
            "data_as_of": data_as_of,
            "candidate_source": candidate_source,
            "model_version": model_version,
            "input_files": [item["path"] for item in source_files if item.get("exists")],
            "generation_mode": "quick" if generation_meta.get("quick_mode") else "full",
            "generation_degraded": bool(generation_meta.get("degraded", False)),
            "generation_reason": str(generation_meta.get("reason", "") or ""),
            "guardrail_mode": str(guardrail.get("mode", "") or ""),
            "data_quality_level": str(quality.get("quality_level", row.get("data_quality_level", "blocked")) or "blocked"),
            "data_quality_score": float(quality.get("quality_score", row.get("data_quality_score", 0.0)) or 0.0),
            "data_quality_blocking_reasons": list(
                quality.get("blocking_reasons")
                or str(row.get("data_quality_blocking_reasons", "") or "").split("|")
            ),
        }
        candidate_payload["lineage_hash"] = _row_hash(candidate_payload)
        candidate_payload["data_quality_blocking_reasons"] = [
            item for item in candidate_payload["data_quality_blocking_reasons"] if item
        ]
        candidates.append(candidate_payload)

    lineage = {
        "schema_version": CANDIDATE_LINEAGE_VERSION,
        "status": "passed",
        "generated_at": _utc_now(),
        "run_id": run_id,
        "data_as_of": data_as_of,
        "candidate_source": candidate_source,
        "candidate_count": len(candidates),
        "model_version": model_version,
        "champion_version": str(generation_meta.get("champion_version", "") or ""),
        "source_files": source_files,
        "generation_meta": dict(generation_meta),
        "guardrail": dict(guardrail),
        "validation_summary": dict(validation_result.get("summary", {}) or {}),
        "data_quality_gate_status": str((data_quality_gate or {}).get("status", "") or ""),
        "candidates": candidates,
    }
    validation = validate_candidate_lineage(lineage)
    lineage["status"] = validation["status"]
    lineage["blocking_reasons"] = validation["blocking_reasons"]
    return lineage


def validate_candidate_lineage(lineage: dict[str, Any]) -> dict[str, Any]:
    blocking_reasons: list[str] = []
    if not str(lineage.get("run_id", "") or "").strip():
        blocking_reasons.append("missing_run_id")
    if not str(lineage.get("data_as_of", "") or "").strip():
        blocking_reasons.append("missing_data_as_of")
    source_files = list(lineage.get("source_files", []) or [])
    if not source_files:
        blocking_reasons.append("missing_source_files")
    elif not any(item.get("role") == "candidate_csv" and item.get("exists") for item in source_files):
        blocking_reasons.append("missing_candidate_csv_source")
    for candidate in lineage.get("candidates", []) or []:
        code = str(candidate.get("ts_code", "") or "").strip()
        if not code:
            blocking_reasons.append("candidate_missing_ts_code")
        if not str(candidate.get("run_id", "") or "").strip():
            blocking_reasons.append(f"{code}:missing_run_id")
        if not str(candidate.get("data_as_of", "") or "").strip():
            blocking_reasons.append(f"{code}:missing_data_as_of")
        if not candidate.get("input_files"):
            blocking_reasons.append(f"{code}:missing_input_files")
    return {
        "status": "failed" if blocking_reasons else "passed",
        "blocking_reasons": blocking_reasons,
    }


def write_candidate_lineage(
    lineage: dict[str, Any],
    *,
    output_dir: str | Path,
    lineage_name: str = "candidate_lineage_latest.json",
) -> str:
    resolved_output_dir = Path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    lineage_path = resolved_output_dir / lineage_name
    lineage_path.write_text(json.dumps(lineage, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(lineage_path)
