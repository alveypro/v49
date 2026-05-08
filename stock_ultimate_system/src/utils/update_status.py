from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from src.utils.project_paths import resolve_project_path
from src.utils.serialization import save_json


def update_status_path(project_root: str | Path | None = None) -> Path:
    root = resolve_project_path(project_root or "")
    return root / "data" / "experiments" / "update_status_latest.json"


def load_update_status_payload(project_root: str | Path | None = None) -> dict[str, Any]:
    path = update_status_path(project_root)
    if not path.exists():
        return {}
    try:
        import json

        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_update_status_payload(payload: dict[str, Any], project_root: str | Path | None = None) -> Path:
    path = update_status_path(project_root)
    save_json(payload, str(path))
    return path


def _merge_mapping(base: dict[str, Any], extra: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(base)
    if isinstance(extra, dict):
        merged.update(extra)
    return merged


def record_manual_run(
    *,
    run_type: str,
    ok: bool,
    detail: str = "",
    config_dir: str = "config",
    meta: dict[str, Any] | None = None,
    project_root: str | Path | None = None,
    started_at: str | None = None,
    ended_at: str | None = None,
) -> dict[str, Any]:
    payload = load_update_status_payload(project_root)
    now = datetime.now().isoformat()
    started_at = str(started_at or now)
    ended_at = str(ended_at or now)

    entry = {
        "ok": bool(ok),
        "detail": str(detail or ""),
        "config_dir": str(config_dir or "config"),
        "started_at": started_at,
        "ended_at": ended_at,
        "meta": meta or {},
    }

    manual_runs = payload.get("manual_runs", {}) or {}
    if not isinstance(manual_runs, dict):
        manual_runs = {}
    manual_runs[str(run_type)] = entry
    payload["manual_runs"] = manual_runs

    if run_type == "candidates":
        payload["post_candidates"] = {"ok": bool(ok), "detail": str(detail or "")}
        payload["post_candidates_meta"] = _merge_mapping(payload.get("post_candidates_meta", {}) or {}, meta)
    elif run_type == "daily_research":
        payload["post_daily_research"] = {"ok": bool(ok), "detail": str(detail or "")}
    elif run_type == "evolution":
        payload["manual_evolution"] = entry

    payload["last_manual_run_at"] = ended_at
    write_update_status_payload(payload, project_root)
    return payload
