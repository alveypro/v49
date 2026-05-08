from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.artifact_source_guard import is_rejected_temp_source_path
from src.utils.project_paths import resolve_project_path


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _iter_latest_like_files(artifacts_dir: Path) -> list[Path]:
    files: list[Path] = []
    for path in artifacts_dir.rglob("*"):
        if not path.is_file():
            continue
        if "_quarantine" in path.parts:
            continue
        name = path.name
        if name.endswith("latest.json") or name.endswith("latest.jsonl") or name in {"current.json", "summary.json", "ledger.jsonl"}:
            files.append(path)
    return sorted(files)


def _collect_pollution_hits(payload: Any, *, json_path: str = "$") -> list[dict[str, str]]:
    hits: list[dict[str, str]] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            child_path = f"{json_path}.{key}"
            if key.endswith("_path") or key == "snapshot_path":
                if is_rejected_temp_source_path(value):
                    hits.append(
                        {
                            "json_path": child_path,
                            "value": str(value),
                        }
                    )
            hits.extend(_collect_pollution_hits(value, json_path=child_path))
    elif isinstance(payload, list):
        for index, item in enumerate(payload):
            hits.extend(_collect_pollution_hits(item, json_path=f"{json_path}[{index}]"))
    return hits


def audit_artifact_source_pollution(
    *,
    artifacts_dir: str | Path = "artifacts",
) -> dict[str, Any]:
    resolved_artifacts_dir = resolve_project_path(artifacts_dir)
    polluted_files: list[dict[str, Any]] = []
    for path in _iter_latest_like_files(resolved_artifacts_dir):
        if path.suffix == ".jsonl":
            hits: list[dict[str, str]] = []
            for line_index, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
                if not line.strip():
                    continue
                payload = json.loads(line)
                for hit in _collect_pollution_hits(payload):
                    hits.append(
                        {
                            "line": str(line_index),
                            **hit,
                        }
                    )
        else:
            hits = _collect_pollution_hits(_load_json(path))
        if hits:
            polluted_files.append(
                {
                    "path": str(path),
                    "hit_total": len(hits),
                    "hits": hits,
                }
            )
    return {
        "artifacts_dir": str(resolved_artifacts_dir),
        "polluted_file_total": len(polluted_files),
        "polluted_files": polluted_files,
    }
