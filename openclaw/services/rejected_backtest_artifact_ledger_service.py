from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List


JsonDict = Dict[str, Any]


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _sha256_file(path: str) -> str:
    p = Path(str(path or ""))
    if not p.exists() or not p.is_file():
        return ""
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def normalize_rejected_backtest_artifact(item: JsonDict) -> JsonDict:
    artifact_path = str(item.get("artifact_path") or item.get("path") or "").strip()
    strategy = str(item.get("strategy") or "").strip().lower()
    reason = str(item.get("reason") or "").strip()
    if not artifact_path:
        raise ValueError("missing_artifact_path")
    if not strategy:
        raise ValueError("missing_strategy")
    if not reason:
        raise ValueError("missing_reason")
    return {
        "artifact_path": artifact_path,
        "strategy": strategy,
        "reason": reason,
        "reused_as_runtime_default": bool(item.get("reused_as_runtime_default") is True),
        "artifact_sha256": str(item.get("artifact_sha256") or _sha256_file(artifact_path)),
        "rejected_at": str(item.get("rejected_at") or _now_text()),
        "source_run_id": str(item.get("source_run_id") or ""),
        "operator_name": str(item.get("operator_name") or ""),
        "note": str(item.get("note") or ""),
    }


def load_rejected_backtest_artifacts(path: str) -> List[JsonDict]:
    if not str(path or "").strip():
        return []
    p = Path(path)
    if not p.exists():
        return []
    text = p.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if p.suffix.lower() == ".jsonl":
        rows = []
        for line in text.splitlines():
            if not line.strip():
                continue
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                rows.append(normalize_rejected_backtest_artifact(parsed))
        return rows
    payload = json.loads(text)
    if isinstance(payload, list):
        return [normalize_rejected_backtest_artifact(item) for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        items = payload.get("rejected_artifacts")
        if isinstance(items, list):
            return [normalize_rejected_backtest_artifact(item) for item in items if isinstance(item, dict)]
    raise ValueError(f"invalid rejected artifact file: {path}")


def append_rejected_backtest_artifact(
    ledger_path: str,
    *,
    artifact_path: str,
    strategy: str,
    reason: str,
    reused_as_runtime_default: bool = False,
    source_run_id: str = "",
    operator_name: str = "",
    note: str = "",
) -> JsonDict:
    entry = normalize_rejected_backtest_artifact(
        {
            "artifact_path": artifact_path,
            "strategy": strategy,
            "reason": reason,
            "reused_as_runtime_default": reused_as_runtime_default,
            "source_run_id": source_run_id,
            "operator_name": operator_name,
            "note": note,
        }
    )
    p = Path(ledger_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
    return entry


def merge_rejected_backtest_artifacts(*collections: Iterable[JsonDict]) -> List[JsonDict]:
    by_key: Dict[tuple[str, str], JsonDict] = {}
    for collection in collections:
        for item in collection or []:
            normalized = normalize_rejected_backtest_artifact(dict(item))
            key = (normalized["artifact_path"], normalized["strategy"])
            existing = by_key.get(key)
            if existing is None or normalized.get("rejected_at", "") >= existing.get("rejected_at", ""):
                by_key[key] = normalized
    return sorted(by_key.values(), key=lambda row: (row["strategy"], row["artifact_path"]))
