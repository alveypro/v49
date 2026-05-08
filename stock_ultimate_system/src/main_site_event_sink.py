from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from src.utils.project_paths import resolve_project_path


class JsonlMainSiteEventSink:
    def __init__(self, path: str | Path) -> None:
        self.path = resolve_project_path(path)

    def write_event(self, event: dict[str, object]) -> Path:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
        return self.path


def read_main_site_event_jsonl(path: str | Path) -> list[dict[str, object]]:
    resolved_path = resolve_project_path(path)
    if not resolved_path.exists():
        return []
    events: list[dict[str, object]] = []
    for line in resolved_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"expected object jsonl event: {resolved_path}")
        events.append(payload)
    return events


def replay_main_site_event_jsonl(path: str | Path) -> Iterable[dict[str, object]]:
    yield from read_main_site_event_jsonl(path)
