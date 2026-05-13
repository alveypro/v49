from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


OBSERVATION_WAIT_STATUS_FILENAME = "primary_result_observation_wait_status_latest.json"
DAILY_CLOSURE_LATEST_FILENAME = "primary_result_daily_closure_latest.json"


@dataclass(frozen=True)
class DashboardRuntimeEvidence:
    observation_wait_status: dict[str, object]
    daily_closure_latest: dict[str, object]


def load_json_object(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def load_dashboard_runtime_evidence(*, artifacts_root: Path, exp_dir: Path) -> DashboardRuntimeEvidence:
    observation_wait_status = load_json_object(Path(artifacts_root) / OBSERVATION_WAIT_STATUS_FILENAME)
    daily_closure_latest = load_json_object(Path(exp_dir) / DAILY_CLOSURE_LATEST_FILENAME)
    return DashboardRuntimeEvidence(
        observation_wait_status=observation_wait_status,
        daily_closure_latest=daily_closure_latest,
    )
