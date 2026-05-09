from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from src.utils.project_paths import resolve_artifacts_path, resolve_experiments_path


CANDIDATE_QUALITY_BASELINE_SNAPSHOT_VERSION = "candidate_quality_baseline_snapshot.v1"
CANDIDATE_QUALITY_BASELINE_POINTER_VERSION = "candidate_quality_baseline_current.v1"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


@dataclass(frozen=True)
class CandidateQualityBaselineSnapshot:
    baseline_id: str
    evaluation_id: str
    generated_at: str
    run_id: str | None
    result_id: str | None
    source_scope: str
    source_summary_path: str
    snapshot_version: str = CANDIDATE_QUALITY_BASELINE_SNAPSHOT_VERSION

    def as_dict(self) -> dict[str, object]:
        return {
            "snapshot_version": self.snapshot_version,
            "baseline_id": self.baseline_id,
            "evaluation_id": self.evaluation_id,
            "generated_at": self.generated_at,
            "run_id": self.run_id,
            "result_id": self.result_id,
            "source_scope": self.source_scope,
            "source_summary_path": self.source_summary_path,
        }


class CandidateQualityBaselineRegistry:
    def __init__(self, *, baselines_dir: str | Path = "artifacts/candidate_quality_baselines") -> None:
        self.baselines_dir = resolve_artifacts_path(baselines_dir)
        self.history_dir = self.baselines_dir / "history"
        self.current_path = self.baselines_dir / "current.json"
        self.ensure_layout()

    def ensure_layout(self) -> None:
        self.history_dir.mkdir(parents=True, exist_ok=True)
        if not self.current_path.exists():
            _write_json(
                self.current_path,
                {
                    "pointer_version": CANDIDATE_QUALITY_BASELINE_POINTER_VERSION,
                    "baseline_id": None,
                    "evaluation_id": None,
                    "snapshot_path": None,
                    "updated_at": None,
                },
            )

    def get_current_pointer(self) -> dict[str, object]:
        return _read_json(self.current_path)

    def get_current_snapshot(self) -> dict[str, object] | None:
        pointer = self.get_current_pointer()
        snapshot_path = pointer.get("snapshot_path")
        if not snapshot_path:
            return None
        path = Path(str(snapshot_path))
        if not path.exists():
            return None
        return _read_json(path)

    def register(
        self,
        *,
        summary_path: str | Path = "data/experiments/candidate_quality_summary.json",
        baseline_id: str | None = None,
        generated_at: str | None = None,
    ) -> dict[str, object]:
        resolved_summary_path = resolve_experiments_path(summary_path)
        if not resolved_summary_path.exists():
            raise FileNotFoundError(f"candidate quality summary missing: {resolved_summary_path}")
        summary = _read_json(resolved_summary_path)
        evaluation_id = str(summary.get("evaluation_id") or "").strip()
        if not evaluation_id:
            raise ValueError("candidate quality summary missing evaluation_id")
        resolved_baseline_id = str(baseline_id or f"candidate-quality-{evaluation_id}").strip()
        snapshot_path = self.history_dir / f"{resolved_baseline_id}.json"
        if snapshot_path.exists():
            raise FileExistsError(f"candidate quality baseline snapshot already exists: {snapshot_path}")
        snapshot = CandidateQualityBaselineSnapshot(
            baseline_id=resolved_baseline_id,
            evaluation_id=evaluation_id,
            generated_at=generated_at or str(summary.get("generated_at") or _utc_now_iso()),
            run_id=str(summary.get("run_id") or "").strip() or None,
            result_id=str(summary.get("result_id") or "").strip() or None,
            source_scope=str(summary.get("source_scope") or "stock").strip() or "stock",
            source_summary_path=str(resolved_summary_path),
        )
        _write_json(snapshot_path, snapshot.as_dict())
        _write_json(
            self.current_path,
            {
                "pointer_version": CANDIDATE_QUALITY_BASELINE_POINTER_VERSION,
                "baseline_id": resolved_baseline_id,
                "evaluation_id": evaluation_id,
                "snapshot_path": str(snapshot_path),
                "updated_at": generated_at or _utc_now_iso(),
            },
        )
        return snapshot.as_dict()
