from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ReleasePipelineContext:
    run_id: str
    output_dir: Path
    started_at: str
    finished_at: str | None = None
    benchmark_report_path: Path | None = None
    benchmark_diff_path: Path | None = None
    candidate_quality_diff_path: Path | None = None
    release_gates_path: Path | None = None
    evidence_bundle_path: Path | None = None
    manifest_path: Path | None = None
    blocking_failures: list[str] = field(default_factory=list)
    stage_timings: dict[str, float] = field(default_factory=dict)

    def as_summary(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "output_dir": str(self.output_dir),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "benchmark_report_path": str(self.benchmark_report_path) if self.benchmark_report_path else None,
            "benchmark_diff_path": str(self.benchmark_diff_path) if self.benchmark_diff_path else None,
            "candidate_quality_diff_path": str(self.candidate_quality_diff_path) if self.candidate_quality_diff_path else None,
            "release_gates_path": str(self.release_gates_path) if self.release_gates_path else None,
            "evidence_bundle_path": str(self.evidence_bundle_path) if self.evidence_bundle_path else None,
            "manifest_path": str(self.manifest_path) if self.manifest_path else None,
            "blocking_failures": list(self.blocking_failures),
            "stage_timings": dict(self.stage_timings),
        }
