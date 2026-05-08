from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def write_release_decision(path: Path, *, decision: str = "approved") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "decision_version": "primary_result_release_decision.v1",
                "decision_id": "release-decision-test001",
                "decided_at": "2026-04-20T08:50:00Z",
                "decision": decision,
                "actor": "release_manager",
                "reason": "test release decision",
                "checklist_id": "checklist-test001",
                "checklist_status": "complete",
                "review_id": "review-test001",
                "result_id": "primary:000001.SZ",
                "ts_code": "000001.SZ",
                "missing_evidence": [],
                "blocking_gate_reason": None,
                "release_pipeline_allowed": decision == "approved",
                "baseline_promotion_allowed": decision == "approved",
                "do_not_auto_apply": True,
                "decision_boundary": "test decision only",
                "source_checklist_path": "checklist.json",
                "source_checklist_hash": "abc123",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def run_release_pipeline_cli(
    *,
    project_root: Path,
    args: list[str],
) -> subprocess.CompletedProcess[str]:
    script_path = project_root / "scripts" / "run_stock_release_pipeline.py"
    return subprocess.run(
        [sys.executable, str(script_path), *args],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )
