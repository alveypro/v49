from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_register_candidate_quality_baseline_registers_current_summary(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "register_candidate_quality_baseline.py"

    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir = tmp_path / "artifacts" / "candidate_quality_baselines"
    summary_path = exp_dir / "candidate_quality_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "evaluation_id": "eval-1",
                "generated_at": "2026-05-01T00:00:00+00:00",
                "run_id": "run-1",
                "result_id": "primary:300750.SZ",
                "source_scope": "stock",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--artifacts-dir",
            str(artifacts_dir),
            "--summary-path",
            str(summary_path),
            "--baseline-id",
            "baseline-eval-1",
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    payload = json.loads(completed.stdout)
    assert payload["status"] == "ok"
    assert payload["baseline_snapshot"]["baseline_id"] == "baseline-eval-1"
    current_payload = json.loads((artifacts_dir / "current.json").read_text(encoding="utf-8"))
    assert current_payload["baseline_id"] == "baseline-eval-1"
