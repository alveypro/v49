import json
import subprocess
import sys
from pathlib import Path

from baseline_test_helpers import write_release_artifacts, write_release_decision


def test_promote_stock_baseline_rejects_blocking_gate(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "promote_stock_baseline.py"
    artifact_paths = write_release_artifacts(tmp_path, gate_status="failed", failed_total=1)
    release_decision_path = write_release_decision(tmp_path)

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--artifacts-dir",
            str(tmp_path / "artifacts" / "baselines"),
            "--policy-path",
            str(project_root / "STOCK_PRIMARY_RESULT_BASELINE_POLICY.md"),
            "--benchmark-report-json",
            str(artifact_paths["report"]),
            "--benchmark-diff-json",
            str(artifact_paths["diff"]),
            "--release-gates-json",
            str(artifact_paths["gates"]),
            "--evidence-bundle-json",
            str(artifact_paths["bundle"]),
            "--manifest-json",
            str(artifact_paths["manifest"]),
            "--release-decision-json",
            str(release_decision_path),
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 1
    payload = json.loads(completed.stdout)
    assert payload["status"] == "error"
    assert "release gates" in payload["error"]
    current_payload = json.loads((tmp_path / "artifacts" / "baselines" / "current.json").read_text(encoding="utf-8"))
    assert current_payload["baseline_id"] is None
    assert list((tmp_path / "artifacts" / "baselines" / "history").glob("*.json")) == []
