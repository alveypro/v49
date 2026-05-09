import json
import subprocess
import sys
from pathlib import Path

from baseline_test_helpers import write_release_artifacts, write_release_decision


def test_promote_stock_baseline_success(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "promote_stock_baseline.py"
    artifact_paths = write_release_artifacts(tmp_path)
    release_decision_path = write_release_decision(tmp_path)

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--artifacts-dir",
            str(tmp_path / "artifacts" / "baselines"),
            "--policy-path",
            str(project_root / "STOCK_PRIMARY_RESULT_BASELINE_POLICY.md"),
            "--baseline-id",
            "stock-baseline-test001",
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

    assert completed.returncode == 0
    payload = json.loads(completed.stdout)
    assert payload["status"] == "ok"
    assert payload["action"] == "promote"
    assert payload["baseline_id"] == "stock-baseline-test001"

    current_payload = json.loads((tmp_path / "artifacts" / "baselines" / "current.json").read_text(encoding="utf-8"))
    snapshot_payload = json.loads(
        (tmp_path / "artifacts" / "baselines" / "history" / "stock-baseline-test001.json").read_text(encoding="utf-8")
    )
    assert current_payload["baseline_id"] == "stock-baseline-test001"
    assert current_payload["run_id"] == "stock-release-test001"
    assert current_payload["rollback_of_baseline_id"] is None
    assert snapshot_payload["baseline_id"] == "stock-baseline-test001"
    assert snapshot_payload["run_id"] == "stock-release-test001"
    assert snapshot_payload["report_hash"]
    assert snapshot_payload["release_decision_hash"]


def test_promote_stock_baseline_requires_release_decision(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "promote_stock_baseline.py"
    artifact_paths = write_release_artifacts(tmp_path)

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--artifacts-dir",
            str(tmp_path / "artifacts" / "baselines"),
            "--policy-path",
            str(project_root / "STOCK_PRIMARY_RESULT_BASELINE_POLICY.md"),
            "--baseline-id",
            "stock-baseline-without-decision",
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
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 1
    payload = json.loads(completed.stdout)
    assert payload["status"] == "error"
    assert "release_decision_json" in payload["error"]
