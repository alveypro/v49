from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


TOOL = Path(__file__).resolve().parents[1] / "_archive" / "tools" / "research_repair_iteration_flow.py"


def test_research_repair_iteration_flow_tool_creates_and_summarizes(tmp_path: Path):
    db = tmp_path / "repair.db"
    create = subprocess.run(
        [
            sys.executable,
            str(TOOL),
            "--db-path",
            str(db),
            "create",
            "--flow-id",
            "flow_cli",
            "--strategy",
            "ensemble_core",
            "--candidate",
            "hard_event_alpha_candidate",
            "--attempt-no",
            "5",
            "--rule-version",
            "hard_event_alpha_candidate.neutral_consensus_turnover_guard.v5",
            "--rule-hash",
            "h1",
            "--repair-objective",
            "fix neutral unthrottled and turnover",
            "--fixed-window-set",
            "20260226,20260302",
        ],
        cwd=str(Path(__file__).resolve().parents[1]),
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(create.stdout)
    assert payload["flow_id"] == "flow_cli"
    assert payload["current_status"] == "repair_attempt_predeclared"

    summary = subprocess.run(
        [sys.executable, str(TOOL), "--db-path", str(db), "summarize", "--flow-id", "flow_cli"],
        cwd=str(Path(__file__).resolve().parents[1]),
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(summary.stdout)
    assert payload["flow_id"] == "flow_cli"
    assert "formal" in payload["prohibited_actions_json"]


def test_research_repair_iteration_flow_tool_exports_evidence_manifest(tmp_path: Path):
    db = tmp_path / "repair.db"
    root = Path(__file__).resolve().parents[1]
    subprocess.run(
        [
            sys.executable,
            str(TOOL),
            "--db-path",
            str(db),
            "create",
            "--flow-id",
            "flow_export",
            "--strategy",
            "ensemble_core",
            "--candidate",
            "hard_event_alpha_candidate",
            "--attempt-no",
            "5",
            "--rule-version",
            "v5",
            "--rule-hash",
            "h1",
            "--repair-objective",
            "fix neutral",
        ],
        cwd=str(root),
        capture_output=True,
        text=True,
        check=True,
    )

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL),
            "--db-path",
            str(db),
            "export-evidence",
            "--flow-id",
            "flow_export",
            "--output-dir",
            str(tmp_path / "evidence"),
        ],
        cwd=str(root),
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(result.stdout)

    assert Path(payload["repair_flow_evidence_manifest"]).exists()
    manifest = json.loads(Path(payload["repair_flow_evidence_manifest"]).read_text(encoding="utf-8"))
    assert manifest["flow_id"] == "flow_export"
    assert Path(manifest["flow_snapshot_artifact"]).exists()
