from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_release_gate_has_explicit_opt_in_fact_readiness_gate():
    script = (ROOT / "_archive" / "tools" / "release_gate.sh").read_text(encoding="utf-8")

    assert "AIRIVO_ENABLE_RELEASE_FACT_GATE" in script
    assert "AIRIVO_RELEASE_DB_PATH" in script
    assert "RELEASE_READINESS_PAYLOAD_FILE" in script
    assert "--operator release_gate" in script
    assert "RELEASE_GATE_FACT_STATUS" in script


def test_release_gate_fact_readiness_gate_is_blocking_when_enabled():
    script = (ROOT / "_archive" / "tools" / "release_gate.sh").read_text(encoding="utf-8")
    match = re.search(
        r"tools/release_dry_run_audit\.py(?P<body>.*?)--operator release_gate",
        script,
        flags=re.DOTALL,
    )

    assert match is not None
    assert "--db \"$AIRIVO_RELEASE_DB_PATH\"" in match.group("body")
    assert "--non-blocking" not in match.group("body")
    assert "exit 1" in script


def test_release_gate_supports_explicit_local_remote_hash_mode():
    script = (ROOT / "_archive" / "tools" / "release_gate.sh").read_text(encoding="utf-8")

    assert "AIRIVO_RELEASE_REMOTE_MODE" in script
    assert '== "local"' in script
    assert 'git -C "$REMOTE_APP_DIR" rev-parse HEAD' in script
    assert "local:$REMOTE_APP_DIR" in script


def test_ci_workflow_uses_governance_ci_runner_with_diff_context():
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert "fetch-depth: 0" in workflow
    assert "bash _archive/tools/run_governance_gate_ci.sh" in workflow
    assert "GOVERNANCE_BASE_SHA" in workflow
    assert "GOVERNANCE_HEAD_SHA" in workflow


def test_ci_workflow_posts_governance_pr_comment_and_preserves_gate_failure():
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert "id: governance_gate" in workflow
    assert "continue-on-error: true" in workflow
    assert "Build governance PR comment" in workflow
    assert "Upsert governance PR comment" in workflow
    assert "actions/github-script@v7" in workflow
    assert "airivo-governance-gate-comment" in workflow
    assert "Append governance summary" in workflow
    assert "governance_pr_comment.md >> \"$GITHUB_STEP_SUMMARY\"" in workflow
    assert "Upload governance comment artifact" in workflow
    assert "name: governance-gate-comment" in workflow
    assert "Fail workflow if archived governance gate is explicitly blocking" in workflow
    assert "AIRIVO_BLOCK_ON_ARCHIVED_GOVERNANCE_GATE" in workflow
    assert "steps.governance_gate.outcome != 'success'" in workflow


def test_release_mainline_runs_governance_gate_before_push():
    script = (ROOT / "_archive" / "tools" / "release_airivo_mainline.sh").read_text(encoding="utf-8")

    assert "tools/run_governance_gate_ci.sh" in script
    assert "GOVERNANCE_BASE_SHA=\"$REMOTE_BEFORE\"" in script
    assert "GOVERNANCE_HEAD_SHA=\"$LOCAL_HEAD\"" in script
    assert "governance gate failed for release diff" in script
