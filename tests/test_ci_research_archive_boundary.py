from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
NIGHTLY_WORKFLOW = ROOT / ".github" / "workflows" / "archived-research-nightly.yml"

ARCHIVED_RESEARCH_PATTERNS = (
    "tests/test_ensemble_",
    "tests/test_strategy_competition_formal_rerun_",
    "tests/test_strategy_competition_post_rerun_",
    "tests/test_promotion_decision_artifact_service.py",
    "tests/test_release_dry_run_service.py",
    "tests/test_release_dry_run_fixture_service.py",
)


def test_push_pr_ci_excludes_archived_research_and_rerun_tests():
    ci_text = CI_WORKFLOW.read_text(encoding="utf-8")

    offenders = [pattern for pattern in ARCHIVED_RESEARCH_PATTERNS if pattern in ci_text]

    assert offenders == []


def test_archived_research_tests_are_nightly_or_manual_only():
    text = NIGHTLY_WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in text
    assert "schedule:" in text
    assert not re.search(r"(?m)^  pull_request:", text)
    assert not re.search(r"(?m)^  push:", text)

    for pattern in ARCHIVED_RESEARCH_PATTERNS:
        assert pattern in text
