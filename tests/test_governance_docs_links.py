from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_governance_docs_cross_linked():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    checklist = (ROOT / "_archive" / "docs" / "AIRIVO_MAINLINE_DELIVERY_ADJUDICATION_CHECKLIST.md").read_text(encoding="utf-8")
    runbook = (ROOT / "_archive" / "docs" / "AIRIVO_GOVERNANCE_GATE_RUNBOOK.md").read_text(encoding="utf-8")
    one_page = (ROOT / "_archive" / "docs" / "AIRIVO_GOVERNANCE_ONE_PAGE_FLOW.md").read_text(encoding="utf-8")

    assert "_archive/docs/AIRIVO_GOVERNANCE_ONE_PAGE_FLOW.md" in readme
    assert "_archive/docs/AIRIVO_GOVERNANCE_ONE_PAGE_FLOW.md" in checklist
    assert "_archive/docs/AIRIVO_GOVERNANCE_ONE_PAGE_FLOW.md" in runbook
    assert "_archive/tools/run_governance_gate_ci.sh" in one_page
