from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_pr_template_includes_governance_gate_checklist():
    template = (ROOT / ".github" / "pull_request_template.md").read_text(encoding="utf-8")

    assert "## Governance Gate Checklist" in template
    assert "_archive/docs/AIRIVO_GOVERNANCE_GATE_RUNBOOK.md" in template
    assert "bash _archive/tools/run_governance_gate_ci.sh" in template
    assert "GOVERNANCE_BASE_SHA/GOVERNANCE_HEAD_SHA" in template
    assert "execution_attribution_backfill_dry_run_*.json" in template
    assert "python tools/backfill_execution_attribution.py --apply" in template
