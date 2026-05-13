from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_pr_template_includes_governance_gate_checklist():
    template = (ROOT / ".github" / "pull_request_template.md").read_text(encoding="utf-8")

    assert "## Runtime Boundary Review" in template
    assert "docs/AIRIVO_RUNTIME_BOUNDARY_MAP.md" in template
    assert "`tools/` boundary" in template
    assert "`/stock` dashboard shell" in template
    assert "archived governance downgrade" in template
    assert "documentation / CI anti-regression gates" in template
    assert "## Archived Governance Gate Checklist" in template
    assert "docs/AIRIVO_GOVERNANCE_GATE_RUNBOOK.md" in template
    assert "python tools/tool_boundary_audit.py --fail-on-archive-candidates --max-manual-review 0 --max-support-review 2" in template
    assert "Stock dashboard boundary tests were run" in template
    assert "bash _archive/tools/run_governance_gate_ci.sh" in template
    assert "GOVERNANCE_BASE_SHA/GOVERNANCE_HEAD_SHA" in template
    assert "AIRIVO_BLOCK_ON_ARCHIVED_GOVERNANCE_GATE=true" in template
    assert "execution_attribution_backfill_dry_run_*.json" in template
    assert "python tools/archive/strategy_competition/backfill_execution_attribution.py --apply" in template
    assert "python3 tools/governance_gate.py" not in template
