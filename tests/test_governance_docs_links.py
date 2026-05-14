from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LEGACY_GOVERNANCE_REFERENCE = "`tools/governance_gate.py`"
LEGACY_GOVERNANCE_ALLOWED_DOCS = {
    "docs/AIRIVO_ENSEMBLE_CORE_SHADOW_PORTFOLIO_DEVELOPMENT_PLAN.md",
    "docs/AIRIVO_STRATEGY_OPTIMIZATION_UPGRADE_EXECUTION_PLAN.md",
}


def test_governance_docs_cross_linked():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    runbook = (ROOT / "docs" / "AIRIVO_GOVERNANCE_GATE_RUNBOOK.md").read_text(encoding="utf-8")
    one_page = (ROOT / "docs" / "AIRIVO_GOVERNANCE_ONE_PAGE_FLOW.md").read_text(encoding="utf-8")
    boundary_map = (ROOT / "docs" / "AIRIVO_RUNTIME_BOUNDARY_MAP.md").read_text(encoding="utf-8")

    assert "docs/AIRIVO_RUNTIME_BOUNDARY_MAP.md" in readme
    assert "AIRIVO_BLOCK_ON_ARCHIVED_GOVERNANCE_GATE" in readme
    assert "Boundary validation commands" in readme
    assert "pytest -q\n" not in readme
    assert "python tools/tool_boundary_audit.py --fail-on-archive-candidates --max-manual-review 0 --max-support-review 2" in readme
    assert (
        "cd stock_ultimate_system && pytest tests/test_ci_dashboard_boundary_gate.py "
        "tests/test_stock_dashboard_page_sections.py "
        "tests/test_stock_dashboard_render_inputs.py "
        "tests/test_stock_dashboard_http_routes.py "
        "tests/test_run_dashboard_primary_result_api.py -q"
    ) in readme
    assert "Do not mix both test trees in one pytest process" in readme
    assert "tests/test_governance_docs_links.py tests/test_pr_template_governance_checklist.py tests/test_release_gate_script.py" in readme
    assert "AIRIVO_BLOCK_ON_ARCHIVED_GOVERNANCE_GATE" in runbook
    assert "AIRIVO_BLOCK_ON_ARCHIVED_GOVERNANCE_GATE" in one_page
    assert "AIRIVO_BLOCK_ON_ARCHIVED_GOVERNANCE_GATE" in boundary_map
    assert "tools/tool_boundary_audit.py --fail-on-archive-candidates --max-manual-review 0 --max-support-review 2" in runbook
    assert "stock dashboard boundary tests" in one_page
    assert "Current CI hard boundary gates" in boundary_map
    assert "stock_ultimate_system/tests/test_ci_dashboard_boundary_gate.py" in boundary_map
    assert "--fail-on-archive-candidates" in boundary_map
    assert "must not mix root `tests/` and" in boundary_map
    assert "Review this slimming batch as four separate boundaries" in boundary_map
    assert "Archived governance downgrade" in boundary_map


def test_current_docs_mark_legacy_governance_gate_references_as_superseded():
    offenders = []
    for path in sorted((ROOT / "docs").glob("*.md")):
        text = path.read_text(encoding="utf-8")
        if LEGACY_GOVERNANCE_REFERENCE not in text:
            continue
        if "Superseded governance note" not in text or "docs/AIRIVO_RUNTIME_BOUNDARY_MAP.md" not in text:
            offenders.append(str(path.relative_to(ROOT)))

    assert offenders == []


def test_legacy_governance_gate_references_are_confined_to_historical_plans():
    searched_roots = [
        ROOT / "README.md",
        ROOT / ".github",
        ROOT / "docs",
    ]
    offenders = []
    for root in searched_roots:
        paths = [root] if root.is_file() else sorted(root.rglob("*"))
        for path in paths:
            if not path.is_file() or path.suffix not in {".md", ".yml", ".yaml"}:
                continue
            relative = str(path.relative_to(ROOT))
            text = path.read_text(encoding="utf-8")
            if LEGACY_GOVERNANCE_REFERENCE in text and relative not in LEGACY_GOVERNANCE_ALLOWED_DOCS:
                offenders.append(relative)
            if "python tools/governance_gate.py" in text or "python3 tools/governance_gate.py" in text:
                if relative not in LEGACY_GOVERNANCE_ALLOWED_DOCS:
                    offenders.append(relative)
            if "_archive/docs/AIRIVO_GOVERNANCE_GATE_RUNBOOK.md" in text:
                offenders.append(relative)

    assert sorted(set(offenders)) == []
