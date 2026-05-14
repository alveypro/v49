from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[2]
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"


def _workflow_step_body(step_name: str) -> str:
    lines = CI_WORKFLOW.read_text(encoding="utf-8").splitlines()
    start = None
    for index, line in enumerate(lines):
        if line.strip() == f"- name: {step_name}":
            start = index
            break

    assert start is not None, f"missing CI step: {step_name}"

    end = len(lines)
    step_indent = lines[start].index("-")
    for index in range(start + 1, len(lines)):
        line = lines[index]
        if line.strip().startswith("- name: ") and line.index("-") == step_indent:
            end = index
            break
    return "\n".join(lines[start:end])


def test_ci_keeps_stock_dashboard_boundary_gate() -> None:
    body = _workflow_step_body("Stock dashboard boundary tests")

    required_tests = [
        "tests/test_ci_dashboard_boundary_gate.py",
        "tests/test_stock_dashboard_page_sections.py",
        "tests/test_stock_dashboard_render_inputs.py",
        "tests/test_stock_dashboard_http_routes.py",
        "tests/test_run_dashboard_primary_result_api.py",
    ]

    assert "working-directory: stock_ultimate_system" in body
    assert "pytest " in body
    for test_path in required_tests:
        assert test_path in body


def test_ci_keeps_governance_boundary_documentation_gate() -> None:
    body = _workflow_step_body("Governance boundary documentation tests")

    required_tests = [
        "tests/test_governance_docs_links.py",
        "tests/test_pr_template_governance_checklist.py",
        "tests/test_release_gate_script.py",
    ]

    assert "pytest " in body
    for test_path in required_tests:
        assert test_path in body


def test_ci_keeps_tool_boundary_audit_as_hard_gate() -> None:
    body = _workflow_step_body("Tool boundary audit")

    assert "python tools/tool_boundary_audit.py" in body
    assert "--fail-on-archive-candidates" in body
    assert "--max-manual-review 0" in body
    assert "--max-support-review 2" in body


def test_ci_does_not_mix_root_and_stock_pytest_suites() -> None:
    workflow = CI_WORKFLOW.read_text(encoding="utf-8")
    offenders = []
    for line in workflow.splitlines():
        if "pytest " not in line:
            continue
        if " tests/" in line and " stock_ultimate_system/tests/" in line:
            offenders.append(line.strip())

    assert offenders == []


def test_ci_command_entrypoint_paths_exist() -> None:
    workflow = CI_WORKFLOW.read_text(encoding="utf-8")
    command_refs = set(
        re.findall(
            r"\b(?:bash|python|python3)\s+((?:tools|_archive/tools|deploy/tools)/[A-Za-z0-9_./-]+\.(?:py|sh))",
            workflow,
        )
    )

    assert command_refs
    missing = sorted(path for path in command_refs if not (REPO_ROOT / path).exists())

    assert missing == []


def test_archived_governance_gate_is_not_default_blocking_ci_gate() -> None:
    workflow = CI_WORKFLOW.read_text(encoding="utf-8")
    body = _workflow_step_body("Fail workflow if archived governance gate is explicitly blocking")

    assert "bash _archive/tools/run_governance_gate_ci.sh" in workflow
    assert "docs/AIRIVO_GOVERNANCE_GATE_RUNBOOK.md" in workflow
    assert "_archive/docs/AIRIVO_GOVERNANCE_GATE_RUNBOOK.md" not in workflow
    assert "continue-on-error: true" in _workflow_step_body("Governance gate")
    assert "AIRIVO_BLOCK_ON_ARCHIVED_GOVERNANCE_GATE" in body
    assert "steps.governance_gate.outcome != 'success'" in body
