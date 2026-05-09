import hashlib
import json
import subprocess
import sys
from pathlib import Path

from src.primary_result_production_readiness_preflight import build_primary_result_production_readiness_preflight


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows) + "\n", encoding="utf-8")
    return path


def _write_ready_inputs(tmp_path: Path):
    decision_snapshot = _write_json(
        tmp_path / "decisions" / "history" / "decision-001.json",
        {
            "decision_version": "primary_result_release_decision.v1",
            "decision_id": "decision-001",
            "decision": "approved",
            "baseline_promotion_allowed": True,
            "do_not_auto_apply": True,
            "source_checklist_hash": "abc123",
        },
    )
    decision_current = _write_json(
        tmp_path / "decisions" / "current.json",
        {
            "pointer_version": "primary_result_release_decision_pointer.v1",
            "decision_id": "decision-001",
            "decision_path": str(decision_snapshot),
            "checklist_id": "checklist-001",
            "decision": "approved",
            "updated_at": "2026-04-20T08:50:00Z",
        },
    )
    baseline_snapshot = _write_json(
        tmp_path / "baselines" / "history" / "stock-baseline-001.json",
        {
            "snapshot_version": "v1",
            "baseline_id": "stock-baseline-001",
            "run_id": "stock-release-001",
            "release_decision_path": str(decision_snapshot),
            "release_decision_hash": _sha(decision_snapshot),
        },
    )
    baseline_current = _write_json(
        tmp_path / "baselines" / "current.json",
        {
            "pointer_version": "v1",
            "baseline_id": "stock-baseline-001",
            "snapshot_path": str(baseline_snapshot),
            "run_id": "stock-release-001",
        },
    )
    terminal = _write_json(
        tmp_path / "terminal.json",
        {
            "terminal_version": "primary_result_terminal.v1",
            "terminal_outcome": "success",
            "result_id": "primary:000001.SZ",
        },
    )
    entry = {
        "ledger_version": "primary_result_performance_ledger.v1",
        "entry_id": "primary:000001.SZ:2026-04-20T15:00:00Z:completed",
        "outcome": "success",
    }
    ledger = _write_jsonl(tmp_path / "performance" / "ledger.jsonl", [entry])
    summary = _write_json(
        tmp_path / "performance" / "summary.json",
        {
            "summary_version": "primary_result_performance_summary.v1",
            "entry_total": 1,
            "latest_entry_id": entry["entry_id"],
        },
    )
    return {
        "decision_current": decision_current,
        "baseline_current": baseline_current,
        "terminal": terminal,
        "ledger": ledger,
        "summary": summary,
    }


def test_primary_result_production_readiness_preflight_reports_ready(tmp_path):
    inputs = _write_ready_inputs(tmp_path)

    exit_code, payload = build_primary_result_production_readiness_preflight(
        release_decision_current_path=inputs["decision_current"],
        baseline_current_path=inputs["baseline_current"],
        terminal_path=inputs["terminal"],
        performance_ledger_path=inputs["ledger"],
        performance_summary_path=inputs["summary"],
    )

    assert exit_code == 0
    assert payload["preflight_version"] == "primary_result_production_readiness_preflight.v1"
    assert payload["status"] == "ready"
    assert payload["missing_artifacts"] == []
    assert payload["blocking_reasons"] == []


def test_primary_result_production_readiness_preflight_reports_missing_artifacts(tmp_path):
    exit_code, payload = build_primary_result_production_readiness_preflight(
        release_decision_current_path=tmp_path / "missing_decision_current.json",
        baseline_current_path=tmp_path / "missing_baseline_current.json",
        terminal_path=tmp_path / "missing_terminal.json",
        performance_ledger_path=tmp_path / "missing_ledger.jsonl",
        performance_summary_path=tmp_path / "missing_summary.json",
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert set(payload["missing_artifacts"]) == {
        "baseline_current",
        "baseline_snapshot",
        "performance_ledger",
        "performance_summary",
        "release_decision_current",
        "release_decision_snapshot",
        "terminal",
    }


def test_primary_result_production_readiness_preflight_blocks_unfinished_lifecycle(tmp_path):
    inputs = _write_ready_inputs(tmp_path)
    _write_json(inputs["terminal"], {"terminal_version": "primary_result_terminal.v1", "terminal_outcome": "failed"})

    exit_code, payload = build_primary_result_production_readiness_preflight(
        release_decision_current_path=inputs["decision_current"],
        baseline_current_path=inputs["baseline_current"],
        terminal_path=inputs["terminal"],
        performance_ledger_path=inputs["ledger"],
        performance_summary_path=inputs["summary"],
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert "terminal artifact must record explicit success" in payload["blocking_reasons"]


def test_inspect_primary_result_production_readiness_cli(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "inspect_primary_result_production_readiness.py"
    inputs = _write_ready_inputs(tmp_path)
    output_path = tmp_path / "preflight.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--release-decision-current-json",
            str(inputs["decision_current"]),
            "--baseline-current-json",
            str(inputs["baseline_current"]),
            "--terminal-json",
            str(inputs["terminal"]),
            "--performance-ledger-jsonl",
            str(inputs["ledger"]),
            "--performance-summary-json",
            str(inputs["summary"]),
            "--output",
            str(output_path),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "ready"
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "ready"
