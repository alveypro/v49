import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.primary_result_production_readiness_ledger import PrimaryResultProductionReadinessLedger


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


def _write_readiness_inputs(tmp_path: Path, *, decision: str = "approved", terminal_outcome: str = "success", performance_outcome: str = "success"):
    decision_path = _write_json(
        tmp_path / "decision.json",
        {
            "decision_version": "primary_result_release_decision.v1",
            "decision_id": "decision-001",
            "decided_at": "2026-04-20T08:50:00Z",
            "decision": decision,
            "actor": "release_manager",
            "reason": "all evidence complete",
            "checklist_id": "checklist-001",
            "checklist_status": "complete",
            "review_id": "review-001",
            "result_id": "primary:000001.SZ",
            "ts_code": "000001.SZ",
            "missing_evidence": [],
            "blocking_gate_reason": None,
            "release_pipeline_allowed": decision == "approved",
            "baseline_promotion_allowed": decision == "approved",
            "do_not_auto_apply": True,
            "decision_boundary": "test decision only",
            "source_checklist_path": "checklist.json",
            "source_checklist_hash": "abc123",
        },
    )
    snapshot_path = tmp_path / "baselines" / "history" / "stock-baseline-001.json"
    _write_json(
        snapshot_path,
        {
            "snapshot_version": "v1",
            "baseline_id": "stock-baseline-001",
            "promoted_at": "2026-04-20T09:00:00Z",
            "run_id": "stock-release-001",
            "source_report_path": "report.json",
            "source_diff_path": "diff.json",
            "source_gates_path": "gates.json",
            "source_evidence_bundle_path": "bundle.json",
            "manifest_path": "manifest.json",
            "release_decision_path": str(decision_path),
            "release_decision_hash": _sha(decision_path),
            "report_hash": "report-hash",
            "policy_path": "policy.md",
        },
    )
    current_path = _write_json(
        tmp_path / "baselines" / "current.json",
        {
            "pointer_version": "v1",
            "baseline_id": "stock-baseline-001",
            "snapshot_path": str(snapshot_path),
            "run_id": "stock-release-001",
            "updated_at": "2026-04-20T09:00:00Z",
            "rollback_of_baseline_id": None,
        },
    )
    terminal_path = _write_json(
        tmp_path / "terminal.json",
        {
            "terminal_version": "primary_result_terminal.v1",
            "generated_at": "2026-04-20T09:10:00Z",
            "terminal_outcome": terminal_outcome,
            "terminal_reason": "observation completed",
            "result_id": "primary:000001.SZ",
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
        },
    )
    entry = {
        "ledger_version": "primary_result_performance_ledger.v1",
        "entry_id": "primary:000001.SZ:2026-04-20T15:00:00Z:completed",
        "recorded_at": "2026-04-20T15:01:00Z",
        "result_id": "primary:000001.SZ",
        "ts_code": "000001.SZ",
        "outcome": performance_outcome,
        "observed_return": 0.08 if performance_outcome == "success" else -0.02,
        "excess_return": 0.05 if performance_outcome == "success" else -0.04,
        "max_drawdown": -0.03,
    }
    ledger_path = _write_jsonl(tmp_path / "performance" / "ledger.jsonl", [entry])
    summary_path = _write_json(
        tmp_path / "performance" / "summary.json",
        {
            "summary_version": "primary_result_performance_summary.v1",
            "generated_at": "2026-04-20T15:02:00Z",
            "entry_total": 1,
            "success_total": 1 if performance_outcome == "success" else 0,
            "failed_total": 0 if performance_outcome == "success" else 1,
            "success_rate": 1.0 if performance_outcome == "success" else 0.0,
            "latest_entry_id": entry["entry_id"],
        },
    )
    return {
        "decision": decision_path,
        "current": current_path,
        "terminal": terminal_path,
        "ledger": ledger_path,
        "summary": summary_path,
    }


def test_primary_result_production_readiness_builds_ready_snapshot(tmp_path):
    inputs = _write_readiness_inputs(tmp_path)
    ledger = PrimaryResultProductionReadinessLedger(readiness_dir=tmp_path / "readiness")

    readiness = ledger.create_readiness(
        release_decision_path=inputs["decision"],
        baseline_current_path=inputs["current"],
        terminal_path=inputs["terminal"],
        performance_ledger_path=inputs["ledger"],
        performance_summary_path=inputs["summary"],
        readiness_id="readiness-001",
        generated_at="2026-04-20T16:00:00Z",
    )

    assert readiness["readiness_version"] == "primary_result_production_readiness.v1"
    assert readiness["status"] == "ready"
    assert readiness["blocking_reasons"] == []
    assert readiness["baseline_id"] == "stock-baseline-001"
    assert readiness["do_not_auto_apply"] is True
    assert all(check["passed"] is True for check in readiness["checks"])
    assert ledger.get_current_pointer()["readiness_id"] == "readiness-001"


def test_primary_result_production_readiness_blocks_unapproved_decision(tmp_path):
    inputs = _write_readiness_inputs(tmp_path, decision="rejected")
    ledger = PrimaryResultProductionReadinessLedger(readiness_dir=tmp_path / "readiness")

    readiness = ledger.create_readiness(
        release_decision_path=inputs["decision"],
        baseline_current_path=inputs["current"],
        terminal_path=inputs["terminal"],
        performance_ledger_path=inputs["ledger"],
        performance_summary_path=inputs["summary"],
        readiness_id="readiness-rejected",
    )

    assert readiness["status"] == "blocked"
    assert "release decision must be approved and allow baseline promotion" in readiness["blocking_reasons"]


def test_primary_result_production_readiness_blocks_non_success_terminal(tmp_path):
    inputs = _write_readiness_inputs(tmp_path, terminal_outcome="failed")
    ledger = PrimaryResultProductionReadinessLedger(readiness_dir=tmp_path / "readiness")

    readiness = ledger.create_readiness(
        release_decision_path=inputs["decision"],
        baseline_current_path=inputs["current"],
        terminal_path=inputs["terminal"],
        performance_ledger_path=inputs["ledger"],
        performance_summary_path=inputs["summary"],
        readiness_id="readiness-terminal-failed",
    )

    assert readiness["status"] == "blocked"
    assert "terminal artifact must record explicit success" in readiness["blocking_reasons"]


def test_primary_result_production_readiness_blocks_failed_performance(tmp_path):
    inputs = _write_readiness_inputs(tmp_path, performance_outcome="failed")
    ledger = PrimaryResultProductionReadinessLedger(readiness_dir=tmp_path / "readiness")

    readiness = ledger.create_readiness(
        release_decision_path=inputs["decision"],
        baseline_current_path=inputs["current"],
        terminal_path=inputs["terminal"],
        performance_ledger_path=inputs["ledger"],
        performance_summary_path=inputs["summary"],
        readiness_id="readiness-performance-failed",
    )

    assert readiness["status"] == "blocked"
    assert "latest performance ledger entry must be success" in readiness["blocking_reasons"]


def test_primary_result_production_readiness_preserves_history(tmp_path):
    inputs = _write_readiness_inputs(tmp_path)
    ledger = PrimaryResultProductionReadinessLedger(readiness_dir=tmp_path / "readiness")

    ledger.create_readiness(
        release_decision_path=inputs["decision"],
        baseline_current_path=inputs["current"],
        terminal_path=inputs["terminal"],
        performance_ledger_path=inputs["ledger"],
        performance_summary_path=inputs["summary"],
        readiness_id="readiness-001",
    )

    with pytest.raises(FileExistsError, match="already exists"):
        ledger.create_readiness(
            release_decision_path=inputs["decision"],
            baseline_current_path=inputs["current"],
            terminal_path=inputs["terminal"],
            performance_ledger_path=inputs["ledger"],
            performance_summary_path=inputs["summary"],
            readiness_id="readiness-001",
        )


def test_build_primary_result_production_readiness_cli(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "build_primary_result_production_readiness.py"
    inputs = _write_readiness_inputs(tmp_path)
    readiness_dir = tmp_path / "readiness"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--readiness-dir",
            str(readiness_dir),
            "--release-decision-json",
            str(inputs["decision"]),
            "--baseline-current-json",
            str(inputs["current"]),
            "--terminal-json",
            str(inputs["terminal"]),
            "--performance-ledger-jsonl",
            str(inputs["ledger"]),
            "--performance-summary-json",
            str(inputs["summary"]),
            "--readiness-id",
            "readiness-cli",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "ready"
    assert payload["readiness_id"] == "readiness-cli"
    assert json.loads((readiness_dir / "current.json").read_text(encoding="utf-8"))["readiness_id"] == "readiness-cli"
