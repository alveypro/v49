import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.primary_result_performance_ledger import PrimaryResultPerformanceLedger


def _write_observation(path: Path, *, status: str = "completed", result_id: str = "primary:000001.SZ") -> Path:
    payload = {
        "observation_version": "primary_result_observation.v1",
        "generated_at": "2026-04-20T08:00:00Z",
        "observation_status": status,
        "requested_observation_status": "completed",
        "observation_reason": "local price history observation window completed",
        "result_id": result_id,
        "ts_code": result_id.replace("primary:", ""),
        "stock_name": "平安银行",
        "source_execution_status": "ready",
        "source_rollback_status": "not_required",
        "checks": [],
        "observation_window": {
            "started_at": "2026-04-15T09:30:00Z",
            "ended_at": "2026-04-20T15:00:00Z",
            "status": "closed",
        },
        "observation_metrics": {
            "observed_return": 0.1 if status == "completed" else -0.02,
            "benchmark_return": 0.02,
            "excess_return": 0.08 if status == "completed" else -0.04,
            "max_drawdown": -0.037037,
        },
        "completion_criteria": {
            "min_success_return": 0.0,
            "max_drawdown_floor": -0.08,
            "passed": status == "completed",
        },
        "observation_plan": {
            "mode": "local_protocol",
            "external_analytics_connected": False,
            "can_observe": True,
            "required_for_terminal_success": status == "completed",
        },
        "primary_result_payload": {},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def test_primary_result_performance_ledger_appends_completed_observation_and_summary(tmp_path):
    ledger = PrimaryResultPerformanceLedger(
        ledger_path=tmp_path / "artifacts" / "primary_result_performance" / "ledger.jsonl",
        summary_path=tmp_path / "artifacts" / "primary_result_performance" / "summary.json",
    )
    observation = _write_observation(tmp_path / "primary_result_observation_latest.json")

    entry = ledger.append_observation(observation_path=observation, recorded_at="2026-04-20T08:01:00Z")

    assert entry["ledger_version"] == "primary_result_performance_ledger.v1"
    assert entry["result_id"] == "primary:000001.SZ"
    assert entry["outcome"] == "success"
    assert entry["source_observation_hash"]
    entries = ledger.list_entries()
    assert len(entries) == 1
    summary = json.loads((tmp_path / "artifacts" / "primary_result_performance" / "summary.json").read_text(encoding="utf-8"))
    assert summary["entry_total"] == 1
    assert summary["success_rate"] == 1.0
    assert summary["average_excess_return"] == 0.08


def test_primary_result_performance_ledger_rejects_open_observation(tmp_path):
    ledger = PrimaryResultPerformanceLedger(
        ledger_path=tmp_path / "ledger.jsonl",
        summary_path=tmp_path / "summary.json",
    )
    observation = _write_observation(tmp_path / "observation.json", status="observing")

    with pytest.raises(ValueError, match="completed or failed"):
        ledger.append_observation(observation_path=observation)

    assert ledger.list_entries() == []


def test_primary_result_performance_ledger_preserves_append_only_entries(tmp_path):
    ledger = PrimaryResultPerformanceLedger(
        ledger_path=tmp_path / "ledger.jsonl",
        summary_path=tmp_path / "summary.json",
    )
    first = _write_observation(tmp_path / "first.json", result_id="primary:000001.SZ")
    second = _write_observation(tmp_path / "second.json", status="failed", result_id="primary:000002.SZ")

    ledger.append_observation(observation_path=first)
    ledger.append_observation(observation_path=second)

    entries = ledger.list_entries()
    assert [entry["result_id"] for entry in entries] == ["primary:000001.SZ", "primary:000002.SZ"]
    summary = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert summary["entry_total"] == 2
    assert summary["success_total"] == 1
    assert summary["failed_total"] == 1
    assert summary["success_rate"] == 0.5


def test_primary_result_performance_ledger_rejects_duplicate_entry(tmp_path):
    ledger = PrimaryResultPerformanceLedger(
        ledger_path=tmp_path / "ledger.jsonl",
        summary_path=tmp_path / "summary.json",
    )
    observation = _write_observation(tmp_path / "observation.json")

    ledger.append_observation(observation_path=observation)

    with pytest.raises(FileExistsError, match="already exists"):
        ledger.append_observation(observation_path=observation)


def test_register_primary_result_performance_cli(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "register_primary_result_performance.py"
    observation = _write_observation(tmp_path / "observation.json")
    ledger_path = tmp_path / "ledger.jsonl"
    summary_path = tmp_path / "summary.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--ledger-jsonl",
            str(ledger_path),
            "--summary-json",
            str(summary_path),
            "--observation-json",
            str(observation),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "registered"
    assert payload["entry"]["outcome"] == "success"
    assert len(ledger_path.read_text(encoding="utf-8").splitlines()) == 1
    assert json.loads(summary_path.read_text(encoding="utf-8"))["entry_total"] == 1
