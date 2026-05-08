import json
import subprocess
import sys
from pathlib import Path

from src.primary_result_feedback_loop import run_primary_result_feedback_loop


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _observation(*, status: str = "failed", criteria_passed: bool = False, excess_return: float = -0.04) -> dict[str, object]:
    return {
        "observation_version": "primary_result_observation.v1",
        "observation_status": status,
        "result_id": "primary:000001.SZ",
        "ts_code": "000001.SZ",
        "stock_name": "平安银行",
        "checks": [],
        "observation_window": {
            "started_at": "2026-04-15T09:30:00Z",
            "ended_at": "2026-04-20T15:00:00Z",
            "status": "closed",
        },
        "observation_metrics": {
            "observed_return": -0.03 if status == "failed" else 0.02,
            "benchmark_return": 0.01,
            "excess_return": excess_return,
            "max_drawdown": -0.09 if status == "failed" else -0.02,
        },
        "completion_criteria": {
            "min_success_return": 0.0,
            "max_drawdown_floor": -0.08,
            "passed": criteria_passed,
        },
        "primary_result_payload": {"risk_level": "medium", "signal_level": "high"},
    }


def test_feedback_loop_skips_open_observation(tmp_path):
    observation_path = tmp_path / "observation.json"
    _write_json(observation_path, {**_observation(status="failed"), "observation_status": "observing"})

    exit_code, payload = run_primary_result_feedback_loop(
        observation_path=observation_path,
        terminal_path=None,
        queue_dir=tmp_path / "queue",
        output_path=tmp_path / "loop.json",
    )

    assert exit_code == 0
    assert payload["status"] == "skipped"
    assert payload["reason"] == "observation is not closed"
    assert not (tmp_path / "queue" / "items").exists()


def test_feedback_loop_enqueues_failed_observation_without_auto_apply(tmp_path):
    observation_path = tmp_path / "observation.json"
    terminal_path = tmp_path / "terminal.json"
    _write_json(observation_path, _observation())
    _write_json(terminal_path, {"terminal_outcome": "failed"})
    ledger_path = tmp_path / "ledger.jsonl"
    ledger_path.write_text(
        json.dumps(
            {
                "entry_id": "entry-1",
                "result_id": "primary:000001.SZ",
                "window_ended_at": "2026-04-20T15:00:00Z",
                "outcome": "failed",
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    exit_code, payload = run_primary_result_feedback_loop(
        observation_path=observation_path,
        terminal_path=terminal_path,
        ledger_jsonl_path=ledger_path,
        attribution_output_path=tmp_path / "attribution.json",
        attribution_ledger_jsonl_path=tmp_path / "attribution_ledger.jsonl",
        attribution_summary_path=tmp_path / "attribution_summary.json",
        feedback_output_path=tmp_path / "feedback.json",
        queue_dir=tmp_path / "queue",
        output_path=tmp_path / "loop.json",
    )

    assert exit_code == 0
    assert payload["status"] == "completed"
    assert payload["attribution_required"] is True
    assert payload["change_total"] == 3
    assert payload["queue_status"] == "enqueued"
    feedback = json.loads((tmp_path / "feedback.json").read_text(encoding="utf-8"))
    assert feedback["do_not_auto_apply"] is True
    attribution_summary = json.loads((tmp_path / "attribution_summary.json").read_text(encoding="utf-8"))
    assert attribution_summary["entry_total"] == 1
    assert attribution_summary["false_positive_cases"] == 1
    item_paths = list((tmp_path / "queue" / "items").glob("*.json"))
    assert len(item_paths) == 1
    assert json.loads(item_paths[0].read_text(encoding="utf-8"))["do_not_auto_apply"] is True


def test_feedback_loop_is_idempotent_for_existing_review_item(tmp_path):
    observation_path = tmp_path / "observation.json"
    _write_json(observation_path, _observation())

    kwargs = {
        "observation_path": observation_path,
        "terminal_path": None,
        "ledger_jsonl_path": tmp_path / "ledger.jsonl",
        "attribution_output_path": tmp_path / "attribution.json",
        "feedback_output_path": tmp_path / "feedback.json",
        "queue_dir": tmp_path / "queue",
        "output_path": tmp_path / "loop.json",
    }
    first_code, first = run_primary_result_feedback_loop(**kwargs)
    second_code, second = run_primary_result_feedback_loop(**kwargs)

    assert first_code == 0
    assert second_code == 0
    assert first["queue_status"] == "enqueued"
    assert second["queue_status"] == "already_enqueued"
    assert len(list((tmp_path / "queue" / "items").glob("*.json"))) == 1


def test_feedback_loop_strong_success_writes_feedback_without_queue(tmp_path):
    observation_path = tmp_path / "observation.json"
    _write_json(observation_path, _observation(status="completed", criteria_passed=True, excess_return=0.05))

    exit_code, payload = run_primary_result_feedback_loop(
        observation_path=observation_path,
        terminal_path=None,
        attribution_output_path=tmp_path / "attribution.json",
        feedback_output_path=tmp_path / "feedback.json",
        queue_dir=tmp_path / "queue",
        output_path=tmp_path / "loop.json",
    )

    assert exit_code == 0
    assert payload["status"] == "completed"
    assert payload["attribution_required"] is False
    assert payload["change_total"] == 0
    assert payload["queue_status"] == "not_required"
    assert not (tmp_path / "queue" / "items").exists()


def test_feedback_loop_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script = project_root / "scripts" / "run_primary_result_feedback_loop.py"
    observation_path = tmp_path / "observation.json"
    _write_json(observation_path, _observation())

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--observation-json",
            str(observation_path),
            "--terminal-json",
            str(tmp_path / "missing_terminal.json"),
            "--ledger-jsonl",
            str(tmp_path / "ledger.jsonl"),
            "--attribution-output",
            str(tmp_path / "attribution.json"),
            "--feedback-output",
            str(tmp_path / "feedback.json"),
            "--queue-dir",
            str(tmp_path / "queue"),
            "--output",
            str(tmp_path / "loop.json"),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "completed"
    assert payload["queue_status"] == "enqueued"
