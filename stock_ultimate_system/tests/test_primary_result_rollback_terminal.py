import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from scripts.record_primary_result_terminal import record_primary_result_terminal
from scripts.run_primary_result_observation import run_primary_result_observation
from scripts.run_primary_result_rollback import run_primary_result_rollback
from tests.primary_result_test_support import seed_current_primary_pointer
from src.primary_result_audit import build_primary_result_audit
from src.primary_result_execution import build_primary_result_execution
from src.primary_result_observation import build_primary_result_observation
from src.primary_result_rollback import build_primary_result_rollback
from src.primary_result_terminal import build_primary_result_terminal
from src.unified_result_builder import build_primary_result_api_payload


def _write_primary_result_artifacts(tmp_path) -> Path:
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "000001.SZ,平安银行,银行,strong_buy,medium,155\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text('{"triggered": false}', encoding="utf-8")
    seed_current_primary_pointer(tmp_path, ts_code="000001.SZ", stock_name="平安银行")
    return exp_dir


def _promote_to_l4(exp_dir: Path) -> dict[str, object]:
    l2_payload = build_primary_result_api_payload(exp_dir)
    audit = build_primary_result_audit(
        l2_payload,
        now=datetime(2026, 4, 15, 7, 30, 20, tzinfo=timezone.utc),
        max_source_age_hours=100000,
    )
    (exp_dir / "primary_result_audit_latest.json").write_text(json.dumps(audit, ensure_ascii=False), encoding="utf-8")
    l3_payload = build_primary_result_api_payload(exp_dir)
    execution = build_primary_result_execution(
        l3_payload,
        now=datetime(2026, 4, 15, 7, 35, 20, tzinfo=timezone.utc),
    )
    (exp_dir / "primary_result_execution_latest.json").write_text(
        json.dumps(execution, ensure_ascii=False),
        encoding="utf-8",
    )
    return build_primary_result_api_payload(exp_dir)


def test_primary_result_rollback_records_not_required_for_ready_execution(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    l4_payload = _promote_to_l4(exp_dir)

    rollback = build_primary_result_rollback(
        l4_payload,
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )

    assert rollback["rollback_version"] == "primary_result_rollback.v1"
    assert rollback["rollback_status"] == "not_required"
    assert rollback["result_id"] == "primary:000001.SZ"
    assert rollback["rollback_plan"]["mode"] == "local_protocol"
    assert rollback["rollback_plan"]["external_broker_connected"] is False


def test_primary_result_builder_uses_rollback_artifact_without_closing_result(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    l4_payload = _promote_to_l4(exp_dir)
    rollback = build_primary_result_rollback(
        l4_payload,
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    (exp_dir / "primary_result_rollback_latest.json").write_text(
        json.dumps(rollback, ensure_ascii=False),
        encoding="utf-8",
    )

    payload = build_primary_result_api_payload(exp_dir)

    assert payload["result_lifecycle_stage"] == "L4"
    assert payload["rollback_status"] == "not_required"
    assert payload["terminal_outcome"] is None
    assert payload["history_source_file"] == "primary_result_rollback_latest.json"
    assert "回滚记录 not_required" in payload["history_summary"]


def test_primary_result_observation_records_observing_after_rollback_clear(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    l4_payload = _promote_to_l4(exp_dir)
    rollback = build_primary_result_rollback(
        l4_payload,
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    (exp_dir / "primary_result_rollback_latest.json").write_text(
        json.dumps(rollback, ensure_ascii=False),
        encoding="utf-8",
    )
    rollback_payload = build_primary_result_api_payload(exp_dir)

    observation = build_primary_result_observation(
        rollback_payload,
        observation_status="observing",
        reason="local observation window opened",
        now=datetime(2026, 4, 15, 7, 42, 20, tzinfo=timezone.utc),
    )

    assert observation["observation_version"] == "primary_result_observation.v1"
    assert observation["observation_status"] == "observing"
    assert observation["observation_plan"]["required_for_terminal_success"] is False
    assert observation["observation_window"]["status"] == "open"
    assert observation["observation_metrics"]["observed_return"] is None
    assert observation["source_execution_status"] == "ready"
    assert observation["source_rollback_status"] == "not_required"


def test_primary_result_builder_uses_observation_artifact_as_l4_source(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    l4_payload = _promote_to_l4(exp_dir)
    rollback = build_primary_result_rollback(
        l4_payload,
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    (exp_dir / "primary_result_rollback_latest.json").write_text(
        json.dumps(rollback, ensure_ascii=False),
        encoding="utf-8",
    )
    rollback_payload = build_primary_result_api_payload(exp_dir)
    observation = build_primary_result_observation(
        rollback_payload,
        observation_status="observing",
        reason="local observation window opened",
        now=datetime(2026, 4, 15, 7, 42, 20, tzinfo=timezone.utc),
    )
    (exp_dir / "primary_result_observation_latest.json").write_text(
        json.dumps(observation, ensure_ascii=False),
        encoding="utf-8",
    )

    payload = build_primary_result_api_payload(exp_dir)

    assert payload["result_lifecycle_stage"] == "L4"
    assert payload["observation_status"] == "observing"
    assert payload["terminal_outcome"] is None
    assert payload["history_source_file"] == "primary_result_observation_latest.json"
    assert "观察记录 observing" in payload["history_summary"]


def test_primary_result_terminal_requires_explicit_reason(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    l4_payload = _promote_to_l4(exp_dir)

    try:
        build_primary_result_terminal(l4_payload, terminal_outcome="success", reason="")
    except ValueError as exc:
        assert "reason is required" in str(exc)
    else:
        raise AssertionError("terminal outcome must require a reason")


def test_primary_result_terminal_success_requires_completed_observation(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    l4_payload = _promote_to_l4(exp_dir)

    try:
        build_primary_result_terminal(
            l4_payload,
            terminal_outcome="success",
            reason="local protocol completed",
        )
    except ValueError as exc:
        assert "completed observation_status" in str(exc)
    else:
        raise AssertionError("terminal success must require completed observation")


def test_completed_observation_requires_metrics_and_completion_criteria(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    l4_payload = _promote_to_l4(exp_dir)
    rollback = build_primary_result_rollback(l4_payload)
    (exp_dir / "primary_result_rollback_latest.json").write_text(json.dumps(rollback, ensure_ascii=False), encoding="utf-8")
    rollback_payload = build_primary_result_api_payload(exp_dir)

    missing_metrics = build_primary_result_observation(
        rollback_payload,
        observation_status="completed",
        reason="local observation window completed",
    )
    failed_metrics = build_primary_result_observation(
        rollback_payload,
        observation_status="completed",
        reason="local observation window completed",
        observed_return=-0.02,
        benchmark_return=-0.01,
        max_drawdown=-0.03,
    )
    passed_metrics = build_primary_result_observation(
        rollback_payload,
        observation_status="completed",
        reason="local observation window completed",
        window_start="2026-04-15T07:42:20Z",
        window_end="2026-04-20T07:42:20Z",
        observed_return=0.031,
        benchmark_return=0.012,
        max_drawdown=-0.025,
    )

    assert missing_metrics["observation_status"] == "blocked"
    assert failed_metrics["observation_status"] == "failed"
    assert passed_metrics["observation_status"] == "completed"
    assert passed_metrics["observation_window"]["status"] == "closed"
    assert passed_metrics["observation_metrics"]["excess_return"] == 0.019
    assert passed_metrics["completion_criteria"]["passed"] is True
    assert passed_metrics["observation_plan"]["required_for_terminal_success"] is True


def test_primary_result_builder_uses_terminal_artifact_as_l5_source(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    l4_payload = _promote_to_l4(exp_dir)
    rollback = build_primary_result_rollback(
        l4_payload,
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    (exp_dir / "primary_result_rollback_latest.json").write_text(
        json.dumps(rollback, ensure_ascii=False),
        encoding="utf-8",
    )
    rollback_payload = build_primary_result_api_payload(exp_dir)
    observation = build_primary_result_observation(
        rollback_payload,
        observation_status="completed",
        reason="local observation window completed",
        window_start="2026-04-15T07:42:20Z",
        window_end="2026-04-20T07:42:20Z",
        observed_return=0.031,
        benchmark_return=0.012,
        max_drawdown=-0.025,
        now=datetime(2026, 4, 15, 7, 42, 20, tzinfo=timezone.utc),
    )
    (exp_dir / "primary_result_observation_latest.json").write_text(
        json.dumps(observation, ensure_ascii=False),
        encoding="utf-8",
    )
    observation_payload = build_primary_result_api_payload(exp_dir)
    terminal = build_primary_result_terminal(
        observation_payload,
        terminal_outcome="success",
        reason="observation window completed in local protocol",
        now=datetime(2026, 4, 15, 7, 45, 20, tzinfo=timezone.utc),
    )
    (exp_dir / "primary_result_terminal_latest.json").write_text(
        json.dumps(terminal, ensure_ascii=False),
        encoding="utf-8",
    )

    payload = build_primary_result_api_payload(exp_dir)

    assert payload["result_lifecycle_stage"] == "L5"
    assert payload["result_type"] == "archive"
    assert payload["observation_status"] == "completed"
    assert payload["rollback_status"] == "not_required"
    assert payload["terminal_outcome"] == "success"
    assert payload["history_source_file"] == "primary_result_terminal_latest.json"
    assert "终局记录 success" in payload["history_summary"]


def test_run_primary_result_rollback_cli_writes_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "run_primary_result_rollback.py"
    exp_dir = _write_primary_result_artifacts(tmp_path)
    _promote_to_l4(exp_dir)
    output_path = tmp_path / "primary_result_rollback_latest.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--exp-dir",
            str(exp_dir),
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
    assert payload["rollback_status"] == "not_required"
    assert json.loads(output_path.read_text(encoding="utf-8"))["rollback_status"] == "not_required"


def test_run_primary_result_observation_cli_writes_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "run_primary_result_observation.py"
    exp_dir = _write_primary_result_artifacts(tmp_path)
    l4_payload = _promote_to_l4(exp_dir)
    rollback = build_primary_result_rollback(l4_payload)
    (exp_dir / "primary_result_rollback_latest.json").write_text(
        json.dumps(rollback, ensure_ascii=False),
        encoding="utf-8",
    )
    output_path = tmp_path / "primary_result_observation_latest.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--exp-dir",
            str(exp_dir),
            "--observation-status",
            "observing",
            "--reason",
            "local observation window opened",
            "--window-start",
            "2026-04-15T07:42:20Z",
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
    assert payload["observation_status"] == "observing"
    assert payload["observation_window"]["started_at"] == "2026-04-15T07:42:20Z"
    assert json.loads(output_path.read_text(encoding="utf-8"))["observation_status"] == "observing"


def test_record_primary_result_terminal_cli_writes_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "record_primary_result_terminal.py"
    exp_dir = _write_primary_result_artifacts(tmp_path)
    l4_payload = _promote_to_l4(exp_dir)
    rollback = build_primary_result_rollback(l4_payload)
    (exp_dir / "primary_result_rollback_latest.json").write_text(json.dumps(rollback, ensure_ascii=False), encoding="utf-8")
    rollback_payload = build_primary_result_api_payload(exp_dir)
    observation = build_primary_result_observation(
        rollback_payload,
        observation_status="completed",
        reason="local observation window completed",
        observed_return=0.031,
        benchmark_return=0.012,
        max_drawdown=-0.025,
    )
    (exp_dir / "primary_result_observation_latest.json").write_text(json.dumps(observation, ensure_ascii=False), encoding="utf-8")
    output_path = tmp_path / "primary_result_terminal_latest.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--exp-dir",
            str(exp_dir),
            "--terminal-outcome",
            "success",
            "--reason",
            "local protocol completed",
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
    assert payload["terminal_outcome"] == "success"
    assert json.loads(output_path.read_text(encoding="utf-8"))["terminal_outcome"] == "success"


def test_rollback_and_terminal_functions_default_to_experiment_artifacts(tmp_path):
    exp_dir = _write_primary_result_artifacts(tmp_path)
    _promote_to_l4(exp_dir)

    rollback_exit_code, rollback = run_primary_result_rollback(
        exp_dir=exp_dir,
        now=datetime(2026, 4, 15, 7, 40, 20, tzinfo=timezone.utc),
    )
    observation_exit_code, observation = run_primary_result_observation(
        exp_dir=exp_dir,
        observation_status="completed",
        reason="local observation window completed",
        observed_return=0.031,
        benchmark_return=0.012,
        max_drawdown=-0.025,
        now=datetime(2026, 4, 15, 7, 42, 20, tzinfo=timezone.utc),
    )
    terminal_exit_code, terminal = record_primary_result_terminal(
        exp_dir=exp_dir,
        terminal_outcome="success",
        reason="local protocol completed",
        now=datetime(2026, 4, 15, 7, 45, 20, tzinfo=timezone.utc),
    )

    assert rollback_exit_code == 0
    assert rollback["rollback_status"] == "not_required"
    assert observation_exit_code == 0
    assert observation["observation_status"] == "completed"
    assert terminal_exit_code == 0
    assert terminal["terminal_outcome"] == "success"
    assert (exp_dir / "primary_result_rollback_latest.json").exists()
    assert (exp_dir / "primary_result_observation_latest.json").exists()
    assert (exp_dir / "primary_result_terminal_latest.json").exists()
