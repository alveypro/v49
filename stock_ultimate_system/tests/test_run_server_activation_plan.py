import json
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.run_server_activation_plan import run_server_activation_plan


def _write_plan(path, *, release_id="release-001"):
    path.write_text(
        json.dumps(
            {
                "activation_plan_version": "server_activation_plan.v1",
                "status": "passed",
                "release_id": release_id,
                "activation_commands": ["echo activate", "echo restart"],
                "rollback_commands": ["echo rollback"],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def test_run_server_activation_plan_dry_run_records_commands_without_running(tmp_path):
    plan_path = tmp_path / "activation_plan.json"
    output_path = tmp_path / "execution.json"
    _write_plan(plan_path)

    def failing_runner(command, timeout=None):
        raise AssertionError("dry-run must not execute commands")

    exit_code, payload = run_server_activation_plan(
        plan_path=plan_path,
        confirm_release_id="release-001",
        dry_run=True,
        output_path=output_path,
        command_runner=failing_runner,
    )

    assert exit_code == 0
    assert payload["activation_execution_version"] == "server_activation_execution.v1"
    assert payload["status"] == "passed"
    assert payload["dry_run"] is True
    assert payload["executed_total"] == 2
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "passed"


def test_run_server_activation_plan_requires_release_id_confirmation(tmp_path):
    plan_path = tmp_path / "activation_plan.json"
    _write_plan(plan_path)

    with pytest.raises(ValueError, match="confirm_release_id"):
        run_server_activation_plan(
            plan_path=plan_path,
            confirm_release_id="wrong-release",
            dry_run=True,
        )


def test_run_server_activation_plan_stops_on_first_failed_command(tmp_path):
    plan_path = tmp_path / "activation_plan.json"
    _write_plan(plan_path)

    def runner(command, timeout=None):
        return subprocess.CompletedProcess(command, 1 if "restart" in command else 0, stdout="", stderr="failed")

    exit_code, payload = run_server_activation_plan(
        plan_path=plan_path,
        confirm_release_id="release-001",
        command_runner=runner,
    )

    assert exit_code == 1
    assert payload["status"] == "failed"
    assert payload["executed_total"] == 2
    assert payload["failed_total"] == 1


def test_run_server_activation_plan_can_execute_rollback_commands(tmp_path):
    plan_path = tmp_path / "activation_plan.json"
    _write_plan(plan_path)
    commands = []

    def runner(command, timeout=None):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    exit_code, payload = run_server_activation_plan(
        plan_path=plan_path,
        confirm_release_id="release-001",
        action="rollback",
        command_runner=runner,
    )

    assert exit_code == 0
    assert payload["action"] == "rollback"
    assert commands == ["echo rollback"]


def test_run_server_activation_plan_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "run_server_activation_plan.py"
    plan_path = tmp_path / "activation_plan.json"
    _write_plan(plan_path, release_id="release-cli")

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--plan",
            str(plan_path),
            "--confirm-release-id",
            "release-cli",
            "--dry-run",
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(completed.stdout)

    assert payload["status"] == "passed"
    assert payload["dry_run"] is True
    assert payload["release_id"] == "release-cli"
