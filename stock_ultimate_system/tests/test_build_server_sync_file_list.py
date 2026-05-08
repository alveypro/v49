import json
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.build_server_sync_file_list import build_server_sync_file_list


def _write_preflight(path, *, status="passed", allowed_files=None):
    payload = {
        "preflight_version": "server_sync_preflight.v1",
        "status": status,
        "sync_policy": "code_config_docs_tests_deploy_only",
        "allowed_files": allowed_files or [
            "src/app.py",
            "README.md",
            "scripts/tool.py",
            "src/app.py",
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_build_server_sync_file_list_requires_passed_preflight(tmp_path):
    preflight_path = tmp_path / "preflight.json"
    output_path = tmp_path / "files.txt"
    _write_preflight(preflight_path, status="failed")

    with pytest.raises(ValueError, match="passed preflight"):
        build_server_sync_file_list(preflight_path=preflight_path, output_path=output_path)

    assert not output_path.exists()


def test_build_server_sync_file_list_writes_sorted_unique_files(tmp_path):
    preflight_path = tmp_path / "preflight.json"
    output_path = tmp_path / "files.txt"
    summary_output_path = tmp_path / "file_list_summary.json"
    _write_preflight(preflight_path)

    summary = build_server_sync_file_list(
        preflight_path=preflight_path,
        output_path=output_path,
        summary_output_path=summary_output_path,
    )

    assert summary["status"] == "passed"
    assert summary["file_total"] == 3
    assert json.loads(summary_output_path.read_text(encoding="utf-8"))["file_total"] == 3
    assert output_path.read_text(encoding="utf-8").splitlines() == [
        "README.md",
        "scripts/tool.py",
        "src/app.py",
    ]


def test_build_server_sync_file_list_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "build_server_sync_file_list.py"
    preflight_path = tmp_path / "preflight.json"
    output_path = tmp_path / "files.txt"
    _write_preflight(preflight_path, allowed_files=["README.md"])

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--preflight",
            str(preflight_path),
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

    assert payload["file_list_version"] == "server_sync_file_list.v1"
    assert payload["file_total"] == 1
    assert output_path.read_text(encoding="utf-8") == "README.md\n"
