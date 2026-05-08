import json
import subprocess
import sys
from pathlib import Path

from scripts.run_server_domain_preflight import run_server_domain_preflight


def test_run_server_domain_preflight_passes_when_domain_is_unclaimed(tmp_path):
    conf_dir = tmp_path / "conf.d"
    conf_dir.mkdir()
    (conf_dir / "v49.conf").write_text("server_name v49.app;\n", encoding="utf-8")

    exit_code, payload = run_server_domain_preflight(nginx_conf_dirs=[conf_dir])

    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["blocking_failures"] == []


def test_run_server_domain_preflight_passes_for_managed_airivo_config(tmp_path):
    conf_dir = tmp_path / "conf.d"
    conf_dir.mkdir()
    (conf_dir / "airivo.online.conf").write_text(
        "# managed_by: stock_ultimate_system\nserver_name airivo.online;\nlocation /stock/ {}\n",
        encoding="utf-8",
    )

    exit_code, payload = run_server_domain_preflight(nginx_conf_dirs=[conf_dir])

    assert exit_code == 0
    assert payload["status"] == "passed"
    scanned = payload["scanned_files"][0]
    assert scanned["mentions_target_domain"] is True
    assert scanned["managed_by_stock_ultimate"] is True


def test_run_server_domain_preflight_rejects_unmanaged_airivo_config(tmp_path):
    conf_dir = tmp_path / "conf.d"
    conf_dir.mkdir()
    (conf_dir / "legacy.conf").write_text("server_name airivo.online;\n", encoding="utf-8")

    exit_code, payload = run_server_domain_preflight(nginx_conf_dirs=[conf_dir])

    assert exit_code == 1
    assert payload["status"] == "failed"
    failure_names = {check["check"] for check in payload["blocking_failures"]}
    assert "target_domain_not_owned_by_unmanaged_config" in failure_names


def test_run_server_domain_preflight_rejects_airivo_mixed_with_v49(tmp_path):
    conf_dir = tmp_path / "conf.d"
    conf_dir.mkdir()
    (conf_dir / "mixed.conf").write_text(
        "# managed_by: stock_ultimate_system\nserver_name airivo.online;\n# upstream v49.app\n",
        encoding="utf-8",
    )

    exit_code, payload = run_server_domain_preflight(nginx_conf_dirs=[conf_dir])

    assert exit_code == 1
    assert payload["status"] == "failed"
    failure_names = {check["check"] for check in payload["blocking_failures"]}
    assert "target_domain_not_mixed_with_existing_system" in failure_names


def test_run_server_domain_preflight_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "run_server_domain_preflight.py"
    conf_dir = tmp_path / "conf.d"
    conf_dir.mkdir()
    output_path = tmp_path / "domain_preflight.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--nginx-conf-dir",
            str(conf_dir),
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

    assert payload["domain_preflight_version"] == "server_domain_preflight.v1"
    assert payload["status"] == "passed"
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "passed"
