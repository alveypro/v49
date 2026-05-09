import json
import subprocess
import sys
from pathlib import Path

from scripts.run_airivo_apex_preflight import run_airivo_apex_preflight


def _runner_with_ports(ports):
    def runner(command, timeout=10.0):
        lines = ["State Recv-Q Send-Q Local Address:Port Peer Address:Port"]
        lines.extend(f"LISTEN 0 128 127.0.0.1:{port} 0.0.0.0:*" for port in ports)
        return subprocess.CompletedProcess(command, 0, stdout="\n".join(lines), stderr="")

    return runner


def test_airivo_apex_preflight_passes_when_path_and_ports_are_free(tmp_path):
    conf_dir = tmp_path / "conf.d"
    conf_dir.mkdir()
    (conf_dir / "airivo.online.conf").write_text("server_name airivo.online;\nlocation /stock/ {}\n", encoding="utf-8")

    exit_code, payload = run_airivo_apex_preflight(
        nginx_conf_dirs=[conf_dir],
        command_runner=_runner_with_ports([8501, 8765]),
    )

    assert exit_code == 0
    assert payload["status"] == "passed"
    assert payload["product_name"] == "Airivo Apex Internal Validation"


def test_airivo_apex_preflight_rejects_unmanaged_apex_path(tmp_path):
    conf_dir = tmp_path / "conf.d"
    conf_dir.mkdir()
    (conf_dir / "airivo.online.conf").write_text("server_name airivo.online;\nlocation /apex/ {}\n", encoding="utf-8")

    exit_code, payload = run_airivo_apex_preflight(
        nginx_conf_dirs=[conf_dir],
        command_runner=_runner_with_ports([]),
    )

    assert exit_code == 1
    assert payload["status"] == "failed"
    assert payload["blocking_failures"][0]["check"] == "apex_path_not_owned_by_unmanaged_nginx_config"


def test_airivo_apex_preflight_allows_managed_apex_path(tmp_path):
    conf_dir = tmp_path / "conf.d"
    conf_dir.mkdir()
    (conf_dir / "airivo-apex.conf").write_text(
        "# managed_by: stock_ultimate_system\nlocation /apex/ {}\n",
        encoding="utf-8",
    )

    exit_code, payload = run_airivo_apex_preflight(
        nginx_conf_dirs=[conf_dir],
        command_runner=_runner_with_ports([]),
    )

    assert exit_code == 0
    assert payload["status"] == "passed"


def test_airivo_apex_preflight_rejects_occupied_apex_port(tmp_path):
    conf_dir = tmp_path / "conf.d"
    conf_dir.mkdir()

    exit_code, payload = run_airivo_apex_preflight(
        nginx_conf_dirs=[conf_dir],
        command_runner=_runner_with_ports([18765]),
    )

    assert exit_code == 1
    port_failure = next(check for check in payload["blocking_failures"] if check["check"] == "apex_ports_available")
    assert port_failure["details"]["occupied_ports"] == [18765]


def test_airivo_apex_preflight_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "run_airivo_apex_preflight.py"
    conf_dir = tmp_path / "conf.d"
    conf_dir.mkdir()

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--nginx-conf-dir",
            str(conf_dir),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(completed.stdout)

    assert payload["airivo_apex_preflight_version"] == "airivo_apex_preflight.v1"
    assert payload["status"] == "passed"
