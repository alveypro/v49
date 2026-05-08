#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


AIRIVO_APEX_PREFLIGHT_VERSION = "airivo_apex_preflight.v1"
DEFAULT_NGINX_CONF_DIRS = ("/etc/nginx/conf.d", "/etc/nginx/sites-enabled")
DEFAULT_APEX_PATH = "/apex"
DEFAULT_PORTS = (18764, 18765, 18766)

CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def _run_command(command: list[str], timeout: float = 10.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, timeout=timeout)


def _write_output(path: str | Path | None, payload: dict[str, object]) -> None:
    if not path:
        return
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _iter_nginx_conf_files(conf_dirs: list[Path]) -> list[Path]:
    files: list[Path] = []
    for conf_dir in conf_dirs:
        if conf_dir.exists() and conf_dir.is_dir():
            files.extend(sorted(path for path in conf_dir.glob("*.conf") if path.is_file()))
    return files


def _scan_apex_path(conf_dirs: list[Path], apex_path: str) -> list[dict[str, object]]:
    matches = []
    for path in _iter_nginx_conf_files(conf_dirs):
        text = path.read_text(encoding="utf-8", errors="replace")
        if apex_path in text:
            matches.append(
                {
                    "path": str(path),
                    "managed_by_stock_ultimate": "managed_by: stock_ultimate_system" in text,
                    "mentions_apex_path": True,
                }
            )
    return matches


def _list_listening_ports(command_runner: CommandRunner) -> set[int]:
    try:
        completed = command_runner(["ss", "-lnt"], timeout=10.0)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return set()
    ports: set[int] = set()
    for line in completed.stdout.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        local_address = parts[3]
        if ":" not in local_address:
            continue
        try:
            ports.add(int(local_address.rsplit(":", 1)[1]))
        except ValueError:
            continue
    return ports


def run_airivo_apex_preflight(
    *,
    nginx_conf_dirs: list[str | Path] | None = None,
    apex_path: str = DEFAULT_APEX_PATH,
    ports: tuple[int, ...] = DEFAULT_PORTS,
    output_path: str | Path | None = None,
    command_runner: CommandRunner = _run_command,
) -> tuple[int, dict[str, object]]:
    conf_dirs = [Path(path) for path in (nginx_conf_dirs or list(DEFAULT_NGINX_CONF_DIRS))]
    apex_matches = _scan_apex_path(conf_dirs, apex_path)
    unmanaged_apex_matches = [match for match in apex_matches if not match["managed_by_stock_ultimate"]]
    listening_ports = _list_listening_ports(command_runner)
    occupied_ports = [port for port in ports if port in listening_ports]
    checks = [
        {
            "check": "apex_path_not_owned_by_unmanaged_nginx_config",
            "passed": not unmanaged_apex_matches,
            "details": {
                "apex_path": apex_path,
                "matches": apex_matches,
                "unmanaged_matches": unmanaged_apex_matches,
            },
        },
        {
            "check": "apex_ports_available",
            "passed": not occupied_ports,
            "details": {
                "required_ports": list(ports),
                "occupied_ports": occupied_ports,
            },
        },
    ]
    blocking_failures = [check for check in checks if not check["passed"]]
    payload = {
        "airivo_apex_preflight_version": AIRIVO_APEX_PREFLIGHT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "passed" if not blocking_failures else "failed",
        "product_name": "Airivo Apex Internal Validation",
        "apex_path": apex_path,
        "required_ports": list(ports),
        "checks": checks,
        "blocking_failures": blocking_failures,
    }
    _write_output(output_path, payload)
    return (0 if payload["status"] == "passed" else 1), payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Preflight Airivo Apex internal-validation routes before adding /apex.")
    parser.add_argument("--nginx-conf-dir", action="append")
    parser.add_argument("--apex-path", default=DEFAULT_APEX_PATH)
    parser.add_argument("--port", type=int, action="append", help="Required local Apex port. Defaults to 18764/18765/18766.")
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    exit_code, payload = run_airivo_apex_preflight(
        nginx_conf_dirs=args.nginx_conf_dir,
        apex_path=args.apex_path,
        ports=tuple(args.port or DEFAULT_PORTS),
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "airivo_apex_preflight_version": payload["airivo_apex_preflight_version"],
                    "blocking_failure_total": len(payload["blocking_failures"]),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
