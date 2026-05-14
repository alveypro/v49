"""Roundtrip checks for scheduler deploy manifest tooling."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_manifest_generate_verify_small_tree(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "t").mkdir(parents=True)
    (repo / "t/hello.txt").write_text("world", encoding="utf-8")
    (repo / "t/other.txt").write_text("zzz", encoding="utf-8")
    plist = repo / "paths.list"
    plist.write_text("t/hello.txt\nt/other.txt\n", encoding="utf-8")

    repo_tool = Path(__file__).resolve().parents[1] / "deploy" / "tools" / "top5_audit_scheduler_manifest.py"

    out_json = repo / "manifest.json"
    out_sum = repo / "sums.sha256"
    gen = subprocess.run(
        [
            sys.executable,
            str(repo_tool),
            "generate",
            "--repo",
            str(repo),
            "--paths",
            str(plist),
            "--out-json",
            str(out_json),
            "--sha256sums",
            str(out_sum),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert gen.returncode == 0, gen.stderr + gen.stdout

    data = json.loads(out_json.read_text(encoding="utf-8"))
    assert data["schema"] == "top5_audit_scheduler_manifest.v1"
    assert len(data["files"]) == 2

    vrf = subprocess.run(
        [
            sys.executable,
            str(repo_tool),
            "verify",
            "--repo",
            str(repo),
            "--manifest",
            str(out_json),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert vrf.returncode == 0, vrf.stderr + vrf.stdout

    (repo / "t/hello.txt").write_bytes(b"tampered")
    bad = subprocess.run(
        [
            sys.executable,
            str(repo_tool),
            "verify",
            "--repo",
            str(repo),
            "--manifest",
            str(out_json),
        ],
        check=False,
    )
    assert bad.returncode != 0
