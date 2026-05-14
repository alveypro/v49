#!/usr/bin/env python3
"""Generate / verify deterministic manifest over Top5 audit deploy-sync tree (manifest.v1).

Usage:
  python3 deploy/tools/top5_audit_scheduler_manifest.py generate \\
    --repo . --paths deploy/top5_audit_sync_paths.txt \\
    --out-json artifacts/top5_audit_scheduler_manifest.json \\
    [--sha256sums artifacts/top5_audit_scheduler_manifest.sha256]

  python3 deploy/tools/top5_audit_scheduler_manifest.py verify \\
    --repo . --manifest path/to/top5_audit_scheduler_manifest.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


SCHEMA_V1 = "top5_audit_scheduler_manifest.v1"


def _repo_root(repo: Path) -> Path:
    r = repo.expanduser().resolve()
    if not r.is_dir():
        raise SystemExit(f"--repo must be directory: {r}")
    return r


def load_paths(paths_file: Path, repo_root: Path) -> list[str]:
    if not paths_file.is_file():
        raise SystemExit(f"paths file not found: {paths_file}")
    out: list[str] = []
    for raw in paths_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        lp = repo_root / line
        if not lp.is_file():
            raise SystemExit(f"missing tracked path (sync list): {line}")
        out.append(line)
    if not out:
        raise SystemExit("paths list is empty")
    return out


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def cmd_generate(repo: Path, paths_file: Path, out_json: Path, sums_out: Path | None) -> int:
    root = _repo_root(repo)
    rels = load_paths(paths_file.resolve(), root)

    git_sha_full = ""
    git_sha_short = ""
    try:
        import subprocess

        gh = os.getenv("GITHUB_SHA", "").strip()
        if gh and len(gh) >= 7:
            git_sha_full = gh
            git_sha_short = gh[:7]
        else:
            out = subprocess.run(
                ["git", "-C", str(root), "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                check=False,
            )
            if out.returncode == 0:
                git_sha_full = out.stdout.strip()
                git_sha_short = subprocess.run(
                    ["git", "-C", str(root), "rev-parse", "--short", "HEAD"],
                    capture_output=True,
                    text=True,
                    check=False,
                ).stdout.strip()

    except OSError:
        pass

    files_payload: list[dict[str, str]] = []
    lines_sums: list[str] = []
    for rel in rels:
        fp = root / rel
        dig = sha256_file(fp)
        files_payload.append({"path": rel, "sha256": dig})
        # GNU sha256sum -c expects: "<hash>  <filepath>" relative to cwd when verifying from repo root
        lines_sums.append(f"{dig}  {rel}")

    pf_abs = paths_file.resolve()
    try:
        paths_file_display = pf_abs.relative_to(root).as_posix()
    except ValueError:
        paths_file_display = pf_abs.as_posix()

    manifest: dict[str, object] = {
        "schema": SCHEMA_V1,
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "paths_file": paths_file_display,
        "git_sha_full": git_sha_full,
        "git_sha_short": git_sha_short,
        "github_run_id": os.getenv("GITHUB_RUN_ID", "").strip() or "",
        "github_workflow_ref": os.getenv("GITHUB_WORKFLOW_REF", "").strip() or "",
        "paths": rels,
        "files": files_payload,
    }

    out_json = out_json.resolve()
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if sums_out is not None:
        sums_out = sums_out.resolve()
        sums_out.parent.mkdir(parents=True, exist_ok=True)
        sums_out.write_text("\n".join(lines_sums) + "\n", encoding="utf-8")

    print(f"wrote manifest: {out_json} ({len(files_payload)} files)")
    if sums_out is not None:
        print(f"wrote checksums (sha256sum -c from repo root): {sums_out}")
    return 0


def cmd_verify(repo: Path, manifest_path: Path) -> int:
    root = _repo_root(repo)
    if not manifest_path.is_file():
        raise SystemExit(f"manifest not found: {manifest_path}")
    blob = json.loads(manifest_path.read_text(encoding="utf-8"))

    schema = blob.get("schema")
    if schema != SCHEMA_V1:
        sys.stderr.write(f"[verify-manifest] unsupported schema={schema} (want {SCHEMA_V1})\n")
        return 4

    files = blob.get("files")
    if not isinstance(files, list) or not files:
        sys.stderr.write("[verify-manifest] missing files[]\n")
        return 5

    for item in files:
        if not isinstance(item, dict):
            sys.stderr.write("[verify-manifest] invalid files[] entry\n")
            return 5
        rel = str(item.get("path") or "").strip().replace("\\", "/")
        expect = str(item.get("sha256") or "").strip().lower()
        if not rel or len(expect) != 64:
            sys.stderr.write(f"[verify-manifest] bad manifest entry for path={rel!r}\n")
            return 5

        fp = root / rel
        if not fp.is_file():
            sys.stderr.write(f"[verify-manifest] MISSING: {rel}\n")
            return 2
        actual = sha256_file(fp).lower()
        if actual != expect:
            sys.stderr.write(f"[verify-manifest] SHA256_MISMATCH: {rel}\nexpected  {expect}\nactual    {actual}\n")
            return 3

    gid = blob.get("git_sha_short") or blob.get("git_sha_full") or ""
    rid = blob.get("github_run_id") or ""
    suffix = ""
    if gid or rid:
        suffix = f" (git_sha={gid!r}" + (f", github_run_id={rid!r}" if rid else "") + ")"
    print(f"[verify-manifest] OK: {len(files)} files match SHA256.{suffix}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Top5 audit deploy manifest.")
    subs = p.add_subparsers(dest="cmd", required=True)

    g = subs.add_parser("generate")
    g.add_argument("--repo", type=Path, default=Path("."))
    g.add_argument("--paths", type=Path, required=True)
    g.add_argument("--out-json", type=Path, required=True)
    g.add_argument("--sha256sums", type=Path, default=None)

    v = subs.add_parser("verify")
    v.add_argument("--repo", type=Path, default=Path("."))
    v.add_argument("--manifest", type=Path, required=True)

    args = p.parse_args()
    if args.cmd == "generate":
        return cmd_generate(args.repo, args.paths, args.out_json, args.sha256sums)
    if args.cmd == "verify":
        return cmd_verify(args.repo, args.manifest)
    raise SystemExit(99)


if __name__ == "__main__":
    raise SystemExit(main())
