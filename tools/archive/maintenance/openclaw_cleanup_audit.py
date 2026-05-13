#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import time
from typing import Any


MANIFEST_VERSION = "openclaw_cleanup_audit.v1"
DEFAULT_OUTPUT_DIR = Path("logs/openclaw/cleanup_audit")
SAFE_DELETE_GLOBS = {
    "*.pyc",
    ".DS_Store",
}
SAFE_DELETE_NAMES = {
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
}
MAC_RESOURCE_PREFIX = "._"
DB_SUFFIXES = (".db", ".sqlite", ".db.gz", ".bak")


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _sha256_text(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _path_size(path: Path) -> int:
    try:
        if path.is_file() or path.is_symlink():
            return int(path.stat().st_size)
        total = 0
        for item in path.rglob("*"):
            try:
                if item.is_file() or item.is_symlink():
                    total += int(item.stat().st_size)
            except OSError:
                continue
        return total
    except OSError:
        return 0


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False


def _entry(path: Path, *, action: str, reason: str, root: Path) -> dict[str, Any]:
    try:
        stat = path.stat()
        mtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stat.st_mtime))
    except OSError:
        mtime = ""
    return {
        "path": str(path),
        "relative_path": str(path.resolve().relative_to(root.resolve())) if _is_relative_to(path, root) else "",
        "kind": "dir" if path.is_dir() else "file",
        "size_bytes": _path_size(path),
        "mtime": mtime,
        "action": action,
        "reason": reason,
    }


def _scan_safe_delete(root: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in root.rglob("*"):
        name = path.name
        if path.is_dir() and name in SAFE_DELETE_NAMES:
            out.append(_entry(path, action="delete", reason=f"cache_dir:{name}", root=root))
            continue
        if path.is_file() and (name in SAFE_DELETE_GLOBS or name.startswith(MAC_RESOURCE_PREFIX)):
            out.append(_entry(path, action="delete", reason=f"cache_file:{name}", root=root))
            continue
        if path.is_file() and path.suffix == ".pyc":
            out.append(_entry(path, action="delete", reason="python_bytecode", root=root))
    return _dedupe_entries(out)


def _scan_manual_review(root: Path, *, large_mb: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    min_size = int(large_mb) * 1024 * 1024
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        size = _path_size(path)
        lower = path.name.lower()
        if size >= min_size and lower.endswith(DB_SUFFIXES):
            out.append(_entry(path, action="manual_review", reason=f"large_database_like_file:>={large_mb}MB", root=root))
        elif size >= min_size and ("/logs/" in str(path) or str(path).startswith("logs/")):
            out.append(_entry(path, action="manual_review", reason=f"large_log_file:>={large_mb}MB", root=root))
    return _dedupe_entries(out)


def _dedupe_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for item in entries:
        path = str(item.get("path") or "")
        if not path or path in seen:
            continue
        seen.add(path)
        out.append(item)
    return sorted(out, key=lambda x: (str(x.get("action")), -int(x.get("size_bytes") or 0), str(x.get("path"))))


def _apply_delete(entries: list[dict[str, Any]], *, root: Path) -> list[dict[str, Any]]:
    applied: list[dict[str, Any]] = []
    for item in entries:
        if item.get("action") != "delete":
            continue
        path = Path(str(item.get("path") or ""))
        if not _is_relative_to(path, root):
            applied.append({**item, "applied": False, "apply_error": "path_outside_root"})
            continue
        try:
            if path.is_dir():
                shutil.rmtree(path)
            elif path.exists():
                path.unlink()
            applied.append({**item, "applied": True})
        except Exception as exc:
            applied.append({**item, "applied": False, "apply_error": str(exc)})
    return applied


def build_manifest(root: Path, *, large_mb: int, apply: bool, operator: str) -> dict[str, Any]:
    root = root.expanduser().resolve()
    safe = _scan_safe_delete(root)
    manual = _scan_manual_review(root, large_mb=large_mb)
    candidates = _dedupe_entries(safe + manual)
    delete_candidates = [x for x in candidates if x.get("action") == "delete"]
    manual_candidates = [x for x in candidates if x.get("action") == "manual_review"]
    applied = _apply_delete(delete_candidates, root=root) if apply else []
    manifest: dict[str, Any] = {
        "artifact_version": MANIFEST_VERSION,
        "created_at": _now_text(),
        "operator": str(operator or ""),
        "root": str(root),
        "mode": "apply" if apply else "dry_run",
        "policy": "docs/AIRIVO_CLEANUP_RETENTION_POLICY.md",
        "summary": {
            "delete_candidates": len(delete_candidates),
            "manual_review_candidates": len(manual_candidates),
            "delete_candidate_bytes": sum(int(x.get("size_bytes") or 0) for x in delete_candidates),
            "manual_review_bytes": sum(int(x.get("size_bytes") or 0) for x in manual_candidates),
            "applied_count": len([x for x in applied if x.get("applied") is True]),
            "applied_bytes": sum(int(x.get("size_bytes") or 0) for x in applied if x.get("applied") is True),
        },
        "candidates": candidates,
        "applied": applied,
        "hard_boundaries": [
            "dry_run_is_default",
            "manual_review_items_are_never_deleted_by_this_script",
            "path_must_be_under_root",
            "production_roots_must_be_explicit",
        ],
    }
    manifest["manifest_hash"] = _sha256_text({k: v for k, v in manifest.items() if k != "manifest_hash"})
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="OpenClaw cleanup audit manifest generator. Dry-run by default.")
    parser.add_argument("--root", default=".", help="Root to scan. Production should pass /opt/openclaw/current or /opt/openclaw.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--large-mb", type=int, default=100)
    parser.add_argument("--operator", default=os.getenv("USER", "cleanup_operator"))
    parser.add_argument("--apply", action="store_true", help="Delete only safe delete candidates. Manual-review items are never deleted.")
    args = parser.parse_args()

    manifest = build_manifest(
        Path(args.root),
        large_mb=int(args.large_mb),
        apply=bool(args.apply),
        operator=str(args.operator or ""),
    )
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"openclaw_cleanup_audit_{stamp}.json"
    out_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"manifest": str(out_path), "summary": manifest["summary"], "mode": manifest["mode"]}, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
