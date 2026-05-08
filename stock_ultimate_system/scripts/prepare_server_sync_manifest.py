#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_VERSION = "server_sync_manifest.v1"

DENY_DIR_PARTS = {
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".venv",
    ".release_gate_runtime",
    "logs",
}
DENY_RELATIVE_PREFIXES = (
    "artifacts/",
    "data/experiments/",
    "data/cache/",
    "data/raw/",
    "data/processed/",
    "data/models/",
    "data/reports/",
    "tmp/",
)
DENY_FILE_NAMES = {
    ".DS_Store",
    ".env",
    ".tushare_token",
    "config.json",
    "notification_config.json",
}
DENY_SUFFIXES = {
    ".pyc",
    ".db",
    ".sqlite",
    ".sqlite3",
    ".log",
    ".csv",
    ".parquet",
}

ALLOW_ROOT_FILES = {"requirements.txt", "requirements.stock-scoped.txt"}
ALLOW_ROOT_SUFFIXES = {".md", ".py"}
ALLOW_PREFIXES = (
    "src/",
    "scripts/",
    "tests/",
    "docs/",
    "config/",
    "deploy/",
    "tools/",
)


def _matches_relative_prefix(relative_path: str, prefixes: tuple[str, ...]) -> bool:
    return any(
        relative_path.startswith(prefix) or f"/{prefix}" in relative_path
        for prefix in prefixes
    )


def _to_relative_path(path: Path, project_root: Path) -> str:
    return path.relative_to(project_root).as_posix()


def _deny_reason(relative_path: str, path: Path) -> str | None:
    if any(part in DENY_DIR_PARTS for part in path.parts):
        return "cache_or_runtime_directory"
    if path.name in DENY_FILE_NAMES:
        return "local_secret_or_machine_file"
    if path.suffix in DENY_SUFFIXES:
        return "runtime_or_generated_file_type"
    if _matches_relative_prefix(relative_path, DENY_RELATIVE_PREFIXES):
        return "stateful_runtime_data"
    return None


def _is_allowed_source(relative_path: str, path: Path) -> bool:
    if relative_path in ALLOW_ROOT_FILES:
        return True
    if "/" not in relative_path and path.suffix in ALLOW_ROOT_SUFFIXES:
        return True
    if any(relative_path.startswith(prefix) for prefix in ALLOW_PREFIXES):
        return True
    if relative_path.startswith("data/") and path.name == "__init__.py":
        return True
    return False


def build_server_sync_manifest(project_root: str | Path = PROJECT_ROOT) -> dict[str, object]:
    root = Path(project_root).resolve()
    allowed_files: list[str] = []
    denied_files: list[dict[str, str]] = []
    unclassified_files: list[str] = []

    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative_path = _to_relative_path(path, root)
        deny_reason = _deny_reason(relative_path, path)
        if deny_reason:
            denied_files.append({"path": relative_path, "reason": deny_reason})
            continue
        if _is_allowed_source(relative_path, path):
            allowed_files.append(relative_path)
            continue
        unclassified_files.append(relative_path)

    return {
        "manifest_version": MANIFEST_VERSION,
        "project_root": str(root),
        "sync_policy": "code_config_docs_tests_deploy_only",
        "allowed_total": len(allowed_files),
        "denied_total": len(denied_files),
        "unclassified_total": len(unclassified_files),
        "allowed_files": allowed_files,
        "denied_files": denied_files,
        "unclassified_files": unclassified_files,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a safe server sync manifest for stock_ultimate_system.")
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument("--output", help="Optional path to write the manifest JSON.")
    args = parser.parse_args()

    manifest = build_server_sync_manifest(args.project_root)
    text = json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
