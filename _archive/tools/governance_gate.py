#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
MANDATE_DOC = "docs/MAINLINE_MANDATE.md"
DELIVERY_STANDARD_DOC = "docs/AIRIVO_UNIQUE_MAINLINE_DELIVERY_STANDARD.md"
EXECUTION_PLAN_DOC = "docs/AIRIVO_30_DAY_EXECUTION_PLAN.md"
ADJUDICATION_DOC = "docs/AIRIVO_MAINLINE_DELIVERY_ADJUDICATION_CHECKLIST.md"
REJECTION_STANDARD_DOC = "docs/AIRIVO_CODE_REVIEW_REJECTION_STANDARD.md"
PR_TEMPLATE = ".github/pull_request_template.md"
REQUIRED_MAINLINE_FILES = (
    MANDATE_DOC,
    DELIVERY_STANDARD_DOC,
    EXECUTION_PLAN_DOC,
    ADJUDICATION_DOC,
    REJECTION_STANDARD_DOC,
    PR_TEMPLATE,
)

ALLOWED_NEW_ROOT_FILES = {
    "README.md",
    ".gitignore",
    ".env.example",
    "config.json.example",
    "pytest.ini",
    "requirements.txt",
    "v49_app.py",
    "终极量价暴涨系统_v49.0_长期稳健版.py",
    "start_v49.sh",
    "start_v49_full.sh",
    "start_v49_streamlit.sh",
}

ROOT_BLOCKED_SUFFIXES = {
    ".py",
    ".sh",
    ".db",
    ".csv",
    ".log",
    ".sqlite",
    ".sqlite3",
    ".json",
}

WARN_LINE_THRESHOLD = 5000
FAIL_NEW_FILE_LINE_THRESHOLD = 1500
GOVERNANCE_SENSITIVE_PATHS = (
    ".github/",
    "tools/governance_gate.py",
    "tools/release_gate.sh",
)


def run_git(args: list[str]) -> str:
    return subprocess.check_output(["git", "-C", str(ROOT), *args], text=True).strip()


def parse_name_status(raw: str) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        status = parts[0]
        path = parts[-1]
        rows.append((status, path))
    return rows


def changed_entries(base: str | None, head: str | None, all_files: bool) -> list[tuple[str, str]]:
    if all_files:
        tracked = run_git(["ls-files"])
        return [("T", path) for path in tracked.splitlines() if path.strip()]

    if base and head:
        raw = run_git(["diff", "--name-status", f"{base}...{head}"])
        return parse_name_status(raw)

    raw = run_git(["diff", "--cached", "--name-status"])
    rows = parse_name_status(raw)
    if rows:
        return rows

    raw = run_git(["diff", "--name-status", "HEAD"])
    rows = parse_name_status(raw)
    if rows:
        return rows

    tracked = run_git(["ls-files"])
    return [("T", path) for path in tracked.splitlines() if path.strip()]


def is_root_level(path: str) -> bool:
    return "/" not in path


def is_added_status(status: str) -> bool:
    return status.startswith("A") or status.startswith("C") or status.startswith("R")


def line_count(path: Path) -> int:
    try:
        return sum(1 for _ in path.open("r", encoding="utf-8"))
    except Exception:
        return -1


def has_prefix(paths: Iterable[str], prefix: str) -> bool:
    return any(path.startswith(prefix) for path in paths)


def has_docs(paths: Iterable[str]) -> bool:
    return any(path.startswith("docs/") and path.endswith(".md") for path in paths)


def path_matches_rule(path: str, rule: str) -> bool:
    return path.startswith(rule) if rule.endswith("/") else path == rule


def has_rule_match(paths: Iterable[str], rules: Iterable[str]) -> bool:
    return any(path_matches_rule(path, rule) for path in paths for rule in rules)


def main() -> int:
    parser = argparse.ArgumentParser(description="Governance gate for repo hygiene and mainline discipline.")
    parser.add_argument("--base", help="git diff base sha")
    parser.add_argument("--head", help="git diff head sha")
    parser.add_argument("--all-files", action="store_true", help="scan tracked files when diff context is unavailable")
    args = parser.parse_args()

    entries = changed_entries(args.base, args.head, args.all_files)
    changed_paths = [path for _, path in entries]

    failures: list[str] = []
    warnings: list[str] = []

    for status, path in entries:
        rel = Path(path)
        suffix = rel.suffix.lower()

        if is_root_level(path) and is_added_status(status):
            if suffix in ROOT_BLOCKED_SUFFIXES and path not in ALLOWED_NEW_ROOT_FILES:
                failures.append(
                    f"root-level addition blocked: {path} "
                    f"(move it under mainline dirs, tools subdirs, archive/, or experiments/)"
                )

        full_path = ROOT / rel
        if not full_path.exists() or suffix != ".py":
            continue

        count = line_count(full_path)
        if count < 0:
            continue
        if is_added_status(status) and count > FAIL_NEW_FILE_LINE_THRESHOLD:
            failures.append(
                f"new python file too large: {path} has {count} lines "
                f"(split before merge; threshold={FAIL_NEW_FILE_LINE_THRESHOLD})"
            )
        elif count > WARN_LINE_THRESHOLD:
            warnings.append(
                f"large python file reminder: {path} has {count} lines "
                f"(high-risk file, avoid adding new responsibilities)"
            )

    migration_changed = has_prefix(changed_paths, "data/migrations/")
    kernel_changed = has_prefix(changed_paths, "trading_kernel/")
    docs_changed = has_docs(changed_paths)
    tests_changed = has_prefix(changed_paths, "tests/")
    governance_sensitive_changed = has_rule_match(changed_paths, GOVERNANCE_SENSITIVE_PATHS)

    for required in REQUIRED_MAINLINE_FILES:
        if not (ROOT / required).exists():
            failures.append(f"required mainline authority file missing: {required}")

    if migration_changed and not docs_changed:
        failures.append("migration changed without docs update under docs/")
    if migration_changed and not tests_changed:
        failures.append("migration changed without tests update under tests/")
    if kernel_changed and not tests_changed:
        failures.append("trading_kernel changed without tests update under tests/")
    if governance_sensitive_changed and not docs_changed:
        failures.append("governance or release paths changed without docs update under docs/")

    print("[governance] mandate:", MANDATE_DOC)
    print("[governance] delivery standard:", DELIVERY_STANDARD_DOC)
    print("[governance] execution plan:", EXECUTION_PLAN_DOC)
    print("[governance] adjudication checklist:", ADJUDICATION_DOC)
    print("[governance] rejection standard:", REJECTION_STANDARD_DOC)
    print("[governance] pr template:", PR_TEMPLATE)
    print("[governance] scanned paths:", len(changed_paths))
    for item in warnings:
        print("[governance][warn]", item)
    if failures:
        for item in failures:
            print("[governance][fail]", item)
        return 1
    print("[governance] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
