#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_VERSION = "top5_execution_day_close.v1"


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _stamp() -> str:
    return f"{time.strftime('%Y%m%d_%H%M%S')}_{time.time_ns() % 1_000_000_000:09d}"


def _run(cmd: list[str]) -> dict[str, Any]:
    started = time.time()
    completed = subprocess.run(cmd, cwd=str(ROOT), text=True, capture_output=True)
    parsed: Any = None
    if completed.stdout.strip().startswith("{"):
        try:
            parsed = json.loads(completed.stdout)
        except Exception:
            parsed = None
    return {
        "cmd": cmd,
        "returncode": completed.returncode,
        "elapsed_sec": round(time.time() - started, 3),
        "stdout_tail": completed.stdout[-4000:],
        "stderr_tail": completed.stderr[-4000:],
        "parsed_stdout": parsed,
        "passed": completed.returncode == 0,
    }


def _md(report: dict[str, Any]) -> str:
    lines = [
        "# Top5 Execution Day Close",
        "",
        f"- Created at: {report.get('created_at')}",
        f"- Mode: {'commit' if report.get('commit') else 'dry-run'}",
        f"- Passed: {report.get('passed')}",
        f"- Blocking reasons: {', '.join(report.get('blocking_reasons') or []) or 'none'}",
        "",
        "## Import Dry Run",
    ]
    dry = report.get("import_dry_run") or {}
    dry_payload = dry.get("parsed_stdout") if isinstance(dry, dict) else {}
    if isinstance(dry_payload, dict):
        lines.extend(
            [
                f"- Imported count: {dry_payload.get('imported_count')}",
                f"- Skipped count: {dry_payload.get('skipped_count')}",
                f"- Manifest: {dry_payload.get('manifest')}",
                f"- Reject CSV: {dry_payload.get('reject_csv')}",
            ]
        )
    lines.extend(["", "## Import Commit"])
    commit = report.get("import_commit") or {}
    commit_payload = commit.get("parsed_stdout") if isinstance(commit, dict) else {}
    if isinstance(commit_payload, dict):
        lines.extend(
            [
                f"- Imported count: {commit_payload.get('imported_count')}",
                f"- Skipped count: {commit_payload.get('skipped_count')}",
                f"- Manifest: {commit_payload.get('manifest')}",
                f"- Reject CSV: {commit_payload.get('reject_csv')}",
            ]
        )
    else:
        lines.append("- not run")
    lines.extend(["", "## Downstream Reports"])
    for name in ("completeness", "evidence_summary", "promotion_readiness"):
        check = report.get(name) or {}
        lines.append(f"- {name}: {'passed' if check.get('passed') else 'failed'}")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Close a Top5 execution day from the operations CSV and rebuild evidence reports.")
    parser.add_argument("--input-csv", default="exports/top5_execution_open_observations.csv")
    parser.add_argument("--ledger", default="logs/openclaw/top5_execution_observations.jsonl")
    parser.add_argument("--operator", default="daily_ops")
    parser.add_argument("--import-dir", default="logs/openclaw/top5_execution_imports")
    parser.add_argument("--output-dir", default="logs/openclaw/top5_execution_day_close")
    parser.add_argument("--commit", action="store_true", help="Append valid updates to the JSONL ledger after a clean dry-run.")
    parser.add_argument("--allow-skipped", action="store_true", help="Allow commit even if the dry-run has skipped rows.")
    args = parser.parse_args()

    py = sys.executable
    stamp = _stamp()
    output_dir = ROOT / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    dry_cmd = [
        py,
        "tools/import_top5_execution_observation_updates.py",
        "--input-csv",
        args.input_csv,
        "--ledger",
        args.ledger,
        "--operator",
        args.operator,
        "--output-dir",
        args.import_dir,
        "--dry-run",
    ]
    dry = _run(dry_cmd)
    blocking: list[str] = []
    dry_payload = dry.get("parsed_stdout") if isinstance(dry.get("parsed_stdout"), dict) else {}
    imported_count = int(dry_payload.get("imported_count") or 0) if isinstance(dry_payload, dict) else 0
    skipped_count = int(dry_payload.get("skipped_count") or 0) if isinstance(dry_payload, dict) else 0
    if not dry.get("passed"):
        blocking.append("import_dry_run_failed")
    if args.commit and imported_count <= 0:
        blocking.append("no_importable_rows")
    if args.commit and skipped_count > 0 and not args.allow_skipped:
        blocking.append("skipped_rows_present")

    commit = None
    if args.commit and not blocking:
        commit_cmd = [
            py,
            "tools/import_top5_execution_observation_updates.py",
            "--input-csv",
            args.input_csv,
            "--ledger",
            args.ledger,
            "--operator",
            args.operator,
            "--output-dir",
            args.import_dir,
        ]
        commit = _run(commit_cmd)
        if not commit.get("passed"):
            blocking.append("import_commit_failed")

    completeness = _run(
        [
            py,
            "tools/check_top5_execution_observation_completeness.py",
            "--ledger",
            args.ledger,
            "--open-output-csv",
            "exports/top5_execution_open_observations.csv",
        ]
    )
    evidence_summary = _run(
        [
            py,
            "tools/build_top5_execution_evidence_summary.py",
            "--ledger",
            args.ledger,
            "--output-json",
            "exports/top5_execution_evidence_summary.json",
            "--output-md",
            "exports/top5_execution_evidence_summary.md",
        ]
    )
    promotion = _run(
        [
            py,
            "tools/evaluate_v9_canary_promotion_readiness.py",
            "--ledger",
            args.ledger,
            "--import-dir",
            args.import_dir,
            "--output-json",
            "exports/v9_canary_promotion_readiness.json",
            "--output-md",
            "exports/v9_canary_promotion_readiness.md",
        ]
    )
    for name, check in (
        ("completeness_failed", completeness),
        ("evidence_summary_failed", evidence_summary),
        ("promotion_readiness_failed", promotion),
    ):
        if not check.get("passed"):
            blocking.append(name)

    report = {
        "artifact_version": ARTIFACT_VERSION,
        "created_at": _now_text(),
        "commit": bool(args.commit),
        "passed": not blocking,
        "blocking_reasons": blocking,
        "input_csv": args.input_csv,
        "ledger": args.ledger,
        "operator": args.operator,
        "import_dry_run": dry,
        "import_commit": commit,
        "completeness": completeness,
        "evidence_summary": evidence_summary,
        "promotion_readiness": promotion,
    }
    json_path = output_dir / f"top5_execution_day_close_{stamp}.json"
    md_path = output_dir / f"top5_execution_day_close_{stamp}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_md(report), encoding="utf-8")
    print(json.dumps({"report": str(json_path), "markdown": str(md_path), "passed": report["passed"], "blocking_reasons": blocking}, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
