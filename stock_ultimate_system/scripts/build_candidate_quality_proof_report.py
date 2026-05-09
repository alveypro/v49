#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.candidate_quality.proof_report import (
    build_candidate_quality_proof_report,
    write_candidate_quality_proof_report,
)
from src.utils.project_paths import resolve_experiments_path, resolve_project_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the candidate quality proof total report.")
    parser.add_argument("--exp-dir", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exp_dir = resolve_project_path(args.exp_dir) if args.exp_dir else resolve_experiments_path()
    output = resolve_project_path(args.output) if args.output else exp_dir / "candidate_quality_proof_report_latest.json"
    report = build_candidate_quality_proof_report(exp_dir=exp_dir)
    output_path = write_candidate_quality_proof_report(report, output_path=output)
    payload = {
        "status": report.get("status"),
        "output_path": output_path,
        "quality_proven": (report.get("decision", {}) or {}).get("quality_proven", False),
        "external_page_mode": (report.get("decision", {}) or {}).get("external_page_mode"),
        "prohibited_claims": report.get("prohibited_claims", []),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"{payload['status']}: quality_proven={payload['quality_proven']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
