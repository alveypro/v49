#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.candidate_quality_baseline_registry import CandidateQualityBaselineRegistry


def main() -> int:
    parser = argparse.ArgumentParser(description="Register the current candidate quality summary as the previous formal baseline.")
    parser.add_argument("--artifacts-dir", default="artifacts/candidate_quality_baselines")
    parser.add_argument("--summary-path", default="data/experiments/candidate_quality_summary.json")
    parser.add_argument("--baseline-id", default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    registry = CandidateQualityBaselineRegistry(baselines_dir=args.artifacts_dir)
    try:
        snapshot = registry.register(
            summary_path=args.summary_path,
            baseline_id=args.baseline_id,
        )
        response = {
            "status": "ok",
            "baseline_snapshot": snapshot,
            "current_pointer_path": str(registry.current_path),
        }
        if args.json:
            json.dump(response, sys.stdout, ensure_ascii=False, indent=2)
            sys.stdout.write("\n")
        return 0
    except Exception as exc:
        response = {
            "status": "error",
            "error": str(exc),
        }
        if args.json:
            json.dump(response, sys.stdout, ensure_ascii=False, indent=2)
            sys.stdout.write("\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
