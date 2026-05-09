from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.candidate_quality_diff import (
    build_candidate_quality_diff,
    write_candidate_quality_diff_artifact,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build current-vs-previous formal candidate quality diff.")
    parser.add_argument("--current-summary-path", default="data/experiments/candidate_quality_summary.json")
    parser.add_argument("--previous-summary-path", default=None)
    parser.add_argument("--failure-summary-path", default=None)
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--artifacts-dir", default="artifacts")
    parser.add_argument("--output-dir", default="data/experiments")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    payload = build_candidate_quality_diff(
        current_summary_path=args.current_summary_path,
        previous_summary_path=args.previous_summary_path,
        failure_summary_path=args.failure_summary_path,
        exp_dir=args.exp_dir,
        artifacts_dir=args.artifacts_dir,
    )
    output_path = write_candidate_quality_diff_artifact(payload, output_dir=args.output_dir)
    response = {"status": "passed", "candidate_quality_diff": payload, "output_path": output_path}
    if args.json:
        json.dump(response, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
