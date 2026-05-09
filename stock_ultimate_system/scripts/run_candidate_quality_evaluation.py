from __future__ import annotations

import argparse
import json
import sys

from src.candidate_quality_evaluation import (
    build_candidate_quality_evaluation,
    write_candidate_quality_evaluation_artifacts,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build formal candidate quality evaluation artifacts.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--artifacts-dir", default="artifacts")
    parser.add_argument("--benchmark-report-path", default=None)
    parser.add_argument("--failure-attribution-path", default=None)
    parser.add_argument("--previous-summary-path", default=None)
    parser.add_argument("--output-dir", default="data/experiments")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    payload = build_candidate_quality_evaluation(
        exp_dir=args.exp_dir,
        artifacts_dir=args.artifacts_dir,
        benchmark_report_path=args.benchmark_report_path,
        failure_attribution_path=args.failure_attribution_path,
        previous_summary_path=args.previous_summary_path,
    )
    output_paths = write_candidate_quality_evaluation_artifacts(payload, output_dir=args.output_dir)
    response = {"status": "passed", "evaluation": payload, "output_paths": output_paths}
    if args.json:
        json.dump(response, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
