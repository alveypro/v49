#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.candidate_quality_density_progress import (
    build_candidate_quality_density_progress,
    write_candidate_quality_density_progress_artifact,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build formal candidate quality long-window density progress artifact.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    payload = build_candidate_quality_density_progress(exp_dir=args.exp_dir)
    output_path = write_candidate_quality_density_progress_artifact(payload, output_dir=args.exp_dir)
    result = {
        "status": payload["status"],
        "blocking_reasons": payload["blocking_reasons"],
        "output_path": output_path,
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"{result['status']}: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
