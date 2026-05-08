#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.candidate_quality_validation_history_archive import build_candidate_quality_validation_history_archive


def main() -> int:
    parser = argparse.ArgumentParser(description="Archive current formal candidate validation into dated history.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    exit_code, payload = build_candidate_quality_validation_history_archive(exp_dir=args.exp_dir)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"{payload['status']}: {payload.get('output_path') or payload.get('blocking_reasons')}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
