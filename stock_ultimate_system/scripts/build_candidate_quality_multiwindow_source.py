#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys

from src.candidate_quality_multiwindow_source import (
    build_candidate_quality_multiwindow_source,
    write_candidate_quality_multiwindow_source_artifact,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build candidate quality multiwindow source artifact.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--source-60d-path", default=None)
    parser.add_argument("--source-120d-path", default=None)
    parser.add_argument("--output-dir", default="data/experiments")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    payload = build_candidate_quality_multiwindow_source(
        exp_dir=args.exp_dir,
        source_60d_path=args.source_60d_path,
        source_120d_path=args.source_120d_path,
    )
    output_path = write_candidate_quality_multiwindow_source_artifact(payload, output_dir=args.output_dir)
    response = {
        "status": "passed",
        "multiwindow_source": payload,
        "output_path": output_path,
    }
    if args.json:
        json.dump(response, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
