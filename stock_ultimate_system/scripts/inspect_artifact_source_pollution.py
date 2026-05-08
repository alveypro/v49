#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.artifact_source_audit import audit_artifact_source_pollution


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect artifacts/latest files for pytest or temporary source-path pollution.")
    parser.add_argument("--artifacts-dir", default="artifacts")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    payload = audit_artifact_source_pollution(artifacts_dir=args.artifacts_dir)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "polluted_file_total": payload["polluted_file_total"],
                    "polluted_paths": [item["path"] for item in payload["polluted_files"]],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return 1 if payload["polluted_file_total"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
