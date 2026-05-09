#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.stock_entry_guard import write_stock_entry_guard_artifact


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate and persist the /stock entry guard artifact.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--artifacts-dir", default="artifacts")
    parser.add_argument("--output", default="artifacts/stock_entry_guard_latest.json")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    payload = write_stock_entry_guard_artifact(
        output_path=args.output,
        exp_dir=args.exp_dir,
        artifacts_dir=args.artifacts_dir,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "ok": payload["ok"],
                    "output_path": payload["output_path"],
                    "problems": payload["problems"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
