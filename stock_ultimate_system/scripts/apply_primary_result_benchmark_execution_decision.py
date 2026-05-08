#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_benchmark_execution_review_update import apply_benchmark_execution_to_review_queue


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply benchmark plan execution evidence back to the feedback review queue.")
    parser.add_argument("--execution-json", required=True)
    parser.add_argument("--queue-dir", default="artifacts/primary_result_feedback_review_queue")
    parser.add_argument("--actor", default="benchmark_executor")
    args = parser.parse_args()
    try:
        payload = apply_benchmark_execution_to_review_queue(
            execution_json_path=args.execution_json,
            queue_dir=args.queue_dir,
            actor=args.actor,
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
