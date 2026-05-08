#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_learning_feedback import build_primary_result_learning_feedback_from_path


def _write_output(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build governed learning feedback from primary result failure attribution.")
    parser.add_argument("--attribution-json", default="data/experiments/primary_result_failure_attribution_latest.json")
    parser.add_argument("--output", default="data/experiments/primary_result_learning_feedback_latest.json")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        payload = build_primary_result_learning_feedback_from_path(attribution_path=args.attribution_json)
        _write_output(Path(args.output), payload)
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(
                json.dumps(
                    {
                        "status": payload["status"],
                        "result_id": payload["result_id"],
                        "change_total": payload["change_total"],
                        "max_severity": payload["max_severity"],
                        "requires_baseline_revalidation": payload["requires_baseline_revalidation"],
                        "do_not_auto_apply": payload["do_not_auto_apply"],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
        return 0
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
