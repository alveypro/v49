#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.primary_result_content_quality import evaluate_primary_result_content_quality
from src.unified_result_builder import build_primary_result_api_payload
from src.utils.project_paths import resolve_project_path


def _write_output(path: str | Path | None, payload: dict[str, object]) -> None:
    if not path:
        return
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def check_primary_result_content_quality(
    *,
    exp_dir: str | Path = "data/experiments",
    candidate_index: int = 0,
    max_source_age_hours: float = 36.0,
    output_path: str | Path | None = None,
    now: datetime | None = None,
) -> tuple[int, dict[str, object]]:
    # Content quality evaluates candidate/readability readiness and may run before pointer governance is established.
    payload = build_primary_result_api_payload(
        resolve_project_path(exp_dir),
        candidate_index=candidate_index,
        require_current_pointer=False,
    )
    result = evaluate_primary_result_content_quality(
        payload,
        now=now,
        max_source_age_hours=max_source_age_hours,
    )
    result["primary_result_payload"] = payload
    _write_output(output_path, result)
    return (0 if result["status"] == "passed" else 1), result


def main() -> int:
    parser = argparse.ArgumentParser(description="Check whether the primary result content is production-candidate quality.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--candidate-index", type=int, default=0)
    parser.add_argument("--max-source-age-hours", type=float, default=36.0)
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    exit_code, payload = check_primary_result_content_quality(
        exp_dir=args.exp_dir,
        candidate_index=args.candidate_index,
        max_source_age_hours=args.max_source_age_hours,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "status": payload["status"],
                    "readiness_level": payload["readiness_level"],
                    "blocking_failure_total": len(payload["blocking_failures"]),
                    "warning_total": len(payload["warnings"]),
                    "summary": payload["summary"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
