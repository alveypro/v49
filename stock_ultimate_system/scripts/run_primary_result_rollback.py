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

from src.primary_result_rollback import build_primary_result_rollback
from src.unified_result_builder import build_primary_result_api_payload
from src.utils.project_paths import resolve_project_path


def _write_output(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run_primary_result_rollback(
    *,
    exp_dir: str | Path = "data/experiments",
    candidate_index: int = 0,
    output_path: str | Path | None = None,
    now: datetime | None = None,
    ignore_current_pointer: bool = False,
) -> tuple[int, dict[str, object]]:
    resolved_exp_dir = resolve_project_path(exp_dir)
    if not ignore_current_pointer:
        primary_result_payload = build_primary_result_api_payload(
            resolved_exp_dir,
            candidate_index=candidate_index,
            require_current_pointer=True,
        )
    else:
        primary_result_payload = build_primary_result_api_payload(
            resolved_exp_dir,
            candidate_index=candidate_index,
            require_current_pointer=False,
            ignore_current_pointer=True,
        )
    rollback = build_primary_result_rollback(primary_result_payload, now=now)
    output = Path(output_path) if output_path is not None else resolved_exp_dir / "primary_result_rollback_latest.json"
    _write_output(output, rollback)
    return (0 if rollback["rollback_status"] in {"not_required", "completed"} else 1), rollback


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a local rollback decision artifact for the stock primary result.")
    parser.add_argument("--exp-dir", default="data/experiments")
    parser.add_argument("--candidate-index", type=int, default=0)
    parser.add_argument("--output")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    exit_code, payload = run_primary_result_rollback(
        exp_dir=args.exp_dir,
        candidate_index=args.candidate_index,
        output_path=args.output,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(json.dumps({"rollback_status": payload["rollback_status"], "result_id": payload["result_id"]}, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
