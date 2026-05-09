#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.ensemble_sleeve_policy_audit_service import (  # noqa: E402
    build_rebuilt_alpha_candidate_sleeve_policy_audit,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Research-only sleeve policy audit for rebuilt alpha candidates.")
    parser.add_argument("--walk-forward-json", required=True)
    parser.add_argument("--candidate", default="hard_event_alpha_candidate")
    parser.add_argument("--min-retained-windows", type=int, default=4)
    parser.add_argument("--operator-name", default="ensemble_rebuilt_candidate_sleeve_policy_audit")
    parser.add_argument("--output-dir", default="logs/openclaw/ensemble_rebuilt_candidate_sleeve_policy_audit")
    args = parser.parse_args()

    input_path = Path(args.walk_forward_json)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    walk_forward = json.loads(input_path.read_text(encoding="utf-8"))
    audit = build_rebuilt_alpha_candidate_sleeve_policy_audit(
        walk_forward,
        candidate=str(args.candidate),
        min_retained_windows=int(args.min_retained_windows),
    )
    payload: dict[str, Any] = {
        "run_version": "ensemble_rebuilt_candidate_sleeve_policy_audit_tool.v1",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "operator_name": str(args.operator_name),
        "research_only": True,
        "source_walk_forward_json": str(input_path),
        "audit": audit,
    }
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"ensemble_rebuilt_candidate_sleeve_policy_audit_{stamp}.json"
    md_path = output_dir / f"ensemble_rebuilt_candidate_sleeve_policy_audit_{stamp}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(_markdown(payload), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "markdown": str(md_path)}, ensure_ascii=False, indent=2))
    return 0


def _markdown(payload: dict[str, Any]) -> str:
    audit = payload.get("audit") or {}
    summary = audit.get("validation_summary") or {}
    lines = [
        "# Rebuilt Alpha Candidate Sleeve Policy Audit",
        "",
        f"- research_only: {payload.get('research_only')}",
        f"- candidate: {audit.get('candidate')}",
        f"- candidate_discussion_eligible: {audit.get('candidate_discussion_eligible')}",
        f"- sleeve_policy_approved: {audit.get('sleeve_policy_approved')}",
        f"- observation_pool_eligible: {audit.get('observation_pool_eligible')}",
        f"- formal_pool_eligible: {audit.get('formal_pool_eligible')}",
        f"- blocking_reasons: {', '.join(audit.get('blocking_reasons') or []) or '(none)'}",
        "",
        "## Validation Summary",
        "",
        f"- passed_predeclared_walk_forward_gate: {summary.get('passed_predeclared_walk_forward_gate')}",
        f"- retained_window_count: {summary.get('retained_window_count')}",
        f"- positive_retained_window_count: {summary.get('positive_retained_window_count')}",
        f"- excluded_window_count: {summary.get('excluded_window_count')}",
        f"- sample_count: {summary.get('sample_count')}",
        f"- raw_sample_count: {summary.get('raw_sample_count')}",
        f"- sample_retention: {summary.get('sample_retention')}",
        f"- ic: {summary.get('ic')}",
        f"- rank_ic: {summary.get('rank_ic')}",
        "",
        "## Required Next Evidence",
        "",
    ]
    lines.extend(f"- {item}" for item in audit.get("required_next_evidence") or [])
    lines.extend(["", "## Hard Boundaries", ""])
    lines.extend(f"- {item}" for item in audit.get("hard_boundaries") or [])
    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
