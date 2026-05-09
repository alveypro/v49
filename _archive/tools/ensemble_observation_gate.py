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

from openclaw.services.ensemble_observation_gate_service import build_ensemble_observation_gate  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Research-only observation gate hardening for ensemble rebuilt candidates.")
    parser.add_argument("--shadow-benchmark-json", required=True)
    parser.add_argument("--candidate", default="hard_event_alpha_candidate")
    parser.add_argument("--min-fresh-windows", type=int, default=8)
    parser.add_argument("--target-fresh-windows", type=int, default=10)
    parser.add_argument("--min-overall-hit-rate", type=float, default=0.60)
    parser.add_argument("--min-regime-hit-rate", type=float, default=0.50)
    parser.add_argument("--max-turnover", type=float, default=0.75)
    parser.add_argument("--max-industry-concentration", type=float, default=0.30)
    parser.add_argument("--max-capacity-utilization", type=float, default=0.10)
    parser.add_argument("--operator-name", default="ensemble_observation_gate")
    parser.add_argument("--output-dir", default="logs/openclaw/ensemble_observation_gate")
    args = parser.parse_args()

    source_path = Path(args.shadow_benchmark_json)
    shadow_payload = json.loads(source_path.read_text(encoding="utf-8"))
    gate = build_ensemble_observation_gate(
        shadow_payload,
        candidate=str(args.candidate),
        min_fresh_windows=int(args.min_fresh_windows),
        target_fresh_windows=int(args.target_fresh_windows),
        min_overall_hit_rate=float(args.min_overall_hit_rate),
        min_regime_hit_rate=float(args.min_regime_hit_rate),
        max_turnover=float(args.max_turnover),
        max_industry_concentration=float(args.max_industry_concentration),
        max_capacity_utilization=float(args.max_capacity_utilization),
    )
    payload: dict[str, Any] = {
        "run_version": "ensemble_observation_gate_tool.v1",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "operator_name": str(args.operator_name),
        "source_shadow_benchmark_json": str(source_path),
        "gate": gate,
        "hard_boundaries": [
            "this_artifact_does_not_update_strategy_pool",
            "failed_gate_means_research_only",
            "passing_gate_requires_separate_observation_promotion_decision",
        ],
    }
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"ensemble_observation_gate_{stamp}.json"
    md_path = output_dir / f"ensemble_observation_gate_{stamp}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(_markdown(payload), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "markdown": str(md_path)}, ensure_ascii=False, indent=2))
    return 0


def _markdown(payload: dict[str, Any]) -> str:
    gate = payload.get("gate") or {}
    evidence = gate.get("evidence_summary") or {}
    lines = [
        "# Ensemble Observation Gate",
        "",
        f"- candidate: {gate.get('candidate')}",
        f"- research_only: {gate.get('research_only')}",
        f"- observation_gate_passed: {gate.get('observation_gate_passed')}",
        f"- observation_review_eligible: {gate.get('observation_review_eligible')}",
        f"- observation_pool_eligible: {gate.get('observation_pool_eligible')}",
        f"- formal_pool_eligible: {gate.get('formal_pool_eligible')}",
        f"- valid_window_count: {evidence.get('valid_window_count')}",
        f"- unique_as_of_window_count: {evidence.get('unique_as_of_window_count')}",
        f"- target_fresh_windows: {evidence.get('target_fresh_windows')}",
        f"- after_cost_excess_return: {evidence.get('after_cost_excess_return')}",
        f"- hit_rate: {evidence.get('hit_rate')}",
        f"- turnover: {evidence.get('turnover')}",
        f"- industry_concentration: {evidence.get('industry_concentration')}",
        f"- blocking_reasons: {', '.join(gate.get('blocking_reasons') or []) or '(none)'}",
        "",
        "## Required Next Evidence",
        "",
    ]
    lines.extend(f"- {item}" for item in gate.get("required_next_evidence") or ["(none)"])
    lines.extend(["", "## Hard Boundaries", ""])
    lines.extend(f"- {item}" for item in gate.get("hard_boundaries") or [])
    lines.extend([""])
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
