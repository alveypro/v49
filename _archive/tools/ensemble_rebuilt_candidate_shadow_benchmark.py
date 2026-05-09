#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.services.ensemble_alpha_failure_attribution_service import _market_regime  # noqa: E402
from openclaw.services.ensemble_alpha_sleeve_service import build_ensemble_alpha_sleeve_fact_chain  # noqa: E402
from openclaw.services.ensemble_execution_cost_service import build_ensemble_execution_cost_replay  # noqa: E402
from openclaw.services.ensemble_rebuilt_candidate_rule_freeze_service import build_rebuilt_candidate_rule_freeze  # noqa: E402
from openclaw.services.ensemble_shadow_portfolio_service import build_rebuilt_candidate_shadow_portfolio  # noqa: E402
from openclaw.services.ensemble_walk_forward_benchmark_service import (  # noqa: E402
    FORMAL_POOL_BENCHMARK_STRATEGIES,
    build_ensemble_walk_forward_shadow_benchmark,
)
from openclaw.services.formal_pool_benchmark_service import build_formal_pool_benchmark_return_series  # noqa: E402


DEFAULT_STRATEGIES = ("v4", "v5", "v8", "v9", "combo", "v6", "v7")


def main() -> int:
    parser = argparse.ArgumentParser(description="Research-only rebuilt candidate shadow portfolio and after-cost benchmark.")
    parser.add_argument("--candidate-policy-audit-json", required=True)
    parser.add_argument("--as-of-dates", required=True)
    parser.add_argument("--candidate", default="hard_event_alpha_candidate")
    parser.add_argument("--candidate-rule-version", default="")
    parser.add_argument("--db-path", default="permanent_stock_database.db")
    parser.add_argument("--strategies", default=",".join(DEFAULT_STRATEGIES))
    parser.add_argument("--holding-days", type=int, default=5)
    parser.add_argument("--top-n-per-strategy", type=int, default=20)
    parser.add_argument("--max-positions", type=int, default=10)
    parser.add_argument("--min-benchmark-windows", type=int, default=5)
    parser.add_argument("--single-name-cap", type=float, default=0.10)
    parser.add_argument("--industry-cap", type=float, default=0.28)
    parser.add_argument("--target-gross-exposure", type=float, default=0.75)
    parser.add_argument("--neutral-gross-exposure", type=float, default=0.45)
    parser.add_argument("--paired-output-dir", default="", help="Optional second output dir built from the same fact chains.")
    parser.add_argument("--paired-target-gross-exposure", type=float, default=0.75)
    parser.add_argument("--paired-neutral-gross-exposure", type=float, default=0.45)
    parser.add_argument("--paired-operator-name", default="ensemble_rebuilt_candidate_shadow_benchmark_paired")
    parser.add_argument("--operator-name", default="ensemble_rebuilt_candidate_shadow_benchmark")
    parser.add_argument("--output-dir", default="logs/openclaw/ensemble_rebuilt_candidate_shadow_benchmark")
    args = parser.parse_args()

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    if str(args.paired_output_dir or "").strip():
        Path(args.paired_output_dir).mkdir(parents=True, exist_ok=True)
    policy_payload = json.loads(Path(args.candidate_policy_audit_json).read_text(encoding="utf-8"))
    policy_audit = policy_payload.get("audit") if isinstance(policy_payload.get("audit"), dict) else policy_payload
    rule_freeze = build_rebuilt_candidate_rule_freeze(
        policy_audit,
        candidate=str(args.candidate),
        rule_version=str(args.candidate_rule_version or "").strip() or None,
    )
    as_of_dates = _csv(args.as_of_dates)
    strategies = _csv(args.strategies)
    base_windows = []
    conn = sqlite3.connect(str(args.db_path), timeout=30)
    try:
        for as_of in as_of_dates:
            fact_chain = build_ensemble_alpha_sleeve_fact_chain(
                conn,
                as_of_date=as_of,
                strategies=strategies,
                holding_days=int(args.holding_days),
                top_n_per_strategy=int(args.top_n_per_strategy),
            )
            regime = _market_regime(conn, str(as_of).replace("-", ""))
            formal = build_formal_pool_benchmark_return_series(
                conn,
                strategies=FORMAL_POOL_BENCHMARK_STRATEGIES,
                as_of_date=as_of,
                holding_days=int(args.holding_days),
            )
            base_windows.append(
                {
                    "as_of_date": str(as_of).replace("-", ""),
                    "market_regime": regime,
                    "market_regime_label": str(regime.get("label") or "unknown"),
                    "fact_chain": fact_chain,
                    "fact_chain_blocking_reasons": fact_chain.get("blocking_reasons") or [],
                    "formal_pool_benchmark": formal,
                }
            )
            print(f"[shadow-benchmark] prepared base window {str(as_of).replace('-', '')}", file=sys.stderr, flush=True)
        print(
            f"[shadow-benchmark] building primary payload target={float(args.target_gross_exposure)} neutral={float(args.neutral_gross_exposure)}",
            file=sys.stderr,
            flush=True,
        )
        payload = _build_payload(
            conn,
            args=args,
            policy_audit=policy_audit,
            rule_freeze=rule_freeze,
            base_windows=base_windows,
            target_gross_exposure=float(args.target_gross_exposure),
            neutral_gross_exposure=float(args.neutral_gross_exposure),
            operator_name=str(args.operator_name),
        )
        paired_payload = None
        if str(args.paired_output_dir or "").strip():
            print(
                f"[shadow-benchmark] building paired payload target={float(args.paired_target_gross_exposure)} neutral={float(args.paired_neutral_gross_exposure)}",
                file=sys.stderr,
                flush=True,
            )
            paired_payload = _build_payload(
                conn,
                args=args,
                policy_audit=policy_audit,
                rule_freeze=rule_freeze,
                base_windows=base_windows,
                target_gross_exposure=float(args.paired_target_gross_exposure),
                neutral_gross_exposure=float(args.paired_neutral_gross_exposure),
                operator_name=str(args.paired_operator_name),
            )
    finally:
        conn.close()

    primary_paths = _write_payload(payload, Path(args.output_dir))
    result: dict[str, Any] = {"primary": primary_paths}
    if paired_payload is not None:
        result["paired"] = _write_payload(paired_payload, Path(args.paired_output_dir))
    print(json.dumps(result if paired_payload is not None else primary_paths, ensure_ascii=False, indent=2))
    return 0


def _build_payload(
    conn: sqlite3.Connection,
    *,
    args: argparse.Namespace,
    policy_audit: dict[str, Any],
    rule_freeze: dict[str, Any],
    base_windows: list[dict[str, Any]],
    target_gross_exposure: float,
    neutral_gross_exposure: float,
    operator_name: str,
) -> dict[str, Any]:
    windows = []
    for base in base_windows:
        fact_chain = base.get("fact_chain") if isinstance(base.get("fact_chain"), dict) else {}
        as_of = str(base.get("as_of_date") or "")
        shadow = build_rebuilt_candidate_shadow_portfolio(
            fact_chain,
            candidate_policy_audit=policy_audit,
            rule_freeze=rule_freeze,
            market_regime_label=str(base.get("market_regime_label") or "unknown"),
            max_positions=int(args.max_positions),
            single_name_cap=float(args.single_name_cap),
            industry_cap=float(args.industry_cap),
            target_gross_exposure=float(target_gross_exposure),
            neutral_gross_exposure=float(neutral_gross_exposure),
        )
        execution = build_ensemble_execution_cost_replay(
            conn,
            shadow,
            as_of_date=as_of,
            holding_days=int(args.holding_days),
        )
        windows.append(
            {
                "as_of_date": as_of,
                "market_regime": base.get("market_regime") or {},
                "market_regime_label": str(base.get("market_regime_label") or "unknown"),
                "fact_chain_blocking_reasons": base.get("fact_chain_blocking_reasons") or [],
                "shadow_portfolio": shadow,
                "execution_cost_replay": execution,
                "formal_pool_benchmark": base.get("formal_pool_benchmark") or {},
            }
        )
    benchmark = build_ensemble_walk_forward_shadow_benchmark(
        windows,
        min_windows=int(args.min_benchmark_windows),
    )
    return {
        "run_version": "ensemble_rebuilt_candidate_shadow_benchmark_tool.v1",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "operator_name": str(operator_name),
        "research_only": True,
        "candidate": str(args.candidate),
        "source_candidate_policy_audit_json": str(args.candidate_policy_audit_json),
        "target_gross_exposure": round(float(target_gross_exposure), 6),
        "neutral_gross_exposure": round(float(neutral_gross_exposure), 6),
        "rule_freeze": rule_freeze,
        "windows": windows,
        "benchmark": benchmark,
        "hard_boundaries": [
            "do_not_promote_from_rebuilt_candidate_shadow_benchmark_without_observation_gate",
            "do_not_use_shadow_weights_as_production_orders",
            "do_not_ignore_after_cost_or_capacity_blocks",
        ],
    }


def _write_payload(payload: dict[str, Any], output_dir: Path) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"ensemble_rebuilt_candidate_shadow_benchmark_{stamp}.json"
    md_path = output_dir / f"ensemble_rebuilt_candidate_shadow_benchmark_{stamp}.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(_markdown(payload), encoding="utf-8")
    return {"json": str(json_path), "markdown": str(md_path)}


def _csv(raw: str) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def _markdown(payload: dict[str, Any]) -> str:
    freeze = payload.get("rule_freeze") or {}
    benchmark = payload.get("benchmark") or {}
    lines = [
        "# Rebuilt Candidate Shadow Benchmark",
        "",
        f"- research_only: {payload.get('research_only')}",
        f"- candidate: {payload.get('candidate')}",
        f"- rule_frozen: {freeze.get('frozen')}",
        f"- rule_version: {freeze.get('rule_version')}",
        f"- benchmark_passed: {benchmark.get('passed')}",
        f"- after_cost_excess_return: {benchmark.get('after_cost_excess_return')}",
        f"- hit_rate: {benchmark.get('hit_rate')}",
        f"- turnover: {benchmark.get('turnover')}",
        f"- capacity_utilization: {benchmark.get('capacity_utilization')}",
        f"- industry_concentration: {benchmark.get('industry_concentration')}",
        f"- blocking_reasons: {', '.join(benchmark.get('blocking_reasons') or []) or '(none)'}",
        "",
        "## Windows",
        "",
    ]
    for window in payload.get("windows") or []:
        execution = window.get("execution_cost_replay") or {}
        formal = window.get("formal_pool_benchmark") or {}
        shadow = window.get("shadow_portfolio") or {}
        lines.append(
            f"- {window.get('as_of_date')}: regime={window.get('market_regime_label')}, "
            f"weights={len(shadow.get('shadow_weights') or [])}, "
            f"net_return={execution.get('net_return')}, "
            f"formal_avg={formal.get('avg_return_pct')}, "
            f"execution_blocks={execution.get('blocking_reasons') or []}"
        )
    lines.extend(["", "## Hard Boundaries", ""])
    lines.extend(f"- {item}" for item in payload.get("hard_boundaries") or [])
    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
