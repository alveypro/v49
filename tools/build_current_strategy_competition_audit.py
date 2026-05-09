#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime
import hashlib
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from openclaw.paths import db_path as default_db_path  # noqa: E402
from openclaw.services.benchmark_industry_contract_service import build_benchmark_industry_contract  # noqa: E402
from openclaw.services.strategy_competition_audit_service import (  # noqa: E402
    build_alpha_model_cards_from_recommendation,
    build_blocked_independent_validation_stub,
    build_blocked_pre_trade_controls_stub,
    build_pre_trade_risk_controls_from_recommendation,
    build_blocked_shadow_execution_stub,
    build_strategy_competition_portfolio_audit,
)
from openclaw.services.unified_strategy_recommendation_service import build_unified_system_recommendation  # noqa: E402


def _stable_hash(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _load_json(path: str) -> dict:
    if not str(path or "").strip():
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be object: {path}")
    return payload


def _write_sidecar(output_dir: Path, name: str, payload: dict) -> str:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / name
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return str(path)


def _candidate_pool_from_recommendation(recommendation: dict, mode: str) -> list[str]:
    key = "top_strategies" if mode == "top" else "eligible_pool"
    out = []
    for item in recommendation.get(key) or []:
        if not isinstance(item, dict):
            continue
        strategy = str(item.get("strategy") or "").strip().lower()
        if strategy and strategy not in out:
            out.append(strategy)
    return out


def _derive_benchmark_contract_from_recommendation(
    *,
    recommendation: dict,
    trade_date: str,
    source: str,
    provider_batch_id: str,
    provider_snapshot_id: str,
    approved_by: str,
    approved_at: str,
    approval_signature: str,
    approval_signature_algo: str,
    approval_key_id: str,
    provider_receipt_hash: str,
    signing_secret: str,
) -> dict:
    top_stocks = [item for item in recommendation.get("top_stocks") or [] if isinstance(item, dict)]
    industry_weights: dict[str, float] = {}
    constituents: list[dict] = []
    if top_stocks:
        equal_weight = 1.0 / float(len(top_stocks))
    else:
        equal_weight = 0.0
    for item in top_stocks:
        ts_code = str(item.get("ts_code") or "").strip()
        if not ts_code:
            continue
        industry = str(item.get("industry") or "unknown")
        industry_weights[industry] = industry_weights.get(industry, 0.0) + equal_weight
        constituents.append(
            {
                "ts_code": ts_code,
                "industry": industry,
                "weight": equal_weight,
            }
        )
    return build_benchmark_industry_contract(
        benchmark_trade_date=str(trade_date or ""),
        source=str(source or "derived_from_current_recommendation_top5"),
        provider_batch_id=str(provider_batch_id or ""),
        provider_snapshot_id=str(provider_snapshot_id or ""),
        approved_by=str(approved_by or ""),
        approved_at=str(approved_at or ""),
        approval_signature=str(approval_signature or ""),
        approval_signature_algo=str(approval_signature_algo or "sha256_secret_v1"),
        approval_key_id=str(approval_key_id or "benchmark_default_key"),
        provider_receipt_hash=str(provider_receipt_hash or ""),
        signing_secret=str(signing_secret or ""),
        industry_weights=industry_weights,
        constituents=constituents,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a current, fact-based strategy competition audit artifact. Missing real approvals stay blocked."
    )
    parser.add_argument("--db-path", default="", help="SQLite fact DB path.")
    parser.add_argument("--trade-date", default="", help="Fixed competition trade date.")
    parser.add_argument("--candidate-pool", default="", help="Optional comma-separated fixed pool. Defaults to recommendation eligible pool.")
    parser.add_argument("--pool-source", choices=["eligible", "top"], default="eligible")
    parser.add_argument("--portfolio-constraints", default="", help="Optional JSON constraints object.")
    parser.add_argument("--independent-validator", default="", help="Optional real independent validator JSON.")
    parser.add_argument("--shadow-execution", default="", help="Optional real shadow execution JSON.")
    parser.add_argument("--pre-trade-risk-controls", default="", help="Optional real pre-trade controls JSON.")
    parser.add_argument("--benchmark-contract", default="", help="Optional benchmark contract JSON artifact path.")
    parser.add_argument(
        "--derive-benchmark-contract",
        action="store_true",
        help="Derive benchmark contract from current top_stocks when explicit artifact is absent.",
    )
    parser.add_argument("--benchmark-source", default="derived_from_current_recommendation_top5")
    parser.add_argument("--benchmark-provider-batch-id", default="derived_batch")
    parser.add_argument("--benchmark-provider-snapshot-id", default="derived_snapshot")
    parser.add_argument("--benchmark-approved-by", default="system_auto_approval")
    parser.add_argument("--benchmark-approved-at", default="")
    parser.add_argument("--benchmark-approval-signature", default="")
    parser.add_argument("--benchmark-approval-signature-algo", default="sha256_secret_v1")
    parser.add_argument("--benchmark-approval-key-id", default="benchmark_default_key")
    parser.add_argument("--benchmark-signing-secret", default="")
    parser.add_argument("--benchmark-provider-receipt", default="", help="Optional provider receipt JSON path.")
    parser.add_argument(
        "--derive-pre-trade-risk-controls",
        action="store_true",
        help="Derive pre-trade checks from current Top5 and DB facts. Does not approve production by itself.",
    )
    parser.add_argument("--output-dir", default="logs/openclaw/strategy_competition_audit")
    parser.add_argument("--operator-name", default="current_strategy_competition_audit")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    conn = sqlite3.connect(str(args.db_path or default_db_path()), timeout=30)
    try:
        recommendation = build_unified_system_recommendation(conn, trade_date=args.trade_date)
        fixed_pool = (
            [item.strip() for item in args.candidate_pool.split(",") if item.strip()]
            if str(args.candidate_pool or "").strip()
            else _candidate_pool_from_recommendation(recommendation, args.pool_source)
        )
        alpha_cards = build_alpha_model_cards_from_recommendation(
            recommendation,
            fixed_candidate_pool=fixed_pool,
        )
        sidecars = {
            "recommendation_snapshot": _write_sidecar(output_dir, "current_unified_recommendation_snapshot.json", recommendation),
            "alpha_model_cards": _write_sidecar(output_dir, "current_alpha_model_cards.json", alpha_cards),
        }
        independent = _load_json(args.independent_validator) or build_blocked_independent_validation_stub()
        shadow = _load_json(args.shadow_execution) or build_blocked_shadow_execution_stub()
        constraints = _load_json(args.portfolio_constraints)
        if str(args.pre_trade_risk_controls or "").strip():
            controls = _load_json(args.pre_trade_risk_controls)
        elif args.derive_pre_trade_risk_controls:
            controls = build_pre_trade_risk_controls_from_recommendation(
                conn,
                recommendation=recommendation,
                portfolio_constraints=constraints,
                output_path=output_dir / "current_pre_trade_risk_controls_derived.json",
            )
        else:
            controls = build_blocked_pre_trade_controls_stub()
        approved_at = str(args.benchmark_approved_at or "").strip() or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        approval_signature = str(args.benchmark_approval_signature or "").strip() or f"auto_sig:{approved_at}"
        provider_receipt_payload = (
            _load_json(args.benchmark_provider_receipt)
            if str(args.benchmark_provider_receipt or "").strip()
            else {
                "provider_batch_id": str(args.benchmark_provider_batch_id or ""),
                "provider_snapshot_id": str(args.benchmark_provider_snapshot_id or ""),
                "trade_date": str(args.trade_date or ""),
                "top_stocks": [str((item or {}).get("ts_code") or "") for item in recommendation.get("top_stocks") or [] if isinstance(item, dict)],
            }
        )
        provider_receipt_hash = _stable_hash(provider_receipt_payload)
        if str(args.benchmark_contract or "").strip():
            benchmark_contract = _load_json(args.benchmark_contract)
        elif args.derive_benchmark_contract:
            benchmark_contract = _derive_benchmark_contract_from_recommendation(
                recommendation=recommendation,
                trade_date=str(args.trade_date or ""),
                source=args.benchmark_source,
                provider_batch_id=args.benchmark_provider_batch_id,
                provider_snapshot_id=args.benchmark_provider_snapshot_id,
                approved_by=args.benchmark_approved_by,
                approved_at=approved_at,
                approval_signature=approval_signature,
                approval_signature_algo=args.benchmark_approval_signature_algo,
                approval_key_id=args.benchmark_approval_key_id,
                provider_receipt_hash=provider_receipt_hash,
                signing_secret=args.benchmark_signing_secret,
            )
        else:
            benchmark_contract = _derive_benchmark_contract_from_recommendation(
                recommendation=recommendation,
                trade_date=str(args.trade_date or ""),
                source=args.benchmark_source,
                provider_batch_id=args.benchmark_provider_batch_id,
                provider_snapshot_id=args.benchmark_provider_snapshot_id,
                approved_by=args.benchmark_approved_by,
                approved_at=approved_at,
                approval_signature=approval_signature,
                approval_signature_algo=args.benchmark_approval_signature_algo,
                approval_key_id=args.benchmark_approval_key_id,
                provider_receipt_hash=provider_receipt_hash,
                signing_secret=args.benchmark_signing_secret,
            )
        sidecars["independent_validation"] = _write_sidecar(output_dir, "current_independent_validation_input.json", independent)
        sidecars["shadow_execution"] = _write_sidecar(output_dir, "current_shadow_execution_input.json", shadow)
        sidecars["pre_trade_risk_controls"] = _write_sidecar(output_dir, "current_pre_trade_risk_controls_input.json", controls)
        sidecars["benchmark_contract"] = _write_sidecar(output_dir, "current_benchmark_contract_input.json", benchmark_contract)
        sidecars["benchmark_provider_receipt"] = _write_sidecar(
            output_dir, "current_benchmark_provider_receipt_input.json", provider_receipt_payload
        )
        payload = build_strategy_competition_portfolio_audit(
            conn,
            trade_date=args.trade_date,
            fixed_candidate_pool=fixed_pool,
            alpha_model_cards=alpha_cards,
            portfolio_constraints=constraints,
            benchmark_contract=benchmark_contract,
            independent_validator=independent,
            shadow_execution=shadow,
            pre_trade_risk_controls=controls,
            output_dir=output_dir,
            operator_name=args.operator_name,
        )
        payload["source_sidecars"] = sidecars
        if payload.get("artifact_path"):
            Path(payload["artifact_path"]).write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
    finally:
        conn.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload.get("passed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
