#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from openclaw.services.benchmark_industry_contract_service import build_benchmark_industry_contract  # noqa: E402


def _stable_hash(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _load_json(path: str) -> dict:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be object: {path}")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Build auditable benchmark industry contract artifact.")
    parser.add_argument("--benchmark-trade-date", required=True)
    parser.add_argument("--source", default="external_index_contract")
    parser.add_argument("--provider-batch-id", required=True)
    parser.add_argument("--provider-snapshot-id", required=True)
    parser.add_argument("--approved-by", required=True)
    parser.add_argument("--approved-at", required=True)
    parser.add_argument("--approval-signature", default="")
    parser.add_argument("--approval-signature-algo", default="sha256_secret_v1")
    parser.add_argument("--approval-key-id", default="benchmark_default_key")
    parser.add_argument("--signing-secret", default="", help="Optional signing secret for deterministic signature generation.")
    parser.add_argument("--provider-receipt-json", default="", help="Optional provider receipt JSON used to compute provider_receipt_hash.")
    parser.add_argument("--industry-weights-json", required=True, help="JSON file path containing industry weight map.")
    parser.add_argument("--constituents-json", required=True, help="JSON file path containing constituent list.")
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    weights_payload = _load_json(args.industry_weights_json)
    raw_constituents = json.loads(Path(args.constituents_json).read_text(encoding="utf-8"))
    if not isinstance(raw_constituents, list):
        raise ValueError("constituents json must be list")
    provider_receipt_hash = ""
    if str(args.provider_receipt_json or "").strip():
        receipt_payload = json.loads(Path(args.provider_receipt_json).read_text(encoding="utf-8"))
        provider_receipt_hash = _stable_hash(receipt_payload)
    contract = build_benchmark_industry_contract(
        benchmark_trade_date=args.benchmark_trade_date,
        source=args.source,
        provider_batch_id=args.provider_batch_id,
        provider_snapshot_id=args.provider_snapshot_id,
        approved_by=args.approved_by,
        approved_at=args.approved_at,
        approval_signature=args.approval_signature,
        approval_signature_algo=args.approval_signature_algo,
        approval_key_id=args.approval_key_id,
        provider_receipt_hash=provider_receipt_hash,
        signing_secret=args.signing_secret,
        industry_weights=weights_payload,
        constituents=raw_constituents,
    )
    output = Path(args.output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(contract, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(contract, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
