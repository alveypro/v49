from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Iterable, List


JsonDict = Dict[str, Any]


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _build_signature_payload(core_without_signature: JsonDict, signing_secret: str) -> str:
    body = _stable_hash(core_without_signature)
    token = f"{body}|{str(signing_secret or '').strip()}"
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _normalize_industry_weights(values: Dict[str, Any]) -> Dict[str, float]:
    cleaned: Dict[str, float] = {}
    for key, value in (values or {}).items():
        industry = str(key or "").strip()
        if not industry:
            continue
        cleaned[industry] = max(0.0, float(value or 0.0))
    total = sum(cleaned.values())
    if total <= 1e-12:
        return {}
    return {key: val / total for key, val in sorted(cleaned.items())}


def _normalize_constituents(values: Iterable[Any]) -> List[JsonDict]:
    rows: List[JsonDict] = []
    for item in values or []:
        if isinstance(item, dict):
            ts_code = str(item.get("ts_code") or "").strip()
            if not ts_code:
                continue
            rows.append(
                {
                    "ts_code": ts_code,
                    "industry": str(item.get("industry") or "unknown"),
                    "weight": max(0.0, float(item.get("weight") or 0.0)),
                }
            )
    rows.sort(key=lambda row: str(row.get("ts_code") or ""))
    return rows


def build_benchmark_industry_contract(
    *,
    benchmark_trade_date: str,
    source: str,
    industry_weights: Dict[str, Any],
    constituents: Iterable[Any],
    provider_batch_id: str = "",
    provider_snapshot_id: str = "",
    approved_by: str = "",
    approved_at: str = "",
    approval_signature: str = "",
    approval_signature_algo: str = "sha256_secret_v1",
    approval_key_id: str = "benchmark_default_key",
    provider_receipt_hash: str = "",
    signing_secret: str = "",
) -> JsonDict:
    normalized_weights = _normalize_industry_weights(industry_weights)
    normalized_constituents = _normalize_constituents(constituents)
    industry_weights_hash = _stable_hash(normalized_weights)
    constituent_hash = _stable_hash(normalized_constituents)
    core_without_signature = {
        "artifact_version": "benchmark_industry_contract.v1",
        "source": str(source or "").strip(),
        "benchmark_trade_date": str(benchmark_trade_date or "").strip(),
        "provider_batch_id": str(provider_batch_id or "").strip(),
        "provider_snapshot_id": str(provider_snapshot_id or "").strip(),
        "approved_by": str(approved_by or "").strip(),
        "approved_at": str(approved_at or "").strip(),
        "approval_signature_algo": str(approval_signature_algo or "").strip(),
        "approval_key_id": str(approval_key_id or "").strip(),
        "provider_receipt_hash": str(provider_receipt_hash or "").strip(),
        "industry_weights": normalized_weights,
        "constituents": normalized_constituents,
        "industry_weights_hash": industry_weights_hash,
        "constituent_hash": constituent_hash,
    }
    signature = str(approval_signature or "").strip()
    if not signature and str(signing_secret or "").strip():
        signature = _build_signature_payload(core_without_signature, str(signing_secret or "").strip())
    core = {
        **core_without_signature,
        "approval_signature": signature,
    }
    return {
        **core,
        "contract_hash": _stable_hash(core),
    }
