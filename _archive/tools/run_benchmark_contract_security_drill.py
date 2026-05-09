#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
GATE_PATH = ROOT / "tools" / "governance_gate.py"
SPEC = importlib.util.spec_from_file_location("governance_gate_drill", str(GATE_PATH))
assert SPEC is not None and SPEC.loader is not None
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _sign(payload: Dict[str, Any], secret: str) -> str:
    body = _stable_hash(payload)
    return hashlib.sha256(f"{body}|{secret}".encode("utf-8")).hexdigest()


def _rebuild_contract_hash(contract: Dict[str, Any]) -> None:
    contract["industry_weights_hash"] = _stable_hash(contract.get("industry_weights") or {})
    contract["constituent_hash"] = _stable_hash(contract.get("constituents") or [])
    contract["contract_hash"] = _stable_hash(
        {
            key: contract.get(key)
            for key in (
                "artifact_version",
                "source",
                "benchmark_trade_date",
                "provider_batch_id",
                "provider_snapshot_id",
                "approved_by",
                "approved_at",
                "approval_signature_algo",
                "approval_key_id",
                "provider_receipt_hash",
                "approval_signature",
                "industry_weights",
                "constituents",
                "constituent_hash",
                "industry_weights_hash",
            )
        }
    )


def _build_base_payload() -> Dict[str, Any]:
    top5 = []
    for idx in range(5):
        top5.append(
            {
                "ts_code": f"00000{idx + 1}.SZ",
                "weight": 0.2,
                "source": {"signal_refs": [{"strategy": "v5", "run_id": "run_v5"}]},
                "risk": {"industry": f"industry_{idx}", "single_name_weight": 0.2},
                "cost": {"estimated_cost_bps": 15.0},
                "constraint_checks": {
                    "single_name_weight": True,
                    "liquidity_amount": True,
                    "estimated_cost_bps": True,
                    "has_signal_refs": True,
                    "has_risk_exposure": True,
                    "risk_contribution_limit": True,
                    "size_factor_exposure_limit": True,
                    "liquidity_factor_exposure_limit": True,
                },
            }
        )
    receipt = {
        "provider_batch_id": "batch_20260502",
        "provider_snapshot_id": "snapshot_20260502",
        "as_of_date": "20260502",
    }
    contract = {
        "artifact_version": "benchmark_industry_contract.v1",
        "source": "external_index_contract",
        "benchmark_trade_date": "20260502",
        "provider_batch_id": "batch_20260502",
        "provider_snapshot_id": "snapshot_20260502",
        "approved_by": "independent_benchmark_reviewer",
        "approved_at": "2026-05-02 09:30:00",
        "approval_signature_algo": "sha256_secret_v1",
        "approval_key_id": "benchmark_default_key",
        "provider_receipt_hash": _stable_hash(receipt),
        "industry_weights": {f"industry_{i}": 0.2 for i in range(5)},
        "constituents": [
            {"ts_code": f"00000{i + 1}.SZ", "industry": f"industry_{i}", "weight": 0.2}
            for i in range(5)
        ],
    }
    contract["industry_weights_hash"] = _stable_hash(contract["industry_weights"])
    contract["constituent_hash"] = _stable_hash(contract["constituents"])
    signing_payload = {
        key: contract.get(key)
        for key in (
            "artifact_version",
            "source",
            "benchmark_trade_date",
            "provider_batch_id",
            "provider_snapshot_id",
            "approved_by",
            "approved_at",
            "approval_signature_algo",
            "approval_key_id",
            "provider_receipt_hash",
            "industry_weights",
            "constituents",
            "industry_weights_hash",
            "constituent_hash",
        )
    }
    contract["approval_signature"] = _sign(signing_payload, "drill-secret")
    _rebuild_contract_hash(contract)
    payload = {
        "artifact_version": "strategy_competition_portfolio_audit.v1",
        "result_status": "industry_benchmark_competition_passed",
        "passed": True,
        "trade_date": "2026-05-02",
        "fixed_candidate_pool": ["v5"],
        "ranking_method_hash": "abc123",
        "ranking_contract": {"no_posthoc_candidate_addition": True, "failed_or_research_only_candidate_banned": True},
        "alpha_model_cards": {"v5": {"model_card": {"description": "v5"}, "hypothesis": "h", "rule_hash": "r", "data_hash": "d", "code_hash": "c"}},
        "top5_portfolio_audit": top5,
        "risk_summary": {
            "risk_budget": {
                "max_single_risk_contribution": 0.4,
                "risk_contribution_model": "adaptive_shrunk_industry_block_covariance_v3",
                "base_shrinkage": 0.35,
                "risk_contribution_share_by_code": {f"00000{i + 1}.SZ": 0.2 for i in range(5)},
                "factor_exposure_summary": {
                    "size": {"portfolio_exposure": 0.01, "cap": 0.35, "within_limit": True},
                    "liquidity": {"portfolio_exposure": -0.01, "cap": 0.35, "within_limit": True},
                },
            }
        },
        "benchmark_contract": contract,
        "independent_validation": {"passed": True, "validator_role": "independent_validator", "validator_name": "risk"},
        "shadow_execution": {"passed": True},
        "pre_trade_risk_controls": {
            "passed": True,
            "controls": {
                "single_name_weight_limit": True,
                "industry_weight_limit": True,
                "liquidity_check": True,
                "price_limit_check": True,
                "suspension_check": True,
                "turnover_budget": True,
                "order_value_limit": True,
                "risk_contribution_limit": True,
                "size_factor_exposure_limit": True,
                "liquidity_factor_exposure_limit": True,
            },
        },
        "production_candidate_allowed": True,
    }
    return {"payload": payload, "receipt": receipt}


def _run_scenario(name: str, *, mutate: Any, env_overrides: Dict[str, str] | None = None) -> Dict[str, Any]:
    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        base = _build_base_payload()
        payload = base["payload"]
        receipt = base["receipt"]
        mutate(payload)
        if isinstance(payload.get("benchmark_contract"), dict):
            _rebuild_contract_hash(payload["benchmark_contract"])
        contract_path = tmpdir / "benchmark_contract.json"
        receipt_path = tmpdir / "benchmark_provider_receipt.json"
        audit_path = tmpdir / "competition_audit.json"
        contract_path.write_text(json.dumps(payload["benchmark_contract"]), encoding="utf-8")
        receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
        payload["source_sidecars"] = {
            "benchmark_contract": str(contract_path),
            "benchmark_provider_receipt": str(receipt_path),
        }
        audit_path.write_text(json.dumps(payload), encoding="utf-8")
        env_backup = os.environ.copy()
        try:
            os.environ[gate.COMPETITION_AUDIT_EVIDENCE_ENV] = str(audit_path)
            os.environ[gate.BENCHMARK_CONTRACT_SIGNING_SECRET_ENV] = "drill-secret"
            for key, value in (env_overrides or {}).items():
                os.environ[str(key)] = str(value)
            failures = gate.validate_competition_audit_evidence_file()
        finally:
            os.environ.clear()
            os.environ.update(env_backup)
        return {"scenario": name, "failures": failures}


def main() -> int:
    kms_delegate = ROOT / "tools" / "kms_pki_delegate_verifier.py"
    kms_iam_policy = json.dumps(
        {
            "schema_version": "benchmark_kms_iam_policy.v1",
            "principals": {
                "benchmark_governance_gate": {
                    "allowed_actions": ["verify"],
                    "allowed_algos": ["pkcs1_sha256_detached_v1", "kms_hmac_sha256_v1"],
                    "allowed_key_ids": ["benchmark_default_key", "benchmark_rotating_key_2026q3"],
                }
            },
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    protocol_ok_hook = (
        f"{os.getenv('PYTHON', 'python3')} -c "
        "\"import json,sys; json.load(sys.stdin); print(json.dumps({'protocol_version':'benchmark_verify_response.v1','verified': True}))\""
    )
    scenarios = [
        _run_scenario("valid_signature", mutate=lambda payload: None),
        _run_scenario(
            "expired_key_not_active",
            mutate=lambda payload: None,
            env_overrides={gate.BENCHMARK_CONTRACT_ACTIVE_KEY_IDS_ENV: "benchmark_rotating_key_2026q3"},
        ),
        _run_scenario(
            "revoked_key",
            mutate=lambda payload: None,
            env_overrides={gate.BENCHMARK_CONTRACT_REVOKED_KEY_IDS_ENV: "benchmark_default_key"},
        ),
        _run_scenario(
            "invalid_signature",
            mutate=lambda payload: payload["benchmark_contract"].__setitem__("approval_signature", "broken_signature"),
        ),
        _run_scenario(
            "dual_key_overlap_accepts_rotating_key",
            mutate=lambda payload: payload["benchmark_contract"].update(
                {
                    "approval_signature_algo": "pkcs1_sha256_detached_v1",
                    "approval_key_id": "benchmark_rotating_key_2026q3",
                    "approval_signature": "rotating_sig_overlap_window",
                }
            ),
            env_overrides={
                gate.BENCHMARK_CONTRACT_ACTIVE_KEY_IDS_ENV: "benchmark_default_key,benchmark_rotating_key_2026q3",
                gate.BENCHMARK_CONTRACT_VERIFY_HOOK_ENV: protocol_ok_hook,
            },
        ),
        _run_scenario(
            "post_cutover_blocks_legacy_key",
            mutate=lambda payload: payload["benchmark_contract"].update(
                {
                    "approval_signature_algo": "pkcs1_sha256_detached_v1",
                    "approval_key_id": "benchmark_default_key",
                    "approval_signature": "legacy_sig_after_cutover",
                }
            ),
            env_overrides={
                gate.BENCHMARK_CONTRACT_ACTIVE_KEY_IDS_ENV: "benchmark_rotating_key_2026q3",
                gate.BENCHMARK_CONTRACT_VERIFY_HOOK_ENV: protocol_ok_hook,
            },
        ),
        _run_scenario(
            "kms_iam_policy_blocks_unauthorized_key",
            mutate=lambda payload: payload["benchmark_contract"].update(
                {
                    "approval_signature_algo": "pkcs1_sha256_detached_v1",
                    "approval_key_id": "benchmark_revoked_key_example",
                    "approval_signature": "unauthorized_key_sig",
                }
            ),
            env_overrides={
                gate.BENCHMARK_CONTRACT_VERIFY_HOOK_ENV: f"python3 {kms_delegate}",
                "AIRIVO_KMS_DELEGATE_MODE": "offline",
                "AIRIVO_KMS_IAM_POLICY_INLINE": kms_iam_policy,
            },
        ),
    ]
    print(json.dumps({"drill_results": scenarios}, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
