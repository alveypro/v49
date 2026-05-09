from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


_P = Path(__file__).resolve().parents[1] / "_archive" / "tools" / "governance_gate.py"
_SPEC = importlib.util.spec_from_file_location("governance_gate_for_strategy_optimization_test", str(_P))
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(gate)


def test_governance_requires_strategy_optimization_plan_and_audit_tool():
    assert "docs/AIRIVO_STRATEGY_OPTIMIZATION_UPGRADE_EXECUTION_PLAN.md" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/strategy_optimization_stage_audit.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/rejected_backtest_artifacts.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/strategy_competition_portfolio_audit.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/build_current_strategy_competition_audit.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/kms_pki_delegate_verifier.py" in gate.REQUIRED_MAINLINE_FILES
    assert "docs/benchmark_kms_iam_policy.example.json" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/build_strategy_competition_evidence_intake_packet.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/review_strategy_competition_evidence_submission.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/build_strategy_competition_operational_controls.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/build_strategy_competition_production_readiness.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/build_strategy_competition_formal_validation_handoff.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/review_strategy_competition_formal_validation_results.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/adjudicate_strategy_competition_release_chain.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/build_strategy_competition_human_release_approval.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/review_strategy_competition_post_rerun_human_release_approval_review.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/check_strategy_competition_live_order_authority.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/check_strategy_competition_broker_submission_guard.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/review_strategy_competition_broker_submission_response.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/review_strategy_competition_broker_execution_feedback.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/review_strategy_competition_post_rerun_broker_execution_feedback.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/reconcile_strategy_competition_post_rerun_post_trade.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/adjudicate_strategy_competition_post_rerun_trade_lifecycle.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/build_strategy_competition_post_rerun_evidence_chain_manifest.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/reconcile_strategy_competition_post_trade.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/adjudicate_strategy_competition_trade_lifecycle.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/build_strategy_competition_evidence_chain_manifest.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/build_strategy_competition_evidence_remediation_work_order.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/build_strategy_competition_remediation_closure_submission.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/review_strategy_competition_remediation_closure.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/build_strategy_competition_formal_rerun_plan.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/build_strategy_competition_formal_rerun_output_submission.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/build_strategy_competition_rerun_court_rebuild_submission.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/build_strategy_competition_post_rerun_release_readiness_submission.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/build_strategy_competition_post_rerun_live_authority_submission.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/build_strategy_competition_post_rerun_broker_guard_submission.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/review_strategy_competition_formal_rerun_results.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/review_strategy_competition_rerun_court_rebuild.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/review_strategy_competition_post_rerun_release_readiness.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/review_strategy_competition_post_rerun_live_authority.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/adjudicate_strategy_competition_post_rerun_release_chain.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/review_strategy_competition_post_rerun_broker_guard.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/review_strategy_competition_post_rerun_broker_response.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/strategy_optimization_stage_audit.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/strategy_competition_portfolio_audit.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/build_current_strategy_competition_audit.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/kms_pki_delegate_verifier.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/build_strategy_competition_evidence_intake_packet.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/review_strategy_competition_evidence_submission.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/build_strategy_competition_operational_controls.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/build_strategy_competition_production_readiness.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/build_strategy_competition_formal_validation_handoff.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/review_strategy_competition_formal_validation_results.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/adjudicate_strategy_competition_release_chain.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/build_strategy_competition_human_release_approval.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/review_strategy_competition_post_rerun_human_release_approval_review.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/check_strategy_competition_live_order_authority.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/check_strategy_competition_broker_submission_guard.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/review_strategy_competition_broker_submission_response.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/review_strategy_competition_broker_execution_feedback.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/review_strategy_competition_post_rerun_broker_execution_feedback.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/reconcile_strategy_competition_post_rerun_post_trade.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/adjudicate_strategy_competition_post_rerun_trade_lifecycle.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/build_strategy_competition_post_rerun_evidence_chain_manifest.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/reconcile_strategy_competition_post_trade.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/adjudicate_strategy_competition_trade_lifecycle.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/build_strategy_competition_evidence_chain_manifest.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/build_strategy_competition_evidence_remediation_work_order.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/build_strategy_competition_remediation_closure_submission.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/review_strategy_competition_remediation_closure.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/build_strategy_competition_formal_rerun_plan.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/build_strategy_competition_formal_rerun_output_submission.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/build_strategy_competition_rerun_court_rebuild_submission.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/build_strategy_competition_post_rerun_release_readiness_submission.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/build_strategy_competition_post_rerun_live_authority_submission.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/build_strategy_competition_post_rerun_broker_guard_submission.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/review_strategy_competition_formal_rerun_results.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/review_strategy_competition_rerun_court_rebuild.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/review_strategy_competition_post_rerun_release_readiness.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/review_strategy_competition_post_rerun_live_authority.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/adjudicate_strategy_competition_post_rerun_release_chain.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/review_strategy_competition_post_rerun_broker_guard.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/review_strategy_competition_post_rerun_broker_response.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert "tools/rejected_backtest_artifacts.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert gate._strategy_optimization_scope_changed(["openclaw/services/ensemble_risk_off_alpha_repair_review_service.py"])
    assert gate._strategy_optimization_scope_changed(["tools/ensemble_alpha_failure_attribution.py"])
    assert gate._strategy_optimization_scope_changed(["tests/test_ensemble_allocator_throttle_attribution_service.py"])
    assert gate._strategy_optimization_scope_changed(["openclaw/services/strategy_competition_remediation_closure_submission_service.py"])
    assert gate._strategy_optimization_scope_changed(["tools/build_strategy_competition_remediation_closure_submission.py"])
    assert gate._strategy_optimization_scope_changed(["openclaw/services/strategy_competition_formal_rerun_output_submission_service.py"])
    assert gate._strategy_optimization_scope_changed(["tools/build_strategy_competition_formal_rerun_output_submission.py"])
    assert gate._strategy_optimization_scope_changed(["openclaw/services/strategy_competition_rerun_court_rebuild_submission_service.py"])
    assert gate._strategy_optimization_scope_changed(["tools/build_strategy_competition_rerun_court_rebuild_submission.py"])
    assert gate._strategy_optimization_scope_changed(["openclaw/services/strategy_competition_post_rerun_release_readiness_submission_service.py"])
    assert gate._strategy_optimization_scope_changed(["tools/build_strategy_competition_post_rerun_release_readiness_submission.py"])
    assert gate._strategy_optimization_scope_changed(["openclaw/services/strategy_competition_post_rerun_live_authority_submission_service.py"])
    assert gate._strategy_optimization_scope_changed(["tools/build_strategy_competition_post_rerun_live_authority_submission.py"])
    assert gate._strategy_optimization_scope_changed(["openclaw/services/strategy_competition_post_rerun_broker_guard_submission_service.py"])
    assert gate._strategy_optimization_scope_changed(["tools/build_strategy_competition_post_rerun_broker_guard_submission.py"])


def _valid_competition_audit_payload() -> dict:
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
    benchmark_contract_core = {
        "artifact_version": "benchmark_industry_contract.v1",
        "source": "external_index_contract",
        "benchmark_trade_date": "20260502",
        "provider_batch_id": "batch_20260502",
        "provider_snapshot_id": "snapshot_20260502",
        "approved_by": "independent_benchmark_reviewer",
        "approved_at": "2026-05-02 09:30:00",
        "approval_signature_algo": "sha256_secret_v1",
        "approval_key_id": "benchmark_default_key",
        "provider_receipt_hash": gate._stable_hash(
            {
                "provider_batch_id": "batch_20260502",
                "provider_snapshot_id": "snapshot_20260502",
                "as_of_date": "20260502",
            }
        ),
        "approval_signature": "sig:benchmark:20260502",
        "industry_weights": {
            "industry_0": 0.2,
            "industry_1": 0.2,
            "industry_2": 0.2,
            "industry_3": 0.2,
            "industry_4": 0.2,
        },
        "constituents": [
            {"ts_code": "000001.SZ", "industry": "industry_0", "weight": 0.2},
            {"ts_code": "000002.SZ", "industry": "industry_1", "weight": 0.2},
            {"ts_code": "000003.SZ", "industry": "industry_2", "weight": 0.2},
            {"ts_code": "000004.SZ", "industry": "industry_3", "weight": 0.2},
            {"ts_code": "000005.SZ", "industry": "industry_4", "weight": 0.2},
        ],
    }
    benchmark_contract = dict(benchmark_contract_core)
    benchmark_contract["industry_weights_hash"] = gate._stable_hash(benchmark_contract_core["industry_weights"])
    benchmark_contract["constituent_hash"] = gate._stable_hash(benchmark_contract_core["constituents"])
    benchmark_contract["contract_hash"] = gate._stable_hash(
        {
            **benchmark_contract_core,
            "industry_weights_hash": benchmark_contract["industry_weights_hash"],
            "constituent_hash": benchmark_contract["constituent_hash"],
        }
    )
    return {
        "artifact_version": "strategy_competition_portfolio_audit.v1",
        "result_status": "industry_benchmark_competition_passed",
        "passed": True,
        "fixed_candidate_pool": ["v5"],
        "ranking_method_hash": "abc123",
        "ranking_contract": {
            "no_posthoc_candidate_addition": True,
            "failed_or_research_only_candidate_banned": True,
        },
        "alpha_model_cards": {
            "v5": {
                "model_card": {"description": "v5"},
                "hypothesis": "predeclared v5 hypothesis",
                "rule_hash": "rule_hash",
                "data_hash": "data_hash",
                "code_hash": "code_hash",
            }
        },
        "top5_portfolio_audit": top5,
        "risk_summary": {
            "risk_budget": {
                "max_single_risk_contribution": 0.4,
                "risk_contribution_model": "adaptive_shrunk_industry_block_covariance_v3",
                "base_shrinkage": 0.35,
                "risk_contribution_share_by_code": {
                    "000001.SZ": 0.2,
                    "000002.SZ": 0.2,
                    "000003.SZ": 0.2,
                    "000004.SZ": 0.2,
                    "000005.SZ": 0.2,
                },
                "factor_exposure_summary": {
                    "size": {
                        "portfolio_exposure": 0.02,
                        "cap": 0.35,
                        "within_limit": True,
                    },
                    "liquidity": {
                        "portfolio_exposure": -0.01,
                        "cap": 0.35,
                        "within_limit": True,
                    },
                },
            }
        },
        "benchmark_contract": benchmark_contract,
        "independent_validation": {
            "passed": True,
            "validator_role": "independent_validator",
            "validator_name": "risk",
        },
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


def _rebuild_benchmark_contract_hashes(payload: dict) -> None:
    contract = payload.get("benchmark_contract") if isinstance(payload.get("benchmark_contract"), dict) else {}
    if not contract:
        return
    contract["industry_weights_hash"] = gate._stable_hash(contract.get("industry_weights") or {})
    contract["constituent_hash"] = gate._stable_hash(contract.get("constituents") or [])
    core = {
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
    contract["contract_hash"] = gate._stable_hash(core)


def test_competition_audit_gate_accepts_complete_industry_benchmark_artifact(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    sidecar = tmp_path / "benchmark_contract.json"
    sidecar.write_text(json.dumps(payload["benchmark_contract"]), encoding="utf-8")
    receipt_sidecar = tmp_path / "benchmark_provider_receipt.json"
    receipt_sidecar.write_text(
        json.dumps(
            {
                "provider_batch_id": "batch_20260502",
                "provider_snapshot_id": "snapshot_20260502",
                "as_of_date": "20260502",
            }
        ),
        encoding="utf-8",
    )
    payload["source_sidecars"] = {
        "benchmark_contract": str(sidecar),
        "benchmark_provider_receipt": str(receipt_sidecar),
    }
    path = tmp_path / "competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))

    assert gate.validate_competition_audit_evidence_file() == []


def test_competition_audit_gate_blocks_research_only_candidate_and_incomplete_top5(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    payload["alpha_model_cards"]["v5"]["status"] = "research_only"
    payload["top5_portfolio_audit"][0]["source"] = {}
    payload["independent_validation"]["passed"] = False
    path = tmp_path / "bad_competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))

    failures = gate.validate_competition_audit_evidence_file()

    assert "strategy competition audit includes failed/research-only candidate: v5" in failures
    assert "strategy competition audit top5 missing signal refs: 000001.SZ" in failures


def test_competition_audit_gate_blocks_missing_risk_budget_fields(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    payload["risk_summary"] = {}
    path = tmp_path / "missing_risk_budget_competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))

    failures = gate.validate_competition_audit_evidence_file()

    assert "strategy competition audit risk budget missing max_single_risk_contribution" in failures
    assert "strategy competition audit risk budget missing risk contribution share map" in failures
    assert "strategy competition audit risk budget missing covariance model" in failures


def test_competition_audit_gate_blocks_missing_benchmark_contract(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    payload.pop("benchmark_contract", None)
    path = tmp_path / "missing_benchmark_contract_competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))

    failures = gate.validate_competition_audit_evidence_file()

    assert "strategy competition audit missing benchmark contract" in failures


def test_competition_audit_gate_blocks_missing_benchmark_sidecar_for_passed_artifact(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    path = tmp_path / "missing_benchmark_sidecar_competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))

    failures = gate.validate_competition_audit_evidence_file()

    assert "strategy competition audit missing benchmark contract sidecar reference" in failures
    assert "strategy competition audit missing benchmark provider receipt sidecar reference" in failures


def test_competition_audit_gate_blocks_missing_benchmark_approval_signature(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    payload["benchmark_contract"]["approval_signature"] = ""
    sidecar = tmp_path / "benchmark_contract_missing_sig.json"
    sidecar.write_text(json.dumps(payload["benchmark_contract"]), encoding="utf-8")
    receipt_sidecar = tmp_path / "benchmark_provider_receipt_missing_sig.json"
    receipt_sidecar.write_text(
        json.dumps(
            {
                "provider_batch_id": "batch_20260502",
                "provider_snapshot_id": "snapshot_20260502",
                "as_of_date": "20260502",
            }
        ),
        encoding="utf-8",
    )
    payload["source_sidecars"] = {"benchmark_contract": str(sidecar), "benchmark_provider_receipt": str(receipt_sidecar)}
    path = tmp_path / "missing_benchmark_sig_competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))

    failures = gate.validate_competition_audit_evidence_file()

    assert "strategy competition audit benchmark contract missing approval signature" in failures


def test_competition_audit_gate_blocks_stale_benchmark_contract(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    payload["trade_date"] = "2026-05-12"
    payload["portfolio_constraints"] = {"benchmark_max_staleness_days": 3}
    sidecar = tmp_path / "benchmark_contract_stale.json"
    sidecar.write_text(json.dumps(payload["benchmark_contract"]), encoding="utf-8")
    receipt_sidecar = tmp_path / "benchmark_provider_receipt_stale.json"
    receipt_sidecar.write_text(
        json.dumps(
            {
                "provider_batch_id": "batch_20260502",
                "provider_snapshot_id": "snapshot_20260502",
                "as_of_date": "20260502",
            }
        ),
        encoding="utf-8",
    )
    payload["source_sidecars"] = {"benchmark_contract": str(sidecar), "benchmark_provider_receipt": str(receipt_sidecar)}
    path = tmp_path / "stale_benchmark_contract_competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))

    failures = gate.validate_competition_audit_evidence_file()

    assert any(reason.startswith("strategy competition audit benchmark contract stale:") for reason in failures)


def test_competition_audit_gate_blocks_benchmark_provider_receipt_hash_mismatch(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    sidecar = tmp_path / "benchmark_contract_receipt_hash_mismatch.json"
    sidecar.write_text(json.dumps(payload["benchmark_contract"]), encoding="utf-8")
    receipt_sidecar = tmp_path / "benchmark_provider_receipt_hash_mismatch.json"
    receipt_sidecar.write_text(json.dumps({"unexpected": "payload"}), encoding="utf-8")
    payload["source_sidecars"] = {"benchmark_contract": str(sidecar), "benchmark_provider_receipt": str(receipt_sidecar)}
    path = tmp_path / "benchmark_provider_receipt_hash_mismatch_competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))

    failures = gate.validate_competition_audit_evidence_file()

    assert "strategy competition audit benchmark provider receipt hash mismatch" in failures


def test_competition_audit_gate_blocks_disallowed_benchmark_signature_algo(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    sidecar = tmp_path / "benchmark_contract_disallowed_algo.json"
    sidecar.write_text(json.dumps(payload["benchmark_contract"]), encoding="utf-8")
    receipt_sidecar = tmp_path / "benchmark_provider_receipt_disallowed_algo.json"
    receipt_sidecar.write_text(
        json.dumps(
            {
                "provider_batch_id": "batch_20260502",
                "provider_snapshot_id": "snapshot_20260502",
                "as_of_date": "20260502",
            }
        ),
        encoding="utf-8",
    )
    payload["source_sidecars"] = {"benchmark_contract": str(sidecar), "benchmark_provider_receipt": str(receipt_sidecar)}
    path = tmp_path / "disallowed_algo_competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))
    monkeypatch.setenv(gate.BENCHMARK_CONTRACT_ALLOWED_ALGOS_ENV, "pkcs1_sha256_detached_v1")

    failures = gate.validate_competition_audit_evidence_file()

    assert "strategy competition audit benchmark contract signature algo not allowed: sha256_secret_v1" in failures


def test_competition_audit_gate_blocks_revoked_benchmark_key(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    sidecar = tmp_path / "benchmark_contract_revoked_key.json"
    sidecar.write_text(json.dumps(payload["benchmark_contract"]), encoding="utf-8")
    receipt_sidecar = tmp_path / "benchmark_provider_receipt_revoked_key.json"
    receipt_sidecar.write_text(
        json.dumps(
            {
                "provider_batch_id": "batch_20260502",
                "provider_snapshot_id": "snapshot_20260502",
                "as_of_date": "20260502",
            }
        ),
        encoding="utf-8",
    )
    payload["source_sidecars"] = {"benchmark_contract": str(sidecar), "benchmark_provider_receipt": str(receipt_sidecar)}
    path = tmp_path / "revoked_key_competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))
    monkeypatch.setenv(gate.BENCHMARK_CONTRACT_REVOKED_KEY_IDS_ENV, "benchmark_default_key")

    failures = gate.validate_competition_audit_evidence_file()

    assert "strategy competition audit benchmark contract key revoked: benchmark_default_key" in failures


def test_competition_audit_gate_blocks_signature_verification_failure_with_secret(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    sidecar = tmp_path / "benchmark_contract_bad_sig.json"
    sidecar.write_text(json.dumps(payload["benchmark_contract"]), encoding="utf-8")
    receipt_sidecar = tmp_path / "benchmark_provider_receipt_bad_sig.json"
    receipt_sidecar.write_text(
        json.dumps(
            {
                "provider_batch_id": "batch_20260502",
                "provider_snapshot_id": "snapshot_20260502",
                "as_of_date": "20260502",
            }
        ),
        encoding="utf-8",
    )
    payload["source_sidecars"] = {"benchmark_contract": str(sidecar), "benchmark_provider_receipt": str(receipt_sidecar)}
    path = tmp_path / "bad_sig_competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))
    monkeypatch.setenv(gate.BENCHMARK_CONTRACT_SIGNING_SECRET_ENV, "test-secret")

    failures = gate.validate_competition_audit_evidence_file()

    assert "strategy competition audit benchmark contract signature verification failed" in failures


def test_competition_audit_gate_accepts_hook_signature_verification(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    payload["benchmark_contract"]["approval_signature_algo"] = "pkcs1_sha256_detached_v1"
    payload["benchmark_contract"]["approval_signature"] = "pki_sig_example"
    _rebuild_benchmark_contract_hashes(payload)
    sidecar = tmp_path / "benchmark_contract_hook_ok.json"
    sidecar.write_text(json.dumps(payload["benchmark_contract"]), encoding="utf-8")
    receipt_sidecar = tmp_path / "benchmark_provider_receipt_hook_ok.json"
    receipt_sidecar.write_text(
        json.dumps(
            {
                "provider_batch_id": "batch_20260502",
                "provider_snapshot_id": "snapshot_20260502",
                "as_of_date": "20260502",
            }
        ),
        encoding="utf-8",
    )
    payload["source_sidecars"] = {"benchmark_contract": str(sidecar), "benchmark_provider_receipt": str(receipt_sidecar)}
    path = tmp_path / "hook_ok_competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))
    monkeypatch.setenv(
        gate.BENCHMARK_CONTRACT_VERIFY_HOOK_ENV,
        f"{sys.executable} -c "
        "\"import json,sys; json.load(sys.stdin); print(json.dumps({'protocol_version':'benchmark_verify_response.v1','verified': True}))\"",
    )

    assert gate.validate_competition_audit_evidence_file() == []


def test_competition_audit_gate_blocks_hook_signature_verification_failure(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    payload["benchmark_contract"]["approval_signature_algo"] = "pkcs1_sha256_detached_v1"
    payload["benchmark_contract"]["approval_signature"] = "pki_sig_bad"
    _rebuild_benchmark_contract_hashes(payload)
    sidecar = tmp_path / "benchmark_contract_hook_fail.json"
    sidecar.write_text(json.dumps(payload["benchmark_contract"]), encoding="utf-8")
    receipt_sidecar = tmp_path / "benchmark_provider_receipt_hook_fail.json"
    receipt_sidecar.write_text(
        json.dumps(
            {
                "provider_batch_id": "batch_20260502",
                "provider_snapshot_id": "snapshot_20260502",
                "as_of_date": "20260502",
            }
        ),
        encoding="utf-8",
    )
    payload["source_sidecars"] = {"benchmark_contract": str(sidecar), "benchmark_provider_receipt": str(receipt_sidecar)}
    path = tmp_path / "hook_fail_competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))
    monkeypatch.setenv(
        gate.BENCHMARK_CONTRACT_VERIFY_HOOK_ENV,
        f"{sys.executable} -c "
        "\"import json,sys; json.load(sys.stdin); print(json.dumps({'protocol_version':'benchmark_verify_response.v1','verified': False, 'reason': 'bad_signature', 'severity': 'high'}))\"",
    )

    failures = gate.validate_competition_audit_evidence_file()

    assert any(reason.startswith("strategy competition audit benchmark contract hook verification failed:") for reason in failures)


def test_competition_audit_gate_warn_policy_allows_medium_severity_hook_failure(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    payload["benchmark_contract"]["approval_signature_algo"] = "pkcs1_sha256_detached_v1"
    payload["benchmark_contract"]["approval_signature"] = "pki_sig_warn"
    payload["portfolio_constraints"] = {"benchmark_hook_failure_policy": "warn"}
    _rebuild_benchmark_contract_hashes(payload)
    sidecar = tmp_path / "benchmark_contract_hook_warn.json"
    sidecar.write_text(json.dumps(payload["benchmark_contract"]), encoding="utf-8")
    receipt_sidecar = tmp_path / "benchmark_provider_receipt_hook_warn.json"
    receipt_sidecar.write_text(
        json.dumps(
            {
                "provider_batch_id": "batch_20260502",
                "provider_snapshot_id": "snapshot_20260502",
                "as_of_date": "20260502",
            }
        ),
        encoding="utf-8",
    )
    payload["source_sidecars"] = {"benchmark_contract": str(sidecar), "benchmark_provider_receipt": str(receipt_sidecar)}
    path = tmp_path / "hook_warn_competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))
    monkeypatch.setenv(
        gate.BENCHMARK_CONTRACT_VERIFY_HOOK_ENV,
        f"{sys.executable} -c "
        "\"import json,sys; json.load(sys.stdin); print(json.dumps({'protocol_version':'benchmark_verify_response.v1','verified': False, 'reason': 'kms_retryable', 'severity': 'medium'}))\"",
    )

    assert gate.validate_competition_audit_evidence_file() == []


def test_competition_audit_gate_blocks_hook_timeout(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    payload["benchmark_contract"]["approval_signature_algo"] = "pkcs1_sha256_detached_v1"
    payload["benchmark_contract"]["approval_signature"] = "pki_sig_timeout"
    payload["portfolio_constraints"] = {"benchmark_hook_timeout_seconds": 1}
    _rebuild_benchmark_contract_hashes(payload)
    sidecar = tmp_path / "benchmark_contract_hook_timeout.json"
    sidecar.write_text(json.dumps(payload["benchmark_contract"]), encoding="utf-8")
    receipt_sidecar = tmp_path / "benchmark_provider_receipt_hook_timeout.json"
    receipt_sidecar.write_text(
        json.dumps(
            {
                "provider_batch_id": "batch_20260502",
                "provider_snapshot_id": "snapshot_20260502",
                "as_of_date": "20260502",
            }
        ),
        encoding="utf-8",
    )
    payload["source_sidecars"] = {"benchmark_contract": str(sidecar), "benchmark_provider_receipt": str(receipt_sidecar)}
    path = tmp_path / "hook_timeout_competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))
    monkeypatch.setenv(
        gate.BENCHMARK_CONTRACT_VERIFY_HOOK_ENV,
        f"{sys.executable} -c \"import json,sys,time; json.load(sys.stdin); time.sleep(2); print(json.dumps({{'protocol_version':'benchmark_verify_response.v1','verified': True}}))\"",
    )

    failures = gate.validate_competition_audit_evidence_file()

    assert any("verify_hook_timeout" in reason for reason in failures)


def test_competition_audit_gate_blocks_legacy_hook_response_without_protocol(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    payload["benchmark_contract"]["approval_signature_algo"] = "pkcs1_sha256_detached_v1"
    payload["benchmark_contract"]["approval_signature"] = "pki_sig_legacy_hook"
    _rebuild_benchmark_contract_hashes(payload)
    sidecar = tmp_path / "benchmark_contract_hook_legacy.json"
    sidecar.write_text(json.dumps(payload["benchmark_contract"]), encoding="utf-8")
    receipt_sidecar = tmp_path / "benchmark_provider_receipt_hook_legacy.json"
    receipt_sidecar.write_text(
        json.dumps(
            {
                "provider_batch_id": "batch_20260502",
                "provider_snapshot_id": "snapshot_20260502",
                "as_of_date": "20260502",
            }
        ),
        encoding="utf-8",
    )
    payload["source_sidecars"] = {"benchmark_contract": str(sidecar), "benchmark_provider_receipt": str(receipt_sidecar)}
    path = tmp_path / "hook_legacy_competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))
    monkeypatch.setenv(
        gate.BENCHMARK_CONTRACT_VERIFY_HOOK_ENV,
        f"{sys.executable} -c \"import json,sys; json.load(sys.stdin); print(json.dumps({{'verified': True}}))\"",
    )

    failures = gate.validate_competition_audit_evidence_file()

    assert any("verify_hook_protocol_version_invalid:missing" in reason for reason in failures)


def test_competition_audit_gate_blocks_invalid_keyring_schema(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    sidecar = tmp_path / "benchmark_contract_keyring_schema.json"
    sidecar.write_text(json.dumps(payload["benchmark_contract"]), encoding="utf-8")
    receipt_sidecar = tmp_path / "benchmark_provider_receipt_keyring_schema.json"
    receipt_sidecar.write_text(
        json.dumps(
            {
                "provider_batch_id": "batch_20260502",
                "provider_snapshot_id": "snapshot_20260502",
                "as_of_date": "20260502",
            }
        ),
        encoding="utf-8",
    )
    payload["source_sidecars"] = {"benchmark_contract": str(sidecar), "benchmark_provider_receipt": str(receipt_sidecar)}
    path = tmp_path / "keyring_schema_competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    keyring = tmp_path / "benchmark_keyring_invalid.json"
    keyring.write_text(json.dumps({"schema_version": "legacy_schema", "keys": {}}), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))
    monkeypatch.setenv(gate.BENCHMARK_CONTRACT_KEYRING_FILE_ENV, str(keyring))

    failures = gate.validate_competition_audit_evidence_file()

    assert "strategy competition audit benchmark keyring schema invalid: legacy_schema" in failures


def test_competition_audit_gate_accepts_blocked_artifact_as_non_production_evidence(monkeypatch, tmp_path):
    payload = _valid_competition_audit_payload()
    payload["result_status"] = "industry_benchmark_competition_blocked"
    payload["passed"] = False
    payload["formal_top_allowed"] = False
    payload["production_candidate_allowed"] = False
    payload["blocking_reasons"] = ["shadow_execution_not_passed"]
    payload["independent_validation"]["passed"] = False
    payload["shadow_execution"]["passed"] = False
    payload["pre_trade_risk_controls"]["passed"] = False
    path = tmp_path / "blocked_competition_audit.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.COMPETITION_AUDIT_EVIDENCE_ENV, str(path))

    assert gate.validate_competition_audit_evidence_file() == []


def test_production_readiness_gate_accepts_blocked_non_release_artifact(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_production_readiness.v1",
        "readiness_status": "production_readiness_blocked",
        "passed": False,
        "production_release_allowed": False,
        "blocking_reasons": ["shadow_execution_not_passed"],
        "top5_symbols": ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ"],
        "hard_boundaries": ["human_approval_required_even_after_readiness_passed"],
    }
    path = tmp_path / "readiness_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.PRODUCTION_READINESS_EVIDENCE_ENV, str(path))

    assert gate.validate_production_readiness_evidence_file() == []


def test_production_readiness_gate_blocks_passed_artifact_without_release_contract(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_production_readiness.v1",
        "readiness_status": "production_readiness_passed",
        "passed": True,
        "production_release_allowed": True,
        "blocking_reasons": [],
        "competition_audit_checks": {"passed": True},
        "operational_controls": {"passed": True},
        "release_contract": {},
        "top5_symbols": ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ"],
        "hard_boundaries": [],
    }
    path = tmp_path / "readiness_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.PRODUCTION_READINESS_EVIDENCE_ENV, str(path))

    failures = gate.validate_production_readiness_evidence_file()

    assert "production readiness release contract missing: requires_shadow_execution" in failures
    assert "production readiness missing human approval boundary" in failures


def test_evidence_submission_review_gate_accepts_blocked_non_production_review(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_evidence_submission_review.v1",
        "review_status": "evidence_submission_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "blocking_reasons": ["shadow_feedback_status_invalid:ord_1"],
        "hard_boundaries": [
            "submission_review_is_not_shadow_execution_pass",
            "submission_review_is_not_independent_validation_pass",
            "submission_review_is_not_operational_controls_pass",
            "production_requires_formal_validation_tools_after_submission_review",
        ],
    }
    path = tmp_path / "submission_review_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.EVIDENCE_SUBMISSION_REVIEW_ENV, str(path))

    assert gate.validate_evidence_submission_review_file() == []


def test_evidence_submission_review_gate_blocks_accepted_review_without_hashes(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_evidence_submission_review.v1",
        "review_status": "evidence_submission_accepted_for_validation",
        "passed": True,
        "production_candidate_allowed": False,
        "blocking_reasons": [],
        "submitted_artifact_hashes": {},
        "next_commands": {},
        "hard_boundaries": [
            "submission_review_is_not_shadow_execution_pass",
            "submission_review_is_not_independent_validation_pass",
            "submission_review_is_not_operational_controls_pass",
            "production_requires_formal_validation_tools_after_submission_review",
        ],
    }
    path = tmp_path / "submission_review_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.EVIDENCE_SUBMISSION_REVIEW_ENV, str(path))

    failures = gate.validate_evidence_submission_review_file()

    assert "evidence submission review missing submitted hash: shadow_feedback" in failures
    assert "evidence submission review missing next command: record_shadow_feedback" in failures


def test_evidence_submission_review_gate_blocks_production_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_evidence_submission_review.v1",
        "review_status": "evidence_submission_blocked",
        "passed": False,
        "production_candidate_allowed": True,
        "blocking_reasons": ["blocked"],
        "hard_boundaries": [
            "submission_review_is_not_shadow_execution_pass",
            "submission_review_is_not_independent_validation_pass",
            "submission_review_is_not_operational_controls_pass",
            "production_requires_formal_validation_tools_after_submission_review",
        ],
    }
    path = tmp_path / "submission_review_prod.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.EVIDENCE_SUBMISSION_REVIEW_ENV, str(path))

    assert "evidence submission review attempted production eligibility" in gate.validate_evidence_submission_review_file()


def test_release_chain_adjudication_gate_accepts_blocked_non_production_verdict(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_release_chain_adjudication.v1",
        "chain_status": "release_chain_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_allowed": False,
        "current_blocking_gate": "shadow_execution",
        "root_blockers": ["shadow_execution:shadow_attribution_missing"],
        "allowed_next_actions": ["submit_real_shadow_feedback_and_run_shadow_evidence_validator"],
        "top5_symbols": ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ"],
        "source_artifact_hashes": {"competition_audit": "abc"},
        "adjudication_hash": "def",
        "hard_boundaries": [
            "release_chain_adjudication_is_not_trade_instruction",
            "blocked_gate_cannot_be_skipped",
            "production_requires_passed_readiness_and_human_approval",
        ],
    }
    path = tmp_path / "release_chain_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.RELEASE_CHAIN_ADJUDICATION_ENV, str(path))

    assert gate.validate_release_chain_adjudication_file() == []


def test_release_chain_adjudication_gate_blocks_production_claim_when_blocked(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_release_chain_adjudication.v1",
        "chain_status": "release_chain_blocked",
        "passed": False,
        "production_candidate_allowed": True,
        "production_release_allowed": False,
        "current_blocking_gate": "",
        "root_blockers": [],
        "allowed_next_actions": [],
        "top5_symbols": ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ"],
        "source_artifact_hashes": {"competition_audit": "abc"},
        "adjudication_hash": "def",
        "hard_boundaries": [],
    }
    path = tmp_path / "release_chain_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.RELEASE_CHAIN_ADJUDICATION_ENV, str(path))

    failures = gate.validate_release_chain_adjudication_file()

    assert "blocked release chain adjudication allowed production" in failures
    assert "blocked release chain adjudication missing current blocking gate" in failures
    assert "blocked release chain adjudication missing root blockers" in failures


def test_formal_validation_handoff_gate_accepts_ready_non_production_handoff(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_formal_validation_handoff.v1",
        "handoff_status": "formal_validation_ready",
        "passed": True,
        "production_candidate_allowed": False,
        "source_artifact_hashes": {"intake_packet": "abc", "evidence_submission_review": "def"},
        "handoff_hash": "ghi",
        "formal_run_order": [
            {"step": "shadow_execution_evidence", "command": "python3 tools/record_strategy_competition_shadow_feedback.py"},
            {"step": "independent_validation", "command": "python3 tools/build_strategy_competition_independent_validation.py"},
            {"step": "operational_controls", "command": "python3 tools/build_strategy_competition_operational_controls.py"},
            {"step": "competition_audit_rerun", "command": "python3 tools/build_current_strategy_competition_audit.py"},
            {"step": "production_readiness", "command": "python3 tools/build_strategy_competition_production_readiness.py"},
            {"step": "release_chain_adjudication", "command": "python3 tools/adjudicate_strategy_competition_release_chain.py"},
        ],
        "handoff_contract": {
            "requires_accepted_submission_review": True,
            "requires_same_source_hashes": True,
            "requires_formal_validator_outputs_before_readiness": True,
            "requires_release_chain_adjudication_after_readiness": True,
            "does_not_create_production_eligibility": True,
        },
        "hard_boundaries": [
            "formal_validation_handoff_is_not_shadow_execution_pass",
            "formal_validation_handoff_is_not_independent_validation_pass",
            "formal_validation_handoff_is_not_operational_controls_pass",
            "formal_validation_handoff_is_not_production_readiness",
            "production_requires_passed_release_chain_adjudication_and_human_approval",
        ],
    }
    path = tmp_path / "handoff_ready.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.FORMAL_VALIDATION_HANDOFF_ENV, str(path))

    assert gate.validate_formal_validation_handoff_file() == []


def test_formal_validation_handoff_gate_blocks_production_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_formal_validation_handoff.v1",
        "handoff_status": "formal_validation_ready",
        "passed": True,
        "production_candidate_allowed": True,
        "source_artifact_hashes": {},
        "handoff_hash": "",
        "formal_run_order": [],
        "handoff_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "handoff_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.FORMAL_VALIDATION_HANDOFF_ENV, str(path))

    failures = gate.validate_formal_validation_handoff_file()

    assert "formal validation handoff attempted production eligibility" in failures
    assert "formal validation handoff run order invalid" in failures


def test_formal_validation_result_review_gate_accepts_blocked_non_production_review(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_formal_validation_result_review.v1",
        "result_review_status": "formal_validation_results_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "blocking_reasons": ["formal_validation_handoff_not_ready"],
        "formal_validation_handoff_hash": "abc",
        "result_review_hash": "def",
        "hard_boundaries": [
            "formal_validation_result_review_is_not_trade_instruction",
            "result_review_does_not_replace_human_release_approval",
            "blocked_formal_result_cannot_advance_to_production",
            "production_requires_separate_human_approval_after_passed_release_chain",
        ],
    }
    path = tmp_path / "result_review_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.FORMAL_VALIDATION_RESULT_REVIEW_ENV, str(path))

    assert gate.validate_formal_validation_result_review_file() == []


def test_formal_validation_result_review_gate_blocks_production_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_formal_validation_result_review.v1",
        "result_review_status": "formal_validation_results_accepted",
        "passed": True,
        "production_candidate_allowed": True,
        "formal_validation_handoff_hash": "",
        "result_review_hash": "",
        "formal_result_status": [],
        "result_review_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "result_review_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.FORMAL_VALIDATION_RESULT_REVIEW_ENV, str(path))

    failures = gate.validate_formal_validation_result_review_file()

    assert "formal validation result review attempted production eligibility" in failures
    assert "formal validation result review step order invalid" in failures


def test_human_release_approval_gate_accepts_blocked_non_release_artifact(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_human_release_approval.v1",
        "approval_status": "human_release_approval_blocked",
        "passed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "blocking_reasons": ["formal_validation_result_review_not_accepted"],
        "source_artifact_hashes": {
            "formal_validation_result_review": "abc",
            "release_chain_adjudication": "def",
        },
        "approval_hash": "ghi",
        "hard_boundaries": [
            "human_release_approval_is_final_pre_live_gate",
            "blocked_human_approval_cannot_release_live_orders",
            "release_approver_cannot_self_approve",
            "approval_does_not_modify_strategy_evidence",
        ],
    }
    path = tmp_path / "human_approval_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.HUMAN_RELEASE_APPROVAL_ENV, str(path))

    assert gate.validate_human_release_approval_file() == []


def test_human_release_approval_gate_blocks_authority_claim_when_blocked(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_human_release_approval.v1",
        "approval_status": "human_release_approval_blocked",
        "passed": False,
        "production_release_authorized": True,
        "live_order_authority_granted": False,
        "blocking_reasons": [],
        "source_artifact_hashes": {},
        "approval_hash": "",
        "hard_boundaries": [],
    }
    path = tmp_path / "human_approval_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.HUMAN_RELEASE_APPROVAL_ENV, str(path))

    failures = gate.validate_human_release_approval_file()

    assert "blocked human release approval granted authority" in failures
    assert "blocked human release approval missing blocking reasons" in failures


def test_post_rerun_human_release_approval_review_gate_accepts_blocked_non_authority_artifact(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_human_release_approval_review.v1",
        "human_release_approval_review_status": "post_rerun_human_release_approval_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "blocking_reasons": ["post_rerun_evidence_chain_manifest_not_complete"],
        "post_rerun_evidence_chain_manifest_hash": "abc",
        "human_approval_decision_hash": "def",
        "human_release_approval_review_hash": "ghi",
        "human_release_approval_review_contract": {
            "requires_post_rerun_evidence_chain_manifest_complete": True,
            "requires_independent_human_release_approver": True,
            "requires_conflict_attestation": True,
            "requires_reviewed_artifacts_match_current_manifest": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "post_rerun_human_release_approval_review_is_not_live_order_authority",
            "approval_review_does_not_execute_orders",
            "approval_review_does_not_authorize_production",
            "approval_review_is_inventory_not_permission",
        ],
    }
    path = tmp_path / "post_rerun_human_approval_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_HUMAN_RELEASE_APPROVAL_REVIEW_ENV, str(path))

    assert gate.validate_post_rerun_human_release_approval_review_file() == []


def test_post_rerun_human_release_approval_review_gate_blocks_authority_claim_when_approved(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_human_release_approval_review.v1",
        "human_release_approval_review_status": "post_rerun_human_release_approved",
        "passed": True,
        "production_candidate_allowed": False,
        "production_release_authorized": True,
        "live_order_authority_granted": True,
        "blocking_reasons": [],
        "post_rerun_evidence_chain_manifest_hash": "",
        "human_approval_decision_hash": "",
        "human_release_approval_review_hash": "",
        "allowed_next_actions": ["run_live_order_authority_check_with_matching_post_rerun_human_release_hash"],
        "human_release_approval_review_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "post_rerun_human_approval_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_HUMAN_RELEASE_APPROVAL_REVIEW_ENV, str(path))

    failures = gate.validate_post_rerun_human_release_approval_review_file()

    assert "post-rerun human release approval review attempted release authorization" in failures
    assert "post-rerun human release approval review attempted live order authority" in failures
    assert "approved post-rerun human release approval review granted authority" in failures


def test_live_order_authority_gate_accepts_blocked_non_submission_artifact(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_live_order_authority_check.v1",
        "authority_status": "live_order_submission_blocked",
        "passed": False,
        "live_order_submission_allowed": False,
        "blocking_reasons": ["human_release_approval_not_authorized"],
        "source_artifact_hashes": {"human_release_approval": "abc", "live_order_intent": ""},
        "authority_hash": "def",
        "hard_boundaries": [
            "live_order_authority_check_does_not_execute_orders",
            "blocked_authority_check_cannot_submit_live_orders",
            "order_intent_must_reference_current_human_approval_hash",
            "live_submission_requires_broker_layer_after_authority_check",
        ],
    }
    path = tmp_path / "live_authority_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.LIVE_ORDER_AUTHORITY_ENV, str(path))

    assert gate.validate_live_order_authority_file() == []


def test_live_order_authority_gate_blocks_allowed_without_intent_hash(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_live_order_authority_check.v1",
        "authority_status": "live_order_submission_allowed",
        "passed": True,
        "live_order_submission_allowed": True,
        "orders": [{"ts_code": "000001.SZ", "side": "buy", "target_qty": 100}],
        "source_artifact_hashes": {"human_release_approval": "abc", "live_order_intent": ""},
        "authority_hash": "def",
        "authority_contract": {},
        "hard_boundaries": [
            "live_order_authority_check_does_not_execute_orders",
            "blocked_authority_check_cannot_submit_live_orders",
            "order_intent_must_reference_current_human_approval_hash",
            "live_submission_requires_broker_layer_after_authority_check",
        ],
    }
    path = tmp_path / "live_authority_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.LIVE_ORDER_AUTHORITY_ENV, str(path))

    failures = gate.validate_live_order_authority_file()

    assert "allowed live order authority missing intent hash" in failures
    assert "live order authority contract missing: requires_human_release_approved" in failures


def test_broker_submission_guard_gate_accepts_blocked_non_submission_artifact(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_broker_submission_guard.v1",
        "guard_status": "broker_submission_guard_blocked",
        "passed": False,
        "broker_submission_allowed": False,
        "blocking_reasons": ["live_order_authority_not_allowed"],
        "source_artifact_hashes": {"live_order_authority": "abc", "broker_submission_intent": ""},
        "broker_guard_hash": "def",
        "hard_boundaries": [
            "broker_submission_guard_does_not_execute_orders",
            "blocked_broker_guard_cannot_call_broker_adapter",
            "broker_submit_requires_separate_broker_response_evidence",
            "fills_must_be_recorded_by_execution_feedback_not_guard",
        ],
    }
    path = tmp_path / "broker_guard_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.BROKER_SUBMISSION_GUARD_ENV, str(path))

    assert gate.validate_broker_submission_guard_file() == []


def test_broker_submission_guard_gate_blocks_passed_without_contract(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_broker_submission_guard.v1",
        "guard_status": "broker_submission_guard_passed",
        "passed": True,
        "broker_submission_allowed": True,
        "submission_mode": "controlled_submit",
        "broker_adapter": "",
        "idempotency_key": "",
        "orders": [{"ts_code": "000001.SZ", "side": "buy", "target_qty": 100}],
        "source_artifact_hashes": {"live_order_authority": "abc", "broker_submission_intent": ""},
        "broker_guard_hash": "def",
        "broker_guard_contract": {},
        "hard_boundaries": [
            "broker_submission_guard_does_not_execute_orders",
            "blocked_broker_guard_cannot_call_broker_adapter",
            "broker_submit_requires_separate_broker_response_evidence",
            "fills_must_be_recorded_by_execution_feedback_not_guard",
        ],
    }
    path = tmp_path / "broker_guard_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.BROKER_SUBMISSION_GUARD_ENV, str(path))

    failures = gate.validate_broker_submission_guard_file()

    assert "broker submission guard adapter missing" in failures
    assert "passed broker submission guard missing intent hash" in failures


def test_broker_submission_response_gate_accepts_blocked_non_execution_artifact(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_broker_submission_response_evidence.v1",
        "response_status": "broker_submission_response_blocked",
        "passed": False,
        "broker_submission_confirmed": False,
        "execution_fills_confirmed": False,
        "blocking_reasons": ["broker_submission_guard_not_passed"],
        "source_artifact_hashes": {"broker_submission_guard": "abc", "broker_submission_response": ""},
        "response_evidence_hash": "def",
        "hard_boundaries": [
            "broker_submission_response_is_not_fill_evidence",
            "broker_submission_confirmed_does_not_mean_filled",
            "fills_require_separate_broker_execution_report",
            "blocked_broker_response_cannot_advance_execution_state",
        ],
    }
    path = tmp_path / "broker_response_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.BROKER_SUBMISSION_RESPONSE_ENV, str(path))

    assert gate.validate_broker_submission_response_file() == []


def test_broker_submission_response_gate_blocks_fill_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_broker_submission_response_evidence.v1",
        "response_status": "broker_submission_response_accepted",
        "passed": True,
        "broker_submission_confirmed": True,
        "execution_fills_confirmed": True,
        "order_responses": [{"ts_code": "000001.SZ", "status": "accepted"}],
        "source_artifact_hashes": {"broker_submission_guard": "abc", "broker_submission_response": "def"},
        "response_evidence_hash": "ghi",
        "response_contract": {},
        "hard_boundaries": [
            "broker_submission_response_is_not_fill_evidence",
            "broker_submission_confirmed_does_not_mean_filled",
            "fills_require_separate_broker_execution_report",
            "blocked_broker_response_cannot_advance_execution_state",
        ],
    }
    path = tmp_path / "broker_response_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.BROKER_SUBMISSION_RESPONSE_ENV, str(path))

    failures = gate.validate_broker_submission_response_file()

    assert "broker submission response attempted fill confirmation" in failures
    assert "broker submission response contract missing: requires_passed_broker_guard" in failures


def test_broker_execution_feedback_gate_accepts_blocked_non_complete_artifact(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_broker_execution_feedback_review.v1",
        "feedback_status": "broker_execution_feedback_blocked",
        "passed": False,
        "execution_feedback_complete": False,
        "blocking_reasons": ["broker_submission_response_not_accepted"],
        "source_artifact_hashes": {"broker_submission_response_evidence": "abc", "broker_execution_feedback": ""},
        "feedback_review_hash": "def",
        "hard_boundaries": [
            "execution_feedback_is_required_after_broker_submission",
            "submitted_or_accepted_orders_are_not_execution_complete",
            "filled_orders_require_fills_costs_slippage_and_attribution",
            "blocked_execution_feedback_cannot_mark_trade_complete",
        ],
    }
    path = tmp_path / "execution_feedback_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.BROKER_EXECUTION_FEEDBACK_ENV, str(path))

    assert gate.validate_broker_execution_feedback_file() == []


def test_broker_execution_feedback_gate_blocks_accepted_without_contract(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_broker_execution_feedback_review.v1",
        "feedback_status": "broker_execution_feedback_accepted",
        "passed": True,
        "execution_feedback_complete": True,
        "execution_reports": [{"ts_code": "000001.SZ", "status": "filled"}],
        "source_artifact_hashes": {"broker_submission_response_evidence": "abc", "broker_execution_feedback": ""},
        "feedback_review_hash": "def",
        "feedback_contract": {},
        "hard_boundaries": [
            "execution_feedback_is_required_after_broker_submission",
            "submitted_or_accepted_orders_are_not_execution_complete",
            "filled_orders_require_fills_costs_slippage_and_attribution",
            "blocked_execution_feedback_cannot_mark_trade_complete",
        ],
    }
    path = tmp_path / "execution_feedback_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.BROKER_EXECUTION_FEEDBACK_ENV, str(path))

    failures = gate.validate_broker_execution_feedback_file()

    assert "accepted broker execution feedback missing feedback hash" in failures
    assert "broker execution feedback contract missing: requires_cost_slippage_and_attribution" in failures


def test_post_rerun_broker_execution_feedback_review_gate_accepts_blocked_non_post_trade_artifact(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_broker_execution_feedback_review.v1",
        "broker_execution_feedback_review_status": "post_rerun_broker_execution_feedback_blocked",
        "passed": False,
        "execution_feedback_complete": False,
        "post_trade_reconciliation_passed": False,
        "trade_lifecycle_complete": False,
        "blocking_reasons": ["broker_execution_feedback_artifact_missing"],
        "source_post_rerun_broker_response_review_hash": "abc",
        "broker_execution_feedback_review_hash": "def",
        "broker_execution_feedback_review_contract": {
            "requires_post_rerun_broker_response_review_ready": True,
            "requires_broker_execution_feedback_accepted": True,
            "requires_response_hash_lineage": True,
            "requires_terminal_order_feedback": True,
            "requires_cost_slippage_and_attribution": True,
            "does_not_create_post_trade_reconciliation": True,
        },
        "hard_boundaries": [
            "post_rerun_broker_execution_feedback_review_is_not_post_trade",
            "submitted_or_accepted_orders_are_not_execution_complete",
            "filled_orders_require_fills_costs_slippage_and_attribution",
            "post_trade_reconciliation_still_required_after_execution_feedback",
        ],
    }
    path = tmp_path / "post_rerun_exec_feedback_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_BROKER_EXECUTION_FEEDBACK_REVIEW_ENV, str(path))

    assert gate.validate_post_rerun_broker_execution_feedback_review_file() == []


def test_post_rerun_broker_execution_feedback_review_gate_blocks_post_trade_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_broker_execution_feedback_review.v1",
        "broker_execution_feedback_review_status": "post_rerun_broker_execution_feedback_ready_for_post_trade",
        "passed": True,
        "execution_feedback_complete": True,
        "production_candidate_allowed": True,
        "production_release_authorized": False,
        "live_order_authority_granted": True,
        "post_trade_reconciliation_passed": True,
        "trade_lifecycle_complete": True,
        "source_post_rerun_broker_response_review_hash": "",
        "broker_execution_feedback_review_hash": "",
        "broker_execution_feedback_hash": "",
        "allowed_next_actions": ["mark_trade_complete"],
        "broker_execution_feedback_review_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "post_rerun_exec_feedback_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_BROKER_EXECUTION_FEEDBACK_REVIEW_ENV, str(path))

    failures = gate.validate_post_rerun_broker_execution_feedback_review_file()

    assert "post-rerun broker execution feedback review attempted production eligibility" in failures
    assert "post-rerun broker execution feedback review attempted live order grant" in failures
    assert "post-rerun broker execution feedback review attempted post-trade completion" in failures
    assert "post-rerun broker execution feedback review attempted trade lifecycle completion" in failures
    assert "ready post-rerun broker execution feedback next action invalid" in failures


def test_post_rerun_post_trade_reconciliation_gate_accepts_blocked_non_complete_artifact(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_post_trade_reconciliation.v1",
        "reconciliation_status": "post_rerun_post_trade_reconciliation_blocked",
        "passed": False,
        "trade_lifecycle_complete": False,
        "blocking_reasons": ["post_rerun_broker_execution_feedback_review_not_ready"],
        "source_artifact_hashes": {"broker_execution_feedback_review": "abc", "post_trade_reconciliation_input": ""},
        "post_trade_reconciliation_input_hash": "",
        "reconciliation_hash": "def",
        "reconciliation_contract": {
            "requires_accepted_execution_feedback": True,
            "requires_feedback_hash_match": True,
            "requires_cash_reconciliation": True,
            "requires_position_reconciliation": True,
            "requires_cost_slippage_reconciliation": True,
            "requires_exception_owners_and_resolution": True,
            "requires_operations_signoff": True,
            "does_not_create_new_trade_permission": True,
        },
        "hard_boundaries": [
            "post_rerun_post_trade_reconciliation_is_required_after_execution_feedback",
            "execution_feedback_complete_is_not_portfolio_reconciled",
            "trade_lifecycle_complete_requires_cash_position_cost_reconciliation",
            "blocked_post_rerun_reconciliation_cannot_mark_lifecycle_complete",
        ],
    }
    path = tmp_path / "post_rerun_recon_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_POST_TRADE_RECONCILIATION_ENV, str(path))

    assert gate.validate_post_rerun_post_trade_reconciliation_file() == []


def test_post_rerun_trade_lifecycle_adjudication_gate_accepts_blocked_non_complete_artifact(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_trade_lifecycle_adjudication.v1",
        "lifecycle_status": "post_rerun_trade_lifecycle_blocked",
        "passed": False,
        "trade_lifecycle_complete": False,
        "current_blocking_stage": "post_rerun_broker_response_review",
        "root_blockers": ["post_rerun_broker_response_review:post_rerun_broker_response_review_not_ready"],
        "allowed_next_actions": ["complete_post_rerun_broker_response_review_and_broker_execution_feedback"],
        "source_artifact_hashes": {"post_rerun_broker_guard_review": "abc", "post_rerun_post_trade_reconciliation": ""},
        "lifecycle_adjudication_hash": "def",
        "lifecycle_contract": {
            "requires_post_rerun_broker_guard_review": True,
            "requires_post_rerun_broker_response_review": True,
            "requires_post_rerun_broker_execution_feedback_review": True,
            "requires_post_rerun_post_trade_reconciliation": True,
            "does_not_create_new_trade_permission": True,
        },
        "hard_boundaries": [
            "post_rerun_trade_lifecycle_adjudication_is_not_trade_instruction",
            "blocked_lifecycle_stage_cannot_be_skipped",
            "broker_submission_does_not_equal_execution_complete",
            "execution_feedback_does_not_equal_post_trade_reconciled",
        ],
    }
    path = tmp_path / "post_rerun_lifecycle_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_TRADE_LIFECYCLE_ADJUDICATION_ENV, str(path))

    assert gate.validate_post_rerun_trade_lifecycle_adjudication_file() == []


def test_post_rerun_trade_lifecycle_adjudication_gate_blocks_complete_without_required_hashes(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_trade_lifecycle_adjudication.v1",
        "lifecycle_status": "post_rerun_trade_lifecycle_complete",
        "passed": True,
        "trade_lifecycle_complete": True,
        "current_blocking_stage": "",
        "lifecycle_statuses": [{"name": "post_rerun_post_trade_reconciliation", "passed": True}],
        "root_blockers": [],
        "allowed_next_actions": ["archive_post_rerun_trade_lifecycle_as_complete"],
        "source_artifact_hashes": {"post_rerun_broker_guard_review": "", "post_rerun_post_trade_reconciliation": ""},
        "lifecycle_adjudication_hash": "",
        "lifecycle_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "post_rerun_lifecycle_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_TRADE_LIFECYCLE_ADJUDICATION_ENV, str(path))

    failures = gate.validate_post_rerun_trade_lifecycle_adjudication_file()

    assert "complete post-rerun trade lifecycle adjudication missing reconciliation hash" in failures
    assert "post-rerun trade lifecycle adjudication contract missing: requires_post_rerun_broker_guard_review" in failures


def test_post_rerun_evidence_chain_manifest_gate_accepts_blocked_inventory(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_evidence_chain_manifest.v1",
        "manifest_status": "post_rerun_evidence_chain_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "trade_lifecycle_complete": False,
        "current_blocking_artifact": "post_rerun_broker_guard_review",
        "root_blockers": ["post_rerun_broker_guard_review:post_rerun_broker_guard_review_blocked"],
        "allowed_next_actions": ["complete_or_repair_post_rerun_broker_guard_review_and_rerun_post_rerun_manifest"],
        "source_artifact_hashes": {
            "post_rerun_release_readiness": "a",
            "post_rerun_live_authority_review": "b",
            "post_rerun_broker_guard_review": "c",
            "post_rerun_broker_response_review": "d",
            "post_rerun_broker_execution_feedback_review": "e",
            "post_rerun_post_trade_reconciliation": "f",
            "post_rerun_trade_lifecycle_adjudication": "g",
            "post_rerun_human_release_approval_review": "h",
        },
        "manifest_hash": "i",
        "hard_boundaries": [
            "post_rerun_evidence_chain_manifest_is_inventory_not_approval",
            "manifest_cannot_create_production_or_live_order_authority",
            "blocked_or_partial_post_rerun_artifacts_cannot_be_packaged_as_passed",
            "trade_lifecycle_complete_requires_post_rerun_post_trade_reconciliation_and_lifecycle_adjudication",
        ],
    }
    path = tmp_path / "post_rerun_manifest_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_EVIDENCE_CHAIN_MANIFEST_ENV, str(path))

    assert gate.validate_post_rerun_evidence_chain_manifest_file() == []


def test_post_rerun_evidence_chain_manifest_gate_blocks_permission_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_evidence_chain_manifest.v1",
        "manifest_status": "post_rerun_evidence_chain_complete",
        "passed": True,
        "production_candidate_allowed": True,
        "production_release_authorized": False,
        "live_order_authority_granted": True,
        "trade_lifecycle_complete": True,
        "current_blocking_artifact": "",
        "artifact_statuses": [{"name": "post_rerun_trade_lifecycle_adjudication", "passed": True}],
        "source_artifact_hashes": {
            "post_rerun_release_readiness": "a",
            "post_rerun_live_authority_review": "b",
            "post_rerun_release_chain_adjudication": "c",
            "post_rerun_broker_guard_review": "d",
            "post_rerun_broker_response_review": "e",
            "post_rerun_broker_execution_feedback_review": "f",
            "post_rerun_post_trade_reconciliation": "g",
            "post_rerun_trade_lifecycle_adjudication": "h",
            "post_rerun_human_release_approval_review": "i",
        },
        "manifest_hash": "j",
        "manifest_contract": {
            "requires_post_rerun_release_readiness": True,
            "requires_post_rerun_live_authority_review": True,
            "requires_post_rerun_release_chain_adjudication": True,
            "requires_post_rerun_broker_guard_review": True,
            "requires_post_rerun_broker_response_review": True,
            "requires_post_rerun_broker_execution_feedback_review": True,
            "requires_post_rerun_post_trade_reconciliation": True,
            "requires_post_rerun_trade_lifecycle_adjudication": True,
            "requires_post_rerun_human_release_approval_review": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
            "does_not_mark_execution_or_reconciliation_complete": True,
        },
        "hard_boundaries": [],
    }
    path = tmp_path / "post_rerun_manifest_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_EVIDENCE_CHAIN_MANIFEST_ENV, str(path))

    failures = gate.validate_post_rerun_evidence_chain_manifest_file()

    assert "post-rerun evidence chain manifest attempted production eligibility" in failures
    assert "post-rerun evidence chain manifest attempted live order authority" in failures
    assert "post-rerun evidence chain manifest hash missing" not in failures


def test_post_trade_reconciliation_gate_accepts_blocked_non_complete_artifact(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_trade_reconciliation.v1",
        "reconciliation_status": "post_trade_reconciliation_blocked",
        "passed": False,
        "trade_lifecycle_complete": False,
        "blocking_reasons": ["broker_execution_feedback_not_accepted"],
        "source_artifact_hashes": {"broker_execution_feedback_review": "abc", "post_trade_reconciliation_input": ""},
        "reconciliation_hash": "def",
        "hard_boundaries": [
            "post_trade_reconciliation_is_required_after_execution_feedback",
            "execution_feedback_complete_is_not_portfolio_reconciled",
            "trade_lifecycle_complete_requires_cash_position_cost_reconciliation",
            "blocked_reconciliation_cannot_mark_lifecycle_complete",
        ],
    }
    path = tmp_path / "post_trade_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_TRADE_RECONCILIATION_ENV, str(path))

    assert gate.validate_post_trade_reconciliation_file() == []


def test_post_trade_reconciliation_gate_blocks_passed_without_reconciliation_sections(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_trade_reconciliation.v1",
        "reconciliation_status": "post_trade_reconciliation_passed",
        "passed": True,
        "trade_lifecycle_complete": True,
        "cash_reconciliation": {},
        "position_reconciliation": [],
        "cost_slippage_reconciliation": [],
        "source_artifact_hashes": {"broker_execution_feedback_review": "abc", "post_trade_reconciliation_input": ""},
        "reconciliation_hash": "def",
        "reconciliation_contract": {},
        "hard_boundaries": [
            "post_trade_reconciliation_is_required_after_execution_feedback",
            "execution_feedback_complete_is_not_portfolio_reconciled",
            "trade_lifecycle_complete_requires_cash_position_cost_reconciliation",
            "blocked_reconciliation_cannot_mark_lifecycle_complete",
        ],
    }
    path = tmp_path / "post_trade_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_TRADE_RECONCILIATION_ENV, str(path))

    failures = gate.validate_post_trade_reconciliation_file()

    assert "passed post-trade reconciliation missing cash reconciliation" in failures
    assert "passed post-trade reconciliation missing input hash" in failures


def test_trade_lifecycle_adjudication_gate_accepts_blocked_lifecycle(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_trade_lifecycle_adjudication.v1",
        "lifecycle_status": "trade_lifecycle_blocked",
        "passed": False,
        "trade_lifecycle_complete": False,
        "current_blocking_stage": "human_release_approval",
        "root_blockers": ["human_release_approval:blocked"],
        "allowed_next_actions": ["complete_formal_result_review_release_chain_and_human_release_decision"],
        "source_artifact_hashes": {"human_release_approval": "abc", "post_trade_reconciliation": ""},
        "lifecycle_adjudication_hash": "def",
        "hard_boundaries": [
            "trade_lifecycle_adjudication_is_not_trade_instruction",
            "blocked_lifecycle_stage_cannot_be_skipped",
            "broker_submission_does_not_equal_execution_complete",
            "execution_feedback_does_not_equal_post_trade_reconciled",
        ],
    }
    path = tmp_path / "lifecycle_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.TRADE_LIFECYCLE_ADJUDICATION_ENV, str(path))

    assert gate.validate_trade_lifecycle_adjudication_file() == []


def test_trade_lifecycle_adjudication_gate_blocks_complete_with_failed_stage(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_trade_lifecycle_adjudication.v1",
        "lifecycle_status": "trade_lifecycle_complete",
        "passed": True,
        "trade_lifecycle_complete": True,
        "current_blocking_stage": "",
        "lifecycle_statuses": [{"name": "post_trade_reconciliation", "passed": False}],
        "source_artifact_hashes": {"human_release_approval": "abc", "post_trade_reconciliation": "def"},
        "lifecycle_adjudication_hash": "ghi",
        "lifecycle_contract": {},
        "hard_boundaries": [
            "trade_lifecycle_adjudication_is_not_trade_instruction",
            "blocked_lifecycle_stage_cannot_be_skipped",
            "broker_submission_does_not_equal_execution_complete",
            "execution_feedback_does_not_equal_post_trade_reconciled",
        ],
    }
    path = tmp_path / "lifecycle_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.TRADE_LIFECYCLE_ADJUDICATION_ENV, str(path))

    failures = gate.validate_trade_lifecycle_adjudication_file()

    assert "complete trade lifecycle contains failed stage: post_trade_reconciliation" in failures
    assert "trade lifecycle adjudication contract missing: requires_post_trade_reconciliation" in failures


def test_evidence_chain_manifest_gate_accepts_blocked_inventory(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_evidence_chain_manifest.v1",
        "manifest_status": "evidence_chain_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "trade_lifecycle_complete": False,
        "current_blocking_artifact": "competition_audit",
        "root_blockers": ["competition_audit:blocked"],
        "allowed_next_actions": ["complete_or_repair_competition_audit_and_rerun_evidence_chain_manifest"],
        "source_artifact_hashes": {"release_chain_adjudication": "abc", "trade_lifecycle_adjudication": "def"},
        "manifest_hash": "ghi",
        "hard_boundaries": [
            "evidence_chain_manifest_is_inventory_not_approval",
            "manifest_cannot_create_production_or_live_order_authority",
            "blocked_or_partial_artifacts_cannot_be_packaged_as_passed",
            "trade_lifecycle_complete_requires_post_trade_reconciliation_and_lifecycle_adjudication",
        ],
    }
    path = tmp_path / "manifest_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.EVIDENCE_CHAIN_MANIFEST_ENV, str(path))

    assert gate.validate_evidence_chain_manifest_file() == []


def test_evidence_chain_manifest_gate_blocks_permission_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_evidence_chain_manifest.v1",
        "manifest_status": "evidence_chain_blocked",
        "passed": False,
        "production_candidate_allowed": True,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "trade_lifecycle_complete": False,
        "current_blocking_artifact": "",
        "root_blockers": [],
        "allowed_next_actions": [],
        "source_artifact_hashes": {"release_chain_adjudication": "abc", "trade_lifecycle_adjudication": "def"},
        "manifest_hash": "ghi",
        "hard_boundaries": [],
    }
    path = tmp_path / "manifest_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.EVIDENCE_CHAIN_MANIFEST_ENV, str(path))

    failures = gate.validate_evidence_chain_manifest_file()

    assert "evidence chain manifest attempted production eligibility" in failures
    assert "blocked evidence chain manifest missing current blocking artifact" in failures
    assert "blocked evidence chain manifest missing root blockers" in failures


def test_evidence_chain_manifest_gate_accepts_complete_inventory_without_new_permission(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_evidence_chain_manifest.v1",
        "manifest_status": "evidence_chain_complete",
        "passed": True,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "trade_lifecycle_complete": True,
        "current_blocking_artifact": "",
        "artifact_statuses": [{"name": "trade_lifecycle_adjudication", "passed": True}],
        "source_artifact_hashes": {"release_chain_adjudication": "abc", "trade_lifecycle_adjudication": "def"},
        "manifest_hash": "ghi",
        "manifest_contract": {
            "requires_release_chain_adjudication": True,
            "requires_trade_lifecycle_adjudication": True,
            "requires_all_chain_artifact_hashes": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
            "does_not_mark_execution_or_reconciliation_complete": True,
        },
        "hard_boundaries": [
            "evidence_chain_manifest_is_inventory_not_approval",
            "manifest_cannot_create_production_or_live_order_authority",
            "blocked_or_partial_artifacts_cannot_be_packaged_as_passed",
            "trade_lifecycle_complete_requires_post_trade_reconciliation_and_lifecycle_adjudication",
        ],
    }
    path = tmp_path / "manifest_complete.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.EVIDENCE_CHAIN_MANIFEST_ENV, str(path))

    assert gate.validate_evidence_chain_manifest_file() == []


def test_evidence_remediation_work_order_gate_accepts_required_non_production_work_order(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_evidence_remediation_work_order.v1",
        "work_order_status": "remediation_required",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_manifest_hash": "abc",
        "work_order_hash": "def",
        "work_items": [
            {
                "artifact": "competition_audit",
                "owner_role": "strategy_governance_owner",
                "validator_tool": "tools/build_current_strategy_competition_audit.py",
                "blocking_reasons": ["shadow_execution_not_passed"],
                "required_evidence": ["real_shadow_feedback_for_each_order"],
                "acceptance_rule": "competition_audit_artifact_must_pass_and_manifest_hash_must_update_after_rerun",
            }
        ],
        "work_order_contract": {
            "requires_current_evidence_chain_manifest": True,
            "requires_manifest_hash_match_for_submission": True,
            "work_items_must_be_closed_by_designated_validators": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "remediation_work_order_is_not_validation_pass",
            "work_order_completion_requires_rerun_formal_validators",
            "partial_work_items_cannot_be_packaged_as_production_evidence",
            "work_order_does_not_create_broker_or_execution_authority",
        ],
    }
    path = tmp_path / "work_order.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.EVIDENCE_REMEDIATION_WORK_ORDER_ENV, str(path))

    assert gate.validate_evidence_remediation_work_order_file() == []


def test_evidence_remediation_work_order_gate_blocks_permission_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_evidence_remediation_work_order.v1",
        "work_order_status": "remediation_required",
        "passed": True,
        "production_candidate_allowed": True,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_manifest_hash": "",
        "work_order_hash": "",
        "work_items": [],
        "work_order_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "work_order_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.EVIDENCE_REMEDIATION_WORK_ORDER_ENV, str(path))

    failures = gate.validate_evidence_remediation_work_order_file()

    assert "evidence remediation work order attempted production eligibility" in failures
    assert "remediation-required work order marked passed" in failures
    assert "remediation-required work order missing work items" in failures


def test_remediation_closure_review_gate_accepts_blocked_non_production_review(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_remediation_closure_review.v1",
        "closure_review_status": "remediation_closure_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_work_order_hash": "abc",
        "source_manifest_hash": "def",
        "closure_review_hash": "ghi",
        "blocking_reasons": ["closure_submission_missing"],
        "closure_reviews": [
            {
                "artifact": "competition_audit",
                "owner_role": "strategy_governance_owner",
                "validator_tool": "tools/build_current_strategy_competition_audit.py",
                "closure_status": "blocked",
                "closed": False,
                "blocking_reasons": ["closure_missing"],
            }
        ],
        "closure_review_contract": {
            "requires_work_order_hash_match": True,
            "requires_source_manifest_hash_match": True,
            "requires_designated_validator_artifacts": True,
            "accepted_closure_only_allows_formal_rerun": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "closure_review_is_not_formal_validation_pass",
            "accepted_closure_review_requires_rerun_of_formal_validators",
            "closure_review_cannot_create_production_or_live_order_authority",
            "partial_closure_submission_cannot_advance_release_chain",
        ],
    }
    path = tmp_path / "closure_review.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.REMEDIATION_CLOSURE_REVIEW_ENV, str(path))

    assert gate.validate_remediation_closure_review_file() == []


def test_remediation_closure_review_gate_blocks_permission_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_remediation_closure_review.v1",
        "closure_review_status": "remediation_closure_accepted_for_rerun",
        "passed": True,
        "production_candidate_allowed": True,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_work_order_hash": "",
        "source_manifest_hash": "",
        "closure_review_hash": "",
        "allowed_next_actions": ["go_live"],
        "closure_reviews": [{"artifact": "competition_audit", "owner_role": "owner", "validator_tool": "tool", "closure_status": "closed", "closed": True}],
        "closure_review_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "closure_review_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.REMEDIATION_CLOSURE_REVIEW_ENV, str(path))

    failures = gate.validate_remediation_closure_review_file()

    assert "remediation closure review attempted production eligibility" in failures
    assert "remediation closure review missing source work order hash" in failures
    assert "accepted remediation closure next action invalid" in failures


def test_remediation_closure_submission_gate_accepts_blocked_non_production_submission(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_remediation_closure_submission.v1",
        "closure_submission_status": "remediation_closure_submission_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_work_order_hash": "abc",
        "source_manifest_hash": "def",
        "closure_submission_hash": "ghi",
        "blocking_reasons": ["post_rerun_human_release_approval_review:closure_missing_or_unpassed"],
        "item_closures": [
            {
                "artifact": "post_rerun_human_release_approval_review",
                "owner_role": "human_release_approver",
                "validator_tool": "tools/review_strategy_competition_post_rerun_human_release_approval_review.py",
                "validator_artifact": "",
                "validator_artifact_hash": "",
                "validator_passed": False,
                "closure_status": "open",
                "closed": False,
                "blocking_reasons": ["closure_artifact_missing_or_unpassed"],
            }
        ],
        "closure_submission_contract": {
            "requires_work_order_hash_match": True,
            "requires_source_manifest_hash_match": True,
            "requires_designated_validator_artifacts": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "closure_submission_is_not_remediation_closure_review",
            "closure_submission_is_not_formal_validation_pass",
            "partial_closure_submissions_cannot_advance_release_chain",
            "closure_submission_does_not_create_production_or_live_order_authority",
        ],
    }
    path = tmp_path / "closure_submission_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.REMEDIATION_CLOSURE_SUBMISSION_ENV, str(path))

    assert gate.validate_remediation_closure_submission_file() == []


def test_remediation_closure_submission_gate_blocks_permission_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_remediation_closure_submission.v1",
        "closure_submission_status": "remediation_closure_submission_ready",
        "passed": True,
        "production_candidate_allowed": True,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_work_order_hash": "",
        "source_manifest_hash": "",
        "closure_submission_hash": "",
        "allowed_next_actions": ["go_live"],
        "item_closures": [{"artifact": "post_rerun_human_release_approval_review", "owner_role": "owner", "validator_tool": "tool", "validator_artifact": "path", "validator_artifact_hash": "", "validator_passed": False, "closure_status": "open", "closed": False, "blocking_reasons": []}],
        "closure_submission_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "closure_submission_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.REMEDIATION_CLOSURE_SUBMISSION_ENV, str(path))

    failures = gate.validate_remediation_closure_submission_file()

    assert "remediation closure submission attempted production eligibility" in failures
    assert "remediation closure submission missing source work order hash" in failures
    assert "remediation closure submission missing source manifest hash" in failures
    assert "remediation closure submission hash missing" in failures
    assert "ready remediation closure submission next action invalid" in failures


def test_formal_rerun_plan_gate_accepts_blocked_non_production_plan(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_formal_rerun_plan.v1",
        "rerun_plan_status": "formal_rerun_plan_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_closure_review_hash": "abc",
        "source_manifest_hash": "def",
        "rerun_plan_hash": "ghi",
        "blocking_reasons": ["remediation_closure_review_not_accepted"],
        "rerun_steps": [],
        "rerun_plan_contract": {
            "requires_accepted_remediation_closure_review": True,
            "requires_closure_review_hash_match": True,
            "requires_fixed_rerun_order": True,
            "each_step_output_must_pass_before_next_step": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "formal_rerun_plan_is_not_validator_pass",
            "ready_rerun_plan_only_allows_sequential_validator_execution",
            "formal_rerun_outputs_must_rebuild_manifest_and_court_of_record",
            "formal_rerun_plan_cannot_create_production_or_live_order_authority",
        ],
    }
    path = tmp_path / "rerun_plan_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.FORMAL_RERUN_PLAN_ENV, str(path))

    assert gate.validate_formal_rerun_plan_file() == []


def test_formal_rerun_plan_gate_blocks_permission_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_formal_rerun_plan.v1",
        "rerun_plan_status": "formal_rerun_plan_ready",
        "passed": True,
        "production_candidate_allowed": True,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_closure_review_hash": "",
        "source_manifest_hash": "",
        "rerun_plan_hash": "",
        "rerun_steps": [{"step": "go_live", "command": "submit"}],
        "rerun_plan_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "rerun_plan_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.FORMAL_RERUN_PLAN_ENV, str(path))

    failures = gate.validate_formal_rerun_plan_file()

    assert "formal rerun plan attempted production eligibility" in failures
    assert "formal rerun plan missing source closure review hash" in failures
    assert "ready formal rerun plan step order invalid" in failures


def test_formal_rerun_output_submission_gate_accepts_blocked_non_production_submission(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_formal_rerun_output_submission.v1",
        "rerun_output_submission_status": "formal_rerun_output_submission_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_rerun_plan_hash": "abc",
        "source_manifest_hash": "def",
        "rerun_output_submission_hash": "ghi",
        "blocking_reasons": ["shadow_execution_evidence:rerun_output_missing"],
        "rerun_outputs": [
            {
                "step": "shadow_execution_evidence",
                "command": "cmd",
                "artifact": "",
                "artifact_hash": "",
                "output_status": "blocked",
                "passed": False,
                "blocking_reasons": ["rerun_output_missing"],
            }
        ],
        "rerun_output_submission_contract": {
            "requires_ready_formal_rerun_plan": True,
            "requires_fixed_step_order": True,
            "requires_each_output_payload_passed": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "formal_rerun_output_submission_is_not_result_review",
            "partial_rerun_outputs_cannot_be_packaged_as_review",
            "formal_rerun_output_submission_does_not_create_production_or_live_order_authority",
        ],
    }
    path = tmp_path / "rerun_output_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.FORMAL_RERUN_OUTPUT_SUBMISSION_ENV, str(path))

    assert gate.validate_formal_rerun_output_submission_file() == []


def test_formal_rerun_output_submission_gate_blocks_permission_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_formal_rerun_output_submission.v1",
        "rerun_output_submission_status": "formal_rerun_output_submission_ready",
        "passed": True,
        "production_candidate_allowed": True,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_rerun_plan_hash": "",
        "source_manifest_hash": "",
        "rerun_output_submission_hash": "",
        "allowed_next_actions": ["go_live"],
        "rerun_outputs": [{"step": "shadow_execution_evidence", "command": "cmd", "artifact": "a", "artifact_hash": "", "output_status": "passed", "passed": True, "blocking_reasons": []}],
        "rerun_output_submission_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "rerun_output_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.FORMAL_RERUN_OUTPUT_SUBMISSION_ENV, str(path))

    failures = gate.validate_formal_rerun_output_submission_file()

    assert "formal rerun output submission attempted production eligibility" in failures
    assert "formal rerun output submission missing source rerun plan hash" in failures
    assert "formal rerun output submission missing source manifest hash" in failures
    assert "formal rerun output submission hash missing" in failures
    assert "accepted formal rerun output next action invalid" in failures


def test_rerun_court_rebuild_submission_gate_accepts_blocked_non_production_submission(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_rerun_court_rebuild_submission.v1",
        "rerun_court_rebuild_submission_status": "rerun_court_rebuild_submission_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_rerun_result_review_hash": "abc",
        "rerun_court_rebuild_submission_hash": "ghi",
        "blocking_reasons": ["formal_rerun_result_review_not_accepted"],
        "court_rebuild_inputs": [
            {
                "name": "rebuilt_evidence_chain_manifest",
                "artifact": "manifest.json",
                "artifact_hash": "hash",
                "status": "evidence_chain_complete",
                "passed": False,
                "blocking_reasons": ["manifest_missing"],
            },
            {
                "name": "rebuilt_release_chain_adjudication",
                "artifact": "release.json",
                "artifact_hash": "hash2",
                "status": "release_chain_passed_for_human_approval",
                "passed": True,
                "blocking_reasons": [],
            },
        ],
        "court_rebuild_submission_contract": {
            "requires_accepted_formal_rerun_result_review": True,
            "requires_rebuilt_manifest_and_release_chain": True,
            "requires_rerun_result_review_hash_lineage": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "rerun_court_rebuild_submission_is_not_rerun_court_rebuild_review",
            "partial_rerun_court_rebuild_inputs_cannot_be_packaged_as_review",
            "rerun_court_rebuild_submission_does_not_create_production_or_live_order_authority",
        ],
    }
    path = tmp_path / "court_rebuild_submission_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.RERUN_COURT_REBUILD_SUBMISSION_ENV, str(path))

    assert gate.validate_rerun_court_rebuild_submission_file() == []


def test_rerun_court_rebuild_submission_gate_blocks_permission_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_rerun_court_rebuild_submission.v1",
        "rerun_court_rebuild_submission_status": "rerun_court_rebuild_submission_ready",
        "passed": True,
        "production_candidate_allowed": True,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_rerun_result_review_hash": "",
        "rerun_court_rebuild_submission_hash": "",
        "allowed_next_actions": ["go_live"],
        "court_rebuild_inputs": [{"name": "rebuilt_evidence_chain_manifest", "artifact": "a", "artifact_hash": "h", "status": "evidence_chain_complete", "passed": True, "blocking_reasons": []}],
        "court_rebuild_submission_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "court_rebuild_submission_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.RERUN_COURT_REBUILD_SUBMISSION_ENV, str(path))

    failures = gate.validate_rerun_court_rebuild_submission_file()

    assert "rerun court rebuild submission attempted production eligibility" in failures
    assert "rerun court rebuild submission missing source rerun result review hash" in failures
    assert "rerun court rebuild submission hash missing" in failures


def test_formal_rerun_result_review_gate_accepts_blocked_non_production_review(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_formal_rerun_result_review.v1",
        "rerun_result_review_status": "formal_rerun_results_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_rerun_plan_hash": "abc",
        "source_manifest_hash": "def",
        "rerun_result_review_hash": "ghi",
        "blocking_reasons": ["formal_rerun_plan_not_ready"],
        "step_reviews": [],
        "rerun_result_review_contract": {
            "requires_ready_formal_rerun_plan": True,
            "requires_rerun_plan_hash_match": True,
            "requires_all_step_outputs_in_fixed_order": True,
            "requires_each_step_payload_passed": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "formal_rerun_result_review_is_not_release_approval",
            "accepted_rerun_results_require_manifest_and_court_of_record_rebuild",
            "partial_rerun_outputs_cannot_advance_release_chain",
            "rerun_result_review_cannot_create_production_or_live_order_authority",
        ],
    }
    path = tmp_path / "rerun_result_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.FORMAL_RERUN_RESULT_REVIEW_ENV, str(path))

    assert gate.validate_formal_rerun_result_review_file() == []


def test_formal_rerun_result_review_gate_blocks_permission_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_formal_rerun_result_review.v1",
        "rerun_result_review_status": "formal_rerun_results_accepted",
        "passed": True,
        "production_candidate_allowed": True,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_rerun_plan_hash": "",
        "source_manifest_hash": "",
        "rerun_result_review_hash": "",
        "allowed_next_actions": ["go_live"],
        "step_reviews": [{"step": "shadow_execution_evidence", "command": "cmd", "step_status": "passed", "passed": True}],
        "rerun_result_review_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "rerun_result_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.FORMAL_RERUN_RESULT_REVIEW_ENV, str(path))

    failures = gate.validate_formal_rerun_result_review_file()

    assert "formal rerun result review attempted production eligibility" in failures
    assert "formal rerun result review missing source rerun plan hash" in failures
    assert "accepted formal rerun result next action invalid" in failures


def test_rerun_court_rebuild_review_gate_accepts_blocked_non_production_review(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_rerun_court_rebuild_review.v1",
        "court_rebuild_status": "rerun_court_rebuild_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_rerun_result_review_hash": "abc",
        "court_rebuild_review_hash": "def",
        "blocking_reasons": ["formal_rerun_result_review_not_accepted"],
        "artifact_reviews": [
            {
                "name": "rebuilt_evidence_chain_manifest",
                "artifact": "manifest.json",
                "artifact_hash": "hash",
                "status": "evidence_chain_blocked",
                "passed": False,
            }
        ],
        "court_rebuild_contract": {
            "requires_accepted_formal_rerun_result_review": True,
            "requires_rebuilt_evidence_chain_manifest": True,
            "requires_rebuilt_release_chain_adjudication": True,
            "requires_rerun_result_hash_lineage": True,
            "does_not_create_production_eligibility": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "rerun_court_rebuild_review_is_not_release_approval",
            "rebuilt_manifest_or_release_chain_cannot_create_live_order_authority",
            "rerun_court_rebuild_requires_accepted_rerun_results",
            "production_still_requires_release_chain_and_human_approval",
        ],
    }
    path = tmp_path / "court_rebuild_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.RERUN_COURT_REBUILD_REVIEW_ENV, str(path))

    assert gate.validate_rerun_court_rebuild_review_file() == []


def test_rerun_court_rebuild_review_gate_blocks_permission_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_rerun_court_rebuild_review.v1",
        "court_rebuild_status": "rerun_court_rebuild_accepted",
        "passed": True,
        "production_candidate_allowed": True,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_rerun_result_review_hash": "",
        "court_rebuild_review_hash": "",
        "allowed_next_actions": ["go_live"],
        "artifact_reviews": [{"name": "rebuilt_evidence_chain_manifest", "artifact": "manifest.json", "artifact_hash": "hash", "status": "ok", "passed": True}],
        "court_rebuild_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "court_rebuild_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.RERUN_COURT_REBUILD_REVIEW_ENV, str(path))

    failures = gate.validate_rerun_court_rebuild_review_file()

    assert "rerun court rebuild review attempted production eligibility" in failures
    assert "rerun court rebuild review missing source rerun result hash" in failures
    assert "accepted rerun court rebuild next action invalid" in failures


def test_post_rerun_release_readiness_gate_accepts_blocked_non_live_review(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_release_readiness.v1",
        "release_readiness_status": "post_rerun_release_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_rerun_court_rebuild_review_hash": "abc",
        "release_readiness_hash": "def",
        "blocking_reasons": ["rerun_court_rebuild_review_not_accepted"],
        "release_readiness_contract": {
            "requires_accepted_rerun_court_rebuild_review": True,
            "requires_release_chain_passed_for_human_approval": True,
            "requires_human_release_approved": True,
            "requires_rerun_court_hash_lineage": True,
            "does_not_create_live_order_authority": True,
            "does_not_submit_broker_orders": True,
        },
        "hard_boundaries": [
            "post_rerun_release_readiness_is_not_live_order_authority",
            "human_release_approval_still_requires_live_order_authority_check",
            "release_readiness_cannot_submit_broker_orders",
            "live_trading_requires_separate_authority_broker_execution_and_post_trade_chain",
        ],
    }
    path = tmp_path / "post_rerun_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_RELEASE_READINESS_ENV, str(path))

    assert gate.validate_post_rerun_release_readiness_file() == []


def test_post_rerun_release_readiness_gate_blocks_permission_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_release_readiness.v1",
        "release_readiness_status": "post_rerun_release_ready_for_live_authority_check",
        "passed": True,
        "production_candidate_allowed": True,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_rerun_court_rebuild_review_hash": "",
        "release_readiness_hash": "",
        "allowed_next_actions": ["go_live"],
        "release_readiness_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "post_rerun_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_RELEASE_READINESS_ENV, str(path))

    failures = gate.validate_post_rerun_release_readiness_file()

    assert "post-rerun release readiness attempted production eligibility" in failures
    assert "post-rerun release readiness missing source court hash" in failures
    assert "ready post-rerun release readiness next action invalid" in failures


def test_post_rerun_release_readiness_submission_gate_accepts_blocked_non_review_submission(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_release_readiness_submission.v1",
        "release_readiness_submission_status": "post_rerun_release_readiness_submission_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_rerun_court_rebuild_review_hash": "court_hash",
        "source_release_chain_adjudication_hash": "release_hash",
        "source_human_release_approval_hash": "human_hash",
        "release_readiness_submission_hash": "hash",
        "blocking_reasons": ["rerun_court_rebuild_review_not_accepted"],
        "release_readiness_inputs": [
            {
                "name": "rerun_court_rebuild_review",
                "artifact": "court.json",
                "artifact_hash": "hash",
                "artifact_version": "strategy_competition_rerun_court_rebuild_review.v1",
                "status": "rerun_court_rebuild_blocked",
                "passed": False,
                "blocking_reasons": ["rerun_court_rebuild_review_not_accepted"],
            }
        ],
        "release_readiness_submission_contract": {
            "requires_accepted_rerun_court_rebuild_review": True,
            "requires_release_chain_passed_for_human_approval": True,
            "requires_human_release_approved": True,
            "requires_rerun_court_hash_lineage": True,
            "does_not_create_live_order_authority": True,
            "does_not_submit_broker_orders": True,
        },
        "hard_boundaries": [
            "post_rerun_release_readiness_submission_is_not_release_readiness_review",
            "post_rerun_release_readiness_submission_is_not_live_order_authority",
            "post_rerun_release_readiness_submission_does_not_submit_broker_orders",
            "post_rerun_release_readiness_submission_cannot_create_production_eligibility",
        ],
    }
    path = tmp_path / "release_readiness_submission_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_RELEASE_READINESS_SUBMISSION_ENV, str(path))

    assert gate.validate_post_rerun_release_readiness_submission_file() == []


def test_post_rerun_release_readiness_submission_gate_blocks_permission_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_release_readiness_submission.v1",
        "release_readiness_submission_status": "post_rerun_release_readiness_submission_ready",
        "passed": True,
        "production_candidate_allowed": True,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "source_rerun_court_rebuild_review_hash": "",
        "source_release_chain_adjudication_hash": "",
        "source_human_release_approval_hash": "",
        "release_readiness_submission_hash": "",
        "allowed_next_actions": ["go_live"],
        "release_readiness_submission_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "release_readiness_submission_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_RELEASE_READINESS_SUBMISSION_ENV, str(path))

    failures = gate.validate_post_rerun_release_readiness_submission_file()

    assert "post-rerun release readiness submission attempted production eligibility" in failures
    assert "post-rerun release readiness submission missing source court hash" in failures
    assert "ready post-rerun release readiness submission next action invalid" in failures


def test_post_rerun_live_authority_submission_gate_accepts_blocked_non_review_submission(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_live_authority_submission.v1",
        "live_authority_submission_status": "post_rerun_live_authority_submission_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "broker_submission_allowed": False,
        "source_post_rerun_release_readiness_hash": "readiness_hash",
        "source_live_order_authority_hash": "authority_hash",
        "live_authority_submission_hash": "hash",
        "blocking_reasons": ["post_rerun_release_readiness_not_ready"],
        "live_authority_inputs": [
            {
                "name": "live_order_authority",
                "artifact": "authority.json",
                "artifact_hash": "hash",
                "artifact_version": "strategy_competition_live_order_authority.v1",
                "status": "live_order_submission_blocked",
                "passed": False,
                "blocking_reasons": ["live_order_authority_not_allowed"],
            }
        ],
        "live_authority_submission_contract": {
            "requires_post_rerun_release_readiness_ready": True,
            "requires_live_order_authority_allowed": True,
            "requires_readiness_hash_lineage": True,
            "does_not_create_new_live_order_authority": True,
            "does_not_call_broker_adapter": True,
        },
        "hard_boundaries": [
            "post_rerun_live_authority_submission_is_not_live_authority_review",
            "live_authority_submission_does_not_execute_orders",
            "live_authority_submission_cannot_create_broker_permission",
            "live_trading_requires_separate_broker_guard_and_execution_chain",
        ],
    }
    path = tmp_path / "live_authority_submission_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_LIVE_AUTHORITY_SUBMISSION_ENV, str(path))

    assert gate.validate_post_rerun_live_authority_submission_file() == []


def test_post_rerun_live_authority_submission_gate_blocks_permission_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_live_authority_submission.v1",
        "live_authority_submission_status": "post_rerun_live_authority_submission_ready",
        "passed": True,
        "production_candidate_allowed": True,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "broker_submission_allowed": True,
        "source_post_rerun_release_readiness_hash": "",
        "source_live_order_authority_hash": "",
        "live_authority_submission_hash": "",
        "allowed_next_actions": ["go_live"],
        "live_authority_submission_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "live_authority_submission_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_LIVE_AUTHORITY_SUBMISSION_ENV, str(path))

    failures = gate.validate_post_rerun_live_authority_submission_file()

    assert "post-rerun live authority submission attempted production eligibility" in failures
    assert "post-rerun live authority submission missing source readiness hash" in failures
    assert "ready post-rerun live authority submission next action invalid" in failures


def test_post_rerun_broker_guard_submission_gate_accepts_blocked_non_review_submission(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_broker_guard_submission.v1",
        "broker_guard_submission_status": "post_rerun_broker_guard_submission_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "broker_submission_confirmed": False,
        "execution_fills_confirmed": False,
        "source_post_rerun_live_authority_review_hash": "authority_review_hash",
        "source_broker_submission_guard_hash": "guard_hash",
        "broker_guard_submission_hash": "hash",
        "blocking_reasons": ["post_rerun_live_authority_review_not_ready"],
        "broker_guard_inputs": [
            {
                "name": "post_rerun_live_authority_review",
                "artifact": "authority.json",
                "artifact_hash": "hash",
                "artifact_version": "strategy_competition_post_rerun_live_authority_review.v1",
                "status": "post_rerun_live_authority_blocked",
                "passed": False,
            }
        ],
        "broker_guard_submission_contract": {
            "requires_post_rerun_live_authority_review_ready": True,
            "requires_broker_submission_guard_passed": True,
            "requires_live_authority_review_hash_lineage": True,
            "does_not_call_broker_adapter": True,
            "does_not_confirm_submission_or_fills": True,
        },
        "hard_boundaries": [
            "post_rerun_broker_guard_submission_is_not_broker_guard_review",
            "broker_guard_submission_does_not_confirm_submission",
            "broker_response_execution_feedback_and_post_trade_reconciliation_remain_required",
            "submitted_or_guard_passed_does_not_equal_filled",
        ],
    }
    path = tmp_path / "broker_guard_submission_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_BROKER_GUARD_SUBMISSION_ENV, str(path))

    assert gate.validate_post_rerun_broker_guard_submission_file() == []


def test_post_rerun_broker_guard_submission_gate_blocks_permission_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_broker_guard_submission.v1",
        "broker_guard_submission_status": "post_rerun_broker_guard_submission_ready",
        "passed": True,
        "production_candidate_allowed": True,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "broker_submission_confirmed": True,
        "execution_fills_confirmed": False,
        "source_post_rerun_live_authority_review_hash": "",
        "source_broker_submission_guard_hash": "",
        "broker_guard_submission_hash": "",
        "allowed_next_actions": ["go_live"],
        "broker_guard_submission_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "broker_guard_submission_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_BROKER_GUARD_SUBMISSION_ENV, str(path))

    failures = gate.validate_post_rerun_broker_guard_submission_file()

    assert "post-rerun broker guard submission attempted production eligibility" in failures
    assert "post-rerun broker guard submission missing source live authority review hash" in failures
    assert "ready post-rerun broker guard submission next action invalid" in failures


def test_post_rerun_live_authority_review_gate_accepts_blocked_non_broker_review(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_live_authority_review.v1",
        "live_authority_review_status": "post_rerun_live_authority_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "broker_submission_allowed": False,
        "source_post_rerun_release_readiness_hash": "abc",
        "live_authority_review_hash": "def",
        "blocking_reasons": ["post_rerun_release_readiness_not_ready"],
        "live_authority_review_contract": {
            "requires_post_rerun_release_readiness_ready": True,
            "requires_live_order_authority_allowed": True,
            "requires_readiness_hash_lineage": True,
            "does_not_create_new_live_order_authority": True,
            "does_not_call_broker_adapter": True,
        },
        "hard_boundaries": [
            "post_rerun_live_authority_review_is_not_broker_submission",
            "live_authority_review_does_not_execute_orders",
            "broker_submission_requires_separate_guard_and_response",
            "execution_and_post_trade_feedback_remain_required",
        ],
    }
    path = tmp_path / "live_authority_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_LIVE_AUTHORITY_REVIEW_ENV, str(path))

    assert gate.validate_post_rerun_live_authority_review_file() == []


def test_post_rerun_live_authority_review_gate_blocks_broker_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_live_authority_review.v1",
        "live_authority_review_status": "post_rerun_live_authority_ready_for_broker_guard",
        "passed": True,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": True,
        "broker_submission_allowed": True,
        "source_post_rerun_release_readiness_hash": "",
        "live_authority_review_hash": "",
        "allowed_next_actions": ["submit_broker"],
        "order_count": 0,
        "live_authority_review_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "live_authority_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_LIVE_AUTHORITY_REVIEW_ENV, str(path))

    failures = gate.validate_post_rerun_live_authority_review_file()

    assert "post-rerun live authority review attempted live order grant" in failures
    assert "post-rerun live authority review attempted broker permission" in failures
    assert "ready post-rerun live authority next action invalid" in failures


def test_post_rerun_release_chain_adjudication_gate_accepts_blocked_non_broker_artifact(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_release_chain_adjudication.v1",
        "release_chain_status": "post_rerun_release_chain_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "broker_submission_allowed": False,
        "source_post_rerun_release_readiness_hash": "abc",
        "release_chain_hash": "def",
        "blocking_reasons": ["post_rerun_release_readiness_not_ready"],
        "release_chain_contract": {
            "requires_post_rerun_release_readiness_ready": True,
            "requires_post_rerun_live_authority_review_ready": True,
            "requires_readiness_hash_lineage": True,
            "does_not_create_new_live_order_authority": True,
            "does_not_call_broker_adapter": True,
        },
        "hard_boundaries": [
            "post_rerun_release_chain_adjudication_is_not_broker_submission",
            "release_chain_adjudication_does_not_execute_orders",
            "broker_submission_requires_separate_guard_and_response",
            "execution_and_post_trade_feedback_remain_required",
        ],
    }
    path = tmp_path / "release_chain_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_RELEASE_CHAIN_ADJUDICATION_ENV, str(path))

    assert gate.validate_post_rerun_release_chain_adjudication_file() == []


def test_post_rerun_release_chain_adjudication_gate_blocks_permission_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_release_chain_adjudication.v1",
        "release_chain_status": "post_rerun_release_chain_ready_for_broker_guard",
        "passed": True,
        "production_candidate_allowed": True,
        "production_release_authorized": False,
        "live_order_authority_granted": True,
        "broker_submission_allowed": True,
        "source_post_rerun_release_readiness_hash": "",
        "release_chain_hash": "",
        "allowed_next_actions": ["submit_broker"],
        "release_chain_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "release_chain_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_RELEASE_CHAIN_ADJUDICATION_ENV, str(path))

    failures = gate.validate_post_rerun_release_chain_adjudication_file()

    assert "post-rerun release chain adjudication attempted production eligibility" in failures
    assert "post-rerun release chain adjudication attempted live order grant" in failures
    assert "post-rerun release chain adjudication attempted broker permission" in failures


def test_post_rerun_broker_guard_review_gate_accepts_blocked_non_submission_review(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_broker_guard_review.v1",
        "broker_guard_review_status": "post_rerun_broker_guard_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "broker_submission_allowed": False,
        "broker_submission_confirmed": False,
        "execution_fills_confirmed": False,
        "source_post_rerun_live_authority_review_hash": "abc",
        "broker_guard_review_hash": "def",
        "blocking_reasons": ["broker_submission_guard_artifact_missing"],
        "broker_guard_review_contract": {
            "requires_post_rerun_live_authority_review_ready": True,
            "requires_broker_submission_guard_passed": True,
            "requires_live_authority_review_hash_lineage": True,
            "does_not_call_broker_adapter": True,
            "does_not_confirm_submission_or_fills": True,
        },
        "hard_boundaries": [
            "post_rerun_broker_guard_review_is_not_broker_response",
            "broker_guard_review_does_not_confirm_submission",
            "broker_response_execution_feedback_and_post_trade_reconciliation_remain_required",
            "submitted_or_guard_passed_does_not_equal_filled",
        ],
    }
    path = tmp_path / "broker_guard_review_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_BROKER_GUARD_REVIEW_ENV, str(path))

    assert gate.validate_post_rerun_broker_guard_review_file() == []


def test_post_rerun_broker_guard_review_gate_blocks_submission_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_broker_guard_review.v1",
        "broker_guard_review_status": "post_rerun_broker_guard_ready_for_adapter",
        "passed": True,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": True,
        "broker_submission_allowed": True,
        "broker_submission_confirmed": True,
        "execution_fills_confirmed": True,
        "source_post_rerun_live_authority_review_hash": "",
        "broker_guard_review_hash": "",
        "broker_submission_guard_hash": "",
        "allowed_next_actions": ["submit_and_mark_filled"],
        "broker_guard_review_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "broker_guard_review_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_BROKER_GUARD_REVIEW_ENV, str(path))

    failures = gate.validate_post_rerun_broker_guard_review_file()

    assert "post-rerun broker guard review attempted live order grant" in failures
    assert "post-rerun broker guard review attempted broker permission" in failures
    assert "post-rerun broker guard review attempted submission confirmation" in failures
    assert "post-rerun broker guard review attempted fill confirmation" in failures
    assert "ready post-rerun broker guard next action invalid" in failures


def test_post_rerun_broker_response_review_gate_accepts_blocked_non_execution_review(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_broker_response_review.v1",
        "broker_response_review_status": "post_rerun_broker_response_blocked",
        "passed": False,
        "production_candidate_allowed": False,
        "production_release_authorized": False,
        "live_order_authority_granted": False,
        "broker_submission_confirmed": False,
        "execution_fills_confirmed": False,
        "post_trade_reconciliation_passed": False,
        "source_post_rerun_broker_guard_review_hash": "abc",
        "broker_response_review_hash": "def",
        "blocking_reasons": ["broker_submission_response_evidence_missing"],
        "broker_response_review_contract": {
            "requires_post_rerun_broker_guard_review_ready": True,
            "requires_broker_submission_response_accepted": True,
            "requires_broker_guard_hash_lineage": True,
            "does_not_confirm_fills": True,
            "does_not_create_post_trade_reconciliation": True,
        },
        "hard_boundaries": [
            "post_rerun_broker_response_review_is_not_execution_feedback",
            "broker_submission_confirmed_does_not_mean_filled",
            "fills_require_separate_execution_feedback",
            "post_trade_reconciliation_remains_required_after_execution_feedback",
        ],
    }
    path = tmp_path / "broker_response_review_blocked.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_BROKER_RESPONSE_REVIEW_ENV, str(path))

    assert gate.validate_post_rerun_broker_response_review_file() == []


def test_post_rerun_broker_response_review_gate_blocks_fill_or_post_trade_claim(monkeypatch, tmp_path):
    payload = {
        "artifact_version": "strategy_competition_post_rerun_broker_response_review.v1",
        "broker_response_review_status": "post_rerun_broker_response_ready_for_execution_feedback",
        "passed": True,
        "production_candidate_allowed": True,
        "production_release_authorized": False,
        "live_order_authority_granted": True,
        "broker_submission_confirmed": False,
        "execution_fills_confirmed": True,
        "post_trade_reconciliation_passed": True,
        "source_post_rerun_broker_guard_review_hash": "",
        "broker_response_review_hash": "",
        "broker_submission_response_evidence_hash": "",
        "allowed_next_actions": ["mark_trade_complete"],
        "broker_response_review_contract": {},
        "hard_boundaries": [],
    }
    path = tmp_path / "broker_response_review_bad.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv(gate.POST_RERUN_BROKER_RESPONSE_REVIEW_ENV, str(path))

    failures = gate.validate_post_rerun_broker_response_review_file()

    assert "post-rerun broker response review attempted production eligibility" in failures
    assert "post-rerun broker response review attempted live order grant" in failures
    assert "post-rerun broker response review attempted fill confirmation" in failures
    assert "post-rerun broker response review attempted post-trade completion" in failures
    assert "ready post-rerun broker response next action invalid" in failures
    assert "ready post-rerun broker response missing submission confirmation" in failures


def _write_strategy_evidence_bundle(tmp_path: Path) -> Path:
    sweep = tmp_path / "backtest_sweep_combo.json"
    sweep.write_text(
        json.dumps(
            {
                "backtest_credibility": {"passed": True},
                "strategy_backtest_diagnostics": {"eligible_for_formal_ranking": True},
            }
        ),
        encoding="utf-8",
    )
    stage_json = tmp_path / "strategy_stage_audit.json"
    stage_json.write_text(
        json.dumps({"audit_version": "strategy_optimization_stage_audit.v1", "passed": True}),
        encoding="utf-8",
    )
    stage_md = tmp_path / "strategy_stage_audit.md"
    stage_md.write_text("# audit\n", encoding="utf-8")
    rejected = tmp_path / "rejected.jsonl"
    rejected.write_text(
        '{"artifact_path":"logs/openclaw/backtest_sweep_v6_failed.json","strategy":"v6","reason":"quality_floor_failed","reused_as_runtime_default":false}\n',
        encoding="utf-8",
    )
    gate_result = tmp_path / "pytest_strategy_gate.txt"
    gate_result.write_text("passed\n", encoding="utf-8")
    evidence = tmp_path / "strategy_pr_evidence.json"
    evidence.write_text(
        json.dumps(
            {
                "backtest_sweep_artifact": str(sweep),
                "stage_audit_json": str(stage_json),
                "stage_audit_markdown": str(stage_md),
                "rejected_artifacts_ledger": str(rejected),
                "gate_test_results": [{"name": "pytest strategy gate", "path": str(gate_result)}],
            }
        ),
        encoding="utf-8",
    )
    return evidence


def test_strategy_optimization_stage_gate_is_idle_without_scope_or_opt_in(monkeypatch):
    monkeypatch.delenv("AIRIVO_ENABLE_STRATEGY_OPTIMIZATION_STAGE_GATE", raising=False)

    assert gate.run_strategy_optimization_stage_gate() == []


def test_strategy_optimization_stage_gate_requires_db_path(monkeypatch):
    monkeypatch.setenv("AIRIVO_ENABLE_STRATEGY_OPTIMIZATION_STAGE_GATE", "1")
    monkeypatch.delenv("AIRIVO_STRATEGY_OPTIMIZATION_DB_PATH", raising=False)

    failures = gate.run_strategy_optimization_stage_gate()

    assert failures == ["strategy optimization stage gate enabled without AIRIVO_STRATEGY_OPTIMIZATION_DB_PATH"]


def test_strategy_optimization_stage_gate_runs_audit_tool(monkeypatch, tmp_path):
    calls = []

    class Result:
        returncode = 0
        stdout = "ok"
        stderr = ""

    def fake_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        return Result()

    db_path = tmp_path / "fact.db"
    rejected = tmp_path / "rejected.jsonl"
    rejected.write_text(
        '{"artifact_path":"logs/openclaw/backtest_sweep_v8_failed.json","strategy":"v8","reason":"quality_floor_failed","reused_as_runtime_default":false}\n',
        encoding="utf-8",
    )
    evidence = _write_strategy_evidence_bundle(tmp_path)
    monkeypatch.setenv("AIRIVO_ENABLE_STRATEGY_OPTIMIZATION_STAGE_GATE", "1")
    monkeypatch.setenv("AIRIVO_STRATEGY_OPTIMIZATION_DB_PATH", str(db_path))
    monkeypatch.setenv("AIRIVO_REJECTED_BACKTEST_ARTIFACTS_FILE", str(rejected))
    monkeypatch.setenv(gate.STRATEGY_PR_EVIDENCE_ENV, str(evidence))
    monkeypatch.setenv("AIRIVO_STRATEGY_OPTIMIZATION_TRADE_DATE", "2026-05-05")
    monkeypatch.setenv("AIRIVO_STRATEGY_OPTIMIZATION_AUDIT_DIR", str(tmp_path / "audit"))
    monkeypatch.setattr(gate.subprocess, "run", fake_run)

    failures = gate.run_strategy_optimization_stage_gate()

    assert failures == []
    cmd = calls[0][0]
    assert str(gate.ROOT / "tools/strategy_optimization_stage_audit.py") in cmd
    assert "--db-path" in cmd
    assert str(db_path) in cmd
    assert "--rejected-artifacts" in cmd
    assert str(rejected) in cmd
    assert "--trade-date" in cmd
    assert "2026-05-05" in cmd


def test_strategy_optimization_stage_gate_requires_pr_evidence_when_scope_changed(monkeypatch):
    monkeypatch.delenv("AIRIVO_ENABLE_STRATEGY_OPTIMIZATION_STAGE_GATE", raising=False)
    monkeypatch.delenv(gate.STRATEGY_PR_EVIDENCE_ENV, raising=False)
    monkeypatch.setenv("AIRIVO_STRATEGY_OPTIMIZATION_DB_PATH", "/tmp/fact.db")
    monkeypatch.setenv("AIRIVO_REJECTED_BACKTEST_ARTIFACTS_FILE", "/tmp/rejected.jsonl")

    failures = gate.run_strategy_optimization_stage_gate(["openclaw/runtime/v8_signal_evaluator.py"])

    assert failures == [f"strategy optimization PR requires {gate.STRATEGY_PR_EVIDENCE_ENV}"]
