#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
from datetime import datetime
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Iterable
import json


ROOT = Path(__file__).resolve().parents[1]
MANDATE_DOC = "docs/MAINLINE_MANDATE.md"
DELIVERY_STANDARD_DOC = "docs/AIRIVO_UNIQUE_MAINLINE_DELIVERY_STANDARD.md"
EXECUTION_PLAN_DOC = "docs/AIRIVO_30_DAY_EXECUTION_PLAN.md"
ADJUDICATION_DOC = "docs/AIRIVO_MAINLINE_DELIVERY_ADJUDICATION_CHECKLIST.md"
STRATEGY_OPTIMIZATION_DOC = "docs/AIRIVO_STRATEGY_OPTIMIZATION_UPGRADE_EXECUTION_PLAN.md"
REJECTION_STANDARD_DOC = "docs/AIRIVO_CODE_REVIEW_REJECTION_STANDARD.md"
STRATEGY_OPTIMIZATION_STAGE_AUDIT_TOOL = "tools/strategy_optimization_stage_audit.py"
REJECTED_BACKTEST_ARTIFACTS_TOOL = "tools/rejected_backtest_artifacts.py"
EXECUTION_ATTRIBUTION_BACKFILL_TOOL = "tools/backfill_execution_attribution.py"
STRATEGY_COMPETITION_AUDIT_TOOL = "tools/strategy_competition_portfolio_audit.py"
CURRENT_STRATEGY_COMPETITION_AUDIT_TOOL = "tools/build_current_strategy_competition_audit.py"
BENCHMARK_INDUSTRY_CONTRACT_TOOL = "tools/build_benchmark_industry_contract.py"
BENCHMARK_SIGNATURE_VERIFY_HOOK_TOOL = "tools/verify_benchmark_contract_signature_hook.py"
BENCHMARK_KMS_PKI_DELEGATE_VERIFIER_TOOL = "tools/kms_pki_delegate_verifier.py"
BENCHMARK_SECURITY_DRILL_TOOL = "tools/run_benchmark_contract_security_drill.py"
BENCHMARK_KEYRING_EXAMPLE_DOC = "docs/benchmark_contract_keyring.example.json"
BENCHMARK_KMS_IAM_POLICY_EXAMPLE_DOC = "docs/benchmark_kms_iam_policy.example.json"
BENCHMARK_SIGNATURE_INTEGRATION_RUNBOOK_DOC = "docs/benchmark_signature_integration_runbook.md"
STRATEGY_COMPETITION_SHADOW_EXECUTION_PLAN_TOOL = "tools/build_strategy_competition_shadow_execution_plan.py"
STRATEGY_COMPETITION_SHADOW_FEEDBACK_TOOL = "tools/record_strategy_competition_shadow_feedback.py"
STRATEGY_COMPETITION_INDEPENDENT_VALIDATION_TOOL = "tools/build_strategy_competition_independent_validation.py"
STRATEGY_COMPETITION_EVIDENCE_INTAKE_TOOL = "tools/build_strategy_competition_evidence_intake_packet.py"
STRATEGY_COMPETITION_EVIDENCE_SUBMISSION_REVIEW_TOOL = "tools/review_strategy_competition_evidence_submission.py"
STRATEGY_COMPETITION_OPERATIONAL_CONTROLS_TOOL = "tools/build_strategy_competition_operational_controls.py"
STRATEGY_COMPETITION_PRODUCTION_READINESS_TOOL = "tools/build_strategy_competition_production_readiness.py"
STRATEGY_COMPETITION_FORMAL_VALIDATION_HANDOFF_TOOL = "tools/build_strategy_competition_formal_validation_handoff.py"
STRATEGY_COMPETITION_FORMAL_VALIDATION_RESULT_REVIEW_TOOL = "tools/review_strategy_competition_formal_validation_results.py"
STRATEGY_COMPETITION_RELEASE_CHAIN_ADJUDICATION_TOOL = "tools/adjudicate_strategy_competition_release_chain.py"
STRATEGY_COMPETITION_HUMAN_RELEASE_APPROVAL_TOOL = "tools/build_strategy_competition_human_release_approval.py"
STRATEGY_COMPETITION_LIVE_ORDER_AUTHORITY_TOOL = "tools/check_strategy_competition_live_order_authority.py"
STRATEGY_COMPETITION_BROKER_SUBMISSION_GUARD_TOOL = "tools/check_strategy_competition_broker_submission_guard.py"
STRATEGY_COMPETITION_BROKER_SUBMISSION_RESPONSE_TOOL = "tools/review_strategy_competition_broker_submission_response.py"
STRATEGY_COMPETITION_BROKER_EXECUTION_FEEDBACK_TOOL = "tools/review_strategy_competition_broker_execution_feedback.py"
STRATEGY_COMPETITION_POST_RERUN_BROKER_EXECUTION_FEEDBACK_TOOL = "tools/review_strategy_competition_post_rerun_broker_execution_feedback.py"
STRATEGY_COMPETITION_POST_RERUN_POST_TRADE_RECONCILIATION_TOOL = "tools/reconcile_strategy_competition_post_rerun_post_trade.py"
STRATEGY_COMPETITION_POST_RERUN_TRADE_LIFECYCLE_ADJUDICATION_TOOL = "tools/adjudicate_strategy_competition_post_rerun_trade_lifecycle.py"
STRATEGY_COMPETITION_POST_RERUN_EVIDENCE_CHAIN_MANIFEST_TOOL = "tools/build_strategy_competition_post_rerun_evidence_chain_manifest.py"
STRATEGY_COMPETITION_POST_RERUN_HUMAN_RELEASE_APPROVAL_REVIEW_TOOL = "tools/review_strategy_competition_post_rerun_human_release_approval_review.py"
STRATEGY_COMPETITION_POST_RERUN_LIVE_AUTHORITY_SUBMISSION_TOOL = "tools/build_strategy_competition_post_rerun_live_authority_submission.py"
STRATEGY_COMPETITION_POST_RERUN_LIVE_AUTHORITY_REVIEW_TOOL = "tools/review_strategy_competition_post_rerun_live_authority.py"
STRATEGY_COMPETITION_POST_RERUN_BROKER_GUARD_SUBMISSION_TOOL = "tools/build_strategy_competition_post_rerun_broker_guard_submission.py"
STRATEGY_COMPETITION_POST_RERUN_BROKER_GUARD_REVIEW_TOOL = "tools/review_strategy_competition_post_rerun_broker_guard.py"
STRATEGY_COMPETITION_POST_TRADE_RECONCILIATION_TOOL = "tools/reconcile_strategy_competition_post_trade.py"
STRATEGY_COMPETITION_TRADE_LIFECYCLE_ADJUDICATION_TOOL = "tools/adjudicate_strategy_competition_trade_lifecycle.py"
STRATEGY_COMPETITION_EVIDENCE_CHAIN_MANIFEST_TOOL = "tools/build_strategy_competition_evidence_chain_manifest.py"
STRATEGY_COMPETITION_EVIDENCE_REMEDIATION_WORK_ORDER_TOOL = "tools/build_strategy_competition_evidence_remediation_work_order.py"
STRATEGY_COMPETITION_REMEDIATION_CLOSURE_SUBMISSION_TOOL = "tools/build_strategy_competition_remediation_closure_submission.py"
STRATEGY_COMPETITION_REMEDIATION_CLOSURE_REVIEW_TOOL = "tools/review_strategy_competition_remediation_closure.py"
STRATEGY_COMPETITION_FORMAL_RERUN_PLAN_TOOL = "tools/build_strategy_competition_formal_rerun_plan.py"
STRATEGY_COMPETITION_FORMAL_RERUN_OUTPUT_SUBMISSION_TOOL = "tools/build_strategy_competition_formal_rerun_output_submission.py"
STRATEGY_COMPETITION_FORMAL_RERUN_RESULT_REVIEW_TOOL = "tools/review_strategy_competition_formal_rerun_results.py"
STRATEGY_COMPETITION_RERUN_COURT_REBUILD_SUBMISSION_TOOL = "tools/build_strategy_competition_rerun_court_rebuild_submission.py"
STRATEGY_COMPETITION_RERUN_COURT_REBUILD_REVIEW_TOOL = "tools/review_strategy_competition_rerun_court_rebuild.py"
STRATEGY_COMPETITION_POST_RERUN_RELEASE_READINESS_SUBMISSION_TOOL = "tools/build_strategy_competition_post_rerun_release_readiness_submission.py"
STRATEGY_COMPETITION_POST_RERUN_RELEASE_READINESS_TOOL = "tools/review_strategy_competition_post_rerun_release_readiness.py"
STRATEGY_COMPETITION_POST_RERUN_LIVE_AUTHORITY_REVIEW_TOOL = "tools/review_strategy_competition_post_rerun_live_authority.py"
STRATEGY_COMPETITION_POST_RERUN_RELEASE_CHAIN_ADJUDICATION_TOOL = "tools/adjudicate_strategy_competition_post_rerun_release_chain.py"
STRATEGY_COMPETITION_POST_RERUN_BROKER_GUARD_REVIEW_TOOL = "tools/review_strategy_competition_post_rerun_broker_guard.py"
STRATEGY_COMPETITION_POST_RERUN_BROKER_RESPONSE_REVIEW_TOOL = "tools/review_strategy_competition_post_rerun_broker_response.py"
PR_TEMPLATE = ".github/pull_request_template.md"
REQUIRED_MAINLINE_FILES = (
    MANDATE_DOC,
    DELIVERY_STANDARD_DOC,
    EXECUTION_PLAN_DOC,
    ADJUDICATION_DOC,
    STRATEGY_OPTIMIZATION_DOC,
    REJECTION_STANDARD_DOC,
    STRATEGY_OPTIMIZATION_STAGE_AUDIT_TOOL,
    REJECTED_BACKTEST_ARTIFACTS_TOOL,
    EXECUTION_ATTRIBUTION_BACKFILL_TOOL,
    STRATEGY_COMPETITION_AUDIT_TOOL,
    CURRENT_STRATEGY_COMPETITION_AUDIT_TOOL,
    BENCHMARK_INDUSTRY_CONTRACT_TOOL,
    BENCHMARK_SIGNATURE_VERIFY_HOOK_TOOL,
    BENCHMARK_KMS_PKI_DELEGATE_VERIFIER_TOOL,
    BENCHMARK_SECURITY_DRILL_TOOL,
    BENCHMARK_KEYRING_EXAMPLE_DOC,
    BENCHMARK_KMS_IAM_POLICY_EXAMPLE_DOC,
    BENCHMARK_SIGNATURE_INTEGRATION_RUNBOOK_DOC,
    STRATEGY_COMPETITION_SHADOW_EXECUTION_PLAN_TOOL,
    STRATEGY_COMPETITION_SHADOW_FEEDBACK_TOOL,
    STRATEGY_COMPETITION_INDEPENDENT_VALIDATION_TOOL,
    STRATEGY_COMPETITION_EVIDENCE_INTAKE_TOOL,
    STRATEGY_COMPETITION_EVIDENCE_SUBMISSION_REVIEW_TOOL,
    STRATEGY_COMPETITION_OPERATIONAL_CONTROLS_TOOL,
    STRATEGY_COMPETITION_PRODUCTION_READINESS_TOOL,
    STRATEGY_COMPETITION_FORMAL_VALIDATION_HANDOFF_TOOL,
    STRATEGY_COMPETITION_FORMAL_VALIDATION_RESULT_REVIEW_TOOL,
    STRATEGY_COMPETITION_RELEASE_CHAIN_ADJUDICATION_TOOL,
    STRATEGY_COMPETITION_HUMAN_RELEASE_APPROVAL_TOOL,
    STRATEGY_COMPETITION_LIVE_ORDER_AUTHORITY_TOOL,
    STRATEGY_COMPETITION_BROKER_SUBMISSION_GUARD_TOOL,
    STRATEGY_COMPETITION_BROKER_SUBMISSION_RESPONSE_TOOL,
    STRATEGY_COMPETITION_BROKER_EXECUTION_FEEDBACK_TOOL,
    STRATEGY_COMPETITION_POST_RERUN_BROKER_EXECUTION_FEEDBACK_TOOL,
    STRATEGY_COMPETITION_POST_RERUN_POST_TRADE_RECONCILIATION_TOOL,
    STRATEGY_COMPETITION_POST_RERUN_TRADE_LIFECYCLE_ADJUDICATION_TOOL,
    STRATEGY_COMPETITION_POST_RERUN_EVIDENCE_CHAIN_MANIFEST_TOOL,
    STRATEGY_COMPETITION_POST_RERUN_HUMAN_RELEASE_APPROVAL_REVIEW_TOOL,
    STRATEGY_COMPETITION_POST_TRADE_RECONCILIATION_TOOL,
    STRATEGY_COMPETITION_TRADE_LIFECYCLE_ADJUDICATION_TOOL,
    STRATEGY_COMPETITION_EVIDENCE_CHAIN_MANIFEST_TOOL,
    STRATEGY_COMPETITION_EVIDENCE_REMEDIATION_WORK_ORDER_TOOL,
    "tools/build_strategy_competition_remediation_closure_submission.py",
    STRATEGY_COMPETITION_REMEDIATION_CLOSURE_REVIEW_TOOL,
    STRATEGY_COMPETITION_FORMAL_RERUN_PLAN_TOOL,
    STRATEGY_COMPETITION_FORMAL_RERUN_OUTPUT_SUBMISSION_TOOL,
    STRATEGY_COMPETITION_FORMAL_RERUN_RESULT_REVIEW_TOOL,
    STRATEGY_COMPETITION_RERUN_COURT_REBUILD_SUBMISSION_TOOL,
    STRATEGY_COMPETITION_RERUN_COURT_REBUILD_REVIEW_TOOL,
    STRATEGY_COMPETITION_POST_RERUN_RELEASE_READINESS_SUBMISSION_TOOL,
    STRATEGY_COMPETITION_POST_RERUN_LIVE_AUTHORITY_SUBMISSION_TOOL,
    STRATEGY_COMPETITION_POST_RERUN_BROKER_GUARD_SUBMISSION_TOOL,
    STRATEGY_COMPETITION_POST_RERUN_RELEASE_READINESS_TOOL,
    STRATEGY_COMPETITION_POST_RERUN_LIVE_AUTHORITY_REVIEW_TOOL,
    STRATEGY_COMPETITION_POST_RERUN_RELEASE_CHAIN_ADJUDICATION_TOOL,
    STRATEGY_COMPETITION_POST_RERUN_BROKER_GUARD_REVIEW_TOOL,
    STRATEGY_COMPETITION_POST_RERUN_BROKER_RESPONSE_REVIEW_TOOL,
    PR_TEMPLATE,
)

ALLOWED_NEW_ROOT_FILES = {
    "README.md",
    ".gitignore",
    ".env.example",
    "config.json.example",
    "pytest.ini",
    "requirements.txt",
    "v49_app.py",
    "终极量价暴涨系统_v49.0_长期稳健版.py",
    "start_v49.sh",
    "start_v49_full.sh",
    "start_v49_streamlit.sh",
}

ROOT_BLOCKED_SUFFIXES = {
    ".py",
    ".sh",
    ".db",
    ".csv",
    ".log",
    ".sqlite",
    ".sqlite3",
    ".json",
}

WARN_LINE_THRESHOLD = 5000
FAIL_NEW_FILE_LINE_THRESHOLD = 1500
GOVERNANCE_SENSITIVE_PATHS = (
    ".github/",
    "tools/governance_gate.py",
    "tools/strategy_optimization_stage_audit.py",
    "tools/strategy_competition_portfolio_audit.py",
    "tools/build_current_strategy_competition_audit.py",
    "tools/build_benchmark_industry_contract.py",
    "tools/verify_benchmark_contract_signature_hook.py",
    "tools/kms_pki_delegate_verifier.py",
    "tools/run_benchmark_contract_security_drill.py",
    "tools/build_strategy_competition_shadow_execution_plan.py",
    "tools/record_strategy_competition_shadow_feedback.py",
    "tools/build_strategy_competition_independent_validation.py",
    "tools/build_strategy_competition_evidence_intake_packet.py",
    "tools/review_strategy_competition_evidence_submission.py",
    "tools/build_strategy_competition_operational_controls.py",
    "tools/build_strategy_competition_production_readiness.py",
    "tools/build_strategy_competition_formal_validation_handoff.py",
    "tools/review_strategy_competition_formal_validation_results.py",
    "tools/adjudicate_strategy_competition_release_chain.py",
    "tools/build_strategy_competition_human_release_approval.py",
    "tools/check_strategy_competition_live_order_authority.py",
    "tools/check_strategy_competition_broker_submission_guard.py",
    "tools/review_strategy_competition_broker_submission_response.py",
    "tools/review_strategy_competition_broker_execution_feedback.py",
    "tools/review_strategy_competition_post_rerun_broker_execution_feedback.py",
    "tools/reconcile_strategy_competition_post_rerun_post_trade.py",
    "tools/adjudicate_strategy_competition_post_rerun_trade_lifecycle.py",
    "tools/build_strategy_competition_post_rerun_evidence_chain_manifest.py",
    "tools/build_strategy_competition_post_rerun_release_readiness_submission.py",
    "tools/build_strategy_competition_post_rerun_live_authority_submission.py",
    "tools/build_strategy_competition_post_rerun_broker_guard_submission.py",
    "tools/build_strategy_competition_post_rerun_live_authority_submission.py",
    "tools/review_strategy_competition_post_rerun_human_release_approval_review.py",
    "tools/review_strategy_competition_post_rerun_live_authority.py",
    "tools/review_strategy_competition_post_rerun_broker_guard.py",
    "tools/reconcile_strategy_competition_post_trade.py",
    "tools/adjudicate_strategy_competition_trade_lifecycle.py",
    "tools/build_strategy_competition_evidence_chain_manifest.py",
    "tools/build_strategy_competition_evidence_remediation_work_order.py",
    "tools/build_strategy_competition_remediation_closure_submission.py",
    "tools/review_strategy_competition_remediation_closure.py",
    "tools/build_strategy_competition_formal_rerun_plan.py",
    "tools/build_strategy_competition_formal_rerun_output_submission.py",
    "tools/build_strategy_competition_rerun_court_rebuild_submission.py",
    "tools/build_strategy_competition_post_rerun_release_readiness_submission.py",
    "tools/build_strategy_competition_post_rerun_live_authority_submission.py",
    "tools/build_strategy_competition_post_rerun_broker_guard_submission.py",
    "tools/review_strategy_competition_formal_rerun_results.py",
    "tools/review_strategy_competition_rerun_court_rebuild.py",
    "tools/build_strategy_competition_formal_rerun_output_submission.py",
    "tools/build_strategy_competition_rerun_court_rebuild_submission.py",
    "tools/review_strategy_competition_post_rerun_release_readiness.py",
    "tools/review_strategy_competition_post_rerun_live_authority.py",
    "tools/adjudicate_strategy_competition_post_rerun_release_chain.py",
    "tools/review_strategy_competition_post_rerun_broker_guard.py",
    "tools/review_strategy_competition_post_rerun_broker_response.py",
    "tools/rejected_backtest_artifacts.py",
    "tools/backfill_execution_attribution.py",
    "tools/release_gate.sh",
)
STRATEGY_OPTIMIZATION_SENSITIVE_PATH_PREFIXES = (
    "openclaw/research/",
    "openclaw/services/backtest_credibility_service.py",
    "openclaw/services/strategy_backtest_diagnostic_service.py",
    "openclaw/services/strategy_optimization_stage_service.py",
    "openclaw/services/strategy_competition_audit_service.py",
    "openclaw/services/strategy_competition_shadow_execution_service.py",
    "openclaw/services/strategy_competition_shadow_feedback_service.py",
    "openclaw/services/strategy_competition_independent_validation_service.py",
    "openclaw/services/strategy_competition_evidence_intake_service.py",
    "openclaw/services/strategy_competition_operational_controls_service.py",
    "openclaw/services/strategy_competition_production_readiness_service.py",
    "openclaw/services/strategy_competition_formal_validation_handoff_service.py",
    "openclaw/services/strategy_competition_release_chain_adjudication_service.py",
    "openclaw/services/strategy_competition_human_release_approval_service.py",
    "openclaw/services/strategy_competition_live_order_authority_service.py",
    "openclaw/services/strategy_competition_broker_submission_guard_service.py",
    "openclaw/services/strategy_competition_broker_submission_response_service.py",
    "openclaw/services/strategy_competition_broker_execution_feedback_service.py",
    "openclaw/services/strategy_competition_post_rerun_broker_execution_feedback_review_service.py",
    "openclaw/services/strategy_competition_post_rerun_post_trade_reconciliation_service.py",
    "openclaw/services/strategy_competition_post_rerun_trade_lifecycle_adjudication_service.py",
    "openclaw/services/strategy_competition_post_rerun_evidence_chain_manifest_service.py",
    "openclaw/services/strategy_competition_post_rerun_human_release_approval_review_service.py",
    "openclaw/services/strategy_competition_post_trade_reconciliation_service.py",
    "openclaw/services/strategy_competition_trade_lifecycle_adjudication_service.py",
    "openclaw/services/strategy_competition_evidence_chain_manifest_service.py",
    "openclaw/services/strategy_competition_evidence_remediation_work_order_service.py",
    "openclaw/services/strategy_competition_remediation_closure_submission_service.py",
    "openclaw/services/strategy_competition_remediation_closure_review_service.py",
    "openclaw/services/strategy_competition_formal_rerun_plan_service.py",
    "openclaw/services/strategy_competition_formal_rerun_output_submission_service.py",
    "tools/build_strategy_competition_formal_rerun_output_submission.py",
    "openclaw/services/strategy_competition_rerun_court_rebuild_submission_service.py",
    "tools/build_strategy_competition_rerun_court_rebuild_submission.py",
    "tools/build_strategy_competition_post_rerun_release_readiness_submission.py",
    "tools/build_strategy_competition_post_rerun_live_authority_submission.py",
    "openclaw/services/strategy_competition_post_rerun_release_readiness_submission_service.py",
    "openclaw/services/strategy_competition_post_rerun_live_authority_submission_service.py",
    "openclaw/services/strategy_competition_post_rerun_broker_guard_submission_service.py",
    "tools/build_strategy_competition_post_rerun_broker_guard_submission.py",
    "openclaw/services/strategy_competition_post_rerun_release_readiness_service.py",
    "openclaw/services/strategy_competition_post_rerun_live_authority_review_service.py",
    "openclaw/services/strategy_competition_post_rerun_broker_guard_review_service.py",
    "openclaw/services/strategy_competition_formal_rerun_result_review_service.py",
    "openclaw/services/strategy_competition_rerun_court_rebuild_review_service.py",
    "openclaw/services/strategy_competition_post_rerun_release_readiness_submission_service.py",
    "openclaw/services/strategy_competition_post_rerun_release_readiness_service.py",
    "openclaw/services/strategy_competition_post_rerun_live_authority_review_service.py",
    "openclaw/services/strategy_competition_post_rerun_release_chain_adjudication_service.py",
    "openclaw/services/strategy_competition_post_rerun_broker_guard_review_service.py",
    "openclaw/services/strategy_competition_post_rerun_broker_response_review_service.py",
    "openclaw/services/execution_evidence_service.py",
    "openclaw/services/ensemble_alpha_failure_attribution_service.py",
    "openclaw/services/ensemble_allocator_throttle_attribution_service.py",
    "openclaw/services/ensemble_observation_monitor_service.py",
    "openclaw/services/ensemble_risk_off_alpha_repair_review_service.py",
    "openclaw/services/research_repair_iteration_flow_service.py",
    "openclaw/services/experiment_governance_service.py",
    "openclaw/runtime/v6_",
    "openclaw/runtime/v8_",
    "openclaw/runtime/combo_",
    "strategies/center_config.py",
    "tools/backtest_param_sweep.py",
    "tools/ensemble_alpha_failure_attribution.py",
    "tools/ensemble_allocator_throttle_attribution.py",
    "tools/ensemble_risk_off_alpha_repair_review.py",
    "tools/research_repair_iteration_flow.py",
    "tools/backfill_hard_event_alpha_v5_repair_flow.py",
    "tools/create_hard_event_alpha_v5_watch_oos_plan.py",
    "tools/run_hard_event_alpha_v5_oos_monitoring.py",
    "tools/predeclare_hard_event_alpha_v6_repair.py",
    "tools/compare_hard_event_alpha_v5_v6_failures.py",
    "tools/archive_failed_hard_event_alpha_candidate.py",
    "tools/strategy_optimization_stage_audit.py",
    "tools/strategy_competition_portfolio_audit.py",
    "tools/build_current_strategy_competition_audit.py",
    "tools/build_strategy_competition_shadow_execution_plan.py",
    "tools/record_strategy_competition_shadow_feedback.py",
    "tools/build_strategy_competition_independent_validation.py",
    "tools/build_strategy_competition_evidence_intake_packet.py",
    "tools/review_strategy_competition_evidence_submission.py",
    "tools/build_strategy_competition_operational_controls.py",
    "tools/build_strategy_competition_production_readiness.py",
    "tools/build_strategy_competition_formal_validation_handoff.py",
    "tools/review_strategy_competition_formal_validation_results.py",
    "tools/adjudicate_strategy_competition_release_chain.py",
    "tools/build_strategy_competition_human_release_approval.py",
    "tools/check_strategy_competition_live_order_authority.py",
    "tools/check_strategy_competition_broker_submission_guard.py",
    "tools/review_strategy_competition_broker_submission_response.py",
    "tools/review_strategy_competition_broker_execution_feedback.py",
    "tools/review_strategy_competition_post_rerun_broker_execution_feedback.py",
    "tools/reconcile_strategy_competition_post_rerun_post_trade.py",
    "tools/adjudicate_strategy_competition_post_rerun_trade_lifecycle.py",
    "tools/build_strategy_competition_post_rerun_evidence_chain_manifest.py",
    "tools/review_strategy_competition_post_rerun_human_release_approval_review.py",
    "openclaw/services/strategy_competition_post_rerun_release_readiness_submission_service.py",
    "openclaw/services/strategy_competition_post_rerun_live_authority_submission_service.py",
    "openclaw/services/strategy_competition_post_rerun_broker_guard_submission_service.py",
    "tools/review_strategy_competition_post_rerun_live_authority.py",
    "tools/review_strategy_competition_post_rerun_broker_guard.py",
    "tools/reconcile_strategy_competition_post_trade.py",
    "tools/adjudicate_strategy_competition_trade_lifecycle.py",
    "tools/build_strategy_competition_evidence_chain_manifest.py",
    "tools/build_strategy_competition_evidence_remediation_work_order.py",
    "tools/build_strategy_competition_remediation_closure_submission.py",
    "tools/review_strategy_competition_remediation_closure.py",
    "tools/build_strategy_competition_formal_rerun_plan.py",
    "tools/build_strategy_competition_formal_rerun_output_submission.py",
    "tools/build_strategy_competition_rerun_court_rebuild_submission.py",
    "tools/build_strategy_competition_rerun_court_rebuild_submission.py",
    "tools/review_strategy_competition_formal_rerun_results.py",
    "tools/review_strategy_competition_rerun_court_rebuild.py",
    "tools/review_strategy_competition_post_rerun_release_readiness.py",
    "tools/review_strategy_competition_post_rerun_live_authority.py",
    "tools/adjudicate_strategy_competition_post_rerun_release_chain.py",
    "tools/review_strategy_competition_post_rerun_broker_guard.py",
    "tools/review_strategy_competition_post_rerun_broker_response.py",
    "tools/rejected_backtest_artifacts.py",
    "tests/test_center_config.py",
    "tests/test_backtest_credibility_service.py",
    "tests/test_strategy_backtest_diagnostic_service.py",
    "tests/test_backtest_param_sweep.py",
    "tests/test_execution_evidence_service.py",
    "tests/test_ensemble_alpha_failure_attribution_service.py",
    "tests/test_ensemble_allocator_throttle_attribution_service.py",
    "tests/test_ensemble_risk_off_alpha_repair_review_service.py",
    "tests/test_research_repair_iteration_flow_service.py",
    "tests/test_research_repair_iteration_flow_tool.py",
    "tests/test_governance_gate_repair_flow.py",
    "tests/test_experiment_governance_service.py",
    "tests/test_strategy_optimization_stage_service.py",
    "tests/test_strategy_optimization_stage_audit_tool.py",
)
EXECUTION_ATTRIBUTION_SENSITIVE_PATH_PREFIXES = (
    "openclaw/services/execution_evidence_service.py",
    "openclaw/services/execution_attribution_backfill_service.py",
    "openclaw/services/execution_analytics_service.py",
    "tools/backfill_execution_attribution.py",
    "tools/stable_execution_evidence_fixture.py",
    "tests/test_execution_evidence_service.py",
    "tests/test_execution_attribution_backfill_service.py",
    "tests/test_stable_execution_evidence_fixture_service.py",
)
STRATEGY_PR_EVIDENCE_ENV = "AIRIVO_STRATEGY_PR_EVIDENCE_FILE"
REPAIR_FLOW_EVIDENCE_ENV = "AIRIVO_REPAIR_FLOW_EVIDENCE_FILE"
COMPETITION_AUDIT_EVIDENCE_ENV = "AIRIVO_COMPETITION_AUDIT_FILE"
BENCHMARK_CONTRACT_SIGNING_SECRET_ENV = "AIRIVO_BENCHMARK_CONTRACT_SIGNING_SECRET"
BENCHMARK_CONTRACT_VERIFY_HOOK_ENV = "AIRIVO_BENCHMARK_CONTRACT_VERIFY_HOOK"
BENCHMARK_CONTRACT_VERIFY_HOOK_TIMEOUT_SECONDS_ENV = "AIRIVO_BENCHMARK_CONTRACT_VERIFY_HOOK_TIMEOUT_SECONDS"
BENCHMARK_CONTRACT_VERIFY_HOOK_FAILURE_POLICY_ENV = "AIRIVO_BENCHMARK_CONTRACT_VERIFY_HOOK_FAILURE_POLICY"
BENCHMARK_CONTRACT_VERIFY_HOOK_ALERT_MIN_LEVEL_ENV = "AIRIVO_BENCHMARK_CONTRACT_VERIFY_HOOK_ALERT_MIN_LEVEL"
BENCHMARK_CONTRACT_ALLOWED_ALGOS_ENV = "AIRIVO_BENCHMARK_CONTRACT_ALLOWED_ALGOS"
BENCHMARK_CONTRACT_ACTIVE_KEY_IDS_ENV = "AIRIVO_BENCHMARK_CONTRACT_ACTIVE_KEY_IDS"
BENCHMARK_CONTRACT_REVOKED_KEY_IDS_ENV = "AIRIVO_BENCHMARK_CONTRACT_REVOKED_KEY_IDS"
BENCHMARK_CONTRACT_KEYRING_FILE_ENV = "AIRIVO_BENCHMARK_CONTRACT_KEYRING_FILE"
EVIDENCE_SUBMISSION_REVIEW_ENV = "AIRIVO_EVIDENCE_SUBMISSION_REVIEW_FILE"
PRODUCTION_READINESS_EVIDENCE_ENV = "AIRIVO_PRODUCTION_READINESS_FILE"
FORMAL_VALIDATION_HANDOFF_ENV = "AIRIVO_FORMAL_VALIDATION_HANDOFF_FILE"
FORMAL_VALIDATION_RESULT_REVIEW_ENV = "AIRIVO_FORMAL_VALIDATION_RESULT_REVIEW_FILE"
RELEASE_CHAIN_ADJUDICATION_ENV = "AIRIVO_RELEASE_CHAIN_ADJUDICATION_FILE"
HUMAN_RELEASE_APPROVAL_ENV = "AIRIVO_HUMAN_RELEASE_APPROVAL_FILE"
LIVE_ORDER_AUTHORITY_ENV = "AIRIVO_LIVE_ORDER_AUTHORITY_FILE"
BROKER_SUBMISSION_GUARD_ENV = "AIRIVO_BROKER_SUBMISSION_GUARD_FILE"
BROKER_SUBMISSION_RESPONSE_ENV = "AIRIVO_BROKER_SUBMISSION_RESPONSE_FILE"
BROKER_EXECUTION_FEEDBACK_ENV = "AIRIVO_BROKER_EXECUTION_FEEDBACK_FILE"
POST_RERUN_BROKER_EXECUTION_FEEDBACK_REVIEW_ENV = "AIRIVO_POST_RERUN_BROKER_EXECUTION_FEEDBACK_REVIEW_FILE"
POST_RERUN_POST_TRADE_RECONCILIATION_ENV = "AIRIVO_POST_RERUN_POST_TRADE_RECONCILIATION_FILE"
POST_RERUN_TRADE_LIFECYCLE_ADJUDICATION_ENV = "AIRIVO_POST_RERUN_TRADE_LIFECYCLE_ADJUDICATION_FILE"
POST_RERUN_EVIDENCE_CHAIN_MANIFEST_ENV = "AIRIVO_POST_RERUN_EVIDENCE_CHAIN_MANIFEST_FILE"
POST_RERUN_HUMAN_RELEASE_APPROVAL_REVIEW_ENV = "AIRIVO_POST_RERUN_HUMAN_RELEASE_APPROVAL_REVIEW_FILE"
POST_RERUN_RELEASE_READINESS_SUBMISSION_ENV = "AIRIVO_POST_RERUN_RELEASE_READINESS_SUBMISSION_FILE"
POST_TRADE_RECONCILIATION_ENV = "AIRIVO_POST_TRADE_RECONCILIATION_FILE"
TRADE_LIFECYCLE_ADJUDICATION_ENV = "AIRIVO_TRADE_LIFECYCLE_ADJUDICATION_FILE"
EVIDENCE_CHAIN_MANIFEST_ENV = "AIRIVO_EVIDENCE_CHAIN_MANIFEST_FILE"
EVIDENCE_REMEDIATION_WORK_ORDER_ENV = "AIRIVO_EVIDENCE_REMEDIATION_WORK_ORDER_FILE"
REMEDIATION_CLOSURE_SUBMISSION_ENV = "AIRIVO_REMEDIATION_CLOSURE_SUBMISSION_FILE"
REMEDIATION_CLOSURE_REVIEW_ENV = "AIRIVO_REMEDIATION_CLOSURE_REVIEW_FILE"
FORMAL_RERUN_PLAN_ENV = "AIRIVO_FORMAL_RERUN_PLAN_FILE"
FORMAL_RERUN_OUTPUT_SUBMISSION_ENV = "AIRIVO_FORMAL_RERUN_OUTPUT_SUBMISSION_FILE"
FORMAL_RERUN_RESULT_REVIEW_ENV = "AIRIVO_FORMAL_RERUN_RESULT_REVIEW_FILE"
RERUN_COURT_REBUILD_SUBMISSION_ENV = "AIRIVO_RERUN_COURT_REBUILD_SUBMISSION_FILE"
RERUN_COURT_REBUILD_REVIEW_ENV = "AIRIVO_RERUN_COURT_REBUILD_REVIEW_FILE"
POST_RERUN_RELEASE_READINESS_ENV = "AIRIVO_POST_RERUN_RELEASE_READINESS_FILE"
POST_RERUN_LIVE_AUTHORITY_REVIEW_ENV = "AIRIVO_POST_RERUN_LIVE_AUTHORITY_REVIEW_FILE"
POST_RERUN_LIVE_AUTHORITY_SUBMISSION_ENV = "AIRIVO_POST_RERUN_LIVE_AUTHORITY_SUBMISSION_FILE"
POST_RERUN_BROKER_GUARD_SUBMISSION_ENV = "AIRIVO_POST_RERUN_BROKER_GUARD_SUBMISSION_FILE"
POST_RERUN_RELEASE_CHAIN_ADJUDICATION_ENV = "AIRIVO_POST_RERUN_RELEASE_CHAIN_ADJUDICATION_FILE"
POST_RERUN_BROKER_GUARD_REVIEW_ENV = "AIRIVO_POST_RERUN_BROKER_GUARD_REVIEW_FILE"
POST_RERUN_BROKER_RESPONSE_REVIEW_ENV = "AIRIVO_POST_RERUN_BROKER_RESPONSE_REVIEW_FILE"
EXECUTION_ATTRIBUTION_HYGIENE_DB_ENV = "AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_DB_PATH"


def run_git(args: list[str]) -> str:
    return subprocess.check_output(["git", "-C", str(ROOT), *args], text=True).strip()


def parse_name_status(raw: str) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        status = parts[0]
        path = parts[-1]
        rows.append((status, path))
    return rows


def changed_entries(base: str | None, head: str | None, all_files: bool) -> list[tuple[str, str]]:
    if all_files:
        tracked = run_git(["ls-files"])
        return [("T", path) for path in tracked.splitlines() if path.strip()]

    if base and head:
        raw = run_git(["diff", "--name-status", f"{base}...{head}"])
        return parse_name_status(raw)

    raw = run_git(["diff", "--cached", "--name-status"])
    rows = parse_name_status(raw)
    if rows:
        return rows

    raw = run_git(["diff", "--name-status", "HEAD"])
    rows = parse_name_status(raw)
    if rows:
        return rows

    tracked = run_git(["ls-files"])
    return [("T", path) for path in tracked.splitlines() if path.strip()]


def is_root_level(path: str) -> bool:
    return "/" not in path


def is_added_status(status: str) -> bool:
    return status.startswith("A") or status.startswith("C") or status.startswith("R")


def line_count(path: Path) -> int:
    try:
        return sum(1 for _ in path.open("r", encoding="utf-8"))
    except Exception:
        return -1


def has_prefix(paths: Iterable[str], prefix: str) -> bool:
    return any(path.startswith(prefix) for path in paths)


def has_docs(paths: Iterable[str]) -> bool:
    return any(path.startswith("docs/") and path.endswith(".md") for path in paths)


def path_matches_rule(path: str, rule: str) -> bool:
    return path.startswith(rule) if rule.endswith("/") else path == rule


def has_rule_match(paths: Iterable[str], rules: Iterable[str]) -> bool:
    return any(path_matches_rule(path, rule) for path in paths for rule in rules)


def _strategy_optimization_scope_changed(paths: Iterable[str]) -> bool:
    return any(path.startswith(prefix) for path in paths for prefix in STRATEGY_OPTIMIZATION_SENSITIVE_PATH_PREFIXES)


def _execution_attribution_scope_changed(paths: Iterable[str]) -> bool:
    return any(path.startswith(prefix) for path in paths for prefix in EXECUTION_ATTRIBUTION_SENSITIVE_PATH_PREFIXES)


def _load_json_file(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("payload_not_object")
    return payload


def _stable_hash(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _build_benchmark_signature(core_without_signature: dict, signing_secret: str) -> str:
    body = _stable_hash(core_without_signature)
    return hashlib.sha256(f"{body}|{str(signing_secret or '').strip()}".encode("utf-8")).hexdigest()


def _parse_csv_env(value: str) -> set[str]:
    return {item.strip() for item in str(value or "").split(",") if item.strip()}


def _int_with_bounds(value: object, *, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(min_value, min(max_value, parsed))


def _severity_rank(level: str) -> int:
    order = {"low": 10, "medium": 20, "high": 30, "critical": 40}
    return order.get(str(level or "").strip().lower(), 30)


def _emit_benchmark_hook_alert(*, severity: str, reason: str, detail: str) -> None:
    threshold = str(os.getenv(BENCHMARK_CONTRACT_VERIFY_HOOK_ALERT_MIN_LEVEL_ENV, "high") or "high").strip().lower()
    sev = str(severity or "high").strip().lower()
    if _severity_rank(sev) < _severity_rank(threshold):
        return
    payload = {
        "event": "benchmark_contract_hook_verification",
        "severity": sev,
        "reason": str(reason or "").strip(),
        "detail": str(detail or "").strip(),
    }
    try:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True), file=sys.stderr)
    except Exception:
        pass


def _hook_failure_should_block(policy: str, severity: str) -> bool:
    normalized_policy = str(policy or "block").strip().lower()
    if normalized_policy not in {"block", "warn"}:
        return True
    if normalized_policy == "block":
        return True
    return _severity_rank(severity) >= _severity_rank("high")


def _load_benchmark_keyring(path_text: str) -> dict:
    path = Path(str(path_text or "").strip())
    if not str(path_text or "").strip() or not path.exists():
        return {}
    payload = _load_json_file(path)
    if not isinstance(payload, dict):
        return {"invalid": "payload_not_object"}
    keys = payload.get("keys")
    if not isinstance(keys, dict):
        return {"invalid": "keys_not_object"}
    return payload


def _verify_with_hook(command_text: str, request: dict, *, timeout_seconds: int = 15) -> tuple[bool, str, str]:
    command = str(command_text or "").strip()
    if not command:
        return False, "missing_verify_hook_command", "critical"
    timeout = _int_with_bounds(timeout_seconds, default=15, min_value=1, max_value=120)
    try:
        result = subprocess.run(
            shlex.split(command),
            input=json.dumps(request, ensure_ascii=False, sort_keys=True),
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, "verify_hook_timeout", "critical"
    except Exception as exc:
        return False, f"verify_hook_execution_error:{exc}", "critical"
    if result.returncode != 0:
        return False, f"verify_hook_nonzero_exit:{result.returncode}", "critical"
    body = str(result.stdout or "").strip()
    if not body:
        return False, "verify_hook_empty_output", "critical"
    try:
        payload = json.loads(body)
    except Exception:
        return False, "verify_hook_invalid_json_output", "critical"
    if not isinstance(payload, dict):
        return False, "verify_hook_payload_not_object", "critical"
    version = str(payload.get("protocol_version") or "").strip()
    if version != "benchmark_verify_response.v1":
        return False, f"verify_hook_protocol_version_invalid:{version or 'missing'}", "critical"
    if payload.get("verified") is True:
        return True, "", "low"
    reason = str(payload.get("reason") or "verify_hook_reported_unverified")
    severity = str(payload.get("severity") or "high").strip().lower()
    if severity not in {"low", "medium", "high", "critical"}:
        severity = "high"
    return False, reason, severity


def _compact_date(value: object) -> str:
    return str(value or "").strip().replace("-", "")


def _date_distance_days(left: str, right: str) -> int | None:
    left_compact = _compact_date(left)
    right_compact = _compact_date(right)
    if len(left_compact) != 8 or len(right_compact) != 8:
        return None
    try:
        left_dt = datetime.strptime(left_compact, "%Y%m%d")
        right_dt = datetime.strptime(right_compact, "%Y%m%d")
    except ValueError:
        return None
    return abs((left_dt - right_dt).days)


def _require_existing_file(value: str, field: str, failures: list[str]) -> Path | None:
    raw = str(value or "").strip()
    if not raw:
        failures.append(f"strategy optimization evidence missing field: {field}")
        return None
    path = Path(raw)
    if not path.exists():
        failures.append(f"strategy optimization evidence file missing: {field}={raw}")
        return None
    return path


def validate_strategy_pr_evidence_file() -> list[str]:
    evidence_path = os.getenv(STRATEGY_PR_EVIDENCE_ENV, "").strip()
    if not evidence_path:
        return [f"strategy optimization PR requires {STRATEGY_PR_EVIDENCE_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"strategy optimization evidence manifest missing: {evidence_path}"]

    failures: list[str] = []
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"strategy optimization evidence manifest invalid: {exc}"]

    sweep_path = _require_existing_file(payload.get("backtest_sweep_artifact", ""), "backtest_sweep_artifact", failures)
    stage_audit_json = _require_existing_file(payload.get("stage_audit_json", ""), "stage_audit_json", failures)
    _require_existing_file(payload.get("stage_audit_markdown", ""), "stage_audit_markdown", failures)
    _require_existing_file(payload.get("rejected_artifacts_ledger", ""), "rejected_artifacts_ledger", failures)

    test_results = payload.get("gate_test_results")
    if not isinstance(test_results, list) or not test_results:
        failures.append("strategy optimization evidence missing gate_test_results")
    else:
        for idx, item in enumerate(test_results):
            if not isinstance(item, dict):
                failures.append(f"strategy optimization gate_test_results[{idx}] invalid")
                continue
            if not str(item.get("name") or "").strip():
                failures.append(f"strategy optimization gate_test_results[{idx}] missing name")
            _require_existing_file(str(item.get("path") or ""), f"gate_test_results[{idx}].path", failures)

    if sweep_path is not None:
        try:
            sweep = _load_json_file(sweep_path)
        except Exception as exc:
            failures.append(f"backtest sweep artifact invalid: {exc}")
        else:
            if not isinstance(sweep.get("backtest_credibility"), dict):
                failures.append("backtest sweep artifact missing backtest_credibility")
            if not isinstance(sweep.get("strategy_backtest_diagnostics"), dict):
                failures.append("backtest sweep artifact missing strategy_backtest_diagnostics")

    if stage_audit_json is not None:
        try:
            audit = _load_json_file(stage_audit_json)
        except Exception as exc:
            failures.append(f"strategy optimization stage audit artifact invalid: {exc}")
        else:
            if str(audit.get("audit_version") or "") != "strategy_optimization_stage_audit.v1":
                failures.append("strategy optimization stage audit artifact missing audit_version")
            if "passed" not in audit:
                failures.append("strategy optimization stage audit artifact missing passed field")

    return failures


def run_strategy_optimization_stage_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    failures: list[str] = []
    enabled = os.getenv("AIRIVO_ENABLE_STRATEGY_OPTIMIZATION_STAGE_GATE", "").strip() == "1"
    if not enabled and not _strategy_optimization_scope_changed(changed_paths):
        return failures

    db_path = os.getenv("AIRIVO_STRATEGY_OPTIMIZATION_DB_PATH", "").strip()
    if not db_path:
        return ["strategy optimization stage gate enabled without AIRIVO_STRATEGY_OPTIMIZATION_DB_PATH"]
    rejected = os.getenv("AIRIVO_REJECTED_BACKTEST_ARTIFACTS_FILE", "").strip()
    if not rejected:
        failures.append("strategy optimization stage gate requires AIRIVO_REJECTED_BACKTEST_ARTIFACTS_FILE")
    failures.extend(validate_strategy_pr_evidence_file())
    if failures:
        return failures

    cmd = [
        sys.executable,
        str(ROOT / STRATEGY_OPTIMIZATION_STAGE_AUDIT_TOOL),
        "--db-path",
        db_path,
        "--output-dir",
        os.getenv("AIRIVO_STRATEGY_OPTIMIZATION_AUDIT_DIR", "logs/openclaw").strip() or "logs/openclaw",
    ]
    trade_date = os.getenv("AIRIVO_STRATEGY_OPTIMIZATION_TRADE_DATE", "").strip()
    if trade_date:
        cmd.extend(["--trade-date", trade_date])
    cmd.extend(["--rejected-artifacts", rejected])
    result = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        detail = (result.stdout + "\n" + result.stderr).strip()
        failures.append("strategy optimization stage audit failed" + (f": {detail}" if detail else ""))
    return failures


def run_execution_attribution_hygiene_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    failures: list[str] = []
    enabled = os.getenv("AIRIVO_ENABLE_EXECUTION_ATTRIBUTION_HYGIENE_GATE", "").strip() == "1"
    if not enabled and not _execution_attribution_scope_changed(changed_paths):
        return failures

    db_path = (
        os.getenv(EXECUTION_ATTRIBUTION_HYGIENE_DB_ENV, "").strip()
        or os.getenv("AIRIVO_STRATEGY_OPTIMIZATION_DB_PATH", "").strip()
    )
    if not db_path:
        return [f"execution attribution hygiene gate requires {EXECUTION_ATTRIBUTION_HYGIENE_DB_ENV} or AIRIVO_STRATEGY_OPTIMIZATION_DB_PATH"]

    cmd = [
        sys.executable,
        str(ROOT / EXECUTION_ATTRIBUTION_BACKFILL_TOOL),
        "--db-path",
        db_path,
        "--statuses",
        os.getenv("AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_STATUSES", "created,submitted").strip() or "created,submitted",
        "--stale-minutes",
        os.getenv("AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_STALE_MINUTES", "30").strip() or "30",
        "--max-orders",
        os.getenv("AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_MAX_ORDERS", "500").strip() or "500",
        "--output-dir",
        os.getenv("AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_AUDIT_DIR", "logs/openclaw").strip() or "logs/openclaw",
        "--json",
    ]
    result = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        detail = (result.stdout + "\n" + result.stderr).strip()
        failures.append("execution attribution hygiene audit failed" + (f": {detail}" if detail else ""))
        return failures

    try:
        payload = json.loads(result.stdout or "{}")
    except Exception:
        detail = (result.stdout + "\n" + result.stderr).strip()
        failures.append("execution attribution hygiene audit output invalid json" + (f": {detail}" if detail else ""))
        return failures

    patched_count = int(payload.get("patched_count", 0) or 0)
    if patched_count > 0:
        artifact = str(payload.get("artifact_path") or "")
        failures.append(
            "execution attribution hygiene found stale missing attribution rows: "
            f"patched_count={patched_count}" + (f" artifact={artifact}" if artifact else "")
        )
    return failures


def validate_repair_flow_evidence_file() -> list[str]:
    evidence_path = os.getenv(REPAIR_FLOW_EVIDENCE_ENV, "").strip()
    if not evidence_path:
        return [f"repair flow PR requires {REPAIR_FLOW_EVIDENCE_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"repair flow evidence manifest missing: {evidence_path}"]
    failures: list[str] = []
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"repair flow evidence manifest invalid: {exc}"]

    flow_path = _require_existing_file(payload.get("flow_snapshot_artifact", ""), "flow_snapshot_artifact", failures)
    watch_review_path = Path(str(payload.get("watch_risk_review_artifact") or "")) if str(payload.get("watch_risk_review_artifact") or "").strip() else None
    oos_plan_path = Path(str(payload.get("oos_monitoring_plan_artifact") or "")) if str(payload.get("oos_monitoring_plan_artifact") or "").strip() else None
    oos_result_path = Path(str(payload.get("oos_monitoring_result_artifact") or "")) if str(payload.get("oos_monitoring_result_artifact") or "").strip() else None
    archive_path = Path(str(payload.get("failed_research_candidate_archive_artifact") or "")) if str(payload.get("failed_research_candidate_archive_artifact") or "").strip() else None

    flow = _load_if_present(flow_path, "flow_snapshot_artifact", failures)
    if str(flow.get("current_status") or flow.get("status") or "") == "repair_attempt_predeclared":
        oos_plan = _load_if_present(oos_plan_path, "oos_monitoring_plan_artifact", failures)
        _validate_predeclared_repair_flow(flow, oos_plan=oos_plan, failures=failures)
        for label, artifact in (("flow_snapshot_artifact", flow), ("oos_monitoring_plan_artifact", oos_plan)):
            if artifact.get("formal_candidate_allowed") is True or artifact.get("formal_ranking_allowed") is True:
                failures.append(f"{label} attempted formal eligibility")
            if artifact.get("production_candidate_allowed") is True:
                failures.append(f"{label} attempted production eligibility")
        return failures

    unthrottled_path = _require_existing_file(payload.get("unthrottled_benchmark_artifact", ""), "unthrottled_benchmark_artifact", failures)
    throttled_path = _require_existing_file(payload.get("throttled_benchmark_artifact", ""), "throttled_benchmark_artifact", failures)
    repair_review_path = _require_existing_file(payload.get("repair_review_artifact", ""), "repair_review_artifact", failures)
    _require_existing_file(payload.get("watch_risk_register_artifact", ""), "watch_risk_register_artifact", failures)
    unthrottled = _load_if_present(unthrottled_path, "unthrottled_benchmark_artifact", failures)
    throttled = _load_if_present(throttled_path, "throttled_benchmark_artifact", failures)
    review = _load_if_present(repair_review_path, "repair_review_artifact", failures)
    watch_review = _load_if_present(watch_review_path, "watch_risk_review_artifact", failures)
    oos_plan = _load_if_present(oos_plan_path, "oos_monitoring_plan_artifact", failures)
    oos_result = _load_if_present(oos_result_path, "oos_monitoring_result_artifact", failures)
    archive = _load_if_present(archive_path, "failed_research_candidate_archive_artifact", failures)
    for label, artifact in (
        ("flow_snapshot_artifact", flow),
        ("unthrottled_benchmark_artifact", unthrottled),
        ("throttled_benchmark_artifact", throttled),
        ("repair_review_artifact", review),
        ("watch_risk_review_artifact", watch_review),
        ("oos_monitoring_plan_artifact", oos_plan),
        ("oos_monitoring_result_artifact", oos_result),
        ("failed_research_candidate_archive_artifact", archive),
    ):
        if artifact.get("formal_candidate_allowed") is True or artifact.get("formal_ranking_allowed") is True:
            failures.append(f"{label} attempted formal eligibility")
        if artifact.get("production_candidate_allowed") is True:
            failures.append(f"{label} attempted production eligibility")

    if flow:
        status = str(flow.get("current_status") or flow.get("status") or "")
        if status not in {"repair_review_blocked", "observation_watch_discussion_allowed"}:
            failures.append(f"repair flow status not allowed for PR evidence: {status}")
        summary = flow.get("unthrottled_summary_json") if isinstance(flow.get("unthrottled_summary_json"), dict) else {}
        if status == "observation_watch_discussion_allowed" and not summary:
            failures.append("repair flow missing unthrottled summary")
        prohibited = json.dumps(flow.get("prohibited_actions_json") or flow.get("prohibited_actions") or [], ensure_ascii=False)
        for required in ("formal", "top", "production"):
            if required not in prohibited:
                failures.append(f"repair flow missing prohibited action: {required}")
        if not flow.get("watch_risks"):
            failures.append("repair flow missing watch risk register")
        block_risks = [str(item.get("risk_code") or "") for item in flow.get("watch_risks") or [] if isinstance(item, dict) and str(item.get("risk_level") or "") == "block"]
        next_actions = json.dumps(flow.get("next_allowed_actions_json") or flow.get("next_allowed_actions") or [], ensure_ascii=False)
        if block_risks and "observation_watch_discussion" in next_actions:
            failures.append(f"repair flow block-level watch risk cannot allow observation discussion: {','.join(block_risks)}")
        if block_risks and not watch_review_path:
            failures.append("repair flow block-level watch risk requires watch_risk_review_artifact")
        if block_risks and not oos_plan_path:
            failures.append("repair flow block-level watch risk requires oos_monitoring_plan_artifact")

    if unthrottled and throttled:
        benchmark = unthrottled.get("benchmark") if isinstance(unthrottled.get("benchmark"), dict) else {}
        flow_status = str(flow.get("current_status") or flow.get("status") or "")
        if flow_status == "observation_watch_discussion_allowed" and benchmark.get("passed") is not True:
            failures.append("repair unthrottled benchmark not passed")
        u_windows = [str(row.get("as_of_date") or "") for row in unthrottled.get("windows") or [] if isinstance(row, dict)]
        t_windows = [str(row.get("as_of_date") or "") for row in throttled.get("windows") or [] if isinstance(row, dict)]
        if u_windows != t_windows:
            failures.append("repair paired benchmark window set mismatch")
        u_hash = str((unthrottled.get("rule_freeze") or {}).get("rule_hash") or "")
        t_hash = str((throttled.get("rule_freeze") or {}).get("rule_hash") or "")
        expected = str(payload.get("rule_hash") or flow.get("rule_hash") or "")
        if not u_hash or u_hash != t_hash or (expected and expected != u_hash):
            failures.append("repair paired benchmark rule hash mismatch")
        if oos_plan:
            oos_hash = str(oos_plan.get("rule_hash") or "")
            if expected and oos_hash != expected:
                failures.append("repair OOS monitoring plan rule hash mismatch")
            fixed = set(u_windows)
            oos_windows = [str(item) for item in oos_plan.get("oos_windows") or []]
            overlap = sorted(fixed.intersection(oos_windows))
            if overlap:
                failures.append(f"repair OOS monitoring windows overlap repair windows: {','.join(overlap)}")
            paired = oos_plan.get("paired_run_requirement") if isinstance(oos_plan.get("paired_run_requirement"), dict) else {}
            if paired.get("unthrottled_required") is not True or paired.get("throttled_required") is not True:
                failures.append("repair OOS monitoring plan missing paired unthrottled/throttled requirement")
            pass_conditions = oos_plan.get("pass_conditions") if isinstance(oos_plan.get("pass_conditions"), dict) else {}
            if pass_conditions.get("block_level_watch_risk_allowed") is not False:
                failures.append("repair OOS monitoring plan allows block-level watch risk")
    if watch_review:
        status = str(watch_review.get("review_status") or "")
        if status not in {"watch_risk_review_blocked", "watch_risk_review_cleared_for_oos_only"}:
            failures.append(f"repair watch risk review status invalid: {status}")
    if oos_result:
        status = str(oos_result.get("result_status") or "")
        if status not in {"oos_monitoring_failed_blocked", "oos_monitoring_passed_discussion_only"}:
            failures.append(f"repair OOS monitoring result status invalid: {status}")
        if status == "oos_monitoring_passed_discussion_only":
            allowed = str(oos_result.get("allowed_next_status") or "")
            if allowed != "observation_watch_discussion_allowed":
                failures.append("repair OOS passed result can only allow observation_watch_discussion_allowed")
        if str(oos_result.get("allowed_next_status") or "") in {"formal_review_candidate", "observation_watch_active", "production"}:
            failures.append("repair OOS result attempted promotion status")
    if archive:
        if str(archive.get("archive_status") or "") != "failed_research_candidate_archived":
            failures.append("failed research candidate archive status invalid")
        if archive.get("observation_watch_allowed") is not False:
            failures.append("failed research candidate archive allowed observation watch")
        if not archive.get("reopen_conditions"):
            failures.append("failed research candidate archive missing reopen conditions")
        prohibited = json.dumps(archive.get("prohibited_actions") or [], ensure_ascii=False)
        for required in ("formal", "top", "production", "posthoc_parameter_search"):
            if required not in prohibited:
                failures.append(f"failed research candidate archive missing prohibited action: {required}")
    return failures


def validate_competition_audit_evidence_file() -> list[str]:
    evidence_path = os.getenv(COMPETITION_AUDIT_EVIDENCE_ENV, "").strip()
    if not evidence_path:
        return [f"strategy competition audit requires {COMPETITION_AUDIT_EVIDENCE_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"strategy competition audit evidence missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"strategy competition audit evidence invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_portfolio_audit.v1":
        failures.append("strategy competition audit artifact_version invalid")
    result_status = str(payload.get("result_status") or "")
    passed_artifact = result_status == "industry_benchmark_competition_passed"
    blocked_artifact = result_status == "industry_benchmark_competition_blocked"
    if passed_artifact and payload.get("passed") is not True:
        failures.append("strategy competition audit passed status inconsistent")
    if not passed_artifact and not blocked_artifact:
        failures.append(f"strategy competition audit result_status invalid: {result_status}")
    if blocked_artifact:
        if payload.get("passed") is True:
            failures.append("strategy competition audit blocked status cannot be passed")
        if payload.get("production_candidate_allowed") is True or payload.get("formal_top_allowed") is True:
            failures.append("strategy competition audit blocked artifact allowed formal/production")
        if not payload.get("blocking_reasons"):
            failures.append("strategy competition audit blocked artifact missing blocking reasons")
    if not payload.get("fixed_candidate_pool"):
        failures.append("strategy competition audit missing fixed candidate pool")
    ranking = payload.get("ranking_contract") if isinstance(payload.get("ranking_contract"), dict) else {}
    if ranking.get("no_posthoc_candidate_addition") is not True:
        failures.append("strategy competition audit allows posthoc candidate addition")
    if ranking.get("failed_or_research_only_candidate_banned") is not True:
        failures.append("strategy competition audit missing failed/research-only ban")
    if not str(payload.get("ranking_method_hash") or "").strip():
        failures.append("strategy competition audit missing ranking method hash")

    pool = [str(item or "").lower() for item in payload.get("fixed_candidate_pool") or []]
    cards = payload.get("alpha_model_cards") if isinstance(payload.get("alpha_model_cards"), dict) else {}
    for strategy in pool:
        card = cards.get(strategy) if isinstance(cards.get(strategy), dict) else {}
        for field in ("model_card", "hypothesis", "rule_hash", "data_hash", "code_hash"):
            if not card or not str(card.get(field) or "").strip():
                failures.append(f"strategy competition audit missing {field}: {strategy}")
        if str(card.get("status") or "").lower() in {"failed", "research_only", "archived"}:
            failures.append(f"strategy competition audit includes failed/research-only candidate: {strategy}")

    top5 = [item for item in payload.get("top5_portfolio_audit") or [] if isinstance(item, dict)]
    if len(top5) != 5:
        failures.append(f"strategy competition audit top5 count invalid: {len(top5)}")
    top5_codes: list[str] = []
    for item in top5:
        ts_code = str(item.get("ts_code") or "")
        if not ts_code:
            failures.append("strategy competition audit top5 item missing ts_code")
        else:
            top5_codes.append(ts_code)
        if "weight" not in item:
            failures.append(f"strategy competition audit top5 missing weight: {ts_code}")
        source = item.get("source") if isinstance(item.get("source"), dict) else {}
        risk = item.get("risk") if isinstance(item.get("risk"), dict) else {}
        cost = item.get("cost") if isinstance(item.get("cost"), dict) else {}
        checks = item.get("constraint_checks") if isinstance(item.get("constraint_checks"), dict) else {}
        if not source.get("signal_refs"):
            failures.append(f"strategy competition audit top5 missing signal refs: {ts_code}")
        if not risk or not str(risk.get("industry") or ""):
            failures.append(f"strategy competition audit top5 missing risk exposure: {ts_code}")
        if "estimated_cost_bps" not in cost:
            failures.append(f"strategy competition audit top5 missing cost estimate: {ts_code}")
        if not checks or any(value is not True for value in checks.values()):
            failures.append(f"strategy competition audit top5 constraints not passed: {ts_code}")

    risk_summary = payload.get("risk_summary") if isinstance(payload.get("risk_summary"), dict) else {}
    risk_budget = risk_summary.get("risk_budget") if isinstance(risk_summary.get("risk_budget"), dict) else {}
    max_single_risk = float(risk_budget.get("max_single_risk_contribution") or 0.0)
    if max_single_risk <= 0:
        failures.append("strategy competition audit risk budget missing max_single_risk_contribution")
    risk_share_map = (
        risk_budget.get("risk_contribution_share_by_code")
        if isinstance(risk_budget.get("risk_contribution_share_by_code"), dict)
        else {}
    )
    if not risk_share_map:
        failures.append("strategy competition audit risk budget missing risk contribution share map")
    if not str(risk_budget.get("risk_contribution_model") or "").strip():
        failures.append("strategy competition audit risk budget missing covariance model")
    if "base_shrinkage" not in risk_budget:
        failures.append("strategy competition audit risk budget missing base shrinkage")
    factor_summary = (
        risk_budget.get("factor_exposure_summary")
        if isinstance(risk_budget.get("factor_exposure_summary"), dict)
        else {}
    )
    if not factor_summary:
        failures.append("strategy competition audit risk budget missing factor exposure summary")
    for factor_name in ("size", "liquidity"):
        factor_payload = factor_summary.get(factor_name) if isinstance(factor_summary.get(factor_name), dict) else {}
        if not factor_payload:
            failures.append(f"strategy competition audit factor exposure missing: {factor_name}")
            continue
        if "portfolio_exposure" not in factor_payload or "cap" not in factor_payload:
            failures.append(f"strategy competition audit factor exposure fields missing: {factor_name}")
        if factor_payload.get("within_limit") is not True:
            failures.append(f"strategy competition audit factor exposure above cap: {factor_name}")
    for ts_code in top5_codes:
        if ts_code not in risk_share_map:
            failures.append(f"strategy competition audit risk budget missing symbol share: {ts_code}")
            continue
        share_value = float(risk_share_map.get(ts_code) or 0.0)
        if max_single_risk > 0 and share_value > max_single_risk + 1e-9:
            failures.append(
                "strategy competition audit risk contribution exceeds limit: "
                f"{ts_code}:{share_value}/{max_single_risk}"
            )

    benchmark_contract = payload.get("benchmark_contract") if isinstance(payload.get("benchmark_contract"), dict) else {}
    if not benchmark_contract:
        failures.append("strategy competition audit missing benchmark contract")
    else:
        if str(benchmark_contract.get("artifact_version") or "") != "benchmark_industry_contract.v1":
            failures.append("strategy competition audit benchmark contract version invalid")
        if not str(benchmark_contract.get("source") or "").strip():
            failures.append("strategy competition audit benchmark contract missing source")
        if not str(benchmark_contract.get("benchmark_trade_date") or "").strip():
            failures.append("strategy competition audit benchmark contract missing trade date")
        provider_batch_id = str(benchmark_contract.get("provider_batch_id") or "")
        provider_snapshot_id = str(benchmark_contract.get("provider_snapshot_id") or "")
        approved_by = str(benchmark_contract.get("approved_by") or "")
        approved_at = str(benchmark_contract.get("approved_at") or "")
        approval_signature = str(benchmark_contract.get("approval_signature") or "")
        approval_signature_algo = str(benchmark_contract.get("approval_signature_algo") or "")
        approval_key_id = str(benchmark_contract.get("approval_key_id") or "")
        provider_receipt_hash = str(benchmark_contract.get("provider_receipt_hash") or "")
        if passed_artifact and not provider_batch_id:
            failures.append("strategy competition audit benchmark contract missing provider batch id")
        if passed_artifact and not provider_snapshot_id:
            failures.append("strategy competition audit benchmark contract missing provider snapshot id")
        if passed_artifact and not approved_by:
            failures.append("strategy competition audit benchmark contract missing approved_by")
        if passed_artifact and not approved_at:
            failures.append("strategy competition audit benchmark contract missing approved_at")
        if passed_artifact and not approval_signature:
            failures.append("strategy competition audit benchmark contract missing approval signature")
        if passed_artifact and not approval_signature_algo:
            failures.append("strategy competition audit benchmark contract missing approval signature algo")
        if passed_artifact and not approval_key_id:
            failures.append("strategy competition audit benchmark contract missing approval key id")
        if passed_artifact and not provider_receipt_hash:
            failures.append("strategy competition audit benchmark contract missing provider receipt hash")
        allowed_algos = _parse_csv_env(
            os.getenv(BENCHMARK_CONTRACT_ALLOWED_ALGOS_ENV, "sha256_secret_v1,pkcs1_sha256_detached_v1,kms_hmac_sha256_v1")
        )
        active_keys = _parse_csv_env(os.getenv(BENCHMARK_CONTRACT_ACTIVE_KEY_IDS_ENV, ""))
        revoked_keys = _parse_csv_env(os.getenv(BENCHMARK_CONTRACT_REVOKED_KEY_IDS_ENV, ""))
        keyring_payload = _load_benchmark_keyring(os.getenv(BENCHMARK_CONTRACT_KEYRING_FILE_ENV, ""))
        keyring_keys = keyring_payload.get("keys") if isinstance(keyring_payload.get("keys"), dict) else {}
        if passed_artifact and keyring_payload.get("invalid"):
            failures.append(f"strategy competition audit benchmark keyring invalid: {keyring_payload.get('invalid')}")
        if passed_artifact and keyring_payload:
            schema_version = str(keyring_payload.get("schema_version") or "")
            if schema_version and schema_version != "benchmark_contract_keyring.v1":
                failures.append(f"strategy competition audit benchmark keyring schema invalid: {schema_version}")
        if passed_artifact and approval_signature_algo and approval_signature_algo not in allowed_algos:
            failures.append(f"strategy competition audit benchmark contract signature algo not allowed: {approval_signature_algo}")
        if passed_artifact and approval_key_id and approval_key_id in revoked_keys:
            failures.append(f"strategy competition audit benchmark contract key revoked: {approval_key_id}")
        if passed_artifact and active_keys and approval_key_id and approval_key_id not in active_keys:
            failures.append(f"strategy competition audit benchmark contract key not active: {approval_key_id}")
        key_meta = {}
        if passed_artifact and keyring_keys:
            key_meta = keyring_keys.get(approval_key_id) if isinstance(keyring_keys.get(approval_key_id), dict) else {}
            if not key_meta:
                failures.append(f"strategy competition audit benchmark contract key missing from keyring: {approval_key_id}")
            else:
                key_status = str(key_meta.get("status") or "")
                if key_status == "revoked":
                    failures.append(f"strategy competition audit benchmark contract key revoked in keyring: {approval_key_id}")
                allowed_by_key = key_meta.get("allowed_algos") if isinstance(key_meta.get("allowed_algos"), list) else []
                if allowed_by_key and approval_signature_algo not in {str(item) for item in allowed_by_key}:
                    failures.append(
                        "strategy competition audit benchmark contract signature algo not allowed by keyring: "
                        f"{approval_signature_algo}:{approval_key_id}"
                    )
                verify_mode = str(key_meta.get("verify_mode") or "")
                if verify_mode and verify_mode not in {"secret_env", "hook"}:
                    failures.append(
                        "strategy competition audit benchmark contract key verify mode invalid: "
                        f"{verify_mode}:{approval_key_id}"
                    )
        industry_weights = (
            benchmark_contract.get("industry_weights")
            if isinstance(benchmark_contract.get("industry_weights"), dict)
            else {}
        )
        if not industry_weights:
            failures.append("strategy competition audit benchmark contract missing industry weights")
        constituents = (
            benchmark_contract.get("constituents")
            if isinstance(benchmark_contract.get("constituents"), list)
            else []
        )
        if not constituents:
            failures.append("strategy competition audit benchmark contract missing constituents")
        industry_weights_hash = str(benchmark_contract.get("industry_weights_hash") or "")
        if not industry_weights_hash:
            failures.append("strategy competition audit benchmark contract missing industry weights hash")
        elif industry_weights and _stable_hash(industry_weights) != industry_weights_hash:
            failures.append("strategy competition audit benchmark contract industry weights hash mismatch")
        constituent_hash = str(benchmark_contract.get("constituent_hash") or "")
        if not constituent_hash:
            failures.append("strategy competition audit benchmark contract missing constituent hash")
        elif constituents and _stable_hash(constituents) != constituent_hash:
            failures.append("strategy competition audit benchmark contract constituent hash mismatch")
        contract_hash = str(benchmark_contract.get("contract_hash") or "")
        if not contract_hash:
            failures.append("strategy competition audit benchmark contract missing contract hash")
        else:
            core = {
                key: benchmark_contract.get(key)
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
            if _stable_hash(core) != contract_hash:
                failures.append("strategy competition audit benchmark contract hash mismatch")
        core_without_signature = {
            key: benchmark_contract.get(key)
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
        signing_secret = os.getenv(BENCHMARK_CONTRACT_SIGNING_SECRET_ENV, "").strip()
        constraints = payload.get("portfolio_constraints") if isinstance(payload.get("portfolio_constraints"), dict) else {}
        hook_timeout_seconds = _int_with_bounds(
            (constraints or {}).get(
                "benchmark_hook_timeout_seconds",
                os.getenv(BENCHMARK_CONTRACT_VERIFY_HOOK_TIMEOUT_SECONDS_ENV, "15"),
            ),
            default=15,
            min_value=1,
            max_value=120,
        )
        hook_failure_policy = str(
            (constraints or {}).get(
                "benchmark_hook_failure_policy",
                os.getenv(BENCHMARK_CONTRACT_VERIFY_HOOK_FAILURE_POLICY_ENV, "block"),
            )
            or "block"
        ).strip().lower()
        if hook_failure_policy not in {"block", "warn"}:
            failures.append(
                "strategy competition audit benchmark hook failure policy invalid: "
                f"{hook_failure_policy}"
            )
            hook_failure_policy = "block"
        verify_mode = str((key_meta or {}).get("verify_mode") or ("secret_env" if approval_signature_algo == "sha256_secret_v1" else "hook"))
        if verify_mode == "secret_env" and approval_signature_algo == "sha256_secret_v1":
            if signing_secret:
                expected_sig = _build_benchmark_signature(core_without_signature, signing_secret)
                if approval_signature != expected_sig:
                    failures.append("strategy competition audit benchmark contract signature verification failed")
        elif verify_mode == "hook":
            hook_command = str((key_meta or {}).get("verify_hook_command") or os.getenv(BENCHMARK_CONTRACT_VERIFY_HOOK_ENV, "")).strip()
            if passed_artifact and not hook_command:
                failures.append("strategy competition audit benchmark contract verify hook missing")
            elif hook_command:
                verified, reason, severity = _verify_with_hook(
                    hook_command,
                    {
                        "protocol_version": "benchmark_verify_request.v1",
                        "request_type": "benchmark_contract_signature_verification",
                        "key_id": approval_key_id,
                        "algo": approval_signature_algo,
                        "signature": approval_signature,
                        "payload": core_without_signature,
                        "public_key_pem": str((key_meta or {}).get("public_key_pem") or ""),
                        "context": {
                            "artifact_version": str(payload.get("artifact_version") or ""),
                            "trade_date": str(payload.get("trade_date") or ""),
                            "benchmark_trade_date": str(benchmark_contract.get("benchmark_trade_date") or ""),
                        },
                    },
                    timeout_seconds=hook_timeout_seconds,
                )
                if not verified:
                    _emit_benchmark_hook_alert(
                        severity=severity,
                        reason=str(reason or ""),
                        detail=f"policy={hook_failure_policy};key={approval_key_id};algo={approval_signature_algo}",
                    )
                    if _hook_failure_should_block(hook_failure_policy, severity):
                        failures.append(
                            "strategy competition audit benchmark contract hook verification failed: "
                            f"{reason} (severity={severity})"
                        )
        max_staleness_days = 3
        if isinstance(constraints, dict):
            max_staleness_days = int(constraints.get("benchmark_max_staleness_days") or max_staleness_days)
        trade_date = str(payload.get("trade_date") or "")
        benchmark_trade_date = str(benchmark_contract.get("benchmark_trade_date") or "")
        distance = _date_distance_days(trade_date, benchmark_trade_date)
        if passed_artifact and distance is not None and distance > max_staleness_days:
            failures.append(
                "strategy competition audit benchmark contract stale: "
                f"{distance}d>{max_staleness_days}d"
            )
        sidecars = payload.get("source_sidecars") if isinstance(payload.get("source_sidecars"), dict) else {}
        benchmark_sidecar = str(sidecars.get("benchmark_contract") or "").strip()
        if passed_artifact and not benchmark_sidecar:
            failures.append("strategy competition audit missing benchmark contract sidecar reference")
        if benchmark_sidecar:
            sidecar_path = Path(benchmark_sidecar)
            if not sidecar_path.exists():
                failures.append("strategy competition audit benchmark contract sidecar missing")
            else:
                try:
                    sidecar_payload = _load_json_file(sidecar_path)
                except Exception:
                    failures.append("strategy competition audit benchmark contract sidecar invalid")
                else:
                    sidecar_hash = str(sidecar_payload.get("contract_hash") or "")
                    artifact_hash = str(benchmark_contract.get("contract_hash") or "")
                    if not sidecar_hash or not artifact_hash or sidecar_hash != artifact_hash:
                        failures.append("strategy competition audit benchmark contract sidecar hash mismatch")
        receipt_sidecar = str(sidecars.get("benchmark_provider_receipt") or "").strip()
        if passed_artifact and not receipt_sidecar:
            failures.append("strategy competition audit missing benchmark provider receipt sidecar reference")
        if receipt_sidecar:
            receipt_path = Path(receipt_sidecar)
            if not receipt_path.exists():
                failures.append("strategy competition audit benchmark provider receipt sidecar missing")
            else:
                try:
                    receipt_payload = json.loads(receipt_path.read_text(encoding="utf-8"))
                except Exception:
                    failures.append("strategy competition audit benchmark provider receipt sidecar invalid")
                else:
                    if _stable_hash(receipt_payload) != provider_receipt_hash:
                        failures.append("strategy competition audit benchmark provider receipt hash mismatch")
                    receipt_batch = str(receipt_payload.get("provider_batch_id") or "")
                    receipt_snapshot = str(receipt_payload.get("provider_snapshot_id") or "")
                    if receipt_batch and provider_batch_id and receipt_batch != provider_batch_id:
                        failures.append("strategy competition audit benchmark provider receipt batch mismatch")
                    if receipt_snapshot and provider_snapshot_id and receipt_snapshot != provider_snapshot_id:
                        failures.append("strategy competition audit benchmark provider receipt snapshot mismatch")

    independent = payload.get("independent_validation") if isinstance(payload.get("independent_validation"), dict) else {}
    if passed_artifact and independent.get("passed") is not True:
        failures.append("strategy competition audit independent validation not passed")
    if passed_artifact and str(independent.get("validator_role") or "").lower() != "independent_validator":
        failures.append("strategy competition audit independent validator role invalid")

    shadow = payload.get("shadow_execution") if isinstance(payload.get("shadow_execution"), dict) else {}
    if passed_artifact and shadow.get("passed") is not True:
        failures.append("strategy competition audit shadow execution not passed")

    controls = payload.get("pre_trade_risk_controls") if isinstance(payload.get("pre_trade_risk_controls"), dict) else {}
    if passed_artifact and controls.get("passed") is not True:
        failures.append("strategy competition audit pre-trade risk controls not passed")

    if payload.get("production_candidate_allowed") is True and failures:
        failures.append("strategy competition audit allowed production despite validation failures")
    return failures


def validate_production_readiness_evidence_file() -> list[str]:
    evidence_path = os.getenv(PRODUCTION_READINESS_EVIDENCE_ENV, "").strip()
    if not evidence_path:
        return [f"production readiness requires {PRODUCTION_READINESS_EVIDENCE_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"production readiness evidence missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"production readiness evidence invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_production_readiness.v1":
        failures.append("production readiness artifact_version invalid")
    status = str(payload.get("readiness_status") or "")
    passed = status == "production_readiness_passed"
    blocked = status == "production_readiness_blocked"
    if not passed and not blocked:
        failures.append(f"production readiness status invalid: {status}")
    if blocked:
        if payload.get("passed") is True or payload.get("production_release_allowed") is True:
            failures.append("blocked production readiness allowed release")
        if not payload.get("blocking_reasons"):
            failures.append("blocked production readiness missing blocking reasons")
    if passed:
        if payload.get("passed") is not True or payload.get("production_release_allowed") is not True:
            failures.append("passed production readiness status inconsistent")
        audit_checks = payload.get("competition_audit_checks") if isinstance(payload.get("competition_audit_checks"), dict) else {}
        if audit_checks.get("passed") is not True:
            failures.append("production readiness competition audit checks not passed")
        controls = payload.get("operational_controls") if isinstance(payload.get("operational_controls"), dict) else {}
        if controls.get("passed") is not True:
            failures.append("production readiness operational controls not passed")
        release_contract = payload.get("release_contract") if isinstance(payload.get("release_contract"), dict) else {}
        for key in (
            "requires_passed_competition_audit",
            "requires_independent_validation",
            "requires_shadow_execution",
            "requires_pre_trade_controls",
            "requires_kill_switch_and_rollback",
            "requires_human_approval_before_live_orders",
        ):
            if release_contract.get(key) is not True:
                failures.append(f"production readiness release contract missing: {key}")
    if len([item for item in payload.get("top5_symbols") or [] if str(item or "").strip()]) != 5:
        failures.append("production readiness top5 symbols invalid")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    if "human_approval_required" not in boundaries:
        failures.append("production readiness missing human approval boundary")
    return failures


def validate_evidence_submission_review_file() -> list[str]:
    evidence_path = os.getenv(EVIDENCE_SUBMISSION_REVIEW_ENV, "").strip()
    if not evidence_path:
        return [f"evidence submission review requires {EVIDENCE_SUBMISSION_REVIEW_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"evidence submission review missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"evidence submission review invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_evidence_submission_review.v1":
        failures.append("evidence submission review artifact_version invalid")
    status = str(payload.get("review_status") or "")
    accepted = status == "evidence_submission_accepted_for_validation"
    blocked = status == "evidence_submission_blocked"
    if not accepted and not blocked:
        failures.append(f"evidence submission review status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("evidence submission review attempted production eligibility")
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked evidence submission review cannot be passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked evidence submission review missing blocking reasons")
    if accepted:
        if payload.get("passed") is not True:
            failures.append("accepted evidence submission review not passed")
        hashes = payload.get("submitted_artifact_hashes") if isinstance(payload.get("submitted_artifact_hashes"), dict) else {}
        for key in ("shadow_feedback", "independent_validator_decision", "operational_controls_input"):
            if not str(hashes.get(key) or "").strip():
                failures.append(f"evidence submission review missing submitted hash: {key}")
        commands = payload.get("next_commands") if isinstance(payload.get("next_commands"), dict) else {}
        for key in ("record_shadow_feedback", "build_independent_validation", "build_operational_controls"):
            if not str(commands.get(key) or "").strip():
                failures.append(f"evidence submission review missing next command: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required in (
        "submission_review_is_not_shadow_execution_pass",
        "submission_review_is_not_independent_validation_pass",
        "submission_review_is_not_operational_controls_pass",
        "production_requires_formal_validation_tools_after_submission_review",
    ):
        if required not in boundaries:
            failures.append(f"evidence submission review missing boundary: {required}")
    return failures


def validate_release_chain_adjudication_file() -> list[str]:
    evidence_path = os.getenv(RELEASE_CHAIN_ADJUDICATION_ENV, "").strip()
    if not evidence_path:
        return [f"release chain adjudication requires {RELEASE_CHAIN_ADJUDICATION_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"release chain adjudication missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"release chain adjudication invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_release_chain_adjudication.v1":
        failures.append("release chain adjudication artifact_version invalid")
    status = str(payload.get("chain_status") or "")
    passed = status == "release_chain_passed_for_human_approval"
    blocked = status == "release_chain_blocked"
    if not passed and not blocked:
        failures.append(f"release chain adjudication status invalid: {status}")
    if blocked:
        if payload.get("passed") is True or payload.get("production_candidate_allowed") is True or payload.get("production_release_allowed") is True:
            failures.append("blocked release chain adjudication allowed production")
        if not str(payload.get("current_blocking_gate") or "").strip():
            failures.append("blocked release chain adjudication missing current blocking gate")
        if not payload.get("root_blockers"):
            failures.append("blocked release chain adjudication missing root blockers")
        if not payload.get("allowed_next_actions"):
            failures.append("blocked release chain adjudication missing allowed next actions")
    if passed:
        if payload.get("passed") is not True or payload.get("production_candidate_allowed") is not True or payload.get("production_release_allowed") is not True:
            failures.append("passed release chain adjudication status inconsistent")
        if str(payload.get("current_blocking_gate") or "").strip():
            failures.append("passed release chain adjudication has blocking gate")
        for item in payload.get("gate_status") or []:
            if isinstance(item, dict) and item.get("passed") is not True:
                failures.append(f"passed release chain contains failed gate: {item.get('name')}")
        release_contract = payload.get("release_contract") if isinstance(payload.get("release_contract"), dict) else {}
        if release_contract.get("requires_human_approval_before_live_orders") is not True:
            failures.append("release chain adjudication missing human approval contract")
    if len([item for item in payload.get("top5_symbols") or [] if str(item or "").strip()]) != 5:
        failures.append("release chain adjudication top5 symbols invalid")
    hashes = payload.get("source_artifact_hashes") if isinstance(payload.get("source_artifact_hashes"), dict) else {}
    if not str(hashes.get("competition_audit") or "").strip():
        failures.append("release chain adjudication missing competition audit hash")
    if not str(payload.get("adjudication_hash") or "").strip():
        failures.append("release chain adjudication hash missing")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required in (
        "release_chain_adjudication_is_not_trade_instruction",
        "blocked_gate_cannot_be_skipped",
        "production_requires_passed_readiness_and_human_approval",
    ):
        if required not in boundaries:
            failures.append(f"release chain adjudication missing boundary: {required}")
    return failures


def validate_formal_validation_handoff_file() -> list[str]:
    evidence_path = os.getenv(FORMAL_VALIDATION_HANDOFF_ENV, "").strip()
    if not evidence_path:
        return [f"formal validation handoff requires {FORMAL_VALIDATION_HANDOFF_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"formal validation handoff missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"formal validation handoff invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_formal_validation_handoff.v1":
        failures.append("formal validation handoff artifact_version invalid")
    status = str(payload.get("handoff_status") or "")
    ready = status == "formal_validation_ready"
    blocked = status == "formal_validation_handoff_blocked"
    if not ready and not blocked:
        failures.append(f"formal validation handoff status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("formal validation handoff attempted production eligibility")
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked formal validation handoff cannot be passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked formal validation handoff missing blocking reasons")
    if ready:
        if payload.get("passed") is not True:
            failures.append("ready formal validation handoff not passed")
        run_order = payload.get("formal_run_order") if isinstance(payload.get("formal_run_order"), list) else []
        expected = [
            "shadow_execution_evidence",
            "independent_validation",
            "operational_controls",
            "competition_audit_rerun",
            "production_readiness",
            "release_chain_adjudication",
        ]
        actual = [str(item.get("step") or "") for item in run_order if isinstance(item, dict)]
        if actual != expected:
            failures.append("formal validation handoff run order invalid")
        for item in run_order:
            if isinstance(item, dict) and not str(item.get("command") or "").strip():
                failures.append(f"formal validation handoff command missing: {item.get('step')}")
        contract = payload.get("handoff_contract") if isinstance(payload.get("handoff_contract"), dict) else {}
        for key in (
            "requires_accepted_submission_review",
            "requires_same_source_hashes",
            "requires_formal_validator_outputs_before_readiness",
            "requires_release_chain_adjudication_after_readiness",
            "does_not_create_production_eligibility",
        ):
            if contract.get(key) is not True:
                failures.append(f"formal validation handoff contract missing: {key}")
    hashes = payload.get("source_artifact_hashes") if isinstance(payload.get("source_artifact_hashes"), dict) else {}
    for key in ("intake_packet", "evidence_submission_review"):
        if not str(hashes.get(key) or "").strip():
            failures.append(f"formal validation handoff missing source hash: {key}")
    if not str(payload.get("handoff_hash") or "").strip():
        failures.append("formal validation handoff hash missing")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required in (
        "formal_validation_handoff_is_not_shadow_execution_pass",
        "formal_validation_handoff_is_not_independent_validation_pass",
        "formal_validation_handoff_is_not_operational_controls_pass",
        "formal_validation_handoff_is_not_production_readiness",
        "production_requires_passed_release_chain_adjudication_and_human_approval",
    ):
        if required not in boundaries:
            failures.append(f"formal validation handoff missing boundary: {required}")
    return failures


def validate_formal_validation_result_review_file() -> list[str]:
    evidence_path = os.getenv(FORMAL_VALIDATION_RESULT_REVIEW_ENV, "").strip()
    if not evidence_path:
        return [f"formal validation result review requires {FORMAL_VALIDATION_RESULT_REVIEW_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"formal validation result review missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"formal validation result review invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_formal_validation_result_review.v1":
        failures.append("formal validation result review artifact_version invalid")
    status = str(payload.get("result_review_status") or "")
    accepted = status == "formal_validation_results_accepted"
    blocked = status == "formal_validation_results_blocked"
    if not accepted and not blocked:
        failures.append(f"formal validation result review status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("formal validation result review attempted production eligibility")
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked formal validation result review cannot be passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked formal validation result review missing blocking reasons")
    if accepted:
        if payload.get("passed") is not True:
            failures.append("accepted formal validation result review not passed")
        expected = [
            "shadow_execution_evidence",
            "independent_validation",
            "operational_controls",
            "competition_audit_rerun",
            "production_readiness",
            "release_chain_adjudication",
        ]
        statuses = payload.get("formal_result_status") if isinstance(payload.get("formal_result_status"), list) else []
        actual = [str(item.get("step") or "") for item in statuses if isinstance(item, dict)]
        if actual != expected:
            failures.append("formal validation result review step order invalid")
        for item in statuses:
            if isinstance(item, dict) and item.get("passed") is not True:
                failures.append(f"accepted formal validation result contains failed step: {item.get('step')}")
        contract = payload.get("result_review_contract") if isinstance(payload.get("result_review_contract"), dict) else {}
        for key in (
            "requires_ready_handoff",
            "requires_all_formal_outputs",
            "requires_outputs_in_handoff_order",
            "requires_release_chain_passed_for_human_approval",
            "does_not_create_live_order_authority",
        ):
            if contract.get(key) is not True:
                failures.append(f"formal validation result review contract missing: {key}")
    if not str(payload.get("formal_validation_handoff_hash") or "").strip():
        failures.append("formal validation result review missing handoff hash")
    if not str(payload.get("result_review_hash") or "").strip():
        failures.append("formal validation result review hash missing")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required in (
        "formal_validation_result_review_is_not_trade_instruction",
        "result_review_does_not_replace_human_release_approval",
        "blocked_formal_result_cannot_advance_to_production",
        "production_requires_separate_human_approval_after_passed_release_chain",
    ):
        if required not in boundaries:
            failures.append(f"formal validation result review missing boundary: {required}")
    return failures


def validate_human_release_approval_file() -> list[str]:
    evidence_path = os.getenv(HUMAN_RELEASE_APPROVAL_ENV, "").strip()
    if not evidence_path:
        return [f"human release approval requires {HUMAN_RELEASE_APPROVAL_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"human release approval missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"human release approval invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_human_release_approval.v1":
        failures.append("human release approval artifact_version invalid")
    status = str(payload.get("approval_status") or "")
    approved = status == "human_release_approved"
    blocked = status == "human_release_approval_blocked"
    if not approved and not blocked:
        failures.append(f"human release approval status invalid: {status}")
    if blocked:
        if payload.get("passed") is True or payload.get("production_release_authorized") is True or payload.get("live_order_authority_granted") is True:
            failures.append("blocked human release approval granted authority")
        if not payload.get("blocking_reasons"):
            failures.append("blocked human release approval missing blocking reasons")
    if approved:
        if (
            payload.get("passed") is not True
            or payload.get("production_release_authorized") is not True
            or payload.get("live_order_authority_granted") is not True
        ):
            failures.append("approved human release approval status inconsistent")
        if str(payload.get("approver_role") or "") != "release_approver":
            failures.append("human release approver role invalid")
        if not str(payload.get("approver_name") or "").strip():
            failures.append("human release approver name missing")
        if not str(payload.get("approval_ticket") or "").strip():
            failures.append("human release approval ticket missing")
        contract = payload.get("approval_contract") if isinstance(payload.get("approval_contract"), dict) else {}
        for key in (
            "requires_accepted_formal_result_review",
            "requires_passed_release_chain_adjudication",
            "requires_independent_human_release_approver",
            "requires_conflict_attestation",
            "requires_reviewed_artifacts_match_current_evidence",
            "live_order_authority_only_after_this_gate_passes",
        ):
            if contract.get(key) is not True:
                failures.append(f"human release approval contract missing: {key}")
    hashes = payload.get("source_artifact_hashes") if isinstance(payload.get("source_artifact_hashes"), dict) else {}
    for key in ("formal_validation_result_review", "release_chain_adjudication"):
        if not str(hashes.get(key) or "").strip():
            failures.append(f"human release approval missing source hash: {key}")
    if approved and not str(hashes.get("human_approval_decision") or "").strip():
        failures.append("approved human release approval missing decision hash")
    if not str(payload.get("approval_hash") or "").strip():
        failures.append("human release approval hash missing")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required in (
        "human_release_approval_is_final_pre_live_gate",
        "blocked_human_approval_cannot_release_live_orders",
        "release_approver_cannot_self_approve",
        "approval_does_not_modify_strategy_evidence",
    ):
        if required not in boundaries:
            failures.append(f"human release approval missing boundary: {required}")
    return failures


def validate_live_order_authority_file() -> list[str]:
    evidence_path = os.getenv(LIVE_ORDER_AUTHORITY_ENV, "").strip()
    if not evidence_path:
        return [f"live order authority requires {LIVE_ORDER_AUTHORITY_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"live order authority missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"live order authority invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_live_order_authority_check.v1":
        failures.append("live order authority artifact_version invalid")
    status = str(payload.get("authority_status") or "")
    allowed = status == "live_order_submission_allowed"
    blocked = status == "live_order_submission_blocked"
    if not allowed and not blocked:
        failures.append(f"live order authority status invalid: {status}")
    if blocked:
        if payload.get("passed") is True or payload.get("live_order_submission_allowed") is True:
            failures.append("blocked live order authority allowed submission")
        if not payload.get("blocking_reasons"):
            failures.append("blocked live order authority missing blocking reasons")
    if allowed:
        if payload.get("passed") is not True or payload.get("live_order_submission_allowed") is not True:
            failures.append("allowed live order authority status inconsistent")
        orders = payload.get("orders") if isinstance(payload.get("orders"), list) else []
        if not orders or len(orders) > 5:
            failures.append("allowed live order authority order count invalid")
        contract = payload.get("authority_contract") if isinstance(payload.get("authority_contract"), dict) else {}
        for key in (
            "requires_human_release_approved",
            "requires_live_order_authority_granted",
            "requires_order_intent_approval_hash_match",
            "requires_order_intent_competition_run_match",
            "does_not_execute_orders",
        ):
            if contract.get(key) is not True:
                failures.append(f"live order authority contract missing: {key}")
    hashes = payload.get("source_artifact_hashes") if isinstance(payload.get("source_artifact_hashes"), dict) else {}
    if not str(hashes.get("human_release_approval") or "").strip():
        failures.append("live order authority missing approval hash")
    if allowed and not str(hashes.get("live_order_intent") or "").strip():
        failures.append("allowed live order authority missing intent hash")
    if not str(payload.get("authority_hash") or "").strip():
        failures.append("live order authority hash missing")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required in (
        "live_order_authority_check_does_not_execute_orders",
        "blocked_authority_check_cannot_submit_live_orders",
        "order_intent_must_reference_current_human_approval_hash",
        "live_submission_requires_broker_layer_after_authority_check",
    ):
        if required not in boundaries:
            failures.append(f"live order authority missing boundary: {required}")
    return failures


def validate_broker_submission_guard_file() -> list[str]:
    evidence_path = os.getenv(BROKER_SUBMISSION_GUARD_ENV, "").strip()
    if not evidence_path:
        return [f"broker submission guard requires {BROKER_SUBMISSION_GUARD_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"broker submission guard missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"broker submission guard invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_broker_submission_guard.v1":
        failures.append("broker submission guard artifact_version invalid")
    status = str(payload.get("guard_status") or "")
    passed = status == "broker_submission_guard_passed"
    blocked = status == "broker_submission_guard_blocked"
    if not passed and not blocked:
        failures.append(f"broker submission guard status invalid: {status}")
    if blocked:
        if payload.get("passed") is True or payload.get("broker_submission_allowed") is True:
            failures.append("blocked broker submission guard allowed submission")
        if not payload.get("blocking_reasons"):
            failures.append("blocked broker submission guard missing blocking reasons")
    if passed:
        if payload.get("passed") is not True or payload.get("broker_submission_allowed") is not True:
            failures.append("passed broker submission guard status inconsistent")
        if str(payload.get("submission_mode") or "").lower() not in {"dry_run", "controlled_submit"}:
            failures.append("broker submission guard mode invalid")
        if not str(payload.get("broker_adapter") or "").strip():
            failures.append("broker submission guard adapter missing")
        if not str(payload.get("idempotency_key") or "").strip():
            failures.append("broker submission guard idempotency key missing")
        orders = payload.get("orders") if isinstance(payload.get("orders"), list) else []
        if not orders or len(orders) > 5:
            failures.append("broker submission guard order count invalid")
        contract = payload.get("broker_guard_contract") if isinstance(payload.get("broker_guard_contract"), dict) else {}
        for key in (
            "requires_live_order_authority_allowed",
            "requires_authority_hash_match",
            "requires_idempotency_key",
            "requires_broker_adapter_declared",
            "does_not_record_fills",
        ):
            if contract.get(key) is not True:
                failures.append(f"broker submission guard contract missing: {key}")
    hashes = payload.get("source_artifact_hashes") if isinstance(payload.get("source_artifact_hashes"), dict) else {}
    if not str(hashes.get("live_order_authority") or "").strip():
        failures.append("broker submission guard missing authority hash")
    if passed and not str(hashes.get("broker_submission_intent") or "").strip():
        failures.append("passed broker submission guard missing intent hash")
    if not str(payload.get("broker_guard_hash") or "").strip():
        failures.append("broker submission guard hash missing")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required in (
        "broker_submission_guard_does_not_execute_orders",
        "blocked_broker_guard_cannot_call_broker_adapter",
        "broker_submit_requires_separate_broker_response_evidence",
        "fills_must_be_recorded_by_execution_feedback_not_guard",
    ):
        if required not in boundaries:
            failures.append(f"broker submission guard missing boundary: {required}")
    return failures


def validate_broker_submission_response_file() -> list[str]:
    evidence_path = os.getenv(BROKER_SUBMISSION_RESPONSE_ENV, "").strip()
    if not evidence_path:
        return [f"broker submission response requires {BROKER_SUBMISSION_RESPONSE_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"broker submission response missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"broker submission response invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_broker_submission_response_evidence.v1":
        failures.append("broker submission response artifact_version invalid")
    status = str(payload.get("response_status") or "")
    accepted = status == "broker_submission_response_accepted"
    blocked = status == "broker_submission_response_blocked"
    if not accepted and not blocked:
        failures.append(f"broker submission response status invalid: {status}")
    if payload.get("execution_fills_confirmed") is True:
        failures.append("broker submission response attempted fill confirmation")
    if blocked:
        if payload.get("passed") is True or payload.get("broker_submission_confirmed") is True:
            failures.append("blocked broker submission response confirmed submission")
        if not payload.get("blocking_reasons"):
            failures.append("blocked broker submission response missing blocking reasons")
    if accepted:
        if payload.get("passed") is not True or payload.get("broker_submission_confirmed") is not True:
            failures.append("accepted broker submission response status inconsistent")
        responses = payload.get("order_responses") if isinstance(payload.get("order_responses"), list) else []
        if not responses or len(responses) > 5:
            failures.append("accepted broker submission response order count invalid")
        contract = payload.get("response_contract") if isinstance(payload.get("response_contract"), dict) else {}
        for key in (
            "requires_passed_broker_guard",
            "requires_guard_hash_match",
            "requires_idempotency_key_match",
            "requires_order_set_match",
            "does_not_confirm_fills",
        ):
            if contract.get(key) is not True:
                failures.append(f"broker submission response contract missing: {key}")
    hashes = payload.get("source_artifact_hashes") if isinstance(payload.get("source_artifact_hashes"), dict) else {}
    if not str(hashes.get("broker_submission_guard") or "").strip():
        failures.append("broker submission response missing guard hash")
    if accepted and not str(hashes.get("broker_submission_response") or "").strip():
        failures.append("accepted broker submission response missing response hash")
    if not str(payload.get("response_evidence_hash") or "").strip():
        failures.append("broker submission response evidence hash missing")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required in (
        "broker_submission_response_is_not_fill_evidence",
        "broker_submission_confirmed_does_not_mean_filled",
        "fills_require_separate_broker_execution_report",
        "blocked_broker_response_cannot_advance_execution_state",
    ):
        if required not in boundaries:
            failures.append(f"broker submission response missing boundary: {required}")
    return failures


def validate_broker_execution_feedback_file() -> list[str]:
    evidence_path = os.getenv(BROKER_EXECUTION_FEEDBACK_ENV, "").strip()
    if not evidence_path:
        return [f"broker execution feedback requires {BROKER_EXECUTION_FEEDBACK_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"broker execution feedback missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"broker execution feedback invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_broker_execution_feedback_review.v1":
        failures.append("broker execution feedback artifact_version invalid")
    status = str(payload.get("feedback_status") or "")
    accepted = status == "broker_execution_feedback_accepted"
    blocked = status == "broker_execution_feedback_blocked"
    if not accepted and not blocked:
        failures.append(f"broker execution feedback status invalid: {status}")
    if blocked:
        if payload.get("passed") is True or payload.get("execution_feedback_complete") is True:
            failures.append("blocked broker execution feedback marked complete")
        if not payload.get("blocking_reasons"):
            failures.append("blocked broker execution feedback missing blocking reasons")
    if accepted:
        if payload.get("passed") is not True or payload.get("execution_feedback_complete") is not True:
            failures.append("accepted broker execution feedback status inconsistent")
        reports = payload.get("execution_reports") if isinstance(payload.get("execution_reports"), list) else []
        if not reports or len(reports) > 5:
            failures.append("accepted broker execution feedback report count invalid")
        contract = payload.get("feedback_contract") if isinstance(payload.get("feedback_contract"), dict) else {}
        for key in (
            "requires_accepted_broker_submission_response",
            "requires_response_hash_match",
            "requires_terminal_order_feedback",
            "requires_fills_for_filled_status",
            "requires_miss_reason_for_unfilled_terminal_status",
            "requires_cost_slippage_and_attribution",
        ):
            if contract.get(key) is not True:
                failures.append(f"broker execution feedback contract missing: {key}")
    hashes = payload.get("source_artifact_hashes") if isinstance(payload.get("source_artifact_hashes"), dict) else {}
    if not str(hashes.get("broker_submission_response_evidence") or "").strip():
        failures.append("broker execution feedback missing response evidence hash")
    if accepted and not str(hashes.get("broker_execution_feedback") or "").strip():
        failures.append("accepted broker execution feedback missing feedback hash")
    if not str(payload.get("feedback_review_hash") or "").strip():
        failures.append("broker execution feedback review hash missing")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required in (
        "execution_feedback_is_required_after_broker_submission",
        "submitted_or_accepted_orders_are_not_execution_complete",
        "filled_orders_require_fills_costs_slippage_and_attribution",
        "blocked_execution_feedback_cannot_mark_trade_complete",
    ):
        if required not in boundaries:
            failures.append(f"broker execution feedback missing boundary: {required}")
    return failures


def validate_post_rerun_broker_execution_feedback_review_file() -> list[str]:
    evidence_path = os.getenv(POST_RERUN_BROKER_EXECUTION_FEEDBACK_REVIEW_ENV, "").strip()
    if not evidence_path:
        return [f"post-rerun broker execution feedback review requires {POST_RERUN_BROKER_EXECUTION_FEEDBACK_REVIEW_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"post-rerun broker execution feedback review missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"post-rerun broker execution feedback review invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_post_rerun_broker_execution_feedback_review.v1":
        failures.append("post-rerun broker execution feedback review artifact_version invalid")
    status = str(payload.get("broker_execution_feedback_review_status") or "")
    ready = status == "post_rerun_broker_execution_feedback_ready_for_post_trade"
    blocked = status == "post_rerun_broker_execution_feedback_blocked"
    if not ready and not blocked:
        failures.append(f"post-rerun broker execution feedback review status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("post-rerun broker execution feedback review attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("post-rerun broker execution feedback review attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("post-rerun broker execution feedback review attempted live order grant")
    if payload.get("post_trade_reconciliation_passed") is True:
        failures.append("post-rerun broker execution feedback review attempted post-trade completion")
    if payload.get("trade_lifecycle_complete") is True:
        failures.append("post-rerun broker execution feedback review attempted trade lifecycle completion")
    if not str(payload.get("source_post_rerun_broker_response_review_hash") or "").strip():
        failures.append("post-rerun broker execution feedback review missing response review hash")
    if not str(payload.get("broker_execution_feedback_review_hash") or "").strip():
        failures.append("post-rerun broker execution feedback review hash missing")
    if blocked:
        if payload.get("passed") is True or payload.get("execution_feedback_complete") is True:
            failures.append("blocked post-rerun broker execution feedback review marked complete")
        if not payload.get("blocking_reasons"):
            failures.append("blocked post-rerun broker execution feedback review missing blocking reasons")
    if ready:
        if payload.get("passed") is not True or payload.get("execution_feedback_complete") is not True:
            failures.append("ready post-rerun broker execution feedback review status inconsistent")
        if payload.get("allowed_next_actions") != ["run_post_trade_reconciliation_with_matching_execution_feedback_hash"]:
            failures.append("ready post-rerun broker execution feedback next action invalid")
        if not str(payload.get("broker_execution_feedback_hash") or "").strip():
            failures.append("ready post-rerun broker execution feedback missing feedback hash")
    contract = payload.get("broker_execution_feedback_review_contract") if isinstance(payload.get("broker_execution_feedback_review_contract"), dict) else {}
    for key in (
        "requires_post_rerun_broker_response_review_ready",
        "requires_broker_execution_feedback_accepted",
        "requires_response_hash_lineage",
        "requires_terminal_order_feedback",
        "requires_cost_slippage_and_attribution",
        "does_not_create_post_trade_reconciliation",
    ):
        if contract.get(key) is not True:
            failures.append(f"post-rerun broker execution feedback review contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required in (
        "post_rerun_broker_execution_feedback_review_is_not_post_trade",
        "submitted_or_accepted_orders_are_not_execution_complete",
        "filled_orders_require_fills_costs_slippage_and_attribution",
        "post_trade_reconciliation_still_required_after_execution_feedback",
    ):
        if required not in boundaries:
            failures.append(f"post-rerun broker execution feedback review missing boundary: {required}")
    return failures


def validate_post_rerun_post_trade_reconciliation_file() -> list[str]:
    evidence_path = os.getenv(POST_RERUN_POST_TRADE_RECONCILIATION_ENV, "").strip()
    if not evidence_path:
        return [f"post-rerun post-trade reconciliation requires {POST_RERUN_POST_TRADE_RECONCILIATION_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"post-rerun post-trade reconciliation missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"post-rerun post-trade reconciliation invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_post_rerun_post_trade_reconciliation.v1":
        failures.append("post-rerun post-trade reconciliation artifact_version invalid")
    status = str(payload.get("reconciliation_status") or "")
    passed = status == "post_rerun_post_trade_reconciliation_passed"
    blocked = status == "post_rerun_post_trade_reconciliation_blocked"
    if not passed and not blocked:
        failures.append(f"post-rerun post-trade reconciliation status invalid: {status}")
    if blocked:
        if payload.get("passed") is True or payload.get("trade_lifecycle_complete") is True:
            failures.append("blocked post-rerun reconciliation marked lifecycle complete")
        if not payload.get("blocking_reasons"):
            failures.append("blocked post-rerun reconciliation missing blocking reasons")
    if passed:
        if payload.get("passed") is not True or payload.get("trade_lifecycle_complete") is not True:
            failures.append("passed post-rerun reconciliation status inconsistent")
        if not payload.get("cash_reconciliation"):
            failures.append("passed post-rerun reconciliation missing cash reconciliation")
        if not payload.get("position_reconciliation"):
            failures.append("passed post-rerun reconciliation missing position reconciliation")
        if not payload.get("cost_slippage_reconciliation"):
            failures.append("passed post-rerun reconciliation missing cost reconciliation")
        contract = payload.get("reconciliation_contract") if isinstance(payload.get("reconciliation_contract"), dict) else {}
        for key in (
            "requires_accepted_execution_feedback",
            "requires_feedback_hash_match",
            "requires_cash_reconciliation",
            "requires_position_reconciliation",
            "requires_cost_slippage_reconciliation",
            "requires_exception_owners_and_resolution",
            "requires_operations_signoff",
            "does_not_create_new_trade_permission",
        ):
            if contract.get(key) is not True:
                failures.append(f"post-rerun post-trade reconciliation contract missing: {key}")
    hashes = payload.get("source_artifact_hashes") if isinstance(payload.get("source_artifact_hashes"), dict) else {}
    if not str(hashes.get("broker_execution_feedback_review") or "").strip():
        failures.append("post-rerun post-trade reconciliation missing feedback review hash")
    if passed and not str(hashes.get("post_trade_reconciliation_input") or "").strip():
        failures.append("passed post-rerun post-trade reconciliation missing input hash")
    if not str(payload.get("reconciliation_hash") or "").strip():
        failures.append("post-rerun post-trade reconciliation hash missing")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required in (
        "post_rerun_post_trade_reconciliation_is_required_after_execution_feedback",
        "execution_feedback_complete_is_not_portfolio_reconciled",
        "trade_lifecycle_complete_requires_cash_position_cost_reconciliation",
        "blocked_post_rerun_reconciliation_cannot_mark_lifecycle_complete",
    ):
        if required not in boundaries:
            failures.append(f"post-rerun post-trade reconciliation missing boundary: {required}")
    return failures


def validate_post_rerun_trade_lifecycle_adjudication_file() -> list[str]:
    evidence_path = os.getenv(POST_RERUN_TRADE_LIFECYCLE_ADJUDICATION_ENV, "").strip()
    if not evidence_path:
        return [f"post-rerun trade lifecycle adjudication requires {POST_RERUN_TRADE_LIFECYCLE_ADJUDICATION_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"post-rerun trade lifecycle adjudication missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"post-rerun trade lifecycle adjudication invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_post_rerun_trade_lifecycle_adjudication.v1":
        failures.append("post-rerun trade lifecycle adjudication artifact_version invalid")
    status = str(payload.get("lifecycle_status") or "")
    complete = status == "post_rerun_trade_lifecycle_complete"
    blocked = status == "post_rerun_trade_lifecycle_blocked"
    if not complete and not blocked:
        failures.append(f"post-rerun trade lifecycle adjudication status invalid: {status}")
    if blocked:
        if payload.get("passed") is True or payload.get("trade_lifecycle_complete") is True:
            failures.append("blocked post-rerun trade lifecycle adjudication marked complete")
        if not payload.get("current_blocking_stage"):
            failures.append("blocked post-rerun trade lifecycle adjudication missing blocking stage")
        if not payload.get("root_blockers"):
            failures.append("blocked post-rerun trade lifecycle adjudication missing root blockers")
    if complete:
        if payload.get("passed") is not True or payload.get("trade_lifecycle_complete") is not True:
            failures.append("complete post-rerun trade lifecycle adjudication status inconsistent")
        if payload.get("current_blocking_stage"):
            failures.append("complete post-rerun trade lifecycle adjudication has blocking stage")
        statuses = payload.get("lifecycle_statuses") if isinstance(payload.get("lifecycle_statuses"), list) else []
        for item in statuses:
            if isinstance(item, dict) and item.get("passed") is not True:
                failures.append(f"complete post-rerun trade lifecycle contains failed stage: {item.get('name')}")
        contract = payload.get("lifecycle_contract") if isinstance(payload.get("lifecycle_contract"), dict) else {}
        for key in (
            "requires_post_rerun_broker_guard_review",
            "requires_post_rerun_broker_response_review",
            "requires_post_rerun_broker_execution_feedback_review",
            "requires_post_rerun_post_trade_reconciliation",
            "does_not_create_new_trade_permission",
        ):
            if contract.get(key) is not True:
                failures.append(f"post-rerun trade lifecycle adjudication contract missing: {key}")
    hashes = payload.get("source_artifact_hashes") if isinstance(payload.get("source_artifact_hashes"), dict) else {}
    if not str(hashes.get("post_rerun_broker_guard_review") or "").strip():
        failures.append("post-rerun trade lifecycle adjudication missing guard review hash")
    if complete and not str(hashes.get("post_rerun_post_trade_reconciliation") or "").strip():
        failures.append("complete post-rerun trade lifecycle adjudication missing reconciliation hash")
    if not str(payload.get("lifecycle_adjudication_hash") or "").strip():
        failures.append("post-rerun trade lifecycle adjudication hash missing")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required in (
        "post_rerun_trade_lifecycle_adjudication_is_not_trade_instruction",
        "blocked_lifecycle_stage_cannot_be_skipped",
        "broker_submission_does_not_equal_execution_complete",
        "execution_feedback_does_not_equal_post_trade_reconciled",
    ):
        if required not in boundaries:
            failures.append(f"post-rerun trade lifecycle adjudication missing boundary: {required}")
    return failures


def validate_post_rerun_evidence_chain_manifest_file() -> list[str]:
    evidence_path = os.getenv(POST_RERUN_EVIDENCE_CHAIN_MANIFEST_ENV, "").strip()
    if not evidence_path:
        return [f"post-rerun evidence chain manifest requires {POST_RERUN_EVIDENCE_CHAIN_MANIFEST_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"post-rerun evidence chain manifest missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"post-rerun evidence chain manifest invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_post_rerun_evidence_chain_manifest.v1":
        failures.append("post-rerun evidence chain manifest artifact_version invalid")
    status = str(payload.get("manifest_status") or "")
    complete = status == "post_rerun_evidence_chain_complete"
    blocked = status == "post_rerun_evidence_chain_blocked"
    if not complete and not blocked:
        failures.append(f"post-rerun evidence chain manifest status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("post-rerun evidence chain manifest attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("post-rerun evidence chain manifest attempted production release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("post-rerun evidence chain manifest attempted live order authority")
    if blocked:
        if payload.get("passed") is True or payload.get("trade_lifecycle_complete") is True:
            failures.append("blocked post-rerun evidence chain manifest marked complete")
        if not payload.get("current_blocking_artifact"):
            failures.append("blocked post-rerun evidence chain manifest missing current blocking artifact")
        if not payload.get("root_blockers"):
            failures.append("blocked post-rerun evidence chain manifest missing root blockers")
    if complete:
        if payload.get("passed") is not True or payload.get("trade_lifecycle_complete") is not True:
            failures.append("complete post-rerun evidence chain manifest status inconsistent")
        if payload.get("current_blocking_artifact"):
            failures.append("complete post-rerun evidence chain manifest has blocking artifact")
        statuses = payload.get("artifact_statuses") if isinstance(payload.get("artifact_statuses"), list) else []
        for item in statuses:
            if isinstance(item, dict) and item.get("passed") is not True:
                failures.append(f"complete post-rerun evidence chain contains failed stage: {item.get('name')}")
        contract = payload.get("manifest_contract") if isinstance(payload.get("manifest_contract"), dict) else {}
        for key in (
            "requires_post_rerun_release_readiness",
            "requires_post_rerun_live_authority_review",
            "requires_post_rerun_broker_guard_review",
            "requires_post_rerun_broker_response_review",
            "requires_post_rerun_broker_execution_feedback_review",
            "requires_post_rerun_post_trade_reconciliation",
            "requires_post_rerun_trade_lifecycle_adjudication",
            "does_not_create_production_eligibility",
            "does_not_create_live_order_authority",
            "does_not_mark_execution_or_reconciliation_complete",
        ):
            if contract.get(key) is not True:
                failures.append(f"post-rerun evidence chain manifest contract missing: {key}")
    hashes = payload.get("source_artifact_hashes") if isinstance(payload.get("source_artifact_hashes"), dict) else {}
    for required_hash in (
        "post_rerun_release_readiness",
        "post_rerun_live_authority_review",
        "post_rerun_broker_guard_review",
        "post_rerun_broker_response_review",
        "post_rerun_broker_execution_feedback_review",
        "post_rerun_post_trade_reconciliation",
        "post_rerun_trade_lifecycle_adjudication",
        "post_rerun_human_release_approval_review",
    ):
        if not str(hashes.get(required_hash) or "").strip():
            failures.append(f"post-rerun evidence chain manifest missing hash: {required_hash}")
    if not str(payload.get("manifest_hash") or "").strip():
        failures.append("post-rerun evidence chain manifest hash missing")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required in (
        "post_rerun_evidence_chain_manifest_is_inventory_not_approval",
        "manifest_cannot_create_production_or_live_order_authority",
        "blocked_or_partial_post_rerun_artifacts_cannot_be_packaged_as_passed",
        "trade_lifecycle_complete_requires_post_rerun_post_trade_reconciliation_and_lifecycle_adjudication",
    ):
        if required not in boundaries:
            failures.append(f"post-rerun evidence chain manifest missing boundary: {required}")
    return failures


def validate_post_rerun_human_release_approval_review_file() -> list[str]:
    evidence_path = os.getenv(POST_RERUN_HUMAN_RELEASE_APPROVAL_REVIEW_ENV, "").strip()
    if not evidence_path:
        return [f"post-rerun human release approval review requires {POST_RERUN_HUMAN_RELEASE_APPROVAL_REVIEW_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"post-rerun human release approval review missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"post-rerun human release approval review invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_post_rerun_human_release_approval_review.v1":
        failures.append("post-rerun human release approval review artifact_version invalid")
    status = str(payload.get("human_release_approval_review_status") or "")
    approved = status == "post_rerun_human_release_approved"
    blocked = status == "post_rerun_human_release_approval_blocked"
    if not approved and not blocked:
        failures.append(f"post-rerun human release approval review status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("post-rerun human release approval review attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("post-rerun human release approval review attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("post-rerun human release approval review attempted live order authority")
    if not str(payload.get("post_rerun_evidence_chain_manifest_hash") or "").strip():
        failures.append("post-rerun human release approval review missing manifest hash")
    if not str(payload.get("human_release_approval_review_hash") or "").strip():
        failures.append("post-rerun human release approval review hash missing")
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked post-rerun human release approval review marked passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked post-rerun human release approval review missing blocking reasons")
    if approved:
        if payload.get("passed") is not True:
            failures.append("approved post-rerun human release approval review status inconsistent")
        if payload.get("production_release_authorized") is not False or payload.get("live_order_authority_granted") is not False:
            failures.append("approved post-rerun human release approval review granted authority")
        if payload.get("allowed_next_actions") != ["run_live_order_authority_check_with_matching_post_rerun_human_release_hash"]:
            failures.append("approved post-rerun human release approval review next action invalid")
        if not str(payload.get("human_approval_decision_hash") or "").strip():
            failures.append("approved post-rerun human release approval review missing decision hash")
    contract = payload.get("human_release_approval_review_contract") if isinstance(payload.get("human_release_approval_review_contract"), dict) else {}
    for key in (
        "requires_post_rerun_evidence_chain_manifest_complete",
        "requires_independent_human_release_approver",
        "requires_conflict_attestation",
        "requires_reviewed_artifacts_match_current_manifest",
        "does_not_create_live_order_authority",
    ):
        if contract.get(key) is not True:
            failures.append(f"post-rerun human release approval review contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required in (
        "post_rerun_human_release_approval_review_is_not_live_order_authority",
        "approval_review_does_not_execute_orders",
        "approval_review_does_not_authorize_production",
        "approval_review_is_inventory_not_permission",
    ):
        if required not in boundaries:
            failures.append(f"post-rerun human release approval review missing boundary: {required}")
    return failures


def validate_post_trade_reconciliation_file() -> list[str]:
    evidence_path = os.getenv(POST_TRADE_RECONCILIATION_ENV, "").strip()
    if not evidence_path:
        return [f"post-trade reconciliation requires {POST_TRADE_RECONCILIATION_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"post-trade reconciliation missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"post-trade reconciliation invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_post_trade_reconciliation.v1":
        failures.append("post-trade reconciliation artifact_version invalid")
    status = str(payload.get("reconciliation_status") or "")
    passed = status == "post_trade_reconciliation_passed"
    blocked = status == "post_trade_reconciliation_blocked"
    if not passed and not blocked:
        failures.append(f"post-trade reconciliation status invalid: {status}")
    if blocked:
        if payload.get("passed") is True or payload.get("trade_lifecycle_complete") is True:
            failures.append("blocked post-trade reconciliation marked lifecycle complete")
        if not payload.get("blocking_reasons"):
            failures.append("blocked post-trade reconciliation missing blocking reasons")
    if passed:
        if payload.get("passed") is not True or payload.get("trade_lifecycle_complete") is not True:
            failures.append("passed post-trade reconciliation status inconsistent")
        if not payload.get("cash_reconciliation"):
            failures.append("passed post-trade reconciliation missing cash reconciliation")
        if not payload.get("position_reconciliation"):
            failures.append("passed post-trade reconciliation missing position reconciliation")
        if not payload.get("cost_slippage_reconciliation"):
            failures.append("passed post-trade reconciliation missing cost reconciliation")
        contract = payload.get("reconciliation_contract") if isinstance(payload.get("reconciliation_contract"), dict) else {}
        for key in (
            "requires_accepted_execution_feedback",
            "requires_feedback_hash_match",
            "requires_cash_reconciliation",
            "requires_position_reconciliation",
            "requires_cost_slippage_reconciliation",
            "requires_exception_owners_and_resolution",
            "requires_operations_signoff",
        ):
            if contract.get(key) is not True:
                failures.append(f"post-trade reconciliation contract missing: {key}")
    hashes = payload.get("source_artifact_hashes") if isinstance(payload.get("source_artifact_hashes"), dict) else {}
    if not str(hashes.get("broker_execution_feedback_review") or "").strip():
        failures.append("post-trade reconciliation missing feedback review hash")
    if passed and not str(hashes.get("post_trade_reconciliation_input") or "").strip():
        failures.append("passed post-trade reconciliation missing input hash")
    if not str(payload.get("reconciliation_hash") or "").strip():
        failures.append("post-trade reconciliation hash missing")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required in (
        "post_trade_reconciliation_is_required_after_execution_feedback",
        "execution_feedback_complete_is_not_portfolio_reconciled",
        "trade_lifecycle_complete_requires_cash_position_cost_reconciliation",
        "blocked_reconciliation_cannot_mark_lifecycle_complete",
    ):
        if required not in boundaries:
            failures.append(f"post-trade reconciliation missing boundary: {required}")
    return failures


def validate_trade_lifecycle_adjudication_file() -> list[str]:
    evidence_path = os.getenv(TRADE_LIFECYCLE_ADJUDICATION_ENV, "").strip()
    if not evidence_path:
        return [f"trade lifecycle adjudication requires {TRADE_LIFECYCLE_ADJUDICATION_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"trade lifecycle adjudication missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"trade lifecycle adjudication invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_trade_lifecycle_adjudication.v1":
        failures.append("trade lifecycle adjudication artifact_version invalid")
    status = str(payload.get("lifecycle_status") or "")
    complete = status == "trade_lifecycle_complete"
    blocked = status == "trade_lifecycle_blocked"
    if not complete and not blocked:
        failures.append(f"trade lifecycle adjudication status invalid: {status}")
    if blocked:
        if payload.get("passed") is True or payload.get("trade_lifecycle_complete") is True:
            failures.append("blocked trade lifecycle adjudication marked complete")
        if not payload.get("current_blocking_stage"):
            failures.append("blocked trade lifecycle adjudication missing blocking stage")
        if not payload.get("root_blockers"):
            failures.append("blocked trade lifecycle adjudication missing root blockers")
        if not payload.get("allowed_next_actions"):
            failures.append("blocked trade lifecycle adjudication missing next actions")
    if complete:
        if payload.get("passed") is not True or payload.get("trade_lifecycle_complete") is not True:
            failures.append("complete trade lifecycle adjudication status inconsistent")
        if payload.get("current_blocking_stage"):
            failures.append("complete trade lifecycle adjudication has blocking stage")
        statuses = payload.get("lifecycle_statuses") if isinstance(payload.get("lifecycle_statuses"), list) else []
        for item in statuses:
            if isinstance(item, dict) and item.get("passed") is not True:
                failures.append(f"complete trade lifecycle contains failed stage: {item.get('name')}")
        contract = payload.get("lifecycle_contract") if isinstance(payload.get("lifecycle_contract"), dict) else {}
        for key in (
            "requires_human_release_approval",
            "requires_live_order_authority",
            "requires_broker_submission_guard",
            "requires_broker_submission_response",
            "requires_broker_execution_feedback",
            "requires_post_trade_reconciliation",
            "does_not_create_new_trade_permission",
        ):
            if contract.get(key) is not True:
                failures.append(f"trade lifecycle adjudication contract missing: {key}")
    hashes = payload.get("source_artifact_hashes") if isinstance(payload.get("source_artifact_hashes"), dict) else {}
    if not str(hashes.get("human_release_approval") or "").strip():
        failures.append("trade lifecycle adjudication missing human approval hash")
    if complete and not str(hashes.get("post_trade_reconciliation") or "").strip():
        failures.append("complete trade lifecycle adjudication missing reconciliation hash")
    if not str(payload.get("lifecycle_adjudication_hash") or "").strip():
        failures.append("trade lifecycle adjudication hash missing")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required in (
        "trade_lifecycle_adjudication_is_not_trade_instruction",
        "blocked_lifecycle_stage_cannot_be_skipped",
        "broker_submission_does_not_equal_execution_complete",
        "execution_feedback_does_not_equal_post_trade_reconciled",
    ):
        if required not in boundaries:
            failures.append(f"trade lifecycle adjudication missing boundary: {required}")
    return failures


def validate_evidence_chain_manifest_file() -> list[str]:
    evidence_path = os.getenv(EVIDENCE_CHAIN_MANIFEST_ENV, "").strip()
    if not evidence_path:
        return [f"evidence chain manifest requires {EVIDENCE_CHAIN_MANIFEST_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"evidence chain manifest missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"evidence chain manifest invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_evidence_chain_manifest.v1":
        failures.append("evidence chain manifest artifact_version invalid")
    status = str(payload.get("manifest_status") or "")
    complete = status == "evidence_chain_complete"
    blocked = status == "evidence_chain_blocked"
    if not complete and not blocked:
        failures.append(f"evidence chain manifest status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("evidence chain manifest attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("evidence chain manifest attempted production release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("evidence chain manifest attempted live order authority")
    if blocked:
        if payload.get("passed") is True or payload.get("trade_lifecycle_complete") is True:
            failures.append("blocked evidence chain manifest marked complete")
        if not payload.get("current_blocking_artifact"):
            failures.append("blocked evidence chain manifest missing current blocking artifact")
        if not payload.get("root_blockers"):
            failures.append("blocked evidence chain manifest missing root blockers")
        if not payload.get("allowed_next_actions"):
            failures.append("blocked evidence chain manifest missing next actions")
    if complete:
        if payload.get("passed") is not True or payload.get("trade_lifecycle_complete") is not True:
            failures.append("complete evidence chain manifest status inconsistent")
        if payload.get("current_blocking_artifact"):
            failures.append("complete evidence chain manifest has blocking artifact")
        statuses = payload.get("artifact_statuses") if isinstance(payload.get("artifact_statuses"), list) else []
        for item in statuses:
            if isinstance(item, dict) and item.get("passed") is not True:
                failures.append(f"complete evidence chain contains failed artifact: {item.get('name')}")
        contract = payload.get("manifest_contract") if isinstance(payload.get("manifest_contract"), dict) else {}
        for key in (
            "requires_release_chain_adjudication",
            "requires_trade_lifecycle_adjudication",
            "requires_all_chain_artifact_hashes",
            "does_not_create_production_eligibility",
            "does_not_create_live_order_authority",
            "does_not_mark_execution_or_reconciliation_complete",
        ):
            if contract.get(key) is not True:
                failures.append(f"evidence chain manifest contract missing: {key}")
    hashes = payload.get("source_artifact_hashes") if isinstance(payload.get("source_artifact_hashes"), dict) else {}
    if not str(hashes.get("release_chain_adjudication") or "").strip():
        failures.append("evidence chain manifest missing release-chain hash")
    if not str(hashes.get("trade_lifecycle_adjudication") or "").strip():
        failures.append("evidence chain manifest missing trade lifecycle hash")
    if not str(payload.get("manifest_hash") or "").strip():
        failures.append("evidence chain manifest hash missing")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required in (
        "evidence_chain_manifest_is_inventory_not_approval",
        "manifest_cannot_create_production_or_live_order_authority",
        "blocked_or_partial_artifacts_cannot_be_packaged_as_passed",
        "trade_lifecycle_complete_requires_post_trade_reconciliation_and_lifecycle_adjudication",
    ):
        if required not in boundaries:
            failures.append(f"evidence chain manifest missing boundary: {required}")
    return failures


def validate_evidence_remediation_work_order_file() -> list[str]:
    evidence_path = os.getenv(EVIDENCE_REMEDIATION_WORK_ORDER_ENV, "").strip()
    if not evidence_path:
        return [f"evidence remediation work order requires {EVIDENCE_REMEDIATION_WORK_ORDER_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"evidence remediation work order missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"evidence remediation work order invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_evidence_remediation_work_order.v1":
        failures.append("evidence remediation work order artifact_version invalid")
    status = str(payload.get("work_order_status") or "")
    required = status == "remediation_required"
    complete = status == "no_remediation_required"
    if not required and not complete:
        failures.append(f"evidence remediation work order status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("evidence remediation work order attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("evidence remediation work order attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("evidence remediation work order attempted live order authority")
    if not str(payload.get("source_manifest_hash") or "").strip():
        failures.append("evidence remediation work order missing source manifest hash")
    if not str(payload.get("work_order_hash") or "").strip():
        failures.append("evidence remediation work order hash missing")
    items = payload.get("work_items") if isinstance(payload.get("work_items"), list) else []
    if required:
        if payload.get("passed") is True:
            failures.append("remediation-required work order marked passed")
        if not items:
            failures.append("remediation-required work order missing work items")
    if complete:
        if payload.get("passed") is not True:
            failures.append("no-remediation work order status inconsistent")
        if items:
            failures.append("no-remediation work order contains open work items")
    for item in items:
        if not isinstance(item, dict):
            failures.append("evidence remediation work order item invalid")
            continue
        for key in ("artifact", "owner_role", "validator_tool", "blocking_reasons", "required_evidence", "acceptance_rule"):
            if not item.get(key):
                failures.append(f"evidence remediation work item missing: {key}")
    contract = payload.get("work_order_contract") if isinstance(payload.get("work_order_contract"), dict) else {}
    for key in (
        "requires_current_evidence_chain_manifest",
        "requires_manifest_hash_match_for_submission",
        "work_items_must_be_closed_by_designated_validators",
        "does_not_create_production_eligibility",
        "does_not_create_live_order_authority",
    ):
        if contract.get(key) is not True:
            failures.append(f"evidence remediation work order contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required_boundary in (
        "remediation_work_order_is_not_validation_pass",
        "work_order_completion_requires_rerun_formal_validators",
        "partial_work_items_cannot_be_packaged_as_production_evidence",
        "work_order_does_not_create_broker_or_execution_authority",
    ):
        if required_boundary not in boundaries:
            failures.append(f"evidence remediation work order missing boundary: {required_boundary}")
    return failures


def validate_remediation_closure_submission_file() -> list[str]:
    evidence_path = os.getenv(REMEDIATION_CLOSURE_SUBMISSION_ENV, "").strip()
    if not evidence_path:
        return [f"remediation closure submission requires {REMEDIATION_CLOSURE_SUBMISSION_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"remediation closure submission missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"remediation closure submission invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_remediation_closure_submission.v1":
        failures.append("remediation closure submission artifact_version invalid")
    status = str(payload.get("closure_submission_status") or "")
    ready = status == "remediation_closure_submission_ready"
    blocked = status == "remediation_closure_submission_blocked"
    if not ready and not blocked:
        failures.append(f"remediation closure submission status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("remediation closure submission attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("remediation closure submission attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("remediation closure submission attempted live order authority")
    if not str(payload.get("source_work_order_hash") or "").strip():
        failures.append("remediation closure submission missing source work order hash")
    if not str(payload.get("source_manifest_hash") or "").strip():
        failures.append("remediation closure submission missing source manifest hash")
    if not str(payload.get("closure_submission_hash") or "").strip():
        failures.append("remediation closure submission hash missing")
    items = payload.get("item_closures") if isinstance(payload.get("item_closures"), list) else []
    if ready:
        if payload.get("passed") is not True:
            failures.append("ready remediation closure submission not passed")
        if not items:
            failures.append("ready remediation closure submission missing item closures")
        if payload.get("blocking_reasons"):
            failures.append("ready remediation closure submission contains blocking reasons")
        if payload.get("allowed_next_actions") != ["submit_to_remediation_closure_review"]:
            failures.append("ready remediation closure submission next action invalid")
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked remediation closure submission marked passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked remediation closure submission missing blocking reasons")
    for item in items:
        if not isinstance(item, dict):
            failures.append("remediation closure submission item invalid")
            continue
        for key in ("artifact", "owner_role", "validator_tool", "closure_status"):
            if not item.get(key):
                failures.append(f"remediation closure submission item missing: {key}")
        if item.get("closed") is True:
            if not item.get("validator_artifact_hash"):
                failures.append(f"closed remediation closure submission item missing validator artifact hash: {item.get('artifact')}")
            if item.get("validator_passed") is not True:
                failures.append(f"closed remediation closure submission item missing validator pass: {item.get('artifact')}")
        elif ready:
            failures.append(f"ready remediation closure submission contains open item: {item.get('artifact')}")
    contract = payload.get("closure_submission_contract") if isinstance(payload.get("closure_submission_contract"), dict) else {}
    for key in (
        "requires_work_order_hash_match",
        "requires_source_manifest_hash_match",
        "requires_designated_validator_artifacts",
        "does_not_create_production_eligibility",
        "does_not_create_live_order_authority",
    ):
        if contract.get(key) is not True:
            failures.append(f"remediation closure submission contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required_boundary in (
        "closure_submission_is_not_remediation_closure_review",
        "closure_submission_is_not_formal_validation_pass",
        "partial_closure_submissions_cannot_advance_release_chain",
        "closure_submission_does_not_create_production_or_live_order_authority",
    ):
        if required_boundary not in boundaries:
            failures.append(f"remediation closure submission missing boundary: {required_boundary}")
    return failures


def validate_remediation_closure_review_file() -> list[str]:
    evidence_path = os.getenv(REMEDIATION_CLOSURE_REVIEW_ENV, "").strip()
    if not evidence_path:
        return [f"remediation closure review requires {REMEDIATION_CLOSURE_REVIEW_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"remediation closure review missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"remediation closure review invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_remediation_closure_review.v1":
        failures.append("remediation closure review artifact_version invalid")
    status = str(payload.get("closure_review_status") or "")
    accepted = status == "remediation_closure_accepted_for_rerun"
    blocked = status == "remediation_closure_blocked"
    if not accepted and not blocked:
        failures.append(f"remediation closure review status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("remediation closure review attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("remediation closure review attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("remediation closure review attempted live order authority")
    if not str(payload.get("source_work_order_hash") or "").strip():
        failures.append("remediation closure review missing source work order hash")
    if not str(payload.get("source_manifest_hash") or "").strip():
        failures.append("remediation closure review missing source manifest hash")
    if not str(payload.get("closure_review_hash") or "").strip():
        failures.append("remediation closure review hash missing")
    reviews = payload.get("closure_reviews") if isinstance(payload.get("closure_reviews"), list) else []
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked remediation closure review marked passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked remediation closure review missing blocking reasons")
    if accepted:
        if payload.get("passed") is not True:
            failures.append("accepted remediation closure review status inconsistent")
        for item in reviews:
            if isinstance(item, dict) and item.get("closed") is not True:
                failures.append(f"accepted remediation closure contains open item: {item.get('artifact')}")
        if payload.get("allowed_next_actions") != ["rerun_formal_validators_then_rebuild_manifest_and_court_of_record"]:
            failures.append("accepted remediation closure next action invalid")
    for item in reviews:
        if not isinstance(item, dict):
            failures.append("remediation closure review item invalid")
            continue
        for key in ("artifact", "owner_role", "validator_tool", "closure_status"):
            if not item.get(key):
                failures.append(f"remediation closure review item missing: {key}")
        if item.get("closed") is True and not item.get("validator_artifact_hash"):
            failures.append(f"closed remediation item missing validator artifact hash: {item.get('artifact')}")
    contract = payload.get("closure_review_contract") if isinstance(payload.get("closure_review_contract"), dict) else {}
    for key in (
        "requires_work_order_hash_match",
        "requires_source_manifest_hash_match",
        "requires_designated_validator_artifacts",
        "accepted_closure_only_allows_formal_rerun",
        "does_not_create_production_eligibility",
        "does_not_create_live_order_authority",
    ):
        if contract.get(key) is not True:
            failures.append(f"remediation closure review contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required_boundary in (
        "closure_review_is_not_formal_validation_pass",
        "accepted_closure_review_requires_rerun_of_formal_validators",
        "closure_review_cannot_create_production_or_live_order_authority",
        "partial_closure_submission_cannot_advance_release_chain",
    ):
        if required_boundary not in boundaries:
            failures.append(f"remediation closure review missing boundary: {required_boundary}")
    return failures


def validate_formal_rerun_plan_file() -> list[str]:
    evidence_path = os.getenv(FORMAL_RERUN_PLAN_ENV, "").strip()
    if not evidence_path:
        return [f"formal rerun plan requires {FORMAL_RERUN_PLAN_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"formal rerun plan missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"formal rerun plan invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_formal_rerun_plan.v1":
        failures.append("formal rerun plan artifact_version invalid")
    status = str(payload.get("rerun_plan_status") or "")
    ready = status == "formal_rerun_plan_ready"
    blocked = status == "formal_rerun_plan_blocked"
    if not ready and not blocked:
        failures.append(f"formal rerun plan status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("formal rerun plan attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("formal rerun plan attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("formal rerun plan attempted live order authority")
    if not str(payload.get("source_closure_review_hash") or "").strip():
        failures.append("formal rerun plan missing source closure review hash")
    if not str(payload.get("source_manifest_hash") or "").strip():
        failures.append("formal rerun plan missing source manifest hash")
    if not str(payload.get("rerun_plan_hash") or "").strip():
        failures.append("formal rerun plan hash missing")
    steps = payload.get("rerun_steps") if isinstance(payload.get("rerun_steps"), list) else []
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked formal rerun plan marked passed")
        if steps:
            failures.append("blocked formal rerun plan contains rerun steps")
        if not payload.get("blocking_reasons"):
            failures.append("blocked formal rerun plan missing blocking reasons")
    if ready:
        if payload.get("passed") is not True:
            failures.append("ready formal rerun plan status inconsistent")
        if len(steps) < 6:
            failures.append("ready formal rerun plan has too few steps")
        expected = [
            "shadow_execution_evidence",
            "independent_validation",
            "operational_controls",
            "competition_audit_rerun",
            "production_readiness",
            "release_chain_adjudication",
            "formal_validation_result_review",
            "evidence_chain_manifest",
            "release_chain_recheck",
        ]
        if [str(item.get("step") or "") for item in steps] != expected:
            failures.append("ready formal rerun plan step order invalid")
    for item in steps:
        if not isinstance(item, dict):
            failures.append("formal rerun plan step invalid")
            continue
        for key in ("step", "command", "requires_closure_review_hash", "requires_source_manifest_hash"):
            if not item.get(key):
                failures.append(f"formal rerun plan step missing: {key}")
        if item.get("output_must_be_collected") is not True or item.get("passed_output_required_before_next_step") is not True:
            failures.append(f"formal rerun plan step contract invalid: {item.get('step')}")
    contract = payload.get("rerun_plan_contract") if isinstance(payload.get("rerun_plan_contract"), dict) else {}
    for key in (
        "requires_accepted_remediation_closure_review",
        "requires_closure_review_hash_match",
        "requires_fixed_rerun_order",
        "each_step_output_must_pass_before_next_step",
        "does_not_create_production_eligibility",
        "does_not_create_live_order_authority",
    ):
        if contract.get(key) is not True:
            failures.append(f"formal rerun plan contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required_boundary in (
        "formal_rerun_plan_is_not_validator_pass",
        "ready_rerun_plan_only_allows_sequential_validator_execution",
        "formal_rerun_outputs_must_rebuild_manifest_and_court_of_record",
        "formal_rerun_plan_cannot_create_production_or_live_order_authority",
    ):
        if required_boundary not in boundaries:
            failures.append(f"formal rerun plan missing boundary: {required_boundary}")
    return failures


def validate_formal_rerun_output_submission_file() -> list[str]:
    evidence_path = os.getenv(FORMAL_RERUN_OUTPUT_SUBMISSION_ENV, "").strip()
    if not evidence_path:
        return [f"formal rerun output submission requires {FORMAL_RERUN_OUTPUT_SUBMISSION_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"formal rerun output submission missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"formal rerun output submission invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_formal_rerun_output_submission.v1":
        failures.append("formal rerun output submission artifact_version invalid")
    status = str(payload.get("rerun_output_submission_status") or "")
    ready = status == "formal_rerun_output_submission_ready"
    blocked = status == "formal_rerun_output_submission_blocked"
    if not ready and not blocked:
        failures.append(f"formal rerun output submission status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("formal rerun output submission attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("formal rerun output submission attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("formal rerun output submission attempted live order authority")
    if not str(payload.get("source_rerun_plan_hash") or "").strip():
        failures.append("formal rerun output submission missing source rerun plan hash")
    if not str(payload.get("source_manifest_hash") or "").strip():
        failures.append("formal rerun output submission missing source manifest hash")
    if not str(payload.get("rerun_output_submission_hash") or "").strip():
        failures.append("formal rerun output submission hash missing")
    reviews = payload.get("rerun_outputs") if isinstance(payload.get("rerun_outputs"), list) else []
    if ready:
        if payload.get("passed") is not True:
            failures.append("ready formal rerun output submission not passed")
        if not reviews:
            failures.append("ready formal rerun output submission missing outputs")
        if payload.get("blocking_reasons"):
            failures.append("ready formal rerun output submission contains blocking reasons")
        if payload.get("allowed_next_actions") != ["submit_to_formal_rerun_result_review"]:
            failures.append("accepted formal rerun output next action invalid")
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked formal rerun output submission marked passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked formal rerun output submission missing blocking reasons")
    for item in reviews:
        if not isinstance(item, dict):
            failures.append("formal rerun output submission item invalid")
            continue
        for key in ("step", "command", "output_status"):
            if not item.get(key):
                failures.append(f"formal rerun output submission item missing: {key}")
        if item.get("passed") is True and not item.get("artifact"):
            failures.append(f"passed formal rerun output missing artifact: {item.get('step')}")
        if item.get("passed") is True and not item.get("artifact_hash"):
            failures.append(f"passed formal rerun output missing artifact hash: {item.get('step')}")
        if item.get("passed") is True and item.get("output_status") != "passed":
            failures.append(f"passed formal rerun output status invalid: {item.get('step')}")
    contract = payload.get("rerun_output_submission_contract") if isinstance(payload.get("rerun_output_submission_contract"), dict) else {}
    for key in (
        "requires_ready_formal_rerun_plan",
        "requires_fixed_step_order",
        "requires_each_output_payload_passed",
        "does_not_create_production_eligibility",
        "does_not_create_live_order_authority",
    ):
        if contract.get(key) is not True:
            failures.append(f"formal rerun output submission contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required_boundary in (
        "formal_rerun_output_submission_is_not_result_review",
        "partial_rerun_outputs_cannot_be_packaged_as_review",
        "formal_rerun_output_submission_does_not_create_production_or_live_order_authority",
    ):
        if required_boundary not in boundaries:
            failures.append(f"formal rerun output submission missing boundary: {required_boundary}")
    return failures


def validate_rerun_court_rebuild_submission_file() -> list[str]:
    evidence_path = os.getenv(RERUN_COURT_REBUILD_SUBMISSION_ENV, "").strip()
    if not evidence_path:
        return [f"rerun court rebuild submission requires {RERUN_COURT_REBUILD_SUBMISSION_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"rerun court rebuild submission missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"rerun court rebuild submission invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_rerun_court_rebuild_submission.v1":
        failures.append("rerun court rebuild submission artifact_version invalid")
    status = str(payload.get("rerun_court_rebuild_submission_status") or "")
    ready = status == "rerun_court_rebuild_submission_ready"
    blocked = status == "rerun_court_rebuild_submission_blocked"
    if not ready and not blocked:
        failures.append(f"rerun court rebuild submission status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("rerun court rebuild submission attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("rerun court rebuild submission attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("rerun court rebuild submission attempted live order authority")
    if not str(payload.get("source_rerun_result_review_hash") or "").strip():
        failures.append("rerun court rebuild submission missing source rerun result review hash")
    if not str(payload.get("rerun_court_rebuild_submission_hash") or "").strip():
        failures.append("rerun court rebuild submission hash missing")
    items = payload.get("court_rebuild_inputs") if isinstance(payload.get("court_rebuild_inputs"), list) else []
    if ready:
        if payload.get("passed") is not True:
            failures.append("ready rerun court rebuild submission not passed")
        if len(items) < 2:
            failures.append("ready rerun court rebuild submission missing inputs")
        if payload.get("blocking_reasons"):
            failures.append("ready rerun court rebuild submission contains blocking reasons")
        if payload.get("allowed_next_actions") != ["submit_to_rerun_court_rebuild_review"]:
            failures.append("ready rerun court rebuild submission next action invalid")
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked rerun court rebuild submission marked passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked rerun court rebuild submission missing blocking reasons")
    for item in items:
        if not isinstance(item, dict):
            failures.append("rerun court rebuild submission item invalid")
            continue
        for key in ("name", "artifact", "artifact_hash", "status"):
            if not item.get(key):
                failures.append(f"rerun court rebuild submission item missing: {key}")
        if item.get("passed") is True and item.get("blocking_reasons"):
            failures.append(f"passed rerun court rebuild submission item contains blocking reasons: {item.get('name')}")
    contract = payload.get("court_rebuild_submission_contract") if isinstance(payload.get("court_rebuild_submission_contract"), dict) else {}
    for key in (
        "requires_accepted_formal_rerun_result_review",
        "requires_rebuilt_manifest_and_release_chain",
        "requires_rerun_result_review_hash_lineage",
        "does_not_create_production_eligibility",
        "does_not_create_live_order_authority",
    ):
        if contract.get(key) is not True:
            failures.append(f"rerun court rebuild submission contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required_boundary in (
        "rerun_court_rebuild_submission_is_not_rerun_court_rebuild_review",
        "partial_rerun_court_rebuild_inputs_cannot_be_packaged_as_review",
        "rerun_court_rebuild_submission_does_not_create_production_or_live_order_authority",
    ):
        if required_boundary not in boundaries:
            failures.append(f"rerun court rebuild submission missing boundary: {required_boundary}")
    return failures


def validate_formal_rerun_result_review_file() -> list[str]:
    evidence_path = os.getenv(FORMAL_RERUN_RESULT_REVIEW_ENV, "").strip()
    if not evidence_path:
        return [f"formal rerun result review requires {FORMAL_RERUN_RESULT_REVIEW_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"formal rerun result review missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"formal rerun result review invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_formal_rerun_result_review.v1":
        failures.append("formal rerun result review artifact_version invalid")
    status = str(payload.get("rerun_result_review_status") or "")
    accepted = status == "formal_rerun_results_accepted"
    blocked = status == "formal_rerun_results_blocked"
    if not accepted and not blocked:
        failures.append(f"formal rerun result review status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("formal rerun result review attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("formal rerun result review attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("formal rerun result review attempted live order authority")
    if not str(payload.get("source_rerun_plan_hash") or "").strip():
        failures.append("formal rerun result review missing source rerun plan hash")
    if not str(payload.get("source_manifest_hash") or "").strip():
        failures.append("formal rerun result review missing source manifest hash")
    if not str(payload.get("rerun_result_review_hash") or "").strip():
        failures.append("formal rerun result review hash missing")
    reviews = payload.get("step_reviews") if isinstance(payload.get("step_reviews"), list) else []
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked formal rerun result review marked passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked formal rerun result review missing blocking reasons")
    if accepted:
        if payload.get("passed") is not True:
            failures.append("accepted formal rerun result review status inconsistent")
        if not reviews:
            failures.append("accepted formal rerun result review missing step reviews")
        for item in reviews:
            if isinstance(item, dict) and item.get("passed") is not True:
                failures.append(f"accepted formal rerun result contains failed step: {item.get('step')}")
        if payload.get("allowed_next_actions") != ["rebuild_evidence_chain_manifest_and_release_chain_court_of_record"]:
            failures.append("accepted formal rerun result next action invalid")
    for item in reviews:
        if not isinstance(item, dict):
            failures.append("formal rerun result step review invalid")
            continue
        for key in ("step", "command", "step_status"):
            if not item.get(key):
                failures.append(f"formal rerun result step missing: {key}")
        if item.get("passed") is True and not item.get("artifact_hash"):
            failures.append(f"passed formal rerun step missing artifact hash: {item.get('step')}")
    contract = payload.get("rerun_result_review_contract") if isinstance(payload.get("rerun_result_review_contract"), dict) else {}
    for key in (
        "requires_ready_formal_rerun_plan",
        "requires_rerun_plan_hash_match",
        "requires_all_step_outputs_in_fixed_order",
        "requires_each_step_payload_passed",
        "does_not_create_production_eligibility",
        "does_not_create_live_order_authority",
    ):
        if contract.get(key) is not True:
            failures.append(f"formal rerun result review contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required_boundary in (
        "formal_rerun_result_review_is_not_release_approval",
        "accepted_rerun_results_require_manifest_and_court_of_record_rebuild",
        "partial_rerun_outputs_cannot_advance_release_chain",
        "rerun_result_review_cannot_create_production_or_live_order_authority",
    ):
        if required_boundary not in boundaries:
            failures.append(f"formal rerun result review missing boundary: {required_boundary}")
    return failures


def validate_rerun_court_rebuild_review_file() -> list[str]:
    evidence_path = os.getenv(RERUN_COURT_REBUILD_REVIEW_ENV, "").strip()
    if not evidence_path:
        return [f"rerun court rebuild review requires {RERUN_COURT_REBUILD_REVIEW_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"rerun court rebuild review missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"rerun court rebuild review invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_rerun_court_rebuild_review.v1":
        failures.append("rerun court rebuild review artifact_version invalid")
    status = str(payload.get("court_rebuild_status") or "")
    accepted = status == "rerun_court_rebuild_accepted"
    blocked = status == "rerun_court_rebuild_blocked"
    if not accepted and not blocked:
        failures.append(f"rerun court rebuild review status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("rerun court rebuild review attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("rerun court rebuild review attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("rerun court rebuild review attempted live order authority")
    if not str(payload.get("source_rerun_result_review_hash") or "").strip():
        failures.append("rerun court rebuild review missing source rerun result hash")
    if not str(payload.get("court_rebuild_review_hash") or "").strip():
        failures.append("rerun court rebuild review hash missing")
    reviews = payload.get("artifact_reviews") if isinstance(payload.get("artifact_reviews"), list) else []
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked rerun court rebuild review marked passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked rerun court rebuild review missing blocking reasons")
    if accepted:
        if payload.get("passed") is not True:
            failures.append("accepted rerun court rebuild status inconsistent")
        if len(reviews) < 2:
            failures.append("accepted rerun court rebuild missing artifact reviews")
        for item in reviews:
            if isinstance(item, dict) and item.get("passed") is not True:
                failures.append(f"accepted rerun court rebuild contains failed artifact: {item.get('name')}")
        if payload.get("allowed_next_actions") != ["proceed_to_release_chain_adjudication_then_human_release_review"]:
            failures.append("accepted rerun court rebuild next action invalid")
    for item in reviews:
        if not isinstance(item, dict):
            failures.append("rerun court rebuild artifact review invalid")
            continue
        for key in ("name", "status"):
            if not item.get(key):
                failures.append(f"rerun court rebuild artifact review missing: {key}")
        if item.get("passed") is True:
            for key in ("artifact", "artifact_hash"):
                if not item.get(key):
                    failures.append(f"passed rerun court rebuild artifact review missing: {key}")
    contract = payload.get("court_rebuild_contract") if isinstance(payload.get("court_rebuild_contract"), dict) else {}
    for key in (
        "requires_accepted_formal_rerun_result_review",
        "requires_rebuilt_evidence_chain_manifest",
        "requires_rebuilt_release_chain_adjudication",
        "requires_rerun_result_hash_lineage",
        "does_not_create_production_eligibility",
        "does_not_create_live_order_authority",
    ):
        if contract.get(key) is not True:
            failures.append(f"rerun court rebuild review contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required_boundary in (
        "rerun_court_rebuild_review_is_not_release_approval",
        "rebuilt_manifest_or_release_chain_cannot_create_live_order_authority",
        "rerun_court_rebuild_requires_accepted_rerun_results",
        "production_still_requires_release_chain_and_human_approval",
    ):
        if required_boundary not in boundaries:
            failures.append(f"rerun court rebuild review missing boundary: {required_boundary}")
    return failures


def validate_post_rerun_release_readiness_submission_file() -> list[str]:
    evidence_path = os.getenv(POST_RERUN_RELEASE_READINESS_SUBMISSION_ENV, "").strip()
    if not evidence_path:
        return [f"post-rerun release readiness submission requires {POST_RERUN_RELEASE_READINESS_SUBMISSION_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"post-rerun release readiness submission missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"post-rerun release readiness submission invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_post_rerun_release_readiness_submission.v1":
        failures.append("post-rerun release readiness submission artifact_version invalid")
    status = str(payload.get("release_readiness_submission_status") or "")
    ready = status == "post_rerun_release_readiness_submission_ready"
    blocked = status == "post_rerun_release_readiness_submission_blocked"
    if not ready and not blocked:
        failures.append(f"post-rerun release readiness submission status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("post-rerun release readiness submission attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("post-rerun release readiness submission attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("post-rerun release readiness submission attempted live order authority")
    if not str(payload.get("source_rerun_court_rebuild_review_hash") or "").strip():
        failures.append("post-rerun release readiness submission missing source court hash")
    if not str(payload.get("source_release_chain_adjudication_hash") or "").strip():
        failures.append("post-rerun release readiness submission missing source release hash")
    if not str(payload.get("source_human_release_approval_hash") or "").strip():
        failures.append("post-rerun release readiness submission missing source human approval hash")
    if not str(payload.get("release_readiness_submission_hash") or "").strip():
        failures.append("post-rerun release readiness submission hash missing")
    items = payload.get("release_readiness_inputs") if isinstance(payload.get("release_readiness_inputs"), list) else []
    if ready:
        if payload.get("passed") is not True:
            failures.append("ready post-rerun release readiness submission not passed")
        if not items:
            failures.append("ready post-rerun release readiness submission missing inputs")
        if payload.get("blocking_reasons"):
            failures.append("ready post-rerun release readiness submission contains blocking reasons")
        if payload.get("allowed_next_actions") != ["submit_to_post_rerun_release_readiness_review"]:
            failures.append("ready post-rerun release readiness submission next action invalid")
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked post-rerun release readiness submission marked passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked post-rerun release readiness submission missing blocking reasons")
    for item in items:
        if not isinstance(item, dict):
            failures.append("post-rerun release readiness submission item invalid")
            continue
        for key in ("name", "artifact", "artifact_hash", "artifact_version", "status", "passed"):
            if key not in item:
                failures.append(f"post-rerun release readiness submission item missing: {key}")
        if item.get("passed") is True and item.get("blocking_reasons"):
            failures.append(f"passed post-rerun release readiness submission item contains blocking reasons: {item.get('name')}")
    contract = payload.get("release_readiness_submission_contract") if isinstance(payload.get("release_readiness_submission_contract"), dict) else {}
    for key in (
        "requires_accepted_rerun_court_rebuild_review",
        "requires_release_chain_passed_for_human_approval",
        "requires_human_release_approved",
        "requires_rerun_court_hash_lineage",
        "does_not_create_live_order_authority",
        "does_not_submit_broker_orders",
    ):
        if contract.get(key) is not True:
            failures.append(f"post-rerun release readiness submission contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required_boundary in (
        "post_rerun_release_readiness_submission_is_not_release_readiness_review",
        "post_rerun_release_readiness_submission_is_not_live_order_authority",
        "post_rerun_release_readiness_submission_does_not_submit_broker_orders",
        "post_rerun_release_readiness_submission_cannot_create_production_eligibility",
    ):
        if required_boundary not in boundaries:
            failures.append(f"post-rerun release readiness submission missing boundary: {required_boundary}")
    return failures


def validate_post_rerun_release_readiness_file() -> list[str]:
    evidence_path = os.getenv(POST_RERUN_RELEASE_READINESS_ENV, "").strip()
    if not evidence_path:
        return [f"post-rerun release readiness requires {POST_RERUN_RELEASE_READINESS_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"post-rerun release readiness missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"post-rerun release readiness invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_post_rerun_release_readiness.v1":
        failures.append("post-rerun release readiness artifact_version invalid")
    status = str(payload.get("release_readiness_status") or "")
    ready = status == "post_rerun_release_ready_for_live_authority_check"
    blocked = status == "post_rerun_release_blocked"
    if not ready and not blocked:
        failures.append(f"post-rerun release readiness status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("post-rerun release readiness attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("post-rerun release readiness attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("post-rerun release readiness attempted live order authority")
    if not str(payload.get("source_rerun_court_rebuild_review_hash") or "").strip():
        failures.append("post-rerun release readiness missing source court hash")
    if str(payload.get("source_release_readiness_submission_artifact") or "").strip() and not str(payload.get("source_release_readiness_submission_hash") or "").strip():
        failures.append("post-rerun release readiness missing source submission hash")
    if not str(payload.get("release_readiness_hash") or "").strip():
        failures.append("post-rerun release readiness hash missing")
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked post-rerun release readiness marked passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked post-rerun release readiness missing blocking reasons")
    if ready:
        if payload.get("passed") is not True:
            failures.append("ready post-rerun release readiness status inconsistent")
        if payload.get("allowed_next_actions") != ["run_live_order_authority_check_with_matching_human_release_hash"]:
            failures.append("ready post-rerun release readiness next action invalid")
        if not str(payload.get("release_chain_adjudication_hash") or "").strip():
            failures.append("ready post-rerun release readiness missing release hash")
        if not str(payload.get("human_release_approval_hash") or "").strip():
            failures.append("ready post-rerun release readiness missing human approval hash")
    contract = payload.get("release_readiness_contract") if isinstance(payload.get("release_readiness_contract"), dict) else {}
    for key in (
        "requires_accepted_rerun_court_rebuild_review",
        "requires_release_chain_passed_for_human_approval",
        "requires_human_release_approved",
        "requires_rerun_court_hash_lineage",
        "does_not_create_live_order_authority",
        "does_not_submit_broker_orders",
    ):
        if contract.get(key) is not True:
            failures.append(f"post-rerun release readiness contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required_boundary in (
        "post_rerun_release_readiness_is_not_live_order_authority",
        "human_release_approval_still_requires_live_order_authority_check",
        "release_readiness_cannot_submit_broker_orders",
        "live_trading_requires_separate_authority_broker_execution_and_post_trade_chain",
    ):
        if required_boundary not in boundaries:
            failures.append(f"post-rerun release readiness missing boundary: {required_boundary}")
    return failures


def validate_post_rerun_live_authority_submission_file() -> list[str]:
    evidence_path = os.getenv(POST_RERUN_LIVE_AUTHORITY_SUBMISSION_ENV, "").strip()
    if not evidence_path:
        return [f"post-rerun live authority submission requires {POST_RERUN_LIVE_AUTHORITY_SUBMISSION_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"post-rerun live authority submission missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"post-rerun live authority submission invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_post_rerun_live_authority_submission.v1":
        failures.append("post-rerun live authority submission artifact_version invalid")
    status = str(payload.get("live_authority_submission_status") or "")
    ready = status == "post_rerun_live_authority_submission_ready"
    blocked = status == "post_rerun_live_authority_submission_blocked"
    if not ready and not blocked:
        failures.append(f"post-rerun live authority submission status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("post-rerun live authority submission attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("post-rerun live authority submission attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("post-rerun live authority submission attempted live authority")
    if payload.get("broker_submission_allowed") is True:
        failures.append("post-rerun live authority submission attempted broker submission")
    if not str(payload.get("source_post_rerun_release_readiness_hash") or "").strip():
        failures.append("post-rerun live authority submission missing source readiness hash")
    if not str(payload.get("source_live_order_authority_hash") or "").strip():
        failures.append("post-rerun live authority submission missing source authority hash")
    if not str(payload.get("live_authority_submission_hash") or "").strip():
        failures.append("post-rerun live authority submission hash missing")
    items = payload.get("live_authority_inputs") if isinstance(payload.get("live_authority_inputs"), list) else []
    if ready:
        if payload.get("passed") is not True:
            failures.append("ready post-rerun live authority submission not passed")
        if not items:
            failures.append("ready post-rerun live authority submission missing inputs")
        if payload.get("blocking_reasons"):
            failures.append("ready post-rerun live authority submission contains blocking reasons")
        if payload.get("allowed_next_actions") != ["submit_to_post_rerun_live_authority_review"]:
            failures.append("ready post-rerun live authority submission next action invalid")
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked post-rerun live authority submission marked passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked post-rerun live authority submission missing blocking reasons")
    for item in items:
        if not isinstance(item, dict):
            failures.append("post-rerun live authority submission item invalid")
            continue
        for key in ("name", "artifact", "artifact_hash", "artifact_version", "status", "passed"):
            if key not in item:
                failures.append(f"post-rerun live authority submission item missing: {key}")
    contract = payload.get("live_authority_submission_contract") if isinstance(payload.get("live_authority_submission_contract"), dict) else {}
    for key in (
        "requires_post_rerun_release_readiness_ready",
        "requires_live_order_authority_allowed",
        "requires_readiness_hash_lineage",
        "does_not_create_new_live_order_authority",
        "does_not_call_broker_adapter",
    ):
        if contract.get(key) is not True:
            failures.append(f"post-rerun live authority submission contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required_boundary in (
        "post_rerun_live_authority_submission_is_not_live_authority_review",
        "live_authority_submission_does_not_execute_orders",
        "live_authority_submission_cannot_create_broker_permission",
        "live_trading_requires_separate_broker_guard_and_execution_chain",
    ):
        if required_boundary not in boundaries:
            failures.append(f"post-rerun live authority submission missing boundary: {required_boundary}")
    return failures


def validate_post_rerun_broker_guard_submission_file() -> list[str]:
    evidence_path = os.getenv(POST_RERUN_BROKER_GUARD_SUBMISSION_ENV, "").strip()
    if not evidence_path:
        return [f"post-rerun broker guard submission requires {POST_RERUN_BROKER_GUARD_SUBMISSION_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"post-rerun broker guard submission missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"post-rerun broker guard submission invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_post_rerun_broker_guard_submission.v1":
        failures.append("post-rerun broker guard submission artifact_version invalid")
    status = str(payload.get("broker_guard_submission_status") or "")
    ready = status == "post_rerun_broker_guard_submission_ready"
    blocked = status == "post_rerun_broker_guard_submission_blocked"
    if not ready and not blocked:
        failures.append(f"post-rerun broker guard submission status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("post-rerun broker guard submission attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("post-rerun broker guard submission attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("post-rerun broker guard submission attempted live authority")
    if payload.get("broker_submission_confirmed") is True:
        failures.append("post-rerun broker guard submission attempted broker confirmation")
    if payload.get("execution_fills_confirmed") is True:
        failures.append("post-rerun broker guard submission attempted fill confirmation")
    if not str(payload.get("source_post_rerun_live_authority_review_hash") or "").strip():
        failures.append("post-rerun broker guard submission missing source live authority review hash")
    if not str(payload.get("source_broker_submission_guard_hash") or "").strip():
        failures.append("post-rerun broker guard submission missing source guard hash")
    if not str(payload.get("broker_guard_submission_hash") or "").strip():
        failures.append("post-rerun broker guard submission hash missing")
    items = payload.get("broker_guard_inputs") if isinstance(payload.get("broker_guard_inputs"), list) else []
    if ready:
        if payload.get("passed") is not True:
            failures.append("ready post-rerun broker guard submission not passed")
        if not items:
            failures.append("ready post-rerun broker guard submission missing inputs")
        if payload.get("blocking_reasons"):
            failures.append("ready post-rerun broker guard submission contains blocking reasons")
        if payload.get("allowed_next_actions") != ["submit_to_post_rerun_broker_guard_review"]:
            failures.append("ready post-rerun broker guard submission next action invalid")
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked post-rerun broker guard submission marked passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked post-rerun broker guard submission missing blocking reasons")
    for item in items:
        if not isinstance(item, dict):
            failures.append("post-rerun broker guard submission item invalid")
            continue
        for key in ("name", "artifact", "artifact_hash", "artifact_version", "status", "passed"):
            if key not in item:
                failures.append(f"post-rerun broker guard submission item missing: {key}")
    contract = payload.get("broker_guard_submission_contract") if isinstance(payload.get("broker_guard_submission_contract"), dict) else {}
    for key in (
        "requires_post_rerun_live_authority_review_ready",
        "requires_broker_submission_guard_passed",
        "requires_live_authority_review_hash_lineage",
        "does_not_call_broker_adapter",
        "does_not_confirm_submission_or_fills",
    ):
        if contract.get(key) is not True:
            failures.append(f"post-rerun broker guard submission contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required_boundary in (
        "post_rerun_broker_guard_submission_is_not_broker_guard_review",
        "broker_guard_submission_does_not_confirm_submission",
        "broker_response_execution_feedback_and_post_trade_reconciliation_remain_required",
        "submitted_or_guard_passed_does_not_equal_filled",
    ):
        if required_boundary not in boundaries:
            failures.append(f"post-rerun broker guard submission missing boundary: {required_boundary}")
    return failures


def validate_post_rerun_live_authority_review_file() -> list[str]:
    evidence_path = os.getenv(POST_RERUN_LIVE_AUTHORITY_REVIEW_ENV, "").strip()
    if not evidence_path:
        return [f"post-rerun live authority review requires {POST_RERUN_LIVE_AUTHORITY_REVIEW_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"post-rerun live authority review missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"post-rerun live authority review invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_post_rerun_live_authority_review.v1":
        failures.append("post-rerun live authority review artifact_version invalid")
    status = str(payload.get("live_authority_review_status") or "")
    ready = status == "post_rerun_live_authority_ready_for_broker_guard"
    blocked = status == "post_rerun_live_authority_blocked"
    if not ready and not blocked:
        failures.append(f"post-rerun live authority review status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("post-rerun live authority review attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("post-rerun live authority review attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("post-rerun live authority review attempted live order grant")
    if payload.get("broker_submission_allowed") is True:
        failures.append("post-rerun live authority review attempted broker permission")
    if not str(payload.get("source_post_rerun_release_readiness_hash") or "").strip():
        failures.append("post-rerun live authority review missing readiness hash")
    if not str(payload.get("live_authority_review_hash") or "").strip():
        failures.append("post-rerun live authority review hash missing")
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked post-rerun live authority review marked passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked post-rerun live authority review missing blocking reasons")
    if ready:
        if payload.get("passed") is not True:
            failures.append("ready post-rerun live authority review status inconsistent")
        if payload.get("allowed_next_actions") != ["run_broker_submission_guard_with_matching_live_authority_hash"]:
            failures.append("ready post-rerun live authority next action invalid")
        if not str(payload.get("live_order_authority_hash") or "").strip():
            failures.append("ready post-rerun live authority missing authority hash")
        if int(payload.get("order_count") or 0) <= 0:
            failures.append("ready post-rerun live authority missing orders")
    contract = payload.get("live_authority_review_contract") if isinstance(payload.get("live_authority_review_contract"), dict) else {}
    for key in (
        "requires_post_rerun_release_readiness_ready",
        "requires_live_order_authority_allowed",
        "requires_readiness_hash_lineage",
        "does_not_create_new_live_order_authority",
        "does_not_call_broker_adapter",
    ):
        if contract.get(key) is not True:
            failures.append(f"post-rerun live authority review contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required_boundary in (
        "post_rerun_live_authority_review_is_not_broker_submission",
        "live_authority_review_does_not_execute_orders",
        "broker_submission_requires_separate_guard_and_response",
        "execution_and_post_trade_feedback_remain_required",
    ):
        if required_boundary not in boundaries:
            failures.append(f"post-rerun live authority review missing boundary: {required_boundary}")
    return failures


def validate_post_rerun_release_chain_adjudication_file() -> list[str]:
    evidence_path = os.getenv(POST_RERUN_RELEASE_CHAIN_ADJUDICATION_ENV, "").strip()
    if not evidence_path:
        return [f"post-rerun release chain adjudication requires {POST_RERUN_RELEASE_CHAIN_ADJUDICATION_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"post-rerun release chain adjudication missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"post-rerun release chain adjudication invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_post_rerun_release_chain_adjudication.v1":
        failures.append("post-rerun release chain adjudication artifact_version invalid")
    status = str(payload.get("release_chain_status") or "")
    ready = status == "post_rerun_release_chain_ready_for_broker_guard"
    blocked = status == "post_rerun_release_chain_blocked"
    if not ready and not blocked:
        failures.append(f"post-rerun release chain adjudication status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("post-rerun release chain adjudication attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("post-rerun release chain adjudication attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("post-rerun release chain adjudication attempted live order grant")
    if payload.get("broker_submission_allowed") is True:
        failures.append("post-rerun release chain adjudication attempted broker permission")
    if not str(payload.get("source_post_rerun_release_readiness_hash") or "").strip():
        failures.append("post-rerun release chain adjudication missing readiness hash")
    if not str(payload.get("release_chain_hash") or "").strip():
        failures.append("post-rerun release chain adjudication hash missing")
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked post-rerun release chain adjudication marked passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked post-rerun release chain adjudication missing blocking reasons")
    if ready:
        if payload.get("passed") is not True:
            failures.append("ready post-rerun release chain adjudication status inconsistent")
        if payload.get("allowed_next_actions") != ["run_broker_submission_guard_with_matching_live_authority_hash"]:
            failures.append("ready post-rerun release chain next action invalid")
    contract = payload.get("release_chain_contract") if isinstance(payload.get("release_chain_contract"), dict) else {}
    for key in (
        "requires_post_rerun_release_readiness_ready",
        "requires_post_rerun_live_authority_review_ready",
        "requires_readiness_hash_lineage",
        "does_not_create_new_live_order_authority",
        "does_not_call_broker_adapter",
    ):
        if contract.get(key) is not True:
            failures.append(f"post-rerun release chain adjudication contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required_boundary in (
        "post_rerun_release_chain_adjudication_is_not_broker_submission",
        "release_chain_adjudication_does_not_execute_orders",
        "broker_submission_requires_separate_guard_and_response",
        "execution_and_post_trade_feedback_remain_required",
    ):
        if required_boundary not in boundaries:
            failures.append(f"post-rerun release chain adjudication missing boundary: {required_boundary}")
    return failures


def validate_post_rerun_broker_guard_review_file() -> list[str]:
    evidence_path = os.getenv(POST_RERUN_BROKER_GUARD_REVIEW_ENV, "").strip()
    if not evidence_path:
        return [f"post-rerun broker guard review requires {POST_RERUN_BROKER_GUARD_REVIEW_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"post-rerun broker guard review missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"post-rerun broker guard review invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_post_rerun_broker_guard_review.v1":
        failures.append("post-rerun broker guard review artifact_version invalid")
    status = str(payload.get("broker_guard_review_status") or "")
    ready = status == "post_rerun_broker_guard_ready_for_adapter"
    blocked = status == "post_rerun_broker_guard_blocked"
    if not ready and not blocked:
        failures.append(f"post-rerun broker guard review status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("post-rerun broker guard review attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("post-rerun broker guard review attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("post-rerun broker guard review attempted live order grant")
    if payload.get("broker_submission_allowed") is True:
        failures.append("post-rerun broker guard review attempted broker permission")
    if payload.get("broker_submission_confirmed") is True:
        failures.append("post-rerun broker guard review attempted submission confirmation")
    if payload.get("execution_fills_confirmed") is True:
        failures.append("post-rerun broker guard review attempted fill confirmation")
    if not str(payload.get("source_post_rerun_live_authority_review_hash") or "").strip():
        failures.append("post-rerun broker guard review missing live authority review hash")
    if not str(payload.get("broker_guard_review_hash") or "").strip():
        failures.append("post-rerun broker guard review hash missing")
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked post-rerun broker guard review marked passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked post-rerun broker guard review missing blocking reasons")
    if ready:
        if payload.get("passed") is not True:
            failures.append("ready post-rerun broker guard review status inconsistent")
        if payload.get("allowed_next_actions") != ["call_broker_adapter_then_record_broker_submission_response"]:
            failures.append("ready post-rerun broker guard next action invalid")
        if not str(payload.get("broker_submission_guard_hash") or "").strip():
            failures.append("ready post-rerun broker guard missing guard hash")
    contract = payload.get("broker_guard_review_contract") if isinstance(payload.get("broker_guard_review_contract"), dict) else {}
    for key in (
        "requires_post_rerun_live_authority_review_ready",
        "requires_broker_submission_guard_passed",
        "requires_live_authority_review_hash_lineage",
        "does_not_call_broker_adapter",
        "does_not_confirm_submission_or_fills",
    ):
        if contract.get(key) is not True:
            failures.append(f"post-rerun broker guard review contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required_boundary in (
        "post_rerun_broker_guard_review_is_not_broker_response",
        "broker_guard_review_does_not_confirm_submission",
        "broker_response_execution_feedback_and_post_trade_reconciliation_remain_required",
        "submitted_or_guard_passed_does_not_equal_filled",
    ):
        if required_boundary not in boundaries:
            failures.append(f"post-rerun broker guard review missing boundary: {required_boundary}")
    return failures


def validate_post_rerun_broker_response_review_file() -> list[str]:
    evidence_path = os.getenv(POST_RERUN_BROKER_RESPONSE_REVIEW_ENV, "").strip()
    if not evidence_path:
        return [f"post-rerun broker response review requires {POST_RERUN_BROKER_RESPONSE_REVIEW_ENV}"]
    path = Path(evidence_path)
    if not path.exists():
        return [f"post-rerun broker response review missing: {evidence_path}"]
    try:
        payload = _load_json_file(path)
    except Exception as exc:
        return [f"post-rerun broker response review invalid: {exc}"]

    failures: list[str] = []
    if str(payload.get("artifact_version") or "") != "strategy_competition_post_rerun_broker_response_review.v1":
        failures.append("post-rerun broker response review artifact_version invalid")
    status = str(payload.get("broker_response_review_status") or "")
    ready = status == "post_rerun_broker_response_ready_for_execution_feedback"
    blocked = status == "post_rerun_broker_response_blocked"
    if not ready and not blocked:
        failures.append(f"post-rerun broker response review status invalid: {status}")
    if payload.get("production_candidate_allowed") is True:
        failures.append("post-rerun broker response review attempted production eligibility")
    if payload.get("production_release_authorized") is True:
        failures.append("post-rerun broker response review attempted release authorization")
    if payload.get("live_order_authority_granted") is True:
        failures.append("post-rerun broker response review attempted live order grant")
    if payload.get("execution_fills_confirmed") is True:
        failures.append("post-rerun broker response review attempted fill confirmation")
    if payload.get("post_trade_reconciliation_passed") is True:
        failures.append("post-rerun broker response review attempted post-trade completion")
    if not str(payload.get("source_post_rerun_broker_guard_review_hash") or "").strip():
        failures.append("post-rerun broker response review missing guard review hash")
    if not str(payload.get("broker_response_review_hash") or "").strip():
        failures.append("post-rerun broker response review hash missing")
    if blocked:
        if payload.get("passed") is True:
            failures.append("blocked post-rerun broker response review marked passed")
        if not payload.get("blocking_reasons"):
            failures.append("blocked post-rerun broker response review missing blocking reasons")
        if payload.get("broker_submission_confirmed") is True:
            failures.append("blocked post-rerun broker response review confirmed submission")
    if ready:
        if payload.get("passed") is not True:
            failures.append("ready post-rerun broker response review status inconsistent")
        if payload.get("allowed_next_actions") != ["record_broker_execution_feedback_with_matching_response_hash"]:
            failures.append("ready post-rerun broker response next action invalid")
        if payload.get("broker_submission_confirmed") is not True:
            failures.append("ready post-rerun broker response missing submission confirmation")
        if not str(payload.get("broker_submission_response_evidence_hash") or "").strip():
            failures.append("ready post-rerun broker response missing response evidence hash")
    contract = payload.get("broker_response_review_contract") if isinstance(payload.get("broker_response_review_contract"), dict) else {}
    for key in (
        "requires_post_rerun_broker_guard_review_ready",
        "requires_broker_submission_response_accepted",
        "requires_broker_guard_hash_lineage",
        "does_not_confirm_fills",
        "does_not_create_post_trade_reconciliation",
    ):
        if contract.get(key) is not True:
            failures.append(f"post-rerun broker response review contract missing: {key}")
    boundaries = json.dumps(payload.get("hard_boundaries") or [], ensure_ascii=False)
    for required_boundary in (
        "post_rerun_broker_response_review_is_not_execution_feedback",
        "broker_submission_confirmed_does_not_mean_filled",
        "fills_require_separate_execution_feedback",
        "post_trade_reconciliation_remains_required_after_execution_feedback",
    ):
        if required_boundary not in boundaries:
            failures.append(f"post-rerun broker response review missing boundary: {required_boundary}")
    return failures


def _validate_predeclared_repair_flow(flow: dict, *, oos_plan: dict, failures: list[str]) -> None:
    if not str(flow.get("rule_hash") or "").strip():
        failures.append("predeclared repair flow missing rule hash")
    if not str(flow.get("rule_version") or "").strip():
        failures.append("predeclared repair flow missing rule version")
    if not flow.get("predeclared_rules_json"):
        failures.append("predeclared repair flow missing predeclared rules")
    if not flow.get("fixed_window_set_json"):
        failures.append("predeclared repair flow missing fixed repair windows")
    prohibited = json.dumps(flow.get("prohibited_actions_json") or flow.get("prohibited_actions") or [], ensure_ascii=False)
    for required in ("formal", "top", "production"):
        if required not in prohibited:
            failures.append(f"predeclared repair flow missing prohibited action: {required}")
    artifacts = [item for item in flow.get("artifacts") or [] if isinstance(item, dict)]
    artifact_types = {str(item.get("artifact_type") or "") for item in artifacts}
    if "failure_attribution" not in artifact_types:
        failures.append("predeclared repair flow missing failure attribution artifact")
    if not oos_plan:
        failures.append("predeclared repair flow missing OOS monitoring plan")
        return
    if str(oos_plan.get("rule_hash") or "") != str(flow.get("rule_hash") or ""):
        failures.append("predeclared repair OOS plan rule hash mismatch")
    if not oos_plan.get("oos_windows"):
        failures.append("predeclared repair OOS plan missing windows")
    paired = oos_plan.get("paired_run_requirement") if isinstance(oos_plan.get("paired_run_requirement"), dict) else {}
    if paired.get("unthrottled_required") is not True or paired.get("throttled_required") is not True:
        failures.append("predeclared repair OOS plan missing paired unthrottled/throttled requirement")
    pass_conditions = oos_plan.get("pass_conditions") if isinstance(oos_plan.get("pass_conditions"), dict) else {}
    if pass_conditions.get("block_level_watch_risk_allowed") is not False:
        failures.append("predeclared repair OOS plan allows block-level watch risk")


def _load_if_present(path: Path | None, label: str, failures: list[str]) -> dict:
    if path is None:
        return {}
    try:
        return _load_json_file(path)
    except Exception as exc:
        failures.append(f"{label} invalid: {exc}")
        return {}


def run_repair_flow_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_REPAIR_FLOW_GATE", "").strip() == "1"
    if not enabled and not _strategy_optimization_scope_changed(changed_paths):
        return []
    return validate_repair_flow_evidence_file()


def run_strategy_competition_audit_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_STRATEGY_COMPETITION_AUDIT_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_audit_service.py")
        or path.startswith("openclaw/services/strategy_competition_shadow_execution_service.py")
        or path.startswith("openclaw/services/strategy_competition_shadow_feedback_service.py")
        or path.startswith("openclaw/services/strategy_competition_independent_validation_service.py")
        or path.startswith("openclaw/services/strategy_competition_evidence_intake_service.py")
        or path.startswith("openclaw/services/strategy_competition_operational_controls_service.py")
        or path.startswith("openclaw/services/strategy_competition_production_readiness_service.py")
        or path.startswith("openclaw/services/strategy_competition_formal_validation_handoff_service.py")
        or path.startswith("openclaw/services/strategy_competition_release_chain_adjudication_service.py")
        or path.startswith("tools/strategy_competition_portfolio_audit.py")
        or path.startswith("tools/build_current_strategy_competition_audit.py")
        or path.startswith("tools/build_strategy_competition_shadow_execution_plan.py")
        or path.startswith("tools/record_strategy_competition_shadow_feedback.py")
        or path.startswith("tools/build_strategy_competition_independent_validation.py")
        or path.startswith("tools/build_strategy_competition_evidence_intake_packet.py")
        or path.startswith("tools/review_strategy_competition_evidence_submission.py")
        or path.startswith("tools/build_strategy_competition_operational_controls.py")
        or path.startswith("tools/build_strategy_competition_production_readiness.py")
        or path.startswith("tools/build_strategy_competition_formal_validation_handoff.py")
        or path.startswith("tools/review_strategy_competition_formal_validation_results.py")
        or path.startswith("tools/adjudicate_strategy_competition_release_chain.py")
        or path.startswith("tools/build_strategy_competition_human_release_approval.py")
        or path.startswith("tools/check_strategy_competition_live_order_authority.py")
        or path.startswith("tools/check_strategy_competition_broker_submission_guard.py")
        or path.startswith("tools/review_strategy_competition_broker_submission_response.py")
        or path.startswith("tools/review_strategy_competition_broker_execution_feedback.py")
        or path.startswith("tools/reconcile_strategy_competition_post_trade.py")
        or path.startswith("tools/adjudicate_strategy_competition_trade_lifecycle.py")
        for path in changed_paths
    ):
        return []
    return validate_competition_audit_evidence_file()


def run_production_readiness_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_PRODUCTION_READINESS_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_production_readiness_service.py")
        or path.startswith("tools/build_strategy_competition_production_readiness.py")
        for path in changed_paths
    ):
        return []
    return validate_production_readiness_evidence_file()


def run_evidence_submission_review_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_EVIDENCE_SUBMISSION_REVIEW_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_evidence_intake_service.py")
        or path.startswith("tools/review_strategy_competition_evidence_submission.py")
        for path in changed_paths
    ):
        return []
    return validate_evidence_submission_review_file()


def run_release_chain_adjudication_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_RELEASE_CHAIN_ADJUDICATION_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_release_chain_adjudication_service.py")
        or path.startswith("tools/adjudicate_strategy_competition_release_chain.py")
        for path in changed_paths
    ):
        return []
    return validate_release_chain_adjudication_file()


def run_formal_validation_handoff_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_FORMAL_VALIDATION_HANDOFF_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_formal_validation_handoff_service.py")
        or path.startswith("tools/build_strategy_competition_formal_validation_handoff.py")
        for path in changed_paths
    ):
        return []
    return validate_formal_validation_handoff_file()


def run_formal_validation_result_review_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_FORMAL_VALIDATION_RESULT_REVIEW_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_formal_validation_handoff_service.py")
        or path.startswith("tools/review_strategy_competition_formal_validation_results.py")
        for path in changed_paths
    ):
        return []
    return validate_formal_validation_result_review_file()


def run_human_release_approval_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_HUMAN_RELEASE_APPROVAL_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_human_release_approval_service.py")
        or path.startswith("tools/build_strategy_competition_human_release_approval.py")
        for path in changed_paths
    ):
        return []
    return validate_human_release_approval_file()


def run_live_order_authority_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_LIVE_ORDER_AUTHORITY_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_live_order_authority_service.py")
        or path.startswith("tools/check_strategy_competition_live_order_authority.py")
        for path in changed_paths
    ):
        return []
    return validate_live_order_authority_file()


def run_broker_submission_guard_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_BROKER_SUBMISSION_GUARD_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_broker_submission_guard_service.py")
        or path.startswith("tools/check_strategy_competition_broker_submission_guard.py")
        for path in changed_paths
    ):
        return []
    return validate_broker_submission_guard_file()


def run_broker_submission_response_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_BROKER_SUBMISSION_RESPONSE_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_broker_submission_response_service.py")
        or path.startswith("tools/review_strategy_competition_broker_submission_response.py")
        for path in changed_paths
    ):
        return []
    return validate_broker_submission_response_file()


def run_broker_execution_feedback_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_BROKER_EXECUTION_FEEDBACK_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_broker_execution_feedback_service.py")
        or path.startswith("tools/review_strategy_competition_broker_execution_feedback.py")
        for path in changed_paths
    ):
        return []
    return validate_broker_execution_feedback_file()


def run_post_trade_reconciliation_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_POST_TRADE_RECONCILIATION_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_post_trade_reconciliation_service.py")
        or path.startswith("tools/reconcile_strategy_competition_post_trade.py")
        for path in changed_paths
    ):
        return []
    return validate_post_trade_reconciliation_file()


def run_trade_lifecycle_adjudication_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_TRADE_LIFECYCLE_ADJUDICATION_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_trade_lifecycle_adjudication_service.py")
        or path.startswith("tools/adjudicate_strategy_competition_trade_lifecycle.py")
        for path in changed_paths
    ):
        return []
    return validate_trade_lifecycle_adjudication_file()


def run_evidence_chain_manifest_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_EVIDENCE_CHAIN_MANIFEST_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_evidence_chain_manifest_service.py")
        or path.startswith("tools/build_strategy_competition_evidence_chain_manifest.py")
        for path in changed_paths
    ):
        return []
    return validate_evidence_chain_manifest_file()


def run_evidence_remediation_work_order_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_EVIDENCE_REMEDIATION_WORK_ORDER_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_evidence_remediation_work_order_service.py")
        or path.startswith("tools/build_strategy_competition_evidence_remediation_work_order.py")
        for path in changed_paths
    ):
        return []
    return validate_evidence_remediation_work_order_file()


def run_remediation_closure_submission_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_REMEDIATION_CLOSURE_SUBMISSION_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_remediation_closure_submission_service.py")
        or path.startswith("tools/build_strategy_competition_remediation_closure_submission.py")
        for path in changed_paths
    ):
        return []
    return validate_remediation_closure_submission_file()


def run_remediation_closure_review_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_REMEDIATION_CLOSURE_REVIEW_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_remediation_closure_review_service.py")
        or path.startswith("tools/review_strategy_competition_remediation_closure.py")
        for path in changed_paths
    ):
        return []
    return validate_remediation_closure_review_file()


def run_formal_rerun_plan_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_FORMAL_RERUN_PLAN_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_formal_rerun_plan_service.py")
        or path.startswith("tools/build_strategy_competition_formal_rerun_plan.py")
        for path in changed_paths
    ):
        return []
    return validate_formal_rerun_plan_file()


def run_formal_rerun_output_submission_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_FORMAL_RERUN_OUTPUT_SUBMISSION_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_formal_rerun_output_submission_service.py")
        or path.startswith("tools/build_strategy_competition_formal_rerun_output_submission.py")
        for path in changed_paths
    ):
        return []
    return validate_formal_rerun_output_submission_file()


def run_rerun_court_rebuild_submission_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_RERUN_COURT_REBUILD_SUBMISSION_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_rerun_court_rebuild_submission_service.py")
        or path.startswith("tools/build_strategy_competition_rerun_court_rebuild_submission.py")
        for path in changed_paths
    ):
        return []
    return validate_rerun_court_rebuild_submission_file()


def run_formal_rerun_result_review_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_FORMAL_RERUN_RESULT_REVIEW_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_formal_rerun_result_review_service.py")
        or path.startswith("tools/review_strategy_competition_formal_rerun_results.py")
        for path in changed_paths
    ):
        return []
    return validate_formal_rerun_result_review_file()


def run_rerun_court_rebuild_review_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_RERUN_COURT_REBUILD_REVIEW_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_rerun_court_rebuild_review_service.py")
        or path.startswith("tools/review_strategy_competition_rerun_court_rebuild.py")
        for path in changed_paths
    ):
        return []
    return validate_rerun_court_rebuild_review_file()


def run_post_rerun_release_readiness_submission_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_POST_RERUN_RELEASE_READINESS_SUBMISSION_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_post_rerun_release_readiness_submission_service.py")
        or path.startswith("tools/build_strategy_competition_post_rerun_release_readiness_submission.py")
        for path in changed_paths
    ):
        return []
    return validate_post_rerun_release_readiness_submission_file()


def run_post_rerun_release_readiness_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_POST_RERUN_RELEASE_READINESS_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_post_rerun_release_readiness_service.py")
        or path.startswith("tools/review_strategy_competition_post_rerun_release_readiness.py")
        for path in changed_paths
    ):
        return []
    return validate_post_rerun_release_readiness_file()


def run_post_rerun_live_authority_review_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_POST_RERUN_LIVE_AUTHORITY_REVIEW_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_post_rerun_live_authority_review_service.py")
        or path.startswith("tools/review_strategy_competition_post_rerun_live_authority.py")
        for path in changed_paths
    ):
        return []
    return validate_post_rerun_live_authority_review_file()


def run_post_rerun_live_authority_submission_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_POST_RERUN_LIVE_AUTHORITY_SUBMISSION_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_post_rerun_live_authority_submission_service.py")
        or path.startswith("tools/build_strategy_competition_post_rerun_live_authority_submission.py")
        for path in changed_paths
    ):
        return []
    return validate_post_rerun_live_authority_submission_file()


def run_post_rerun_broker_guard_submission_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_POST_RERUN_BROKER_GUARD_SUBMISSION_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_post_rerun_broker_guard_submission_service.py")
        or path.startswith("tools/build_strategy_competition_post_rerun_broker_guard_submission.py")
        for path in changed_paths
    ):
        return []
    return validate_post_rerun_broker_guard_submission_file()


def run_post_rerun_release_chain_adjudication_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_POST_RERUN_RELEASE_CHAIN_ADJUDICATION_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_post_rerun_release_chain_adjudication_service.py")
        or path.startswith("tools/adjudicate_strategy_competition_post_rerun_release_chain.py")
        for path in changed_paths
    ):
        return []
    return validate_post_rerun_release_chain_adjudication_file()


def run_post_rerun_broker_guard_review_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_POST_RERUN_BROKER_GUARD_REVIEW_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_post_rerun_broker_guard_review_service.py")
        or path.startswith("tools/review_strategy_competition_post_rerun_broker_guard.py")
        for path in changed_paths
    ):
        return []
    return validate_post_rerun_broker_guard_review_file()


def run_post_rerun_broker_response_review_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_POST_RERUN_BROKER_RESPONSE_REVIEW_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_post_rerun_broker_response_review_service.py")
        or path.startswith("tools/review_strategy_competition_post_rerun_broker_response.py")
        for path in changed_paths
    ):
        return []
    return validate_post_rerun_broker_response_review_file()


def run_post_rerun_broker_execution_feedback_review_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_POST_RERUN_BROKER_EXECUTION_FEEDBACK_REVIEW_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_post_rerun_broker_execution_feedback_review_service.py")
        or path.startswith("tools/review_strategy_competition_post_rerun_broker_execution_feedback.py")
        for path in changed_paths
    ):
        return []
    return validate_post_rerun_broker_execution_feedback_review_file()


def run_post_rerun_post_trade_reconciliation_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_POST_RERUN_POST_TRADE_RECONCILIATION_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_post_rerun_post_trade_reconciliation_service.py")
        or path.startswith("tools/reconcile_strategy_competition_post_rerun_post_trade.py")
        for path in changed_paths
    ):
        return []
    return validate_post_rerun_post_trade_reconciliation_file()


def run_post_rerun_trade_lifecycle_adjudication_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_POST_RERUN_TRADE_LIFECYCLE_ADJUDICATION_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_post_rerun_trade_lifecycle_adjudication_service.py")
        or path.startswith("tools/adjudicate_strategy_competition_post_rerun_trade_lifecycle.py")
        for path in changed_paths
    ):
        return []
    return validate_post_rerun_trade_lifecycle_adjudication_file()


def run_post_rerun_evidence_chain_manifest_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_POST_RERUN_EVIDENCE_CHAIN_MANIFEST_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_post_rerun_evidence_chain_manifest_service.py")
        or path.startswith("tools/build_strategy_competition_post_rerun_evidence_chain_manifest.py")
        for path in changed_paths
    ):
        return []
    return validate_post_rerun_evidence_chain_manifest_file()


def run_post_rerun_human_release_approval_review_gate(changed_paths: Iterable[str] = ()) -> list[str]:
    enabled = os.getenv("AIRIVO_ENABLE_POST_RERUN_HUMAN_RELEASE_APPROVAL_REVIEW_GATE", "").strip() == "1"
    if not enabled and not any(
        path.startswith("openclaw/services/strategy_competition_post_rerun_human_release_approval_review_service.py")
        or path.startswith("tools/review_strategy_competition_post_rerun_human_release_approval_review.py")
        for path in changed_paths
    ):
        return []
    return validate_post_rerun_human_release_approval_review_file()


def main() -> int:
    parser = argparse.ArgumentParser(description="Governance gate for repo hygiene and mainline discipline.")
    parser.add_argument("--base", help="git diff base sha")
    parser.add_argument("--head", help="git diff head sha")
    parser.add_argument("--all-files", action="store_true", help="scan tracked files when diff context is unavailable")
    args = parser.parse_args()

    entries = changed_entries(args.base, args.head, args.all_files)
    changed_paths = [path for _, path in entries]

    failures: list[str] = []
    warnings: list[str] = []

    for status, path in entries:
        rel = Path(path)
        suffix = rel.suffix.lower()

        if is_root_level(path) and is_added_status(status):
            if suffix in ROOT_BLOCKED_SUFFIXES and path not in ALLOWED_NEW_ROOT_FILES:
                failures.append(
                    f"root-level addition blocked: {path} "
                    f"(move it under mainline dirs, tools subdirs, archive/, or experiments/)"
                )

        full_path = ROOT / rel
        if not full_path.exists() or suffix != ".py":
            continue

        count = line_count(full_path)
        if count < 0:
            continue
        if is_added_status(status) and count > FAIL_NEW_FILE_LINE_THRESHOLD:
            failures.append(
                f"new python file too large: {path} has {count} lines "
                f"(split before merge; threshold={FAIL_NEW_FILE_LINE_THRESHOLD})"
            )
        elif count > WARN_LINE_THRESHOLD:
            warnings.append(
                f"large python file reminder: {path} has {count} lines "
                f"(high-risk file, avoid adding new responsibilities)"
            )

    migration_changed = has_prefix(changed_paths, "data/migrations/")
    kernel_changed = has_prefix(changed_paths, "trading_kernel/")
    docs_changed = has_docs(changed_paths)
    tests_changed = has_prefix(changed_paths, "tests/")
    governance_sensitive_changed = has_rule_match(changed_paths, GOVERNANCE_SENSITIVE_PATHS)

    for required in REQUIRED_MAINLINE_FILES:
        if not (ROOT / required).exists():
            failures.append(f"required mainline authority file missing: {required}")

    if migration_changed and not docs_changed:
        failures.append("migration changed without docs update under docs/")
    if migration_changed and not tests_changed:
        failures.append("migration changed without tests update under tests/")
    if kernel_changed and not tests_changed:
        failures.append("trading_kernel changed without tests update under tests/")
    if governance_sensitive_changed and not docs_changed:
        failures.append("governance or release paths changed without docs update under docs/")
    failures.extend(run_strategy_optimization_stage_gate(changed_paths))
    failures.extend(run_execution_attribution_hygiene_gate(changed_paths))
    failures.extend(run_repair_flow_gate(changed_paths))
    failures.extend(run_strategy_competition_audit_gate(changed_paths))
    failures.extend(run_evidence_submission_review_gate(changed_paths))
    failures.extend(run_production_readiness_gate(changed_paths))
    failures.extend(run_formal_validation_handoff_gate(changed_paths))
    failures.extend(run_formal_validation_result_review_gate(changed_paths))
    failures.extend(run_release_chain_adjudication_gate(changed_paths))
    failures.extend(run_human_release_approval_gate(changed_paths))
    failures.extend(run_live_order_authority_gate(changed_paths))
    failures.extend(run_broker_submission_guard_gate(changed_paths))
    failures.extend(run_broker_submission_response_gate(changed_paths))
    failures.extend(run_broker_execution_feedback_gate(changed_paths))
    failures.extend(run_post_trade_reconciliation_gate(changed_paths))
    failures.extend(run_trade_lifecycle_adjudication_gate(changed_paths))
    failures.extend(run_evidence_chain_manifest_gate(changed_paths))
    failures.extend(run_evidence_remediation_work_order_gate(changed_paths))
    failures.extend(run_remediation_closure_submission_gate(changed_paths))
    failures.extend(run_remediation_closure_review_gate(changed_paths))
    failures.extend(run_formal_rerun_plan_gate(changed_paths))
    failures.extend(run_formal_rerun_output_submission_gate(changed_paths))
    failures.extend(run_rerun_court_rebuild_submission_gate(changed_paths))
    failures.extend(run_formal_rerun_result_review_gate(changed_paths))
    failures.extend(run_rerun_court_rebuild_review_gate(changed_paths))
    failures.extend(run_post_rerun_release_readiness_submission_gate(changed_paths))
    failures.extend(run_post_rerun_release_readiness_gate(changed_paths))
    failures.extend(run_post_rerun_live_authority_submission_gate(changed_paths))
    failures.extend(run_post_rerun_live_authority_review_gate(changed_paths))
    failures.extend(run_post_rerun_broker_guard_submission_gate(changed_paths))
    failures.extend(run_post_rerun_release_chain_adjudication_gate(changed_paths))
    failures.extend(run_post_rerun_broker_guard_review_gate(changed_paths))
    failures.extend(run_post_rerun_broker_response_review_gate(changed_paths))
    failures.extend(run_post_rerun_broker_execution_feedback_review_gate(changed_paths))
    failures.extend(run_post_rerun_post_trade_reconciliation_gate(changed_paths))
    failures.extend(run_post_rerun_trade_lifecycle_adjudication_gate(changed_paths))
    failures.extend(run_post_rerun_evidence_chain_manifest_gate(changed_paths))
    failures.extend(run_post_rerun_human_release_approval_review_gate(changed_paths))

    print("[governance] mandate:", MANDATE_DOC)
    print("[governance] delivery standard:", DELIVERY_STANDARD_DOC)
    print("[governance] execution plan:", EXECUTION_PLAN_DOC)
    print("[governance] adjudication checklist:", ADJUDICATION_DOC)
    print("[governance] strategy optimization plan:", STRATEGY_OPTIMIZATION_DOC)
    print("[governance] strategy optimization audit tool:", STRATEGY_OPTIMIZATION_STAGE_AUDIT_TOOL)
    print("[governance] execution attribution hygiene tool:", EXECUTION_ATTRIBUTION_BACKFILL_TOOL)
    print("[governance] strategy competition audit tool:", STRATEGY_COMPETITION_AUDIT_TOOL)
    print("[governance] current strategy competition audit tool:", CURRENT_STRATEGY_COMPETITION_AUDIT_TOOL)
    print("[governance] strategy competition shadow execution plan tool:", STRATEGY_COMPETITION_SHADOW_EXECUTION_PLAN_TOOL)
    print("[governance] rejected backtest artifacts tool:", REJECTED_BACKTEST_ARTIFACTS_TOOL)
    print("[governance] repair flow evidence env:", REPAIR_FLOW_EVIDENCE_ENV)
    print("[governance] competition audit evidence env:", COMPETITION_AUDIT_EVIDENCE_ENV)
    print("[governance] rejection standard:", REJECTION_STANDARD_DOC)
    print("[governance] pr template:", PR_TEMPLATE)
    print("[governance] scanned paths:", len(changed_paths))
    for item in warnings:
        print("[governance][warn]", item)
    if failures:
        for item in failures:
            print("[governance][fail]", item)
        return 1
    print("[governance] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
