from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List

from openclaw.services.lineage_service import apply_professional_migrations, canonical_json


JsonDict = Dict[str, Any]

FORMAL_RESULT_STEPS = (
    "shadow_execution_evidence",
    "independent_validation",
    "operational_controls",
    "competition_audit_rerun",
    "production_readiness",
    "release_chain_adjudication",
)

EXPECTED_RESULT_VERSIONS = {
    "shadow_execution_evidence": "strategy_competition_shadow_execution_evidence.v1",
    "independent_validation": "strategy_competition_independent_validation.v1",
    "operational_controls": "strategy_competition_production_operational_controls.v1",
    "competition_audit_rerun": "strategy_competition_portfolio_audit.v1",
    "production_readiness": "strategy_competition_production_readiness.v1",
    "release_chain_adjudication": "strategy_competition_release_chain_adjudication.v1",
}


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _load_json(path: str | Path) -> JsonDict:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"json payload must be object: {path}")
    return payload


def _hash_file(path: str) -> str:
    text = _clean_text(path)
    if not text:
        return ""
    file_path = Path(text)
    if not file_path.exists() or not file_path.is_file():
        return ""
    return sha256(file_path.read_bytes()).hexdigest()


def _as_path_map(value: Any) -> Dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {str(key): _clean_text(path) for key, path in value.items()}


def _source_hash_failures(packet: JsonDict) -> List[str]:
    failures: List[str] = []
    sources = packet.get("source_artifacts") if isinstance(packet.get("source_artifacts"), dict) else {}
    expected = packet.get("source_artifact_hashes") if isinstance(packet.get("source_artifact_hashes"), dict) else {}
    for key, value in sources.items():
        expected_hash = _clean_text(expected.get(key))
        if expected_hash and _hash_file(_clean_text(value)) != expected_hash:
            failures.append(f"source_artifact_hash_mismatch:{key}")
    return failures


def _formal_run_order(*, packet: JsonDict, review: JsonDict, output_root: str) -> List[JsonDict]:
    submitted = review.get("submitted_artifacts") if isinstance(review.get("submitted_artifacts"), dict) else {}
    sources = packet.get("source_artifacts") if isinstance(packet.get("source_artifacts"), dict) else {}
    audit = _clean_text(sources.get("competition_audit"))
    shadow_plan = _clean_text(sources.get("shadow_plan"))
    shadow_feedback = _clean_text(submitted.get("shadow_feedback"))
    independent_decision = _clean_text(submitted.get("independent_validator_decision"))
    controls_input = _clean_text(submitted.get("operational_controls_input"))
    return [
        {
            "step": "shadow_execution_evidence",
            "command": (
                "python3 tools/record_strategy_competition_shadow_feedback.py "
                f"--shadow-plan-artifact {shadow_plan} "
                f"--shadow-feedback-artifact {shadow_feedback} "
                f"--output-dir {output_root}/shadow_execution"
            ),
            "required_input_hashes": {
                "shadow_plan": _hash_file(shadow_plan),
                "shadow_feedback": _hash_file(shadow_feedback),
            },
        },
        {
            "step": "independent_validation",
            "command": (
                "python3 tools/build_strategy_competition_independent_validation.py "
                f"--competition-audit-artifact {audit} "
                f"--validator-decision-artifact {independent_decision} "
                f"--output-dir {output_root}/independent_validation"
            ),
            "required_input_hashes": {
                "competition_audit": _hash_file(audit),
                "independent_validator_decision": _hash_file(independent_decision),
            },
        },
        {
            "step": "operational_controls",
            "command": (
                "python3 tools/build_strategy_competition_operational_controls.py "
                f"--competition-audit-artifact {audit} "
                f"--controls-input-artifact {controls_input} "
                f"--output-dir {output_root}/operational_controls"
            ),
            "required_input_hashes": {
                "competition_audit": _hash_file(audit),
                "operational_controls_input": _hash_file(controls_input),
            },
        },
        {
            "step": "competition_audit_rerun",
            "command": (
                "python3 tools/build_current_strategy_competition_audit.py "
                "--derive-pre-trade-risk-controls "
                "--shadow-execution <passed_shadow_execution_evidence> "
                "--independent-validator <passed_independent_validation> "
                f"--output-dir {output_root}/competition_audit_rerun"
            ),
            "required_input_hashes": {
                "previous_competition_audit": _hash_file(audit),
            },
        },
        {
            "step": "production_readiness",
            "command": (
                "python3 tools/build_strategy_competition_production_readiness.py "
                "--competition-audit-artifact <passed_competition_audit> "
                "--operational-controls-artifact <passed_operational_controls> "
                f"--output-dir {output_root}/production_readiness"
            ),
            "required_input_hashes": {},
        },
        {
            "step": "release_chain_adjudication",
            "command": (
                "python3 tools/adjudicate_strategy_competition_release_chain.py "
                "--competition-audit-artifact <passed_or_blocked_competition_audit> "
                "--shadow-execution-artifact <shadow_execution_evidence> "
                "--independent-validation-artifact <independent_validation> "
                "--operational-controls-artifact <operational_controls> "
                "--evidence-submission-review-artifact <accepted_submission_review> "
                "--production-readiness-artifact <production_readiness> "
                f"--output-dir {output_root}/release_chain_adjudication"
            ),
            "required_input_hashes": {
                "accepted_submission_review": _hash_file(_clean_text(review.get("artifact_path"))),
            },
        },
    ]


def build_strategy_competition_formal_validation_handoff(
    conn: sqlite3.Connection,
    *,
    intake_packet_artifact_path: str,
    evidence_submission_review_artifact_path: str,
    output_dir: str | Path,
    validation_output_root: str = "logs/openclaw/strategy_competition_formal_validation_outputs",
    operator_name: str = "strategy_competition_formal_validation_handoff",
) -> JsonDict:
    """Create a fixed, hash-bound handoff from accepted evidence submission to formal validators."""

    apply_professional_migrations(conn)
    packet = _load_json(intake_packet_artifact_path)
    review = _load_json(evidence_submission_review_artifact_path)
    blocking: List[str] = []
    if packet.get("artifact_version") != "strategy_competition_evidence_intake_packet.v1":
        blocking.append("intake_packet_version_invalid")
    if review.get("artifact_version") != "strategy_competition_evidence_submission_review.v1":
        blocking.append("evidence_submission_review_version_invalid")
    if review.get("review_status") != "evidence_submission_accepted_for_validation" or review.get("passed") is not True:
        blocking.append("evidence_submission_review_not_accepted")
    if review.get("production_candidate_allowed") is True:
        blocking.append("evidence_submission_review_attempted_production_eligibility")
    blocking.extend(_source_hash_failures(packet))
    accepted = not blocking
    run_order = _formal_run_order(packet=packet, review=review, output_root=validation_output_root) if accepted else []
    payload: JsonDict = {
        "artifact_version": "strategy_competition_formal_validation_handoff.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(packet.get("competition_run_id")),
        "handoff_status": "formal_validation_ready" if accepted else "formal_validation_handoff_blocked",
        "passed": accepted,
        "production_candidate_allowed": False,
        "intake_packet_artifact": str(intake_packet_artifact_path or ""),
        "evidence_submission_review_artifact": str(evidence_submission_review_artifact_path or ""),
        "source_artifact_hashes": {
            "intake_packet": _hash_file(str(intake_packet_artifact_path or "")),
            "evidence_submission_review": _hash_file(str(evidence_submission_review_artifact_path or "")),
        },
        "blocking_reasons": blocking,
        "formal_run_order": run_order,
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
    payload["handoff_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "handoff_status": payload["handoff_status"],
                "source_artifact_hashes": payload["source_artifact_hashes"],
                "formal_run_order": payload["formal_run_order"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_formal_validation_handoff_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def build_strategy_competition_formal_validation_result_review(
    conn: sqlite3.Connection,
    *,
    formal_validation_handoff_artifact_path: str,
    formal_result_artifact_paths: Dict[str, str],
    output_dir: str | Path,
    operator_name: str = "strategy_competition_formal_validation_result_review",
) -> JsonDict:
    """Review formal validator outputs against a ready handoff without creating production approval."""

    apply_professional_migrations(conn)
    handoff = _load_json(formal_validation_handoff_artifact_path)
    result_paths = _as_path_map(formal_result_artifact_paths)
    blocking: List[str] = []
    if handoff.get("artifact_version") != "strategy_competition_formal_validation_handoff.v1":
        blocking.append("formal_validation_handoff_version_invalid")
    if handoff.get("handoff_status") != "formal_validation_ready" or handoff.get("passed") is not True:
        blocking.append("formal_validation_handoff_not_ready")
    run_order = [item for item in handoff.get("formal_run_order") or [] if isinstance(item, dict)]
    if [str(item.get("step") or "") for item in run_order] != list(FORMAL_RESULT_STEPS):
        blocking.append("formal_validation_handoff_run_order_invalid")

    result_status: List[JsonDict] = []
    previous_failed = False
    for step in FORMAL_RESULT_STEPS:
        path = result_paths.get(step, "")
        payload = _load_json(path) if path else {}
        step_blocking: List[str] = []
        if previous_failed:
            step_blocking.append("previous_formal_step_not_passed")
        if not path:
            step_blocking.append(f"{step}_artifact_missing")
        elif not Path(path).exists():
            step_blocking.append(f"{step}_artifact_path_missing")
        if payload:
            expected_version = EXPECTED_RESULT_VERSIONS[step]
            if payload.get("artifact_version") != expected_version:
                step_blocking.append(f"{step}_artifact_version_invalid")
            if payload.get("passed") is not True:
                step_blocking.append(f"{step}_not_passed")
            if step == "competition_audit_rerun" and payload.get("production_candidate_allowed") is not True:
                step_blocking.append("competition_audit_rerun_not_production_candidate_allowed")
            if step == "production_readiness" and payload.get("production_release_allowed") is not True:
                step_blocking.append("production_readiness_not_release_allowed")
            if step == "release_chain_adjudication" and payload.get("chain_status") != "release_chain_passed_for_human_approval":
                step_blocking.append("release_chain_adjudication_not_passed_for_human_approval")
        status = {
            "step": step,
            "artifact": path,
            "artifact_hash": _hash_file(path),
            "passed": bool(payload) and not step_blocking,
            "blocking_reasons": step_blocking,
        }
        result_status.append(status)
        blocking.extend(step_blocking)
        if status["passed"] is not True:
            previous_failed = True

    accepted = not blocking
    payload: JsonDict = {
        "artifact_version": "strategy_competition_formal_validation_result_review.v1",
        "created_at": _now_text(),
        "operator_name": operator_name,
        "competition_run_id": _clean_text(handoff.get("competition_run_id")),
        "result_review_status": "formal_validation_results_accepted" if accepted else "formal_validation_results_blocked",
        "passed": accepted,
        "production_candidate_allowed": False,
        "formal_validation_handoff_artifact": str(formal_validation_handoff_artifact_path or ""),
        "formal_validation_handoff_hash": _hash_file(str(formal_validation_handoff_artifact_path or "")),
        "formal_result_artifacts": result_paths,
        "formal_result_status": result_status,
        "blocking_reasons": blocking,
        "result_review_contract": {
            "requires_ready_handoff": True,
            "requires_all_formal_outputs": True,
            "requires_outputs_in_handoff_order": True,
            "requires_release_chain_passed_for_human_approval": True,
            "does_not_create_live_order_authority": True,
        },
        "hard_boundaries": [
            "formal_validation_result_review_is_not_trade_instruction",
            "result_review_does_not_replace_human_release_approval",
            "blocked_formal_result_cannot_advance_to_production",
            "production_requires_separate_human_approval_after_passed_release_chain",
        ],
    }
    payload["result_review_hash"] = sha256(
        canonical_json(
            {
                "competition_run_id": payload["competition_run_id"],
                "result_review_status": payload["result_review_status"],
                "formal_validation_handoff_hash": payload["formal_validation_handoff_hash"],
                "formal_result_status": payload["formal_result_status"],
            }
        ).encode("utf-8")
    ).hexdigest()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"strategy_competition_formal_validation_result_review_{payload['competition_run_id'] or 'unknown'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    payload["artifact_path"] = str(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
