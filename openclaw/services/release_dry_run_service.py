from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from openclaw.services.data_version_service import build_code_version
from openclaw.services.professional_audit_service import audit_professional_fact_chains


REQUIRED_CHAIN_EVIDENCE = {
    "signal": ("total_runs", "signal_chain_empty"),
    "decision": ("total_events", "decision_chain_empty"),
    "execution": ("total_orders", "execution_chain_empty"),
}

ENVIRONMENT_BLOCKING_PREFIXES = (
    "database_not_found:",
    "missing_table:",
    "payload_parse_failed:",
    "payload_not_object:",
)


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_json_loads(value: Any) -> Dict[str, Any]:
    try:
        parsed = json.loads(str(value or "{}"))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _latest_rollback_reference(conn: sqlite3.Connection) -> Dict[str, Any]:
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='release_events'").fetchone():
        return {"available": False, "reason": "release_events_missing"}
    row = conn.execute(
        """
        SELECT release_id, release_type, code_version, gate_result, payload_json, created_at
        FROM release_events
        WHERE COALESCE(code_version, '') != ''
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return {"available": False, "reason": "no_prior_release_event"}
    gate_result = _safe_json_loads(row[3])
    payload = _safe_json_loads(row[4])
    return {
        "available": True,
        "release_id": str(row[0] or ""),
        "release_type": str(row[1] or ""),
        "code_version": str(row[2] or ""),
        "created_at": str(row[5] or ""),
        "gate_result": gate_result,
        "rollback_context": payload.get("rollback_context") if isinstance(payload.get("rollback_context"), dict) else {},
    }


def _connect_readonly(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"database_not_found:{path}")
    return sqlite3.connect(f"file:{path}?mode=ro", timeout=20, uri=True)


def _evaluate_chain_evidence(audit: Dict[str, Any]) -> List[str]:
    chains = audit.get("chains") if isinstance(audit.get("chains"), dict) else {}
    missing: List[str] = []
    for chain_name, (metric_key, reason) in REQUIRED_CHAIN_EVIDENCE.items():
        chain = chains.get(chain_name) if isinstance(chains.get(chain_name), dict) else {}
        if int(chain.get(metric_key) or 0) <= 0:
            missing.append(reason)
    return missing


def build_release_dry_run_payload(
    *,
    db_path: str | Path,
    code_root: str | Path,
    operator_name: str = "",
) -> Dict[str, Any]:
    try:
        conn = _connect_readonly(db_path)
        try:
            audit = audit_professional_fact_chains(conn)
            rollback_reference = _latest_rollback_reference(conn)
        finally:
            conn.close()
    except FileNotFoundError as exc:
        audit = {
            "passed": False,
            "missing_tables": [],
            "blocking_reasons": [str(exc)],
            "chains": {},
        }
        rollback_reference = {"available": False, "reason": "database_not_found"}

    audit_blocking = [str(reason) for reason in (audit.get("blocking_reasons") or [])]
    evidence_missing = _evaluate_chain_evidence(audit)
    rollback_missing = [] if rollback_reference.get("available") else [f"rollback_reference:{rollback_reference.get('reason') or 'missing'}"]
    blocking_reasons = audit_blocking + evidence_missing + rollback_missing

    validation_statuses = {
        "professional_fact_audit": "passed" if audit.get("passed") is True else "failed",
        "signal_chain_evidence": "passed" if "signal_chain_empty" not in evidence_missing else "failed",
        "decision_chain_evidence": "passed" if "decision_chain_empty" not in evidence_missing else "failed",
        "execution_chain_evidence": "passed" if "execution_chain_empty" not in evidence_missing else "failed",
        "rollback_reference": "passed" if rollback_reference.get("available") else "failed",
        "dry_run_no_side_effects": "passed",
    }
    satisfied = [name for name, status in validation_statuses.items() if status == "passed"]
    unsatisfied = [name for name, status in validation_statuses.items() if status != "passed"]
    allow_release_gate = not blocking_reasons

    return {
        "tool": "tools/release_dry_run_audit.py",
        "dry_run": True,
        "generated_at": _now_text(),
        "operator_name": operator_name,
        "code_version": build_code_version(root=Path(code_root)),
        "allow_release_gate": allow_release_gate,
        "decision": "allow_release_gate" if allow_release_gate else "block_release_gate",
        "blocking_reasons": blocking_reasons,
        "satisfied_validations": satisfied,
        "unsatisfied_validations": unsatisfied,
        "validation_statuses": validation_statuses,
        "professional_fact_audit": {
            "passed": audit.get("passed") is True,
            "missing_tables": audit.get("missing_tables") or [],
            "blocking_reasons": audit_blocking,
            "chains": audit.get("chains") or {},
        },
        "rollback_context": {
            "available": bool(rollback_reference.get("available")),
            "reference": rollback_reference,
            "blocking_reasons": blocking_reasons,
        },
    }


def run_release_dry_run_audit(
    *,
    db_path: str | Path,
    code_root: str | Path,
    output_path: str | Path = "",
    operator_name: str = "",
) -> Dict[str, Any]:
    payload = build_release_dry_run_payload(db_path=db_path, code_root=code_root, operator_name=operator_name)
    if output_path:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return payload


def _iter_payload_files(paths: List[str | Path]) -> List[Path]:
    files: List[Path] = []
    for raw_path in paths:
        path = Path(raw_path)
        if path.is_dir():
            files.extend(sorted(path.glob("*.json")))
        elif path.exists():
            files.append(path)
    return files


def _is_environment_blocking_reason(reason: str) -> bool:
    return any(str(reason or "").startswith(prefix) for prefix in ENVIRONMENT_BLOCKING_PREFIXES)


def _has_rollback_reference(payload: Dict[str, Any]) -> bool:
    rollback_context = payload.get("rollback_context") if isinstance(payload.get("rollback_context"), dict) else {}
    return rollback_context.get("available") is True


def _validation_failed(payload: Dict[str, Any], validation_name: str) -> bool:
    statuses = payload.get("validation_statuses") if isinstance(payload.get("validation_statuses"), dict) else {}
    if validation_name not in statuses:
        return False
    return str(statuses.get(validation_name)) != "passed"


def _trailing_validation_failures(payloads: List[Dict[str, Any]], validation_name: str) -> List[Dict[str, Any]]:
    failures: List[Dict[str, Any]] = []
    for payload in reversed(payloads):
        if not _validation_failed(payload, validation_name):
            break
        failures.append(payload)
    return list(reversed(failures))


def summarize_release_dry_run_trend(
    *,
    payload_paths: List[str | Path],
    output_path: str | Path = "",
    stable_threshold: int = 2,
) -> Dict[str, Any]:
    payload_files = _iter_payload_files(payload_paths)
    reason_counts: Counter[str] = Counter()
    validation_failure_counts: Counter[str] = Counter()
    decisions: Counter[str] = Counter()
    parsed_payloads: List[Dict[str, Any]] = []
    skipped_files: List[str] = []

    for payload_file in payload_files:
        try:
            payload = json.loads(payload_file.read_text(encoding="utf-8"))
        except Exception as exc:
            reason_counts[f"payload_parse_failed:{payload_file.name}:{exc.__class__.__name__}"] += 1
            continue
        if not isinstance(payload, dict):
            reason_counts[f"payload_not_object:{payload_file.name}"] += 1
            continue
        if "allow_release_gate" not in payload or not isinstance(payload.get("validation_statuses"), dict):
            skipped_files.append(str(payload_file))
            continue
        parsed_payloads.append(payload)
        decisions[str(payload.get("decision") or "unknown")] += 1
        for reason in payload.get("blocking_reasons") or []:
            reason_counts[str(reason)] += 1
        validation_statuses = payload.get("validation_statuses") if isinstance(payload.get("validation_statuses"), dict) else {}
        for name, status in validation_statuses.items():
            if str(status) != "passed":
                validation_failure_counts[str(name)] += 1

    total_payloads = len(parsed_payloads)
    blocked_payloads = sum(1 for payload in parsed_payloads if payload.get("allow_release_gate") is not True)
    stable_reasons = [
        {"reason": reason, "count": count}
        for reason, count in sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))
        if count >= max(int(stable_threshold or 1), 1)
    ]
    threshold = max(int(stable_threshold or 1), 1)
    hard_gate_candidates: List[Dict[str, Any]] = []
    for name, count in sorted(validation_failure_counts.items(), key=lambda item: (-item[1], item[0])):
        consecutive_failures = _trailing_validation_failures(parsed_payloads, name)
        if len(consecutive_failures) < threshold:
            continue
        reasons = [
            str(reason)
            for payload in consecutive_failures
            for reason in (payload.get("blocking_reasons") or [])
        ]
        if any(_is_environment_blocking_reason(reason) for reason in reasons):
            continue
        if not all(_has_rollback_reference(payload) for payload in consecutive_failures):
            continue
        hard_gate_candidates.append(
            {
                "validation": name,
                "failed_count": count,
                "consecutive_failures": len(consecutive_failures),
                "rollback_reference_available": True,
            }
        )
    summary = {
        "tool": "tools/release_dry_run_audit.py --trend",
        "dry_run": True,
        "generated_at": _now_text(),
        "total_payload_files": len(payload_files),
        "total_payloads": total_payloads,
        "skipped_non_payload_files": skipped_files,
        "blocked_payloads": blocked_payloads,
        "allowed_payloads": total_payloads - blocked_payloads,
        "decision_counts": dict(sorted(decisions.items())),
        "blocking_reason_counts": dict(sorted(reason_counts.items())),
        "validation_failure_counts": dict(sorted(validation_failure_counts.items())),
        "stable_threshold": threshold,
        "hard_gate_upgrade_policy": {
            "consecutive_failures_required": threshold,
            "environment_blocking_prefixes": list(ENVIRONMENT_BLOCKING_PREFIXES),
            "requires_rollback_reference": True,
            "candidate_scope": "latest_consecutive_payloads",
        },
        "stable_blocking_reasons": stable_reasons,
        "hard_gate_upgrade_candidates": hard_gate_candidates,
        "recommendation": "observe_more_payloads" if not hard_gate_candidates else "review_hard_gate_candidates",
    }
    if output_path:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return summary


def render_release_dry_run_trend_markdown(summary: Dict[str, Any]) -> str:
    candidates = summary.get("hard_gate_upgrade_candidates") if isinstance(summary.get("hard_gate_upgrade_candidates"), list) else []
    stable_reasons = summary.get("stable_blocking_reasons") if isinstance(summary.get("stable_blocking_reasons"), list) else []
    policy = summary.get("hard_gate_upgrade_policy") if isinstance(summary.get("hard_gate_upgrade_policy"), dict) else {}
    lines = [
        "# Airivo Release Dry-Run Trend",
        "",
        f"- recommendation: `{summary.get('recommendation', '')}`",
        f"- total_payloads: `{summary.get('total_payloads', 0)}`",
        f"- allowed_payloads: `{summary.get('allowed_payloads', 0)}`",
        f"- blocked_payloads: `{summary.get('blocked_payloads', 0)}`",
        f"- stable_threshold: `{summary.get('stable_threshold', 0)}`",
        "",
        "## Hard Gate Upgrade Policy",
        f"- candidate_scope: `{policy.get('candidate_scope', '')}`",
        f"- consecutive_failures_required: `{policy.get('consecutive_failures_required', '')}`",
        f"- requires_rollback_reference: `{policy.get('requires_rollback_reference', '')}`",
        "",
        "## Hard Gate Upgrade Candidates",
    ]
    if candidates:
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            lines.append(
                "- "
                f"`{candidate.get('validation', '')}` "
                f"failed_count=`{candidate.get('failed_count', '')}` "
                f"consecutive_failures=`{candidate.get('consecutive_failures', '')}` "
                f"rollback_reference_available=`{candidate.get('rollback_reference_available', '')}`"
            )
    else:
        lines.append("- none")

    lines.extend(["", "## Stable Blocking Reasons"])
    if stable_reasons:
        for item in stable_reasons:
            if isinstance(item, dict):
                lines.append(f"- `{item.get('reason', '')}` count=`{item.get('count', '')}`")
    else:
        lines.append("- none")

    skipped = summary.get("skipped_non_payload_files") if isinstance(summary.get("skipped_non_payload_files"), list) else []
    lines.extend(["", "## Skipped Non-Payload Files"])
    if skipped:
        for path in skipped:
            lines.append(f"- `{path}`")
    else:
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)
