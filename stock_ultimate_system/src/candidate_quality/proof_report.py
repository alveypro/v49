from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CANDIDATE_QUALITY_PROOF_REPORT_VERSION = "candidate_quality_proof_report.v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: str | Path) -> dict[str, Any]:
    resolved = Path(path)
    if not resolved.exists():
        return {"status": "missing", "missing": True}
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"status": "invalid", "error": str(exc)}
    return payload if isinstance(payload, dict) else {"status": "invalid", "error": "payload is not object"}


def _status(payload: dict[str, Any]) -> str:
    return str(payload.get("status") or "unknown")


def _state(statuses: list[str]) -> str:
    lowered = {status.lower() for status in statuses}
    if lowered & {"missing", "invalid", "failed", "blocked"}:
        return "blocked"
    if lowered & {"review", "pending", "unknown"}:
        return "review"
    return "passed"


def build_candidate_quality_proof_report(*, exp_dir: str | Path) -> dict[str, Any]:
    root = Path(exp_dir)
    artifacts = {
        "data_quality": _read_json(root / "data_quality_report_latest.json"),
        "data_quality_gate": _read_json(root / "candidate_data_quality_gate_latest.json"),
        "lineage": _read_json(root / "candidate_lineage_latest.json"),
        "realistic_backtest": _read_json(root / "realistic_backtest_latest.json"),
        "observation_snapshot": _read_json(root / "candidate_observation_snapshot_latest.json"),
        "observation_result": _read_json(root / "candidate_observation_result_latest.json"),
        "failure_attribution": _read_json(root / "candidate_failure_attribution_latest.json"),
        "sample_report": _read_json(root / "candidate_quality_20_sample_report.json"),
        "portfolio": _read_json(root / "candidate_portfolio_latest.json"),
        "portfolio_capacity": _read_json(root / "portfolio_capacity_report_latest.json"),
        "portfolio_quality": _read_json(root / "candidate_portfolio_quality_latest.json"),
        "risk_state": _read_json(root / "candidate_risk_state_latest.json"),
        "public_explanation": _read_json(root / "candidate_public_explanation_latest.json"),
        "internal_explanation": _read_json(root / "candidate_internal_explanation_latest.json"),
        "rejection_explanation": _read_json(root / "candidate_rejection_explanation_latest.json"),
    }
    checks = []
    for name, payload in artifacts.items():
        checks.append(
            {
                "name": name,
                "status": _status(payload),
                "schema_version": payload.get("schema_version"),
                "candidate_count": payload.get("candidate_count")
                or (payload.get("summary", {}) or {}).get("candidate_count"),
                "blocking_reasons": payload.get("blocking_reasons", []),
                "review_reasons": payload.get("review_reasons", []),
            }
        )
    statuses = [str(check["status"]) for check in checks]
    status = _state(statuses)
    sample_count = int(float(artifacts["sample_report"].get("sample_count", 0) or 0))
    quality_score = artifacts["portfolio_quality"].get("quality_score")
    risk_state_counts = artifacts["risk_state"].get("state_counts", {})
    public_items = artifacts["public_explanation"].get("items", []) or []
    normal_public_count = sum(1 for item in public_items if item.get("external_display_allowed"))
    prohibited_claims = []
    if sample_count < 20:
        prohibited_claims.append("candidate_quality_proven")
    if _status(artifacts["portfolio_quality"]) == "blocked":
        prohibited_claims.append("portfolio_quality_passed")
    if normal_public_count == 0:
        prohibited_claims.append("normal_external_watchlist")
    conclusion = "blocked"
    if status == "passed" and sample_count >= 20 and not prohibited_claims:
        conclusion = "passed"
    elif status != "blocked":
        conclusion = "review"
    return {
        "schema_version": CANDIDATE_QUALITY_PROOF_REPORT_VERSION,
        "status": conclusion,
        "generated_at": _utc_now(),
        "summary": {
            "artifact_status": status,
            "sample_count": sample_count,
            "portfolio_quality_score": quality_score,
            "risk_state_counts": risk_state_counts,
            "normal_public_display_count": normal_public_count,
            "rejection_count": artifacts["rejection_explanation"].get("candidate_count", 0),
        },
        "prohibited_claims": prohibited_claims,
        "decision": {
            "quality_proven": conclusion == "passed",
            "external_page_mode": "degraded_review" if normal_public_count == 0 else "normal_watch",
            "message": (
                "候选质量尚未被证明，外部页面只能展示降级复核解释。"
                if conclusion != "passed"
                else "候选质量证明链满足当前发布条件。"
            ),
        },
        "checks": checks,
    }


def write_candidate_quality_proof_report(payload: dict[str, Any], *, output_path: str | Path) -> str:
    resolved = Path(output_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(resolved)
