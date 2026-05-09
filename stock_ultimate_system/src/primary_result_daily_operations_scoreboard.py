from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.project_paths import resolve_artifacts_path, resolve_experiments_path, resolve_project_path


PRIMARY_RESULT_DAILY_OPERATIONS_SCOREBOARD_VERSION = "primary_result_daily_operations_scoreboard.v1"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"_read_error": f"invalid json: {path}"}
    return payload if isinstance(payload, dict) else {"_read_error": f"expected object json: {path}"}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _line_count(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def _status(payload: dict[str, Any]) -> str:
    if not payload:
        return "missing"
    if payload.get("_read_error"):
        return "invalid"
    value = payload.get("status")
    if value is None:
        value = payload.get("health")
    return str(value or "unknown").strip().lower()


def _blocking_reasons(payload: dict[str, Any]) -> list[str]:
    reasons = payload.get("blocking_reasons")
    if isinstance(reasons, list):
        return [str(reason) for reason in reasons if str(reason).strip()]
    if payload.get("_read_error"):
        return [str(payload["_read_error"])]
    return []


def _update_health(update_status: dict[str, Any]) -> tuple[str, list[str]]:
    if not update_status:
        return "missing", ["update_status_latest.json is missing"]
    if update_status.get("_read_error"):
        return "invalid", [str(update_status["_read_error"])]
    status = str(update_status.get("status") or "").strip().lower()
    post_candidates = update_status.get("post_candidates")
    candidate_ok = None
    if isinstance(post_candidates, dict):
        candidate_ok = post_candidates.get("ok")
    if status in {"failed", "error"} or candidate_ok is False:
        return "failed", ["daily update chain reported failure"]
    if status in {"completed", "up_to_date", "success"}:
        return "healthy", []
    if status == "partial_success":
        return "degraded", ["daily update chain reported partial_success"]
    return "unknown", ["daily update status is present but inconclusive"]


def _classify_section(
    *,
    section_id: str,
    name: str,
    payload: dict[str, Any],
    accepted_green: set[str],
    accepted_yellow: set[str],
    missing_is_red: bool = True,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    status = _status(payload)
    reasons = _blocking_reasons(payload)
    if status in accepted_green:
        level = "green"
    elif status in accepted_yellow:
        level = "yellow"
    elif status == "missing" and not missing_is_red:
        level = "yellow"
    else:
        level = "red"
    return {
        "section_id": section_id,
        "name": name,
        "level": level,
        "status": status,
        "blocking_reasons": reasons,
        "evidence_present": bool(payload) and not bool(payload.get("_read_error")),
        **(extra or {}),
    }


def _classify_gap_assessment(gap_assessment: dict[str, Any]) -> dict[str, Any]:
    positioning = str(gap_assessment.get("positioning") or gap_assessment.get("status") or "missing").strip().lower()
    if not gap_assessment:
        level = "yellow"
        positioning = "missing"
    elif gap_assessment.get("_read_error"):
        level = "red"
        positioning = "invalid"
    elif positioning in {"strong challenger", "credible challenger"}:
        level = "green"
    elif positioning in {"early challenger", "unknown"}:
        level = "yellow"
    else:
        level = "yellow"
    return {
        "section_id": "competitive_gap_assessment",
        "name": "Competitive gap assessment",
        "level": level,
        "status": positioning,
        "blocking_reasons": _blocking_reasons(gap_assessment),
        "evidence_present": bool(gap_assessment) and not bool(gap_assessment.get("_read_error")),
        "overall_score": gap_assessment.get("overall_score"),
        "critical_gaps": gap_assessment.get("critical_gaps", []),
    }


def _score(sections: list[dict[str, Any]]) -> int:
    weights = {"green": 100, "yellow": 65, "red": 0}
    if not sections:
        return 0
    return round(sum(weights.get(str(section["level"]), 0) for section in sections) / len(sections))


def _overall_status(sections: list[dict[str, Any]]) -> str:
    levels = {str(section["level"]) for section in sections}
    if "red" in levels:
        return "red"
    if "yellow" in levels:
        return "yellow"
    return "green"


def _next_actions(sections: list[dict[str, Any]], *, primary_entries: int, basket_entries: int) -> list[str]:
    actions: list[str] = []
    red_sections = [section for section in sections if section["level"] == "red"]
    yellow_sections = [section for section in sections if section["level"] == "yellow"]
    if red_sections:
        actions.append("Fix red production evidence first; do not treat today as a clean operating day.")
    feedback_section_payload: dict[str, Any] | None = None
    for section in sections:
        if section.get("section_id") == "feedback_learning_loop":
            feedback_section_payload = section
            break
    for section in red_sections + yellow_sections:
        for reason in section.get("blocking_reasons", []) or []:
            text = str(reason)
            if text and text not in actions:
                actions.append(text)
    if feedback_section_payload is not None:
        critical_total = int(feedback_section_payload.get("queue_open_critical_priority_total") or 0)
        high_total = int(feedback_section_payload.get("queue_open_high_priority_total") or 0)
        taxonomy_hotspots = feedback_section_payload.get("queue_taxonomy_hotspots", {})
        owner_workloads = feedback_section_payload.get("queue_open_owner_workloads", {})
        if critical_total:
            actions.append(f"resolve {critical_total} critical-priority feedback review items before any promotion or baseline move")
        elif high_total:
            actions.append(f"schedule high-priority feedback review items first; open_high_priority_total={high_total}")
        if isinstance(taxonomy_hotspots, dict) and taxonomy_hotspots:
            hottest = sorted(taxonomy_hotspots.items(), key=lambda item: (-int(item[1]), str(item[0])))[0]
            actions.append(f"current feedback hotspot: {hottest[0]} ({hottest[1]} open/recorded cases)")
        if isinstance(owner_workloads, dict) and owner_workloads:
            busiest = sorted(
                owner_workloads.items(),
                key=lambda item: (
                    -int(dict(item[1]).get("critical_priority_total") or 0),
                    -int(dict(item[1]).get("high_priority_total") or 0),
                    -int(dict(item[1]).get("open_total") or 0),
                    str(item[0]),
                ),
            )[0]
            busiest_name, busiest_payload = busiest
            actions.append(
                f"rebalance feedback workload: owner={busiest_name} open_total={int(dict(busiest_payload).get('open_total') or 0)} "
                f"critical_priority_total={int(dict(busiest_payload).get('critical_priority_total') or 0)}"
            )
    if primary_entries < 20 or basket_entries < 20:
        actions.append("Keep accumulating primary and basket ledger entries toward the first 20-trading-day evidence floor.")
    if not actions:
        actions.append("No operational blocker detected; continue daily evidence accumulation.")
    return actions


def build_primary_result_daily_operations_scoreboard(
    *,
    exp_dir: str | Path = "data/experiments",
    artifacts_dir: str | Path = "artifacts",
    output_path: str | Path | None = None,
) -> tuple[int, dict[str, Any]]:
    resolved_exp_dir = resolve_experiments_path(exp_dir)
    resolved_artifacts_dir = resolve_artifacts_path(artifacts_dir)
    basket_dir = resolved_artifacts_dir / "primary_result_candidate_baskets"
    performance_dir = resolved_artifacts_dir / "primary_result_performance"

    update_status = _read_json(resolved_exp_dir / "update_status_latest.json")
    handoff_gate = _read_json(resolved_artifacts_dir / "primary_result_candidate_handoff_gate_latest.json")
    daily_closure = _read_json(resolved_exp_dir / "primary_result_daily_closure_latest.json")
    market_readiness = _read_json(resolved_exp_dir / "primary_result_market_data_readiness_latest.json")
    basket_pointer = _read_json(basket_dir / "current.json")
    basket_observation = _read_json(basket_dir / "observation_latest.json")
    primary_summary = _read_json(performance_dir / "summary.json")
    basket_summary = _read_json(basket_dir / "performance_summary.json")
    performance_evidence = _read_json(resolved_artifacts_dir / "primary_result_performance_evidence_latest.json")
    promotion_gate = _read_json(resolved_artifacts_dir / "primary_result_promotion_readiness_gate_latest.json")
    gap_assessment = _read_json(resolved_artifacts_dir / "primary_result_competitive_gap_assessment_latest.json")
    feedback_loop = _read_json(resolved_exp_dir / "primary_result_feedback_loop_latest.json")
    feedback_queue_summary = _read_json(resolved_artifacts_dir / "primary_result_feedback_review_queue" / "summary.json")

    update_health, update_reasons = _update_health(update_status)
    primary_entries = int(primary_summary.get("entry_total") or _line_count(performance_dir / "ledger.jsonl"))
    basket_entries = int(basket_summary.get("entry_total") or _line_count(basket_dir / "performance_ledger.jsonl"))

    sections = [
        {
            "section_id": "daily_update",
            "name": "Daily data and candidate update",
            "level": "green"
            if update_health == "healthy"
            else "yellow"
            if update_health in {"degraded", "unknown"}
            else "red",
            "status": update_health,
            "blocking_reasons": update_reasons,
            "evidence_present": bool(update_status) and not bool(update_status.get("_read_error")),
        },
        _classify_section(
            section_id="candidate_handoff_gate",
            name="Candidate lifecycle handoff gate",
            payload=handoff_gate,
            accepted_green={"passed"},
            accepted_yellow={"blocked", "missing"},
            missing_is_red=False,
            extra={
                "decision": handoff_gate.get("decision"),
                "next_actions": handoff_gate.get("next_actions", []),
            },
        ),
        _classify_section(
            section_id="market_data_readiness",
            name="Market data readiness",
            payload=market_readiness,
            accepted_green={"passed", "ready", "completed"},
            accepted_yellow={"blocked", "not_ready", "pending", "pending_window"},
            missing_is_red=False,
        ),
        _classify_section(
            section_id="primary_result_daily_closure",
            name="Primary result daily closure",
            payload=daily_closure,
            accepted_green={"closed_success", "completed", "passed"},
            accepted_yellow={"blocked", "closed_failed", "pending", "pending_window"},
        ),
        _classify_section(
            section_id="candidate_basket_registry",
            name="Candidate basket current pointer",
            payload=basket_pointer,
            accepted_green={"approved", "active", "current"},
            accepted_yellow={"conditional", "draft", "pending"},
            extra={"basket_id": basket_pointer.get("basket_id")},
        ),
        _classify_section(
            section_id="candidate_basket_observation",
            name="Candidate basket observation",
            payload=basket_observation,
            accepted_green={"completed"},
            accepted_yellow={"blocked", "pending", "pending_window"},
        ),
        _classify_section(
            section_id="performance_evidence",
            name="Primary and basket performance evidence",
            payload=performance_evidence,
            accepted_green={"ready"},
            accepted_yellow={"accumulating", "missing"},
            missing_is_red=False,
            extra={
                "primary_ledger_entries": primary_entries,
                "basket_ledger_entries": basket_entries,
                "first_evidence_floor": 20,
                "next_actions": performance_evidence.get("next_actions", []),
            },
        ),
        _classify_section(
            section_id="feedback_learning_loop",
            name="Failure feedback and review loop",
            payload=feedback_loop,
            accepted_green={"completed", "skipped"},
            accepted_yellow={"missing", "pending"},
            missing_is_red=False,
            extra={
                "queue_status": feedback_loop.get("queue_status"),
                "review_id": feedback_loop.get("review_id"),
                "queue_open_items": dict(feedback_queue_summary.get("status_counts", {}) or {}).get("open"),
                "queue_open_high_severity_total": feedback_queue_summary.get("open_high_severity_total"),
                "queue_open_high_priority_total": feedback_queue_summary.get("open_high_priority_total"),
                "queue_open_critical_priority_total": feedback_queue_summary.get("open_critical_priority_total"),
                "queue_priority_counts": feedback_queue_summary.get("priority_counts", {}),
                "queue_taxonomy_hotspots": feedback_queue_summary.get("taxonomy_hotspots", {}),
                "queue_open_owner_workloads": feedback_queue_summary.get("open_owner_workloads", {}),
            },
        ),
        _classify_section(
            section_id="promotion_readiness_gate",
            name="Promotion readiness gate",
            payload=promotion_gate,
            accepted_green={"passed"},
            accepted_yellow={"blocked", "missing"},
            missing_is_red=False,
            extra={
                "decision": promotion_gate.get("decision"),
                "next_actions": promotion_gate.get("next_actions", []),
            },
        ),
        _classify_gap_assessment(gap_assessment),
    ]

    status = _overall_status(sections)
    payload = {
        "scoreboard_version": PRIMARY_RESULT_DAILY_OPERATIONS_SCOREBOARD_VERSION,
        "generated_at": _utc_now_iso(),
        "overall_status": status,
        "score": _score(sections),
        "operational_state": (
            "clean_operating_day"
            if status == "green"
            else "blocked_or_accumulating"
            if status == "yellow"
            else "needs_operator_attention"
        ),
        "sections": sections,
        "blocking_reasons": [
            reason
            for section in sections
            for reason in (section.get("blocking_reasons", []) or [])
            if str(reason).strip()
        ],
        "next_actions": _next_actions(sections, primary_entries=primary_entries, basket_entries=basket_entries),
        "evidence_paths": {
            "update_status": str(resolved_exp_dir / "update_status_latest.json"),
            "candidate_handoff_gate": str(resolved_artifacts_dir / "primary_result_candidate_handoff_gate_latest.json"),
            "market_data_readiness": str(resolved_exp_dir / "primary_result_market_data_readiness_latest.json"),
            "primary_result_daily_closure": str(resolved_exp_dir / "primary_result_daily_closure_latest.json"),
            "candidate_basket_current": str(basket_dir / "current.json"),
            "candidate_basket_observation": str(basket_dir / "observation_latest.json"),
            "primary_performance_summary": str(performance_dir / "summary.json"),
            "candidate_basket_performance_summary": str(basket_dir / "performance_summary.json"),
            "performance_evidence": str(resolved_artifacts_dir / "primary_result_performance_evidence_latest.json"),
            "promotion_readiness_gate": str(resolved_artifacts_dir / "primary_result_promotion_readiness_gate_latest.json"),
            "feedback_loop": str(resolved_exp_dir / "primary_result_feedback_loop_latest.json"),
            "feedback_review_queue_summary": str(resolved_artifacts_dir / "primary_result_feedback_review_queue" / "summary.json"),
            "competitive_gap_assessment": str(resolved_artifacts_dir / "primary_result_competitive_gap_assessment_latest.json"),
        },
        "truth_boundary": (
            "This scoreboard does not claim access to any competitor private core. "
            "It measures local evidence against observable industry-leading operating discipline."
        ),
        "production_boundary": (
            "daily operations scoreboard only reads local reports and writes a summary artifact; "
            "it does not trade, mutate strategy rules, promote baselines, deploy, or call external analytics platforms"
        ),
    }
    if output_path:
        _write_json(resolve_project_path(output_path), payload)
    return (0 if status != "red" else 1), payload
