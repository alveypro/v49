from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import Any

import yaml

from src.primary_result_candidate_basket import TARGET_MAX_INDUSTRY_WEIGHT
from src.utils.project_paths import resolve_project_path
from src.utils.serialization import save_json


@dataclass
class AuditItem:
    name: str
    ok: bool
    severity: str
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "ok": bool(self.ok),
            "severity": self.severity,
            "detail": self.detail,
        }


def _now() -> datetime:
    return datetime.now()


def _file_age_hours(path: Path) -> float:
    if not path.exists():
        return -1.0
    delta = _now() - datetime.fromtimestamp(path.stat().st_mtime)
    return round(delta.total_seconds() / 3600.0, 2)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _optional_artifact_present(path: Path, payload: Any) -> bool:
    if path.exists():
        return True
    if isinstance(payload, dict):
        return bool(payload)
    if isinstance(payload, list):
        return len(payload) > 0
    return payload is not None


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "ok", "pass"}
    return False


def _load_framework(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _required_artifacts(framework_cfg: dict[str, Any]) -> list[str]:
    required: list[str] = []
    artifacts = framework_cfg.get("experiment_framework", {}).get("artifacts", {})
    for group in ("required_json", "required_markdown", "required_tabular"):
        for item in artifacts.get(group, []) or []:
            text = str(item).strip()
            if text:
                required.append(text)
    return required


def _build_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary", {})
    items = payload.get("items", [])
    lines = [
        "# Governance Audit Report",
        "",
        f"Generated at: {payload.get('generated_at', '')}",
        "",
        "## Summary",
        f"- Overall status: `{summary.get('overall_status', 'unknown')}`",
        f"- Pass count: {summary.get('pass_count', 0)}",
        f"- Fail count: {summary.get('fail_count', 0)}",
        f"- Warn count: {summary.get('warn_count', 0)}",
        "",
        "## Checks",
    ]
    for item in items:
        symbol = "PASS" if item.get("ok") else ("WARN" if item.get("severity") == "warn" else "FAIL")
        lines.append(f"- [{symbol}] `{item.get('name', '')}`: {item.get('detail', '')}")
    lines.append("")
    return "\n".join(lines)


def run_governance_audit(
    *,
    config_dir: str = "config",
    output_dir: str = "data/experiments",
    max_status_age_hours: float = 36.0,
    max_candidate_age_hours: float = 36.0,
    max_research_age_hours: float = 36.0,
) -> dict[str, Any]:
    cfg_dir = resolve_project_path(config_dir)
    out_dir = resolve_project_path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    update_status_path = out_dir / "update_status_latest.json"
    research_status_path = out_dir / "daily_research_status_latest.json"
    candidates_md_path = out_dir / "candidates_top_latest.md"
    daily_research_md_path = out_dir / "daily_research_latest.md"
    buylist_json_path = out_dir / "buylist_latest.json"
    t1_checklist_json_path = out_dir / "t1_execution_checklist_latest.json"
    t12_alert_json_path = out_dir / "t12_alert_center_latest.json"
    t12_review_json_path = out_dir / "t12_review_checklist_latest.json"
    t12_threshold_control_json_path = out_dir / "t12_threshold_control_latest.json"
    t12_rollback_drill_json_path = out_dir / "t12_rollback_drill_latest.json"
    t12_threshold_rollback_events_json_path = out_dir / "t12_threshold_rollback_events.json"
    t12_threshold_stable_snapshot_json_path = out_dir / "t12_threshold_stable_snapshot.json"
    framework_path = cfg_dir / "experiment_framework.example.yaml"

    update_status = _read_json(update_status_path)
    research_status = _read_json(research_status_path)
    buylist_snapshot = _read_json(buylist_json_path)
    t1_checklist_snapshot = _read_json(t1_checklist_json_path)
    t12_alert_snapshot = _read_json(t12_alert_json_path)
    t12_review_snapshot = _read_json(t12_review_json_path)
    t12_threshold_control_snapshot = _read_json(t12_threshold_control_json_path)
    t12_rollback_drill_snapshot = _read_json(t12_rollback_drill_json_path)
    t12_threshold_rollback_events_snapshot = _read_json(t12_threshold_rollback_events_json_path)
    t12_threshold_stable_snapshot = _read_json(t12_threshold_stable_snapshot_json_path)
    rollback_events_file = str((t12_threshold_control_snapshot or {}).get("rollback_events_file", "")).strip()
    stable_snapshot_file = str((t12_threshold_control_snapshot or {}).get("stable_snapshot_file", "")).strip()
    if (not t12_threshold_rollback_events_snapshot) and rollback_events_file:
        t12_threshold_rollback_events_snapshot = _read_json(Path(rollback_events_file))
    if (not t12_threshold_stable_snapshot) and stable_snapshot_file:
        t12_threshold_stable_snapshot = _read_json(Path(stable_snapshot_file))
    framework = _load_framework(framework_path)

    items: list[AuditItem] = []

    update_age = _file_age_hours(update_status_path)
    items.append(
        AuditItem(
            name="update_status_freshness",
            ok=0 <= update_age <= float(max_status_age_hours),
            severity="critical",
            detail=f"age_hours={update_age}, threshold={max_status_age_hours}",
        )
    )

    update_state = str(update_status.get("status", "")).strip().lower()
    post_candidates_ok = _safe_bool((update_status.get("post_candidates", {}) or {}).get("ok"))
    post_daily_ok = _safe_bool((update_status.get("post_daily_research", {}) or {}).get("ok"))
    partial_success_with_core_ok = update_state == "partial_success" and post_candidates_ok and post_daily_ok
    items.append(
        AuditItem(
            name="update_status_health",
            ok=(update_state in {"success", "completed", "ok"}) or partial_success_with_core_ok,
            severity="critical",
            detail=(
                f"status={update_state or 'missing'}, "
                f"post_candidates_ok={post_candidates_ok}, post_daily_research_ok={post_daily_ok}"
            ),
        )
    )

    items.append(
        AuditItem(
            name="post_candidates_ok",
            ok=post_candidates_ok,
            severity="critical",
            detail=f"detail={str((update_status.get('post_candidates', {}) or {}).get('detail', ''))[:160]}",
        )
    )

    items.append(
        AuditItem(
            name="post_daily_research_ok",
            ok=post_daily_ok,
            severity="critical",
            detail=f"detail={str((update_status.get('post_daily_research', {}) or {}).get('detail', ''))[:160]}",
        )
    )

    research_state = str(research_status.get("state", "")).strip().lower()
    items.append(
        AuditItem(
            name="daily_research_runtime_state",
            ok=research_state in {"completed", "done"},
            severity="warn",
            detail=f"state={research_state or 'missing'}",
        )
    )

    candidate_age = _file_age_hours(candidates_md_path)
    items.append(
        AuditItem(
            name="candidate_output_freshness",
            ok=0 <= candidate_age <= float(max_candidate_age_hours),
            severity="warn",
            detail=f"age_hours={candidate_age}, threshold={max_candidate_age_hours}",
        )
    )

    research_age = _file_age_hours(daily_research_md_path)
    items.append(
        AuditItem(
            name="daily_research_output_freshness",
            ok=0 <= research_age <= float(max_research_age_hours),
            severity="warn",
            detail=f"age_hours={research_age}, threshold={max_research_age_hours}",
        )
    )

    target_count = int((buylist_snapshot or {}).get("target_count", 0) or 0)
    buyable_count = int((buylist_snapshot or {}).get("buyable_count", 0) or 0)
    items.append(
        AuditItem(
            name="buylist_target_fill_rate",
            ok=(not _optional_artifact_present(buylist_json_path, buylist_snapshot)) or (
                target_count > 0 and buyable_count >= target_count
            ),
            severity="warn",
            detail=(
                "buylist_optional_missing"
                if not _optional_artifact_present(buylist_json_path, buylist_snapshot)
                else f"buyable_count={buyable_count}, target_count={target_count}"
            ),
        )
    )

    constraints = (buylist_snapshot or {}).get("constraints", {}) or {}
    configured_max_industry_weight = float(
        constraints.get("max_industry_weight", TARGET_MAX_INDUSTRY_WEIGHT) or TARGET_MAX_INDUSTRY_WEIGHT
    )
    actual_max_industry_weight = float(constraints.get("actual_max_industry_weight", 1.0) or 1.0)
    items.append(
        AuditItem(
            name="buylist_industry_concentration",
            ok=(not _optional_artifact_present(buylist_json_path, buylist_snapshot)) or (
                actual_max_industry_weight <= configured_max_industry_weight
            ),
            severity="warn",
            detail=(
                "buylist_optional_missing"
                if not _optional_artifact_present(buylist_json_path, buylist_snapshot)
                else (
                    f"actual_max_industry_weight={round(actual_max_industry_weight, 4)}, "
                    f"max_industry_weight={round(configured_max_industry_weight, 4)}"
                )
            ),
        )
    )

    turnover_ratio = float((buylist_snapshot or {}).get("turnover_ratio", -1.0))
    max_turnover_ratio = float((buylist_snapshot or {}).get("max_turnover_ratio", 0.8) or 0.8)
    has_previous = bool((buylist_snapshot or {}).get("previous_generated_at"))
    items.append(
        AuditItem(
            name="buylist_turnover_control",
            ok=(not _optional_artifact_present(buylist_json_path, buylist_snapshot)) or (
                (not has_previous) or (0 <= turnover_ratio <= max_turnover_ratio)
            ),
            severity="warn",
            detail=(
                "buylist_optional_missing"
                if not _optional_artifact_present(buylist_json_path, buylist_snapshot)
                else (
                    f"turnover_ratio={round(turnover_ratio, 4)}, "
                    f"max_turnover_ratio={round(max_turnover_ratio, 4)}, has_previous={has_previous}"
                )
            ),
        )
    )

    t1_checklist_age = _file_age_hours(t1_checklist_json_path)
    items.append(
        AuditItem(
            name="t1_execution_checklist_freshness",
            ok=(not _optional_artifact_present(t1_checklist_json_path, t1_checklist_snapshot)) or (
                0 <= t1_checklist_age <= float(max_candidate_age_hours)
            ),
            severity="warn",
            detail=(
                "t1_optional_missing"
                if not _optional_artifact_present(t1_checklist_json_path, t1_checklist_snapshot)
                else f"age_hours={t1_checklist_age}, threshold={max_candidate_age_hours}"
            ),
        )
    )
    t1_status = str((t1_checklist_snapshot.get("summary", {}) or {}).get("overall_status", "")).strip().lower()
    items.append(
        AuditItem(
            name="t1_execution_checklist_status",
            ok=(not _optional_artifact_present(t1_checklist_json_path, t1_checklist_snapshot)) or (
                t1_status in {"pass", "warn"}
            ),
            severity="warn",
            detail=(
                "t1_optional_missing"
                if not _optional_artifact_present(t1_checklist_json_path, t1_checklist_snapshot)
                else f"overall_status={t1_status or 'missing'}"
            ),
        )
    )

    t12_alert_age = _file_age_hours(t12_alert_json_path)
    items.append(
        AuditItem(
            name="t12_alert_center_freshness",
            ok=(not _optional_artifact_present(t12_alert_json_path, t12_alert_snapshot)) or (
                0 <= t12_alert_age <= float(max_candidate_age_hours)
            ),
            severity="warn",
            detail=(
                "t12_alert_optional_missing"
                if not _optional_artifact_present(t12_alert_json_path, t12_alert_snapshot)
                else f"age_hours={t12_alert_age}, threshold={max_candidate_age_hours}"
            ),
        )
    )
    t12_alert_status = str((t12_alert_snapshot.get("summary", {}) or {}).get("overall_status", "")).strip().lower()
    items.append(
        AuditItem(
            name="t12_alert_center_status",
            ok=(not _optional_artifact_present(t12_alert_json_path, t12_alert_snapshot)) or (
                t12_alert_status in {"pass", "warn"}
            ),
            severity="warn",
            detail=(
                "t12_alert_optional_missing"
                if not _optional_artifact_present(t12_alert_json_path, t12_alert_snapshot)
                else f"overall_status={t12_alert_status or 'missing'}"
            ),
        )
    )

    adaptive_cfg = (t12_alert_snapshot or {}).get("adaptive_thresholds", {}) or {}
    adaptive_ok = all(
        key in adaptive_cfg
        for key in (
            "current_warn_hot_threshold",
            "current_fail_hot_threshold",
            "current_fail_streak_hot_threshold",
            "suggested_warn_hot_threshold",
            "suggested_fail_hot_threshold",
            "suggested_fail_streak_hot_threshold",
            "recommendation",
        )
    )
    items.append(
        AuditItem(
            name="t12_alert_adaptive_thresholds",
            ok=(not _optional_artifact_present(t12_alert_json_path, t12_alert_snapshot)) or adaptive_ok,
            severity="warn",
            detail=(
                "t12_alert_optional_missing"
                if not _optional_artifact_present(t12_alert_json_path, t12_alert_snapshot)
                else (
                    f"recommendation={str(adaptive_cfg.get('recommendation', 'missing'))}, "
                    f"suggested_warn={adaptive_cfg.get('suggested_warn_hot_threshold', 'missing')}, "
                    f"suggested_fail={adaptive_cfg.get('suggested_fail_hot_threshold', 'missing')}"
                )
            ),
        )
    )

    review_actions = (t12_review_snapshot or {}).get("actions", [])
    review_count = len(review_actions) if isinstance(review_actions, list) else -1
    items.append(
        AuditItem(
            name="t12_review_checklist_presence",
            ok=(not _optional_artifact_present(t12_review_json_path, t12_review_snapshot)) or review_count >= 0,
            severity="warn",
            detail=(
                "t12_review_optional_missing"
                if not _optional_artifact_present(t12_review_json_path, t12_review_snapshot)
                else f"actions_count={review_count}"
            ),
        )
    )

    t12_threshold_control_age = _file_age_hours(t12_threshold_control_json_path)
    items.append(
        AuditItem(
            name="t12_threshold_autotune_freshness",
            ok=(not _optional_artifact_present(t12_threshold_control_json_path, t12_threshold_control_snapshot)) or (
                0 <= t12_threshold_control_age <= float(max_candidate_age_hours)
            ),
            severity="warn",
            detail=(
                "t12_threshold_optional_missing"
                if not _optional_artifact_present(t12_threshold_control_json_path, t12_threshold_control_snapshot)
                else f"age_hours={t12_threshold_control_age}, threshold={max_candidate_age_hours}"
            ),
        )
    )
    t12_threshold_control_ok = (
        "applied" in t12_threshold_control_snapshot
        and isinstance((t12_threshold_control_snapshot or {}).get("before", {}), dict)
        and isinstance((t12_threshold_control_snapshot or {}).get("after", {}), dict)
    )
    items.append(
        AuditItem(
            name="t12_threshold_autotune_payload",
            ok=(not _optional_artifact_present(t12_threshold_control_json_path, t12_threshold_control_snapshot)) or (
                t12_threshold_control_ok
            ),
            severity="warn",
            detail=(
                "t12_threshold_optional_missing"
                if not _optional_artifact_present(t12_threshold_control_json_path, t12_threshold_control_snapshot)
                else (
                    f"applied={str((t12_threshold_control_snapshot or {}).get('applied', 'missing'))}, "
                    f"reason={str((t12_threshold_control_snapshot or {}).get('reason', 'missing'))[:120]}"
                )
            ),
        )
    )

    t12_rollback_drill_age = _file_age_hours(t12_rollback_drill_json_path)
    items.append(
        AuditItem(
            name="t12_rollback_drill_freshness",
            ok=(not _optional_artifact_present(t12_rollback_drill_json_path, t12_rollback_drill_snapshot)) or (
                0 <= t12_rollback_drill_age <= float(max_candidate_age_hours)
            ),
            severity="warn",
            detail=(
                "t12_rollback_optional_missing"
                if not _optional_artifact_present(t12_rollback_drill_json_path, t12_rollback_drill_snapshot)
                else f"age_hours={t12_rollback_drill_age}, threshold={max_candidate_age_hours}"
            ),
        )
    )
    t12_rollback_drill_ok = (
        isinstance(t12_rollback_drill_snapshot, dict)
        and "mode" in t12_rollback_drill_snapshot
        and "triggered" in t12_rollback_drill_snapshot
        and "would_rollback_apply" in t12_rollback_drill_snapshot
    )
    items.append(
        AuditItem(
            name="t12_rollback_drill_payload",
            ok=(not _optional_artifact_present(t12_rollback_drill_json_path, t12_rollback_drill_snapshot)) or (
                t12_rollback_drill_ok
            ),
            severity="warn",
            detail=(
                "t12_rollback_optional_missing"
                if not _optional_artifact_present(t12_rollback_drill_json_path, t12_rollback_drill_snapshot)
                else (
                    f"mode={str((t12_rollback_drill_snapshot or {}).get('mode', 'missing'))}, "
                    f"triggered={str((t12_rollback_drill_snapshot or {}).get('triggered', 'missing'))}"
                )
            ),
        )
    )

    rollback_items = (t12_threshold_rollback_events_snapshot or {}).get("items", [])
    rollback_events_ok = isinstance(rollback_items, list)
    items.append(
        AuditItem(
            name="t12_threshold_rollback_events_payload",
            ok=(not _optional_artifact_present(t12_threshold_rollback_events_json_path, t12_threshold_rollback_events_snapshot)) or (
                rollback_events_ok
            ),
            severity="warn",
            detail=(
                "t12_rollback_events_optional_missing"
                if not _optional_artifact_present(t12_threshold_rollback_events_json_path, t12_threshold_rollback_events_snapshot)
                else f"events_count={len(rollback_items) if rollback_events_ok else -1}"
            ),
        )
    )
    stable_thresholds = (t12_threshold_stable_snapshot or {}).get("thresholds", {}) or {}
    stable_snapshot_ok = all(
        k in stable_thresholds for k in ("warn_hot_threshold", "fail_hot_threshold", "fail_streak_hot_threshold")
    )
    items.append(
        AuditItem(
            name="t12_threshold_stable_snapshot_presence",
            ok=(not _optional_artifact_present(t12_threshold_stable_snapshot_json_path, t12_threshold_stable_snapshot)) or (
                stable_snapshot_ok
            ),
            severity="warn",
            detail=(
                "t12_stable_snapshot_optional_missing"
                if not _optional_artifact_present(t12_threshold_stable_snapshot_json_path, t12_threshold_stable_snapshot)
                else (
                    f"warn={stable_thresholds.get('warn_hot_threshold', 'missing')}, "
                    f"fail={stable_thresholds.get('fail_hot_threshold', 'missing')}, "
                    f"fail_streak={stable_thresholds.get('fail_streak_hot_threshold', 'missing')}"
                )
            ),
        )
    )

    required_artifacts = _required_artifacts(framework)
    missing_artifacts: list[str] = []
    for artifact in required_artifacts:
        artifact_path = out_dir / artifact
        if not artifact_path.exists():
            missing_artifacts.append(artifact)
    items.append(
        AuditItem(
            name="framework_required_artifacts",
            ok=len(missing_artifacts) == 0,
            severity="warn",
            detail="missing=" + (", ".join(missing_artifacts[:12]) if missing_artifacts else "none"),
        )
    )

    pass_count = sum(1 for x in items if x.ok)
    fail_count = sum(1 for x in items if (not x.ok) and x.severity == "critical")
    warn_count = sum(1 for x in items if (not x.ok) and x.severity == "warn")
    overall_status = "pass" if fail_count == 0 and warn_count == 0 else ("warn" if fail_count == 0 else "fail")

    payload: dict[str, Any] = {
        "generated_at": _now().isoformat(),
        "summary": {
            "overall_status": overall_status,
            "pass_count": pass_count,
            "fail_count": fail_count,
            "warn_count": warn_count,
        },
        "inputs": {
            "config_dir": str(cfg_dir),
            "output_dir": str(out_dir),
            "max_status_age_hours": float(max_status_age_hours),
            "max_candidate_age_hours": float(max_candidate_age_hours),
            "max_research_age_hours": float(max_research_age_hours),
        },
        "items": [x.to_dict() for x in items],
    }

    latest_json = out_dir / "governance_audit_latest.json"
    latest_md = out_dir / "governance_audit_latest.md"
    dated = _now().strftime("%Y%m%d_%H%M%S")
    dated_json = out_dir / f"governance_audit_{dated}.json"
    dated_md = out_dir / f"governance_audit_{dated}.md"

    save_json(payload, str(latest_json))
    save_json(payload, str(dated_json))
    report_md = _build_markdown(payload)
    latest_md.write_text(report_md, encoding="utf-8")
    dated_md.write_text(report_md, encoding="utf-8")

    payload["outputs"] = {
        "latest_json": str(latest_json),
        "latest_md": str(latest_md),
        "dated_json": str(dated_json),
        "dated_md": str(dated_md),
    }
    return payload
