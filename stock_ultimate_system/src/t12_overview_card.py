from __future__ import annotations

import html
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from src.dashboard_support import read_json


T12_SOURCE_PRIORITY = (
    "t12_threshold_control_latest.json",
    "t12_rollback_drill_latest.json",
    "t12_alert_center_latest.json",
    "t12_review_checklist_latest.json",
    "t12_threshold_stable_snapshot.json",
    "t12_threshold_rollback_events.json",
)

_T12_MISSING_TEXT = {
    "missing": "暂缺",
    "pending": "待补充",
    "degraded": "降级说明",
}


@dataclass
class T12OverviewCardViewModel:
    formal_stage_title: str
    result_type_label: str
    phase_detail: str
    governance_status_label: str
    governance_status_detail: str
    primary_source_label: str
    primary_source_detail: str
    note_label: str
    note_detail: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def t12_missing_text(kind: str) -> str:
    return _T12_MISSING_TEXT.get(kind, _T12_MISSING_TEXT["missing"])


def _normalize_text(value: object) -> str | None:
    text = str(value or "").strip()
    if not text or text in {"-", "None", "null"}:
        return None
    return text


def _format_mtime(path: Path) -> str | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def build_t12_overview_minimal_facts(exp_dir: Path) -> dict[str, object]:
    alert_path = exp_dir / "t12_alert_center_latest.json"
    review_path = exp_dir / "t12_review_checklist_latest.json"
    threshold_path = exp_dir / "t12_threshold_control_latest.json"
    rollback_path = exp_dir / "t12_rollback_drill_latest.json"
    rollback_events_path = exp_dir / "t12_threshold_rollback_events.json"
    stable_snapshot_path = exp_dir / "t12_threshold_stable_snapshot.json"

    alert_snapshot = read_json(alert_path)
    review_snapshot = read_json(review_path)
    threshold_snapshot = read_json(threshold_path)
    rollback_snapshot = read_json(rollback_path)
    rollback_events_snapshot = read_json(rollback_events_path)
    stable_snapshot = read_json(stable_snapshot_path)

    rollback_items = rollback_events_snapshot.get("items", [])
    rollback_event_count: int | None = None
    if isinstance(rollback_items, list):
        rollback_event_count = len(rollback_items)

    stable_thresholds = stable_snapshot.get("thresholds", {})
    stable_threshold_count: int | None = None
    if isinstance(stable_thresholds, dict):
        stable_threshold_count = len(stable_thresholds)

    source_timestamps = {
        "t12_alert_center_latest.json": _format_mtime(alert_path) or "",
        "t12_review_checklist_latest.json": _format_mtime(review_path) or "",
        "t12_threshold_control_latest.json": _format_mtime(threshold_path) or "",
        "t12_rollback_drill_latest.json": _format_mtime(rollback_path) or "",
        "t12_threshold_rollback_events.json": _format_mtime(rollback_events_path) or "",
        "t12_threshold_stable_snapshot.json": _format_mtime(stable_snapshot_path) or "",
    }

    return {
        "alert_status": _normalize_text((alert_snapshot.get("summary", {}) or {}).get("overall_status"))
        or _normalize_text(alert_snapshot.get("overall_status")),
        "alert_generated_at": _normalize_text(alert_snapshot.get("generated_at")),
        "review_status": _normalize_text(review_snapshot.get("overall_status")),
        "review_generated_at": _normalize_text(review_snapshot.get("generated_at")),
        "threshold_applied": threshold_snapshot.get("applied") if "applied" in threshold_snapshot else None,
        "threshold_recommendation": _normalize_text(threshold_snapshot.get("recommendation")),
        "threshold_generated_at": _normalize_text(threshold_snapshot.get("generated_at")),
        "rollback_mode": _normalize_text(rollback_snapshot.get("mode")),
        "rollback_triggered": rollback_snapshot.get("triggered") if "triggered" in rollback_snapshot else None,
        "rollback_would_apply": rollback_snapshot.get("would_rollback_apply")
        if "would_rollback_apply" in rollback_snapshot
        else None,
        "rollback_generated_at": _normalize_text(rollback_snapshot.get("generated_at")),
        "rollback_event_count": rollback_event_count,
        "stable_threshold_count": stable_threshold_count,
        "source_timestamps": source_timestamps,
    }


def _has_threshold_fact(facts: dict[str, object]) -> bool:
    return facts.get("threshold_applied") is not None or _normalize_text(facts.get("threshold_recommendation")) is not None


def _has_rollback_fact(facts: dict[str, object]) -> bool:
    return (
        _normalize_text(facts.get("rollback_mode")) is not None
        or facts.get("rollback_triggered") is not None
        or facts.get("rollback_would_apply") is not None
    )


def t12_formal_stage_title(facts: dict[str, object]) -> str:
    if _has_threshold_fact(facts):
        return "T12 阈值控制"
    if _has_rollback_fact(facts):
        return "T12 回滚演练"
    if _normalize_text(facts.get("alert_status")) is not None:
        return "T12 告警中心"
    if _normalize_text(facts.get("review_status")) is not None:
        return "T12 复核清单"
    if facts.get("stable_threshold_count") is not None:
        return "T12 稳定阈值"
    if facts.get("rollback_event_count") is not None:
        return "T12 回滚记录"
    return "T12 制度总览"


def _translate_status(value: object) -> str:
    key = str(value or "").strip().lower()
    mapping = {
        "pass": "通过",
        "warn": "告警",
        "fail": "失败",
        "off": "关闭",
        "shadow": "影子",
        "on": "开启",
    }
    if not key:
        return t12_missing_text("pending")
    return mapping.get(key, str(value))


def _translate_recommendation(value: object) -> str:
    key = str(value or "").strip().lower()
    mapping = {
        "tighten_thresholds": "收紧阈值",
        "loosen_thresholds": "放宽阈值",
        "keep_thresholds": "保持阈值",
    }
    if not key:
        return t12_missing_text("pending")
    return mapping.get(key, str(value))


def _bool_label(value: object, *, true_text: str, false_text: str) -> str:
    if value is True:
        return true_text
    if value is False:
        return false_text
    return t12_missing_text("pending")


def select_t12_primary_source(facts: dict[str, object]) -> tuple[str | None, str | None]:
    source_timestamps = facts.get("source_timestamps")
    if not isinstance(source_timestamps, dict):
        return None, None
    for source_name in T12_SOURCE_PRIORITY:
        timestamp = _normalize_text(source_timestamps.get(source_name))
        if timestamp is not None:
            return source_name, timestamp
    return None, None


def build_t12_overview_card_view_model(facts: dict[str, object]) -> T12OverviewCardViewModel:
    stage_title = t12_formal_stage_title(facts)
    primary_source_name, primary_source_timestamp = select_t12_primary_source(facts)
    threshold_applied = facts.get("threshold_applied")
    rollback_event_count = facts.get("rollback_event_count")
    stable_threshold_count = facts.get("stable_threshold_count")

    if _has_threshold_fact(facts):
        result_type_label = (
            "阈值已应用" if threshold_applied is True else "阈值未应用" if threshold_applied is False else t12_missing_text("pending")
        )
        phase_detail = _translate_recommendation(facts.get("threshold_recommendation"))
    elif _has_rollback_fact(facts):
        result_type_label = _bool_label(
            facts.get("rollback_triggered"),
            true_text="回滚已触发",
            false_text="回滚未触发",
        )
        phase_detail = _bool_label(
            facts.get("rollback_would_apply"),
            true_text="演练结果可回滚",
            false_text="演练结果不回滚",
        )
    elif _normalize_text(facts.get("alert_status")) is not None:
        result_type_label = f"告警 {_translate_status(facts.get('alert_status'))}"
        phase_detail = t12_missing_text("pending")
    elif _normalize_text(facts.get("review_status")) is not None:
        result_type_label = f"复核 {_translate_status(facts.get('review_status'))}"
        phase_detail = t12_missing_text("pending")
    else:
        result_type_label = t12_missing_text("pending")
        phase_detail = t12_missing_text("pending")

    governance_status_label = (
        f"告警 {_translate_status(facts.get('alert_status'))} / "
        f"复核 {_translate_status(facts.get('review_status'))} / "
        f"演练 {_bool_label(facts.get('rollback_triggered'), true_text='触发', false_text='未触发')}"
    )
    governance_parts: list[str] = []
    if rollback_event_count is not None:
        governance_parts.append(f"回滚事件 {rollback_event_count}")
    if stable_threshold_count is not None:
        governance_parts.append(f"稳定阈值 {stable_threshold_count}")
    governance_status_detail = " · ".join(governance_parts) if governance_parts else t12_missing_text("pending")

    primary_source_label = primary_source_name or t12_missing_text("missing")
    primary_source_detail = (
        f"{primary_source_timestamp} · 固定优先级"
        if primary_source_name and primary_source_timestamp
        else t12_missing_text("degraded")
    )

    missing_items: list[str] = []
    if _normalize_text(facts.get("alert_status")) is None:
        missing_items.append("alert")
    if _normalize_text(facts.get("review_status")) is None:
        missing_items.append("review")
    if facts.get("threshold_applied") is None and _normalize_text(facts.get("threshold_recommendation")) is None:
        missing_items.append("threshold")
    if facts.get("rollback_triggered") is None and _normalize_text(facts.get("rollback_mode")) is None:
        missing_items.append("rollback")
    note_label = t12_missing_text("degraded") if missing_items else "制度说明"
    note_detail = t12_missing_text("degraded") if missing_items else "共享事实层最小镜像"

    return T12OverviewCardViewModel(
        formal_stage_title=stage_title,
        result_type_label=result_type_label,
        phase_detail=phase_detail,
        governance_status_label=governance_status_label,
        governance_status_detail=governance_status_detail,
        primary_source_label=primary_source_label,
        primary_source_detail=primary_source_detail,
        note_label=note_label,
        note_detail=note_detail,
    )


def render_t12_overview_card_template(view_model: T12OverviewCardViewModel) -> str:
    return (
        '<div class="card t12-overview-card" id="t12-overview-card">'
        '<div class="section-title t12-overview-header">'
        '<div><div class="eyebrow">T12 镜像卡</div><h3>T12 最小制度总览</h3></div>'
        '<div class="muted">共享制度事实层只读，T12 仅负责展示最小镜像。</div>'
        '</div>'
        '<div class="t12-overview-grid">'
        '<section class="t12-overview-block t12-overview-block-stage">'
        '<div class="t12-overview-label">主阶段</div>'
        f'<div class="t12-overview-value">{html.escape(view_model.formal_stage_title)}</div>'
        f'<div class="t12-overview-sub">{html.escape(view_model.result_type_label)}</div>'
        f'<div class="t12-overview-meta">{html.escape(view_model.phase_detail)}</div>'
        '</section>'
        '<section class="t12-overview-block t12-overview-block-status">'
        '<div class="t12-overview-label">状态镜像</div>'
        f'<div class="t12-overview-value t12-overview-value-compact">{html.escape(view_model.governance_status_label)}</div>'
        f'<div class="t12-overview-meta">{html.escape(view_model.governance_status_detail)}</div>'
        '</section>'
        '<section class="t12-overview-block t12-overview-block-source">'
        '<div class="t12-overview-label">主来源</div>'
        f'<div class="t12-overview-value t12-overview-value-compact">{html.escape(view_model.primary_source_label)}</div>'
        f'<div class="t12-overview-meta">{html.escape(view_model.primary_source_detail)}</div>'
        '</section>'
        '<section class="t12-overview-block t12-overview-block-note">'
        '<div class="t12-overview-label">备注</div>'
        f'<div class="t12-overview-value t12-overview-value-compact">{html.escape(view_model.note_label)}</div>'
        f'<div class="t12-overview-meta">{html.escape(view_model.note_detail)}</div>'
        '</section>'
        '</div>'
        '</div>'
    )
