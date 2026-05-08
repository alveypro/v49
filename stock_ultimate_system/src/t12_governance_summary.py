from __future__ import annotations

import html
from dataclasses import asdict, dataclass
from typing import Any


_GOVERNANCE_PLACEHOLDERS = {
    "missing": "暂缺",
    "pending": "待补充",
    "degraded": "降级说明",
}

GOVERNANCE_PROGRESS_STATUS = {
    "progressable": "可推进",
    "pending_review": "待审核",
    "blocked": "不可推进",
    "terminal": "已终局",
    "pending": _GOVERNANCE_PLACEHOLDERS["pending"],
}

_ALLOWED_FACT_KEYS = (
    "disabled_reason",
    "audit_status",
    "promotion_status",
    "terminal_outcome",
    "risk_level",
    "data_sync_note",
    "result_lifecycle_stage",
    "result_type",
    "execution_status",
    "candidate_status",
    "research_status",
)

GOVERNANCE_AUDIT_SHORT_TEXT = {
    "in_review": "审核进行中",
    "failed": "审核未通过",
}

GOVERNANCE_PROMOTION_SHORT_TEXT = {
    "rejected": "晋升未通过",
}

GOVERNANCE_TERMINAL_OUTCOME_SHORT_TEXT = {
    "success": "结果已完成",
    "expired": "结果已终局",
    "superseded": "结果已替代",
    "rejected": "结果已驳回",
    "failed": "结果已失败",
    "cancelled": "结果已取消",
    "archived": "结果已归档",
}

GOVERNANCE_ALLOWED_PLACEHOLDER_VALUES = frozenset(_GOVERNANCE_PLACEHOLDERS.values())
GOVERNANCE_ALLOWED_PROGRESS_VALUES = frozenset(GOVERNANCE_PROGRESS_STATUS.values())


@dataclass(frozen=True)
class T12GovernanceSummaryViewModel:
    current_progress_status: str
    current_primary_blocker: str
    current_governance_note: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def normalize_governance_placeholder(kind: str) -> str:
    return _GOVERNANCE_PLACEHOLDERS.get(kind, _GOVERNANCE_PLACEHOLDERS["missing"])


def _normalize_text(value: object) -> str | None:
    text = str(value or "").strip()
    if not text or text in {"-", "None", "null"}:
        return None
    return text


def extract_governance_summary_facts(source_facts: dict) -> dict:
    return {key: source_facts.get(key) for key in _ALLOWED_FACT_KEYS}


def _risk_has_existing_blocker_support(facts: dict[str, object]) -> bool:
    return (
        _normalize_text(facts.get("disabled_reason")) is not None
        or _normalize_text(facts.get("audit_status")) in {"failed", "in_review"}
        or _normalize_text(facts.get("promotion_status")) == "rejected"
        or _normalize_text(facts.get("terminal_outcome")) is not None
    )


def _has_progress_supporting_facts(facts: dict[str, object]) -> bool:
    if _normalize_text(facts.get("terminal_outcome")) is not None:
        return False
    if _normalize_text(facts.get("disabled_reason")) is not None:
        return False
    if _normalize_text(facts.get("audit_status")) in {"in_review", "failed"}:
        return False
    if _normalize_text(facts.get("promotion_status")) == "rejected":
        return False
    if _normalize_text(facts.get("risk_level")) in {"high", "critical"} and _risk_has_existing_blocker_support(facts):
        return False
    support_keys = (
        "result_lifecycle_stage",
        "result_type",
        "execution_status",
        "candidate_status",
        "research_status",
        "audit_status",
        "promotion_status",
        "risk_level",
    )
    return any(_normalize_text(facts.get(key)) is not None for key in support_keys)


def build_current_progress_status(facts: dict[str, object]) -> str:
    terminal_outcome = _normalize_text(facts.get("terminal_outcome"))
    if terminal_outcome is not None:
        return GOVERNANCE_PROGRESS_STATUS["terminal"]
    if _normalize_text(facts.get("disabled_reason")) is not None:
        return GOVERNANCE_PROGRESS_STATUS["blocked"]
    audit_status = _normalize_text(facts.get("audit_status"))
    if audit_status == "in_review":
        return GOVERNANCE_PROGRESS_STATUS["pending_review"]
    if audit_status == "failed":
        return GOVERNANCE_PROGRESS_STATUS["blocked"]
    if _normalize_text(facts.get("promotion_status")) == "rejected":
        return GOVERNANCE_PROGRESS_STATUS["blocked"]
    risk_level = _normalize_text(facts.get("risk_level"))
    if risk_level in {"high", "critical"} and _risk_has_existing_blocker_support(facts):
        return GOVERNANCE_PROGRESS_STATUS["blocked"]
    if _has_progress_supporting_facts(facts):
        return GOVERNANCE_PROGRESS_STATUS["progressable"]
    return GOVERNANCE_PROGRESS_STATUS["pending"]


def build_current_primary_blocker(facts: dict[str, object]) -> str:
    disabled_reason = _normalize_text(facts.get("disabled_reason"))
    if disabled_reason is not None:
        return disabled_reason
    terminal_outcome = _normalize_text(facts.get("terminal_outcome"))
    if terminal_outcome is not None:
        return GOVERNANCE_TERMINAL_OUTCOME_SHORT_TEXT.get(terminal_outcome.lower(), "结果已终局")
    audit_status = _normalize_text(facts.get("audit_status"))
    if audit_status == "in_review":
        return GOVERNANCE_AUDIT_SHORT_TEXT["in_review"]
    if audit_status == "failed":
        return GOVERNANCE_AUDIT_SHORT_TEXT["failed"]
    if _normalize_text(facts.get("promotion_status")) == "rejected":
        return GOVERNANCE_PROMOTION_SHORT_TEXT["rejected"]
    if _has_progress_supporting_facts(facts):
        return normalize_governance_placeholder("missing")
    return normalize_governance_placeholder("pending")


def build_current_governance_note(facts: dict[str, object]) -> str:
    data_sync_note = _normalize_text(facts.get("data_sync_note"))
    if data_sync_note is not None:
        return data_sync_note
    key_fields = (
        "disabled_reason",
        "audit_status",
        "promotion_status",
        "terminal_outcome",
        "risk_level",
    )
    if any(_normalize_text(facts.get(key)) is None for key in key_fields):
        return normalize_governance_placeholder("degraded")
    return normalize_governance_placeholder("pending")


def build_t12_governance_summary_view_model(facts: dict[str, object]) -> T12GovernanceSummaryViewModel:
    summary_facts = extract_governance_summary_facts(facts)
    return T12GovernanceSummaryViewModel(
        current_progress_status=build_current_progress_status(summary_facts),
        current_primary_blocker=build_current_primary_blocker(summary_facts),
        current_governance_note=build_current_governance_note(summary_facts),
    )


def render_t12_governance_summary_template(view_model: T12GovernanceSummaryViewModel) -> str:
    return (
        '<section class="t12-governance-summary" id="t12-governance-summary">'
        '<div class="t12-governance-summary__header">'
        '<div class="t12-governance-summary__eyebrow">Governance Summary</div>'
        '<h3 class="t12-governance-summary__title">T12 治理摘要</h3>'
        '<p class="t12-governance-summary__desc">只读展示共享制度事实的治理摘要，不触发治理动作。</p>'
        '</div>'
        '<div class="t12-governance-summary__grid">'
        '<article class="t12-governance-summary__item">'
        '<div class="t12-governance-summary__label">当前推进状态</div>'
        f'<div class="t12-governance-summary__value">{html.escape(view_model.current_progress_status)}</div>'
        '</article>'
        '<article class="t12-governance-summary__item">'
        '<div class="t12-governance-summary__label">当前主要阻断</div>'
        f'<div class="t12-governance-summary__value t12-governance-summary__value--compact">{html.escape(view_model.current_primary_blocker)}</div>'
        '</article>'
        '<article class="t12-governance-summary__item">'
        '<div class="t12-governance-summary__label">当前治理备注</div>'
        f'<div class="t12-governance-summary__value t12-governance-summary__value--compact">{html.escape(view_model.current_governance_note)}</div>'
        '</article>'
        '</div>'
        '</section>'
    )
