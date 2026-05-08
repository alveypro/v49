from __future__ import annotations

import html
from dataclasses import asdict, dataclass
from typing import Any


STOCK_PRIMARY_ALLOWED_PLACEHOLDERS = {
    "empty": "暂缺",
    "error": "待补充",
    "degraded": "降级说明",
}

STOCK_PRIMARY_STAGE_ZH = {
    "L1": "研究中",
    "L2": "候选结果",
    "L3": "审核阶段结果",
    "L4": "执行阶段结果",
    "L5": "已沉淀",
}

STOCK_PRIMARY_RISK_ZH = {
    "low": "低风险",
    "medium": "中风险",
    "high": "高风险",
    "critical": "高风险",
}

STOCK_PRIMARY_RESULT_TYPE_ZH = {
    "candidate": "候选结果",
    "audit": "审核结果",
    "execution": "执行结果",
    "archive": "沉淀结果",
    "research": "研究结果",
}
STOCK_PRIMARY_STATUS_SUFFIX_MAPS = {
    "execution_status": {
        "queued": "待执行",
        "ready": "待执行准备",
        "running": "运行中",
        "completed": "已完成",
        "failed": "执行失败",
        "cancelled": "已取消",
    },
    "audit_status": {
        "in_review": "审核中",
        "passed": "已通过",
        "failed": "未通过",
        "waived": "已豁免",
    },
    "candidate_status": {
        "candidate": "候选中",
        "shortlisted": "已入池",
        "rejected": "已驳回",
        "expired": "已失效",
    },
    "research_status": {
        "in_progress": "研究中",
        "completed": "研究完成",
        "suspended": "研究暂停",
        "abandoned": "研究终止",
    },
}
STOCK_PRIMARY_INVALID_TEXT_MAP = {
    "success": "结果已完成",
    "expired": "结果已失效",
    "superseded": "结果已替代",
    "rejected": "结果已驳回",
    "failed": "结果已失败",
    "cancelled": "结果已取消",
    "archived": "结果已归档",
}
STOCK_PRIMARY_STANDARD_SYNC_NOTES = {
    "aligned": "制度字段已对齐。",
    "degraded": "降级说明",
}
STOCK_PRIMARY_STANDARD_INVALID_TERMS = set(STOCK_PRIMARY_INVALID_TEXT_MAP.values())
STOCK_PRIMARY_DISALLOWED_CONCLUSION_TERMS = {
    "当前推进状态",
    "当前主要阻断",
    "当前治理备注",
    "Governance Summary",
    "Airivo 是统一母平台",
    "面向未来",
}

STOCK_PRIMARY_GOVERNANCE_MIX_FIELDS = {
    "current_progress_status",
    "current_primary_blocker",
    "current_governance_note",
}

STOCK_PRIMARY_MAIN_SITE_MIX_FIELDS = {
    "platform_identity",
    "platform_story",
    "main_site_banner",
    "brand_slogan",
    "product_matrix_summary",
}

STOCK_PRIMARY_ALLOWED_INPUT_FIELDS = {
    "ts_code",
    "stock_name",
    "result_lifecycle_stage",
    "result_type",
    "risk_level",
    "data_sync_note",
    "source_timestamps",
    "history_summary",
    "history_source_file",
    "history_source_timestamp",
    "history_generation_mode",
    "disabled_reason",
    "invalid_reason",
    "terminal_outcome",
    "audit_status",
    "promotion_status",
    "execution_status",
    "candidate_status",
    "research_status",
}

STOCK_PRIMARY_MAX_CONCLUSION_TEXT_LENGTH = 24
STOCK_PRIMARY_MAX_TARGET_NAME_LENGTH = 24
STOCK_PRIMARY_MAX_EXPLANATION_ITEMS = 3
STOCK_PRIMARY_MAX_EXPLANATION_ITEM_LENGTH = 48
STOCK_PRIMARY_BOUNDARY_NOTE_COUNT = 3
STOCK_PRIMARY_CONCLUSION_PRIORITY_FIELDS = (
    "target_code",
    "target_name",
    "primary_result_label",
    "risk_label",
)
STOCK_PRIMARY_EXPLANATION_ORDER_RULE = (
    "history_summary_first",
    "source_reference_last",
)
STOCK_PRIMARY_BOUNDARY_NOTES = (
    "主结果仅承载业务主判断。",
    "解释层只作辅助，不反向定义统一事实。",
    "治理主解释权位于 /T12，此处仅作边界说明。",
)


@dataclass(frozen=True)
class StockPrimaryResultConclusionViewModel:
    target_code: str
    target_name: str
    primary_stage_label: str
    primary_stage_secondary_label: str
    primary_result_label: str
    risk_label: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class StockPrimaryResultExplanationViewModel:
    sync_note: str
    source_timestamp: str
    history_visible: bool
    history_items: tuple[str, ...]
    disabled_visible: bool
    disabled_text: str
    invalid_visible: bool
    invalid_text: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class StockPrimaryResultBoundaryViewModel:
    scope_note: str
    reference_note: str
    governance_boundary_note: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class StockPrimaryResultViewModel:
    conclusion: StockPrimaryResultConclusionViewModel
    explanation: StockPrimaryResultExplanationViewModel
    boundary: StockPrimaryResultBoundaryViewModel

    def as_dict(self) -> dict[str, object]:
        return {
            "conclusion": self.conclusion.as_dict(),
            "explanation": self.explanation.as_dict(),
            "boundary": self.boundary.as_dict(),
        }


def _normalize_text(value: object) -> str | None:
    text = str(value or "").strip()
    if not text or text in {"-", "None", "null"}:
        return None
    return text


def _placeholder(kind: str) -> str:
    return STOCK_PRIMARY_ALLOWED_PLACEHOLDERS.get(kind, STOCK_PRIMARY_ALLOWED_PLACEHOLDERS["empty"])


def _bounded_text(value: str, limit: int) -> str:
    text = value.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _stage_label(primary_result: dict[str, object]) -> str:
    stage = str(primary_result.get("result_lifecycle_stage", "") or "").strip().upper()
    return STOCK_PRIMARY_STAGE_ZH.get(stage, _placeholder("error"))


def _stage_secondary_label(primary_result: dict[str, object]) -> str:
    result_type = str(primary_result.get("result_type", "") or "").strip().lower()
    if result_type:
        return STOCK_PRIMARY_RESULT_TYPE_ZH.get(result_type, result_type)
    return _placeholder("degraded")


def _risk_label(primary_result: dict[str, object]) -> str:
    risk = str(primary_result.get("risk_level", "") or "").strip().lower()
    if not risk:
        return _placeholder("error")
    return STOCK_PRIMARY_RISK_ZH.get(risk, risk)


def _status_suffix(primary_result: dict[str, object]) -> str | None:
    stage = str(primary_result.get("result_lifecycle_stage", "") or "").strip().upper()
    if stage == "L4":
        raw = _normalize_text(primary_result.get("execution_status"))
        if raw is not None:
            return STOCK_PRIMARY_STATUS_SUFFIX_MAPS["execution_status"].get(raw.lower(), raw)
    if stage == "L3":
        raw = _normalize_text(primary_result.get("audit_status"))
        if raw is not None:
            return STOCK_PRIMARY_STATUS_SUFFIX_MAPS["audit_status"].get(raw.lower(), raw)
    for key in ("candidate_status", "research_status"):
        raw = _normalize_text(primary_result.get(key))
        if raw is not None:
            return STOCK_PRIMARY_STATUS_SUFFIX_MAPS[key].get(raw.lower(), raw)
    return None


def _primary_result_label(primary_result: dict[str, object]) -> str:
    stage_label = _stage_label(primary_result)
    secondary = _status_suffix(primary_result)
    if secondary is None:
        return stage_label
    return _bounded_text(f"{stage_label}（{secondary}）", STOCK_PRIMARY_MAX_CONCLUSION_TEXT_LENGTH)


def _source_timestamp(primary_result: dict[str, object]) -> str:
    timestamps = primary_result.get("source_timestamps")
    if not isinstance(timestamps, dict):
        return _placeholder("degraded")
    values = [str(value).strip() for value in timestamps.values() if str(value).strip() and str(value).strip() != "-"]
    return max(values) if values else _placeholder("degraded")


def _sync_note(primary_result: dict[str, object]) -> str:
    note = _normalize_text(primary_result.get("data_sync_note"))
    if note is None:
        return STOCK_PRIMARY_STANDARD_SYNC_NOTES["degraded"]
    lowered = note.lower()
    if note.startswith("降级显示："):
        return STOCK_PRIMARY_STANDARD_SYNC_NOTES["degraded"]
    if "batch_prediction_timeout" in lowered:
        return "当前结果仍带补证痕迹"
    return note


def _history_items(primary_result: dict[str, object]) -> tuple[str, ...]:
    history_segments: list[str] = []
    history_summary = _normalize_text(primary_result.get("history_summary"))
    if history_summary is not None:
        replacements = {
            "执行记录 ready": "执行记录 待执行准备",
            "执行记录 completed": "执行记录 已完成",
            "审核记录 passed": "审核记录 已通过",
            "研究记录 completed": "研究记录 已完成",
        }
        for segment in [segment.strip() for segment in history_summary.split("；") if segment.strip()]:
            history_segments.append(replacements.get(segment, segment))
    source_item = None
    source_file = _normalize_text(primary_result.get("history_source_file"))
    source_timestamp = _normalize_text(primary_result.get("history_source_timestamp"))
    generation_mode = _normalize_text(primary_result.get("history_generation_mode"))
    if source_file is not None and source_timestamp is not None:
        mode_label = "直接事实" if generation_mode == "direct" else "降级说明"
        source_item = f"来源 {source_file} · 同步 {source_timestamp} · {mode_label}"
    if not history_segments and source_item is None:
        return tuple()
    items = history_segments[:STOCK_PRIMARY_MAX_EXPLANATION_ITEMS]
    if source_item is not None:
        if len(items) >= STOCK_PRIMARY_MAX_EXPLANATION_ITEMS:
            items = items[: STOCK_PRIMARY_MAX_EXPLANATION_ITEMS - 1]
        items.append(source_item)
    trimmed = []
    for item in items[:STOCK_PRIMARY_MAX_EXPLANATION_ITEMS]:
        text = item.strip()
        if len(text) > STOCK_PRIMARY_MAX_EXPLANATION_ITEM_LENGTH:
            text = text[: STOCK_PRIMARY_MAX_EXPLANATION_ITEM_LENGTH - 1].rstrip() + "…"
        trimmed.append(text)
    return tuple(trimmed)


def _disabled_text(primary_result: dict[str, object]) -> str:
    disabled_reason = _normalize_text(primary_result.get("disabled_reason"))
    return disabled_reason or _placeholder("degraded")


def _invalid_text(primary_result: dict[str, object]) -> str:
    invalid_reason = _normalize_text(primary_result.get("invalid_reason"))
    terminal_outcome = _normalize_text(primary_result.get("terminal_outcome"))
    if invalid_reason is not None:
        return invalid_reason
    if terminal_outcome is not None:
        return STOCK_PRIMARY_INVALID_TEXT_MAP.get(terminal_outcome.lower(), terminal_outcome)
    return _placeholder("degraded")


def _filter_mixed_fields(primary_result: dict[str, object]) -> dict[str, object]:
    disallowed = STOCK_PRIMARY_GOVERNANCE_MIX_FIELDS | STOCK_PRIMARY_MAIN_SITE_MIX_FIELDS
    return {key: value for key, value in primary_result.items() if key not in disallowed}


def adapt_stock_primary_result_input(primary_result: dict | object) -> dict[str, object]:
    raw_input = primary_result if isinstance(primary_result, dict) else {}
    safe_fact = _filter_mixed_fields(dict(raw_input))
    adapted: dict[str, object] = {}
    for key in STOCK_PRIMARY_ALLOWED_INPUT_FIELDS:
        if key not in safe_fact:
            continue
        value = safe_fact.get(key)
        if key == "source_timestamps":
            adapted[key] = dict(value) if isinstance(value, dict) else {}
            continue
        adapted[key] = value
    if "source_timestamps" not in adapted:
        adapted["source_timestamps"] = {}
    return adapted


def build_stock_primary_result_view_model(primary_result: dict) -> StockPrimaryResultViewModel:
    safe_fact = adapt_stock_primary_result_input(primary_result)
    conclusion = StockPrimaryResultConclusionViewModel(
        target_code=_bounded_text(_normalize_text(safe_fact.get("ts_code")) or _placeholder("empty"), STOCK_PRIMARY_MAX_CONCLUSION_TEXT_LENGTH),
        target_name=_bounded_text(_normalize_text(safe_fact.get("stock_name")) or _placeholder("empty"), STOCK_PRIMARY_MAX_TARGET_NAME_LENGTH),
        primary_stage_label=_bounded_text(_stage_label(safe_fact), STOCK_PRIMARY_MAX_CONCLUSION_TEXT_LENGTH),
        primary_stage_secondary_label=_bounded_text(_stage_secondary_label(safe_fact), STOCK_PRIMARY_MAX_CONCLUSION_TEXT_LENGTH),
        primary_result_label=_bounded_text(_primary_result_label(safe_fact), STOCK_PRIMARY_MAX_CONCLUSION_TEXT_LENGTH),
        risk_label=_bounded_text(_risk_label(safe_fact), STOCK_PRIMARY_MAX_CONCLUSION_TEXT_LENGTH),
    )
    history_items = _history_items(safe_fact)
    disabled_raw = _normalize_text(safe_fact.get("disabled_reason"))
    invalid_raw = _normalize_text(safe_fact.get("invalid_reason")) or _normalize_text(safe_fact.get("terminal_outcome"))
    explanation = StockPrimaryResultExplanationViewModel(
        sync_note=_sync_note(safe_fact),
        source_timestamp=_source_timestamp(safe_fact),
        history_visible=bool(history_items),
        history_items=history_items,
        disabled_visible=disabled_raw is not None,
        disabled_text=_disabled_text(safe_fact),
        invalid_visible=invalid_raw is not None,
        invalid_text=_invalid_text(safe_fact),
    )
    boundary = StockPrimaryResultBoundaryViewModel(
        scope_note=STOCK_PRIMARY_BOUNDARY_NOTES[0],
        reference_note=STOCK_PRIMARY_BOUNDARY_NOTES[1],
        governance_boundary_note=STOCK_PRIMARY_BOUNDARY_NOTES[2],
    )
    return StockPrimaryResultViewModel(
        conclusion=conclusion,
        explanation=explanation,
        boundary=boundary,
    )


def render_stock_primary_result(view_model: StockPrimaryResultViewModel) -> str:
    history_html = ""
    if view_model.explanation.history_visible:
        history_html = (
            '<section class="stock-primary-result__explanation-block stock-primary-result__explanation-block--history">'
            '<h4 class="stock-primary-result__block-title">历史验证</h4>'
            '<ul class="stock-primary-result__history-list">'
            + "".join(
                f'<li class="stock-primary-result__history-item">{html.escape(item)}</li>'
                for item in view_model.explanation.history_items
            )
            + "</ul></section>"
        )
    disabled_html = ""
    if view_model.explanation.disabled_visible:
        disabled_html = (
            '<section class="stock-primary-result__explanation-block stock-primary-result__explanation-block--warning">'
            '<h4 class="stock-primary-result__block-title">禁用解释</h4>'
            f'<p class="stock-primary-result__block-text">{html.escape(view_model.explanation.disabled_text)}</p>'
            '</section>'
        )
    invalid_html = ""
    if view_model.explanation.invalid_visible:
        invalid_html = (
            '<section class="stock-primary-result__explanation-block stock-primary-result__explanation-block--critical">'
            '<h4 class="stock-primary-result__block-title">失效解释</h4>'
            f'<p class="stock-primary-result__block-text">{html.escape(view_model.explanation.invalid_text)}</p>'
            '</section>'
        )
    return (
        '<div class="stock-primary-result" id="stock-primary-result">'
        '<section class="stock-primary-result__conclusion">'
        '<div class="stock-primary-result__eyebrow">主结果</div>'
        '<div class="display-contract stock-primary-result__display-contract" data-display-contract="primary-result">'
        '<div class="display-contract-cell display-contract-conclusion">'
        '<span>结论</span>'
        f'<strong>{html.escape(view_model.conclusion.primary_result_label)}</strong>'
        f'<small>{html.escape(view_model.conclusion.target_code)} {html.escape(view_model.conclusion.target_name)}</small>'
        '</div>'
        '<div class="display-contract-cell">'
        '<span>证据</span>'
        f'<strong>{html.escape(view_model.explanation.sync_note)}</strong>'
        f'<small>最近来源时间 {html.escape(view_model.explanation.source_timestamp)}</small>'
        '</div>'
        '<div class="display-contract-cell display-contract-boundary">'
        '<span>边界</span>'
        '<strong>主结果只读</strong>'
        f'<small>{html.escape(view_model.boundary.scope_note)}</small>'
        '</div>'
        '<div class="display-contract-cell display-contract-next">'
        '<span>下一步</span>'
        '<strong>回到第一屏证据驾驶舱</strong>'
        '<small>先确认行动锁和数据门，再讨论样本闭合。</small>'
        '</div>'
        '</div>'
        '<div class="stock-primary-result__summary-grid">'
        '<div class="stock-primary-result__summary-item stock-primary-result__summary-item--target">'
        '<div class="stock-primary-result__label">当前对象</div>'
        f'<div class="stock-primary-result__value">{html.escape(view_model.conclusion.target_code)}</div>'
        f'<div class="stock-primary-result__subvalue">{html.escape(view_model.conclusion.target_name)}</div>'
        '</div>'
        '<div class="stock-primary-result__summary-item stock-primary-result__summary-item--stage">'
        '<div class="stock-primary-result__label">主阶段</div>'
        f'<div class="stock-primary-result__value">{html.escape(view_model.conclusion.primary_result_label)}</div>'
        f'<div class="stock-primary-result__subvalue">{html.escape(view_model.conclusion.primary_stage_secondary_label)}</div>'
        '</div>'
        '<div class="stock-primary-result__summary-item stock-primary-result__summary-item--risk">'
        '<div class="stock-primary-result__label">风险提示</div>'
        f'<div class="stock-primary-result__value">{html.escape(view_model.conclusion.risk_label)}</div>'
        '</div>'
        '</div>'
        '</section>'
        '<section class="stock-primary-result__explanation">'
        '<h3 class="stock-primary-result__section-title">解释层</h3>'
        '<div class="stock-primary-result__explanation-topline">'
        f'<span class="stock-primary-result__sync-note">{html.escape(view_model.explanation.sync_note)}</span>'
        f'<span class="stock-primary-result__source-ts">最近来源时间 {html.escape(view_model.explanation.source_timestamp)}</span>'
        '</div>'
        f'{history_html}'
        f'{disabled_html}'
        f'{invalid_html}'
        '</section>'
        '<section class="stock-primary-result__boundary">'
        '<h3 class="stock-primary-result__section-title">边界说明</h3>'
        f'<p class="stock-primary-result__boundary-text">{html.escape(view_model.boundary.scope_note)}</p>'
        f'<p class="stock-primary-result__boundary-text">{html.escape(view_model.boundary.reference_note)}</p>'
        f'<p class="stock-primary-result__boundary-text">{html.escape(view_model.boundary.governance_boundary_note)}</p>'
        '</section>'
        '</div>'
    )
