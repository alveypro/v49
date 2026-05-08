from src.dashboard_support import STOCK_PRIMARY_RESULT_FALLBACK_REASONS, stock_primary_result_runtime_metadata
from src.stock_primary_result import (
    STOCK_PRIMARY_ALLOWED_PLACEHOLDERS,
    STOCK_PRIMARY_BOUNDARY_NOTE_COUNT,
    STOCK_PRIMARY_BOUNDARY_NOTES,
    STOCK_PRIMARY_CONCLUSION_PRIORITY_FIELDS,
    STOCK_PRIMARY_DISALLOWED_CONCLUSION_TERMS,
    STOCK_PRIMARY_EXPLANATION_ORDER_RULE,
    STOCK_PRIMARY_STANDARD_SYNC_NOTES,
    STOCK_PRIMARY_STATUS_SUFFIX_MAPS,
    STOCK_PRIMARY_MAX_CONCLUSION_TEXT_LENGTH,
    STOCK_PRIMARY_MAX_EXPLANATION_ITEM_LENGTH,
    STOCK_PRIMARY_MAX_EXPLANATION_ITEMS,
    build_stock_primary_result_view_model,
)


DISALLOWED_GOVERNANCE_TERMS = ("当前推进状态", "当前主要阻断", "当前治理备注", "Governance Summary")
DISALLOWED_MAIN_SITE_TERMS = ("Airivo 是统一母平台", "面向未来")


def test_stock_primary_result_content_quality_conclusion_is_present_and_controlled():
    vm = build_stock_primary_result_view_model(
        {
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "result_lifecycle_stage": "L2",
            "result_type": "candidate",
            "candidate_status": "shortlisted",
            "risk_level": "low",
            "data_sync_note": "制度字段已对齐。",
            "current_progress_status": "不可推进",
            "platform_story": "Airivo 是统一母平台",
        }
    )
    assert vm.conclusion.target_code
    assert vm.conclusion.target_name
    assert len(vm.conclusion.primary_stage_label) <= STOCK_PRIMARY_MAX_CONCLUSION_TEXT_LENGTH
    assert len(vm.conclusion.primary_stage_secondary_label) <= STOCK_PRIMARY_MAX_CONCLUSION_TEXT_LENGTH
    assert len(vm.conclusion.primary_result_label) <= STOCK_PRIMARY_MAX_CONCLUSION_TEXT_LENGTH
    assert len(vm.conclusion.risk_label) <= STOCK_PRIMARY_MAX_CONCLUSION_TEXT_LENGTH
    assert STOCK_PRIMARY_CONCLUSION_PRIORITY_FIELDS == (
        "target_code",
        "target_name",
        "primary_result_label",
        "risk_label",
    )
    serialized = str(vm.conclusion.as_dict())
    for term in DISALLOWED_GOVERNANCE_TERMS + DISALLOWED_MAIN_SITE_TERMS:
        assert term not in serialized
    assert STOCK_PRIMARY_DISALLOWED_CONCLUSION_TERMS >= set(DISALLOWED_GOVERNANCE_TERMS + DISALLOWED_MAIN_SITE_TERMS)


def test_stock_primary_result_content_quality_conclusion_status_labels_stay_in_allowed_set():
    vm = build_stock_primary_result_view_model(
        {
            "result_lifecycle_stage": "L4",
            "result_type": "execution",
            "execution_status": "failed",
            "risk_level": "high",
        }
    )
    allowed_suffixes = set().union(*[set(mapping.values()) for mapping in STOCK_PRIMARY_STATUS_SUFFIX_MAPS.values()])
    assert "（" in vm.conclusion.primary_result_label
    suffix = vm.conclusion.primary_result_label.split("（", 1)[1].rstrip("）")
    assert suffix in allowed_suffixes


def test_stock_primary_result_content_quality_explanation_is_bounded_and_ordered():
    vm = build_stock_primary_result_view_model(
        {
            "result_lifecycle_stage": "L2",
            "data_sync_note": "制度字段已对齐。",
            "history_summary": "候选记录 shortlisted；研究记录 completed；审核记录 in_review；执行记录 queued",
            "history_source_file": "buylist_latest.json",
            "history_source_timestamp": "2026-04-12 09:11:19",
            "history_generation_mode": "degraded",
            "disabled_reason": "当前进入冻结窗口。",
            "invalid_reason": "对象已过有效窗口。",
        }
    )
    assert vm.explanation.sync_note == "制度字段已对齐。"
    assert len(vm.explanation.history_items) <= STOCK_PRIMARY_MAX_EXPLANATION_ITEMS
    assert STOCK_PRIMARY_EXPLANATION_ORDER_RULE == (
        "history_summary_first",
        "source_reference_last",
    )
    assert vm.explanation.history_items[0].startswith("候选记录")
    assert vm.explanation.history_items[-1].startswith("来源 ")
    for item in vm.explanation.history_items:
        assert len(item) <= STOCK_PRIMARY_MAX_EXPLANATION_ITEM_LENGTH
    assert vm.explanation.disabled_visible is True
    assert vm.explanation.invalid_visible is True


def test_stock_primary_result_content_quality_boundary_stays_lightweight():
    vm = build_stock_primary_result_view_model({"result_lifecycle_stage": "L1"})
    notes = (
        vm.boundary.scope_note,
        vm.boundary.reference_note,
        vm.boundary.governance_boundary_note,
    )
    assert notes == STOCK_PRIMARY_BOUNDARY_NOTES
    assert len(notes) == STOCK_PRIMARY_BOUNDARY_NOTE_COUNT
    for note in notes:
        assert len(note) <= 24
    assert notes[-1] == "治理主解释权位于 /T12，此处仅作边界说明。"
    serialized = str(notes)
    for term in DISALLOWED_GOVERNANCE_TERMS:
        assert term not in serialized


def test_stock_primary_result_content_quality_placeholders_remain_controlled():
    vm = build_stock_primary_result_view_model({})
    assert vm.conclusion.target_code == STOCK_PRIMARY_ALLOWED_PLACEHOLDERS["empty"]
    assert vm.conclusion.primary_stage_label == STOCK_PRIMARY_ALLOWED_PLACEHOLDERS["error"]
    assert vm.explanation.sync_note == STOCK_PRIMARY_ALLOWED_PLACEHOLDERS["degraded"]
    assert vm.explanation.source_timestamp == STOCK_PRIMARY_ALLOWED_PLACEHOLDERS["degraded"]


def test_stock_primary_result_content_quality_disabled_and_degraded_text_stays_controlled():
    vm = build_stock_primary_result_view_model(
        {
            "result_lifecycle_stage": "L2",
            "data_sync_note": "降级显示：历史文件缺失。",
            "disabled_reason": "当前进入冻结窗口。",
            "terminal_outcome": "expired",
        }
    )
    assert vm.explanation.sync_note == STOCK_PRIMARY_STANDARD_SYNC_NOTES["degraded"]
    assert vm.explanation.disabled_text == "当前进入冻结窗口。"
    assert vm.explanation.invalid_text == "结果已失效"


def test_stock_primary_result_content_quality_fallback_reason_set_is_controlled():
    metadata = stock_primary_result_runtime_metadata({})
    assert metadata["stock_primary_result_fallback_reason"] in STOCK_PRIMARY_RESULT_FALLBACK_REASONS
    assert STOCK_PRIMARY_RESULT_FALLBACK_REASONS == {"none"}
