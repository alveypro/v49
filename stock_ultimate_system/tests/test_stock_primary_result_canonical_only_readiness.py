from src.stock_primary_result import (
    STOCK_PRIMARY_ALLOWED_PLACEHOLDERS,
    STOCK_PRIMARY_BOUNDARY_NOTE_COUNT,
    STOCK_PRIMARY_BOUNDARY_NOTES,
    STOCK_PRIMARY_MAX_CONCLUSION_TEXT_LENGTH,
    STOCK_PRIMARY_MAX_EXPLANATION_ITEMS,
    build_stock_primary_result_view_model,
    render_stock_primary_result,
)


def _canonical_only_samples() -> dict[str, dict[str, object]]:
    return {
        "normal": {
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "result_lifecycle_stage": "L2",
            "result_type": "candidate",
            "candidate_status": "shortlisted",
            "risk_level": "low",
            "data_sync_note": "制度字段已对齐。",
            "source_timestamps": {"buylist_latest.json": "2026-04-12 09:11:19"},
            "history_summary": "候选记录 shortlisted",
        },
        "empty": {},
        "degraded": {
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "result_lifecycle_stage": "L2",
            "result_type": "candidate",
            "candidate_status": "shortlisted",
            "risk_level": "medium",
            "data_sync_note": "降级显示：历史文件缺失。",
            "history_source_file": "buylist_latest.json",
            "history_source_timestamp": "2026-04-12 09:11:19",
            "history_generation_mode": "degraded",
            "source_timestamps": {},
        },
        "disabled_invalid": {
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "result_lifecycle_stage": "L2",
            "result_type": "candidate",
            "disabled_reason": "当前进入冻结窗口。",
            "terminal_outcome": "expired",
            "data_sync_note": "制度字段已对齐。",
        },
        "historical_noise": {
            "ts_code": "000001.SZ",
            "stock_name": "平安银行股份有限公司超长名称用于裁剪验证",
            "result_lifecycle_stage": "L2",
            "result_type": "candidate",
            "candidate_status": "shortlisted",
            "risk_level": "low",
            "data_sync_note": "制度字段已对齐。",
            "history_summary": "候选记录 shortlisted；研究记录 completed；审核记录 in_review；执行记录 queued",
            "history_source_file": "buylist_latest.json",
            "history_source_timestamp": "2026-04-12 09:11:19",
            "history_generation_mode": "degraded",
            "legacy_debug_dump": "unused",
        },
        "governance_polluted": {
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "result_lifecycle_stage": "L2",
            "result_type": "candidate",
            "candidate_status": "shortlisted",
            "risk_level": "low",
            "data_sync_note": "制度字段已对齐。",
            "current_progress_status": "不可推进",
            "current_primary_blocker": "治理阻断",
            "current_governance_note": "治理备注",
        },
        "main_site_polluted": {
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "result_lifecycle_stage": "L2",
            "result_type": "candidate",
            "candidate_status": "shortlisted",
            "risk_level": "low",
            "data_sync_note": "制度字段已对齐。",
            "platform_story": "Airivo 是统一母平台",
            "brand_slogan": "面向未来",
        },
    }


def test_stock_primary_result_canonical_only_readiness_holds_for_core_samples():
    for sample in _canonical_only_samples().values():
        vm = build_stock_primary_result_view_model(sample)
        html_text = render_stock_primary_result(vm)

        assert vm.conclusion.target_code
        assert len(vm.conclusion.primary_result_label) <= STOCK_PRIMARY_MAX_CONCLUSION_TEXT_LENGTH
        assert len(vm.explanation.history_items) <= STOCK_PRIMARY_MAX_EXPLANATION_ITEMS
        assert (
            vm.boundary.scope_note,
            vm.boundary.reference_note,
            vm.boundary.governance_boundary_note,
        ) == STOCK_PRIMARY_BOUNDARY_NOTES
        assert len(STOCK_PRIMARY_BOUNDARY_NOTES) == STOCK_PRIMARY_BOUNDARY_NOTE_COUNT

        conclusion_index = html_text.index('class="stock-primary-result__conclusion"')
        explanation_index = html_text.index('class="stock-primary-result__explanation"')
        boundary_index = html_text.index('class="stock-primary-result__boundary"')
        assert conclusion_index < explanation_index < boundary_index
        assert "当前推进状态" not in html_text
        assert "Governance Summary" not in html_text
        assert "Airivo 是统一母平台" not in html_text
        assert "面向未来" not in html_text


def test_stock_primary_result_canonical_only_readiness_keeps_controlled_empty_and_degraded_output():
    empty_vm = build_stock_primary_result_view_model(_canonical_only_samples()["empty"])
    degraded_vm = build_stock_primary_result_view_model(_canonical_only_samples()["degraded"])

    assert empty_vm.conclusion.target_code == STOCK_PRIMARY_ALLOWED_PLACEHOLDERS["empty"]
    assert empty_vm.explanation.sync_note == STOCK_PRIMARY_ALLOWED_PLACEHOLDERS["degraded"]
    assert degraded_vm.explanation.sync_note == STOCK_PRIMARY_ALLOWED_PLACEHOLDERS["degraded"]
    assert degraded_vm.explanation.source_timestamp == STOCK_PRIMARY_ALLOWED_PLACEHOLDERS["degraded"]


def test_stock_primary_result_canonical_only_readiness_keeps_disabled_and_invalid_explanations_stable():
    vm = build_stock_primary_result_view_model(_canonical_only_samples()["disabled_invalid"])
    assert vm.explanation.disabled_visible is True
    assert vm.explanation.invalid_visible is True
    assert vm.explanation.disabled_text == "当前进入冻结窗口。"
    assert vm.explanation.invalid_text == "结果已失效"
