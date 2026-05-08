from src.stock_primary_result import (
    STOCK_PRIMARY_ALLOWED_PLACEHOLDERS,
    STOCK_PRIMARY_GOVERNANCE_MIX_FIELDS,
    STOCK_PRIMARY_MAIN_SITE_MIX_FIELDS,
    build_stock_primary_result_view_model,
)


def test_stock_primary_result_view_model_structure():
    vm = build_stock_primary_result_view_model(
        {
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "result_lifecycle_stage": "L2",
            "result_type": "candidate",
            "candidate_status": "shortlisted",
            "risk_level": "low",
            "data_sync_note": "制度字段已对齐。",
        }
    )
    data = vm.as_dict()
    assert set(data.keys()) == {"conclusion", "explanation", "boundary"}
    assert set(data["conclusion"].keys()) == {
        "target_code",
        "target_name",
        "primary_stage_label",
        "primary_stage_secondary_label",
        "primary_result_label",
        "risk_label",
    }
    assert set(data["explanation"].keys()) == {
        "sync_note",
        "source_timestamp",
        "history_visible",
        "history_items",
        "disabled_visible",
        "disabled_text",
        "invalid_visible",
        "invalid_text",
    }
    assert set(data["boundary"].keys()) == {
        "scope_note",
        "reference_note",
        "governance_boundary_note",
    }


def test_stock_primary_result_conclusion_priority():
    vm = build_stock_primary_result_view_model(
        {
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "result_lifecycle_stage": "L3",
            "result_type": "audit",
            "audit_status": "in_review",
            "risk_level": "medium",
        }
    )
    assert vm.conclusion.primary_stage_label == "审核阶段结果"
    assert vm.conclusion.primary_result_label == "审核阶段结果（审核中）"


def test_stock_primary_result_explanation_is_controlled():
    vm = build_stock_primary_result_view_model(
        {
            "result_lifecycle_stage": "L2",
            "history_summary": "候选记录 shortlisted；研究记录 completed；审核记录 in_review；执行记录 queued",
            "history_source_file": "buylist_latest.json",
            "history_source_timestamp": "2026-04-12 09:11:19",
            "history_generation_mode": "degraded",
            "data_sync_note": "制度字段已对齐。",
        }
    )
    assert vm.explanation.history_visible is True
    assert len(vm.explanation.history_items) <= 3


def test_stock_primary_result_boundary_is_lightweight():
    vm = build_stock_primary_result_view_model({"result_lifecycle_stage": "L1"})
    assert vm.boundary.scope_note == "主结果仅承载业务主判断。"
    assert "/T12" in vm.boundary.governance_boundary_note
    assert "Governance Summary" not in vm.boundary.governance_boundary_note


def test_governance_summary_fields_must_not_mix_into_stock_primary_result():
    payload = {
        "ts_code": "000001.SZ",
        "result_lifecycle_stage": "L2",
        "current_progress_status": "不可推进",
        "current_primary_blocker": "治理阻断",
        "current_governance_note": "治理备注",
    }
    vm = build_stock_primary_result_view_model(payload)
    data = vm.as_dict()
    assert "治理阻断" not in str(data)
    assert "治理备注" not in str(data)
    assert STOCK_PRIMARY_GOVERNANCE_MIX_FIELDS <= set(payload.keys())


def test_main_site_platform_narrative_must_not_mix_into_stock_primary_result():
    payload = {
        "ts_code": "000001.SZ",
        "result_lifecycle_stage": "L2",
        "platform_story": "Airivo 是统一母平台",
        "brand_slogan": "面向未来",
    }
    vm = build_stock_primary_result_view_model(payload)
    data = vm.as_dict()
    assert "Airivo 是统一母平台" not in str(data)
    assert "面向未来" not in str(data)
    assert STOCK_PRIMARY_MAIN_SITE_MIX_FIELDS & set(payload.keys())


def test_stock_primary_result_empty_and_degraded_states():
    vm = build_stock_primary_result_view_model({})
    assert vm.conclusion.target_code == STOCK_PRIMARY_ALLOWED_PLACEHOLDERS["empty"]
    assert vm.conclusion.target_name == STOCK_PRIMARY_ALLOWED_PLACEHOLDERS["empty"]
    assert vm.conclusion.primary_stage_label == STOCK_PRIMARY_ALLOWED_PLACEHOLDERS["error"]
    assert vm.explanation.sync_note == STOCK_PRIMARY_ALLOWED_PLACEHOLDERS["degraded"]
    assert vm.explanation.source_timestamp == STOCK_PRIMARY_ALLOWED_PLACEHOLDERS["degraded"]
    assert vm.boundary.scope_note == "主结果仅承载业务主判断。"


def test_stock_primary_result_invalid_and_disabled_states():
    vm = build_stock_primary_result_view_model(
        {
            "result_lifecycle_stage": "L2",
            "disabled_reason": "当前进入冻结窗口。",
            "terminal_outcome": "expired",
        }
    )
    assert vm.explanation.disabled_visible is True
    assert vm.explanation.disabled_text == "当前进入冻结窗口。"
    assert vm.explanation.invalid_visible is True
    assert vm.explanation.invalid_text == "结果已失效"
