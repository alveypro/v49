from src import dashboard_support
from src.stock_primary_result import build_stock_primary_result_view_model, render_stock_primary_result


def _sample_primary_results() -> dict[str, dict[str, object]]:
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
            "risk_level": "medium",
            "disabled_reason": "当前进入冻结窗口。",
            "terminal_outcome": "expired",
            "data_sync_note": "制度字段已对齐。",
        },
        "polluted": {
            "ts_code": "000001.SZ",
            "stock_name": "平安银行",
            "result_lifecycle_stage": "L2",
            "result_type": "candidate",
            "candidate_status": "shortlisted",
            "risk_level": "low",
            "data_sync_note": "制度字段已对齐。",
            "current_progress_status": "不可推进",
            "platform_story": "Airivo 是统一母平台",
        },
    }


def test_stock_primary_result_single_track_normalization_is_stable():
    for sample in _sample_primary_results().values():
        vm = build_stock_primary_result_view_model(sample)
        html_text = render_stock_primary_result(vm)
        assert vm.conclusion.primary_result_label
        assert "当前推进状态" not in str(vm.as_dict())
        assert "Airivo 是统一母平台" not in str(vm.as_dict())
        assert "Governance Summary" not in html_text


def test_stock_primary_result_single_track_runtime_contract_is_canonical_only():
    metadata = dashboard_support.stock_primary_result_runtime_metadata({})
    assert dashboard_support.stock_primary_result_runtime_mode() == "canonical"
    assert dashboard_support.stock_primary_result_canonical_render_enabled() is True
    assert metadata["stock_primary_result_runtime_mode"] == "canonical"
    assert metadata["stock_primary_result_fallback_reason"] == "none"
    assert metadata["stock_primary_result_has_problem_fallback"] is False
