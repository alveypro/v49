from src.stock_primary_result import build_stock_primary_result_view_model, render_stock_primary_result


def test_stock_primary_result_rendering_order():
    html_text = render_stock_primary_result(
        build_stock_primary_result_view_model(
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
    )
    conclusion_index = html_text.index('class="stock-primary-result__conclusion"')
    explanation_index = html_text.index('class="stock-primary-result__explanation"')
    boundary_index = html_text.index('class="stock-primary-result__boundary"')
    assert conclusion_index < explanation_index < boundary_index


def test_stock_primary_result_rendering_keeps_conclusion_primary():
    html_text = render_stock_primary_result(
        build_stock_primary_result_view_model(
            {
                "ts_code": "000001.SZ",
                "stock_name": "平安银行",
                "result_lifecycle_stage": "L3",
                "result_type": "audit",
                "audit_status": "in_review",
                "risk_level": "medium",
            }
        )
    )
    assert "主结果" in html_text
    assert "审核阶段结果（审核中）" in html_text
    assert "解释层" in html_text
    assert "边界说明" in html_text


def test_stock_primary_result_rendering_explanation_does_not_overpower_conclusion():
    html_text = render_stock_primary_result(
        build_stock_primary_result_view_model(
            {
                "result_lifecycle_stage": "L2",
                "history_summary": "候选记录 shortlisted",
                "history_source_file": "buylist_latest.json",
                "history_source_timestamp": "2026-04-12 09:11:19",
                "history_generation_mode": "degraded",
                "data_sync_note": "制度字段已对齐。",
            }
        )
    )
    assert html_text.count('stock-primary-result__section-title') == 2
    assert "历史验证" in html_text
    assert "Governance Summary" not in html_text


def test_stock_primary_result_rendering_boundary_is_lightweight():
    html_text = render_stock_primary_result(build_stock_primary_result_view_model({"result_lifecycle_stage": "L1"}))
    assert "治理主解释权位于 /T12，此处仅作边界说明。" in html_text
    assert "当前推进状态" not in html_text
    assert "当前主要阻断" not in html_text
    assert "当前治理备注" not in html_text


def test_stock_primary_result_rendering_excludes_governance_and_main_site_mixed_text():
    html_text = render_stock_primary_result(
        build_stock_primary_result_view_model(
            {
                "result_lifecycle_stage": "L2",
                "current_progress_status": "不可推进",
                "platform_story": "Airivo 是统一母平台",
                "brand_slogan": "面向未来",
            }
        )
    )
    assert "不可推进" not in html_text
    assert "Airivo 是统一母平台" not in html_text
    assert "面向未来" not in html_text
