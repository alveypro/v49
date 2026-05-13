from src.stock_dashboard_fail_closed_page import render_stock_fail_closed_page, select_hard_fail_closed_problems


def test_select_hard_fail_closed_problems_only_keeps_lifecycle_evidence_failures():
    problems = select_hard_fail_closed_problems(
        {
            "problems": [
                "primary_result_lifecycle_evidence_latest.json missing",
                "current pointer missing",
            ]
        }
    )

    assert problems == ["primary_result_lifecycle_evidence_latest.json missing"]


def test_render_stock_fail_closed_page_escapes_problem_text_and_links_api():
    html = render_stock_fail_closed_page(
        base_path="/stock",
        entry_guard={"problems": ['primary_result_lifecycle_evidence_latest.json missing <script>alert(1)</script>']},
    )

    assert "/stock 当前禁止输出主结果" in html
    assert 'href="/stock/api/primary-result"' in html
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html


def test_render_stock_fail_closed_page_falls_back_when_problem_list_empty():
    html = render_stock_fail_closed_page(base_path="/stock", entry_guard={"problems": []})

    assert "未提供具体阻断原因" in html
