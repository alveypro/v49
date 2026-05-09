from src.stock_dashboard_assets import compose_dashboard_script_tag
from src.stock_dashboard_assets import compose_dashboard_stylesheet
from src.stock_dashboard_assets import compose_fail_closed_stylesheet
from src.stock_dashboard_assets import compose_inline_style_tag


def test_compose_dashboard_stylesheet_excludes_t12_scope_by_default():
    css = compose_dashboard_stylesheet(is_t12_scope=False)
    assert ":root {" in css
    assert "#t12-governance-summary.t12-governance-summary" not in css
    assert "__T12_GOVERNANCE_SCOPE_CSS__" not in css


def test_compose_dashboard_stylesheet_includes_t12_scope_when_enabled():
    css = compose_dashboard_stylesheet(is_t12_scope=True)
    assert "#t12-governance-summary.t12-governance-summary" in css
    assert "#t12-readonly-note.t12-readonly-note" in css
    assert "__T12_GOVERNANCE_SCOPE_CSS__" not in css


def test_compose_fail_closed_stylesheet_contains_fail_closed_shell_contract():
    css = compose_fail_closed_stylesheet()
    assert ".shell {" in css
    assert ".panel {" in css
    assert "background: #f5f1ea;" in css


def test_compose_inline_style_tag_wraps_stylesheet():
    html = compose_inline_style_tag("body { color: red; }")
    assert html.startswith("<style>\n")
    assert "body { color: red; }" in html
    assert html.endswith("\n  </style>")


def test_compose_dashboard_script_tag_joins_non_empty_scripts():
    html = compose_dashboard_script_tag("const A = 1;", "", "const B = 2;")
    assert html.startswith("<script>\n")
    assert "const A = 1;" in html
    assert "const B = 2;" in html
    assert html.endswith("\n  </script>")
