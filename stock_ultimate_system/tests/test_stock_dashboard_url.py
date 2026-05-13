from src.stock_dashboard_url import (
    base_href,
    is_main_site_scope,
    is_t12_scope,
    normalize_base_path,
    report_href,
    resolve_namespace_id,
    resolve_scope_id,
    view_href,
)


def test_normalize_base_path_keeps_empty_root_and_strips_slashes():
    assert normalize_base_path("") == ""
    assert normalize_base_path("/") == ""
    assert normalize_base_path("stock") == "/stock"
    assert normalize_base_path("/apex/stock/") == "/apex/stock"


def test_base_href_and_view_href_are_mount_aware():
    assert base_href("/stock", "/api/primary-result") == "/stock/api/primary-result"
    assert base_href("", "/api/primary-result") == "/api/primary-result"
    assert view_href("overview", 0, "/stock") == "/stock/?view=overview"
    assert view_href("candidates", 2, "/stock") == "/stock/?view=candidates&candidate=2"
    assert report_href("evolution", 1, "/stock") == "/stock/?view=reports&candidate=1&report=evolution"


def test_namespace_and_scope_resolution_support_canonical_mounts():
    assert is_t12_scope("/T12") is True
    assert is_t12_scope("/apex/T12") is True
    assert is_main_site_scope("/") is True
    assert is_main_site_scope("/apex") is True
    assert resolve_namespace_id("/") == "production"
    assert resolve_namespace_id("/apex/stock") == "apex"
    assert resolve_scope_id("/stock") == "stock"
    assert resolve_scope_id("/apex/T12") == "t12"
