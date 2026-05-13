from src.stock_dashboard_constants import VIEW_LABELS, VIEW_SUBTITLES
from src.stock_dashboard_view_contract import T12_VIEW_SUBTITLE, view_labels, view_subtitles


def test_view_contract_keeps_stock_scope_to_public_views_only():
    assert view_labels("/stock") == dict(VIEW_LABELS)
    assert view_subtitles("/stock") == dict(VIEW_SUBTITLES)


def test_view_contract_adds_only_minimal_t12_read_only_view():
    labels = view_labels("/T12")
    subtitles = view_subtitles("/T12")

    assert set(labels) == set(VIEW_LABELS) | {"t12"}
    assert labels["t12"] == "T12"
    assert subtitles["t12"] == T12_VIEW_SUBTITLE


def test_view_contract_supports_apex_t12_mount():
    assert view_labels("/apex/T12")["t12"] == "T12"
    assert view_subtitles("/apex/T12")["t12"] == T12_VIEW_SUBTITLE
