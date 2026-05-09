from src.main_site_conversion_events import (
    MAIN_SITE_CONVERSION_EVENT_FIELDS,
    MAIN_SITE_CONVERSION_EVENT_SPECS,
    build_main_site_conversion_event_definition,
    build_main_site_conversion_event_payload,
)


def test_main_site_conversion_event_payloads_are_stable():
    assert set(MAIN_SITE_CONVERSION_EVENT_SPECS.keys()) == {
        "hero_impression",
        "primary_cta_click",
        "stock_card_click",
        "t12_card_click",
    }

    primary_payload = build_main_site_conversion_event_payload("primary_cta_click").as_dict()
    t12_payload = build_main_site_conversion_event_payload("t12_card_click").as_dict()

    assert tuple(primary_payload.keys()) == MAIN_SITE_CONVERSION_EVENT_FIELDS
    assert primary_payload["event_name"] == "main_site_primary_cta_click"
    assert primary_payload["entry_role"] == "primary"
    assert primary_payload["target_path"] == "/stock/"
    assert t12_payload["event_name"] == "main_site_t12_card_click"
    assert t12_payload["entry_role"] == "auxiliary"
    assert t12_payload["target_path"] == "/T12/"
    assert "event_id" not in primary_payload
    assert "occurred_at" not in primary_payload
    assert "session_id" not in primary_payload


def test_main_site_conversion_event_hook_is_derived_from_component_id():
    definition = build_main_site_conversion_event_definition("primary_cta_click")

    assert definition["hook"] == definition["component_id"]
