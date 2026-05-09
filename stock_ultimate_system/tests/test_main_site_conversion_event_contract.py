from src.main_site_conversion_events import MAIN_SITE_CONVERSION_EVENT_SPECS
from src.main_site_home import MAIN_SITE_CONVERSION_EVENTS, render_main_site_home


def test_main_site_conversion_event_contract_matches_hooks():
    html_text = render_main_site_home()

    assert set(MAIN_SITE_CONVERSION_EVENTS.keys()) == {
        "hero_impression",
        "primary_cta_click",
        "stock_card_click",
        "t12_card_click",
    }
    for event in MAIN_SITE_CONVERSION_EVENTS.values():
        assert f'data-airivo-hook="{event["hook"]}"' in html_text
        assert event["hook"] == event["component_id"]
        assert event["event_name"].startswith("main_site_")
    assert MAIN_SITE_CONVERSION_EVENTS["primary_cta_click"]["component_id"] == MAIN_SITE_CONVERSION_EVENT_SPECS["primary_cta_click"]["component_id"]
    assert MAIN_SITE_CONVERSION_EVENTS["stock_card_click"]["component_id"] == MAIN_SITE_CONVERSION_EVENT_SPECS["stock_card_click"]["component_id"]
    assert MAIN_SITE_CONVERSION_EVENTS["t12_card_click"]["component_id"] == MAIN_SITE_CONVERSION_EVENT_SPECS["t12_card_click"]["component_id"]

    assert MAIN_SITE_CONVERSION_EVENTS["primary_cta_click"]["target_path"] == "/stock/"
    assert MAIN_SITE_CONVERSION_EVENTS["primary_cta_click"]["entry_role"] == "primary"
    assert MAIN_SITE_CONVERSION_EVENTS["stock_card_click"]["entry_role"] == "primary"
    assert MAIN_SITE_CONVERSION_EVENTS["t12_card_click"]["target_path"] == "/T12/"
    assert MAIN_SITE_CONVERSION_EVENTS["t12_card_click"]["entry_role"] == "auxiliary"
