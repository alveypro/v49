from __future__ import annotations

from dataclasses import dataclass


MAIN_SITE_CONVERSION_EVENT_FIELDS = (
    "event_name",
    "surface",
    "component_id",
    "entry_role",
    "target_path",
)

MAIN_SITE_CONVERSION_EVENT_SPECS = {
    "hero_impression": {
        "event_name": "main_site_hero_impression",
        "surface": "main_site_home",
        "component_id": "main-site-hero",
        "entry_role": "platform",
        "target_path": "",
    },
    "primary_cta_click": {
        "event_name": "main_site_primary_cta_click",
        "surface": "main_site_home",
        "component_id": "main-site-primary-cta",
        "entry_role": "primary",
        "target_path": "/stock/",
    },
    "stock_card_click": {
        "event_name": "main_site_stock_card_click",
        "surface": "main_site_home",
        "component_id": "main-site-stock-card-link",
        "entry_role": "primary",
        "target_path": "/stock/",
    },
    "t12_card_click": {
        "event_name": "main_site_t12_card_click",
        "surface": "main_site_home",
        "component_id": "main-site-t12-card-link",
        "entry_role": "auxiliary",
        "target_path": "/T12/",
    },
}


@dataclass(frozen=True)
class MainSiteConversionEventPayload:
    event_name: str
    surface: str
    component_id: str
    entry_role: str
    target_path: str

    def as_dict(self) -> dict[str, str]:
        return {
            "event_name": self.event_name,
            "surface": self.surface,
            "component_id": self.component_id,
            "entry_role": self.entry_role,
            "target_path": self.target_path,
        }


def build_main_site_conversion_event_payload(event_key: str) -> MainSiteConversionEventPayload:
    spec = MAIN_SITE_CONVERSION_EVENT_SPECS[event_key]
    return MainSiteConversionEventPayload(
        event_name=str(spec["event_name"]),
        surface=str(spec["surface"]),
        component_id=str(spec["component_id"]),
        entry_role=str(spec["entry_role"]),
        target_path=str(spec["target_path"]),
    )


def build_main_site_conversion_event_definition(event_key: str) -> dict[str, str]:
    payload = build_main_site_conversion_event_payload(event_key).as_dict()
    return {
        **payload,
        "hook": payload["component_id"],
    }


def build_main_site_conversion_event_definitions() -> dict[str, dict[str, str]]:
    return {
        event_key: build_main_site_conversion_event_definition(event_key)
        for event_key in MAIN_SITE_CONVERSION_EVENT_SPECS
    }
