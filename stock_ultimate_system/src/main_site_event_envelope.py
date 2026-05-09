from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4


MAIN_SITE_EVENT_ENVELOPE_FIELDS = (
    "event_id",
    "occurred_at",
    "session_id",
    "page_version",
    "schema_version",
    "environment",
)
MAIN_SITE_EVENT_SCHEMA_VERSION = "main_site_runtime_event.v1"
MAIN_SITE_EVENT_DEFAULT_ENVIRONMENT = "local"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _require_text(value: str, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field_name} must not be empty")
    return text


@dataclass(frozen=True)
class MainSiteEventEnvelope:
    event_id: str
    occurred_at: str
    session_id: str
    page_version: str
    schema_version: str
    environment: str

    def as_dict(self) -> dict[str, str]:
        return {
            "event_id": self.event_id,
            "occurred_at": self.occurred_at,
            "session_id": self.session_id,
            "page_version": self.page_version,
            "schema_version": self.schema_version,
            "environment": self.environment,
        }


def build_main_site_event_envelope(
    *,
    session_id: str,
    page_version: str,
    environment: str = MAIN_SITE_EVENT_DEFAULT_ENVIRONMENT,
    schema_version: str = MAIN_SITE_EVENT_SCHEMA_VERSION,
    event_id: str | None = None,
    occurred_at: str | None = None,
) -> MainSiteEventEnvelope:
    return MainSiteEventEnvelope(
        event_id=_require_text(event_id or f"main-site-event-{uuid4().hex}", "event_id"),
        occurred_at=_require_text(occurred_at or _utc_now_iso(), "occurred_at"),
        session_id=_require_text(session_id, "session_id"),
        page_version=_require_text(page_version, "page_version"),
        schema_version=_require_text(schema_version, "schema_version"),
        environment=_require_text(environment, "environment"),
    )
