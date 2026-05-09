from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class UnifiedResultRecord:
    result_id: str
    ts_code: str
    stock_name: str
    result_lifecycle_stage: str
    source_scope: str | None = None
    run_id: str | None = None
    lifecycle_id: str | None = None
    artifact_ids: list[str] = field(default_factory=list)
    as_of_date: str | None = None
    result_type: str | None = None
    research_status: str | None = None
    candidate_status: str | None = None
    signal_level: str | None = None
    risk_level: str | None = None
    audit_status: str | None = None
    promotion_status: str | None = None
    execution_status: str | None = None
    observation_status: str | None = None
    rollback_status: str | None = None
    terminal_outcome: str | None = None
    history_summary: str | None = None
    history_source_file: str | None = None
    history_source_timestamp: str | None = None
    history_generation_mode: str | None = None
    disabled_reason: str | None = None
    invalid_reason: str | None = None
    source_timestamps: dict[str, str] = field(default_factory=dict)
    data_sync_note: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
