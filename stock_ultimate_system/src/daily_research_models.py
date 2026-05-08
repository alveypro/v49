from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ResearchPaths:
    summary_path: Path
    history_path: Path
    health_csv_path: Path
    status_path: Path

    @classmethod
    def from_summary_path(cls, summary_path: Path) -> 'ResearchPaths':
        return cls(
            summary_path=summary_path,
            history_path=summary_path.with_name('daily_research_history.jsonl'),
            health_csv_path=summary_path.with_name('daily_health_trend_latest.csv'),
            status_path=summary_path.with_name('daily_research_status_latest.json'),
        )


@dataclass(frozen=True)
class AlertRecord:
    level: str
    category: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {
            'level': self.level,
            'category': self.category,
            'message': self.message,
        }


@dataclass(frozen=True)
class HealthSnapshot:
    score: float
    success_rate: float
    failed_count: float
    success_component: float
    failure_penalty: float
    category_penalty: float

    @classmethod
    def from_dict(cls, payload: dict[str, float]) -> 'HealthSnapshot':
        return cls(
            score=float(payload.get('score', 0.0)),
            success_rate=float(payload.get('success_rate', 0.0)),
            failed_count=float(payload.get('failed_count', 0.0)),
            success_component=float(payload.get('success_component', 0.0)),
            failure_penalty=float(payload.get('failure_penalty', 0.0)),
            category_penalty=float(payload.get('category_penalty', 0.0)),
        )

    def to_dict(self) -> dict[str, float]:
        return {
            'score': self.score,
            'success_rate': self.success_rate,
            'failed_count': self.failed_count,
            'success_component': self.success_component,
            'failure_penalty': self.failure_penalty,
            'category_penalty': self.category_penalty,
        }

    def trend_entry(self, alerts_count: int, generated_at: str) -> dict[str, float | str | int]:
        return {
            'generated_at': generated_at,
            'score': self.score,
            'success_rate': self.success_rate,
            'failed_count': self.failed_count,
            'alerts_count': alerts_count,
        }


@dataclass
class ProfileRunRecord:
    profile: str
    status: str
    attempts: int
    duration_sec: float
    payload: dict[str, Any]

    @classmethod
    def from_payload(
        cls,
        profile: str,
        status: str,
        attempts: int,
        duration_sec: float,
        payload: dict[str, Any] | None = None,
    ) -> 'ProfileRunRecord':
        return cls(
            profile=profile,
            status=status,
            attempts=attempts,
            duration_sec=duration_sec,
            payload=dict(payload or {}),
        )

    @property
    def top_result(self) -> dict[str, Any]:
        top = self.payload.get('top_result', {})
        return top if isinstance(top, dict) else {}

    @property
    def error(self) -> str:
        return str(self.payload.get('error', ''))

    @property
    def failure_category(self) -> str:
        return str(self.payload.get('failure_category', ''))

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.payload,
            'profile': self.profile,
            'status': self.status,
            'attempts': self.attempts,
            'duration_sec': self.duration_sec,
        }
