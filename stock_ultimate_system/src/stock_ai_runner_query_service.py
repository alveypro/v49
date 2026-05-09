from __future__ import annotations

from pathlib import Path
from typing import Any

from src.stock_ai_runner_read_model import (
    load_stock_ai_runner_read_model,
    read_stock_ai_failure_top_causes,
    read_stock_ai_provider_attempt_replay,
    read_stock_ai_provider_health_rollups,
    read_stock_ai_provider_latest_health_snapshot,
    read_stock_ai_provider_trend_summaries,
    read_stock_ai_result_replay,
)


class StockAIRunnerQueryService:
    def __init__(self, storage_dir: str | Path = "artifacts/stock_ai_runner") -> None:
        self.storage_dir = storage_dir

    def load_read_model(self) -> dict[str, Any]:
        return load_stock_ai_runner_read_model(self.storage_dir)

    def read_latest_health(self) -> list[dict[str, Any]]:
        return read_stock_ai_provider_latest_health_snapshot(self.storage_dir)

    def read_health_rollups(self, *, window: int = 8) -> list[dict[str, Any]]:
        return read_stock_ai_provider_health_rollups(self.storage_dir, window=window)

    def read_trend_summaries(self, *, short_window: int = 8, long_window: int = 16) -> list[dict[str, Any]]:
        return read_stock_ai_provider_trend_summaries(
            self.storage_dir,
            short_window=short_window,
            long_window=long_window,
        )

    def read_failure_top_causes(self, *, top_n: int = 5) -> list[dict[str, Any]]:
        return read_stock_ai_failure_top_causes(self.storage_dir, top_n=top_n)

    def read_result_replay(
        self,
        *,
        result_id: str,
        window: int = 8,
        recorded_at_from: str = "",
        recorded_at_to: str = "",
    ) -> dict[str, Any]:
        return read_stock_ai_result_replay(
            self.storage_dir,
            result_id=result_id,
            window=window,
            recorded_at_from=recorded_at_from,
            recorded_at_to=recorded_at_to,
        )

    def read_provider_detail(
        self,
        *,
        provider_name: str,
        replay_window: int = 8,
        recorded_at_from: str = "",
        recorded_at_to: str = "",
        health_window: int = 8,
        trend_short_window: int = 8,
        trend_long_window: int = 16,
    ) -> dict[str, Any]:
        latest_health = next(
            (item for item in self.read_latest_health() if str(item.get("provider_name", "") or "") == provider_name),
            None,
        )
        health_rollup = next(
            (item for item in self.read_health_rollups(window=health_window) if str(item.get("provider_name", "") or "") == provider_name),
            None,
        )
        trend_summary = next(
            (
                item
                for item in self.read_trend_summaries(short_window=trend_short_window, long_window=trend_long_window)
                if str(item.get("provider_name", "") or "") == provider_name
            ),
            None,
        )
        provider_replay = read_stock_ai_provider_attempt_replay(
            self.storage_dir,
            provider_name=provider_name,
            window=replay_window,
            recorded_at_from=recorded_at_from,
            recorded_at_to=recorded_at_to,
        )
        return {
            "provider_name": str(provider_name or ""),
            "latest_health": latest_health,
            "health_rollup": health_rollup,
            "trend_summary": trend_summary,
            "provider_replay": provider_replay,
        }
