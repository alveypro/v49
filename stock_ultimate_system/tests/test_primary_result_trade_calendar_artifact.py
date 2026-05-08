import json

import pytest

from src.primary_result_trade_calendar_artifact import build_primary_result_trade_calendar_artifact


def test_trade_calendar_artifact_writes_normalized_open_dates(tmp_path):
    output = tmp_path / "artifacts" / "calendar.json"

    payload = build_primary_result_trade_calendar_artifact(
        start_date="20260417",
        end_date="2026-04-21",
        output_path=output,
        fetch_open_trade_dates=lambda **_: ["20260420", "2026-04-17", "20260420"],
    )

    assert payload["trade_dates"] == ["2026-04-17", "2026-04-20"]
    assert payload["trade_date_total"] == 2
    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["calendar_version"] == "primary_result_trade_calendar_artifact.v1"


def test_trade_calendar_artifact_rejects_empty_calendar(tmp_path):
    with pytest.raises(ValueError, match="no open trade dates"):
        build_primary_result_trade_calendar_artifact(
            start_date="2026-04-17",
            end_date="2026-04-21",
            output_path=tmp_path / "calendar.json",
            fetch_open_trade_dates=lambda **_: [],
        )
