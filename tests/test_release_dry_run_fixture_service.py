from __future__ import annotations

import sqlite3

import pytest

from openclaw.services.release_dry_run_fixture_service import build_release_dry_run_fixture


def test_build_release_dry_run_fixture_creates_rehearsal_evidence(tmp_path):
    db_path = tmp_path / "fixture.db"
    payload_path = tmp_path / "dry_run.json"
    report_path = tmp_path / "dry_run.md"

    result = build_release_dry_run_fixture(
        db_path=db_path,
        code_root=tmp_path,
        report_path=report_path,
        payload_path=payload_path,
        operator_name="alice",
    )

    assert result["payload"]["allow_release_gate"] is True
    assert result["payload"]["blocking_reasons"] == []
    assert report_path.exists()
    assert payload_path.exists()
    report = report_path.read_text(encoding="utf-8")
    assert "Airivo Release Dry-Run Report" in report
    assert result["fixture_ids"]["release_id"] in report
    assert "unfilled_expired" in report
    assert "cancelled_manual" in report
    assert "partial_fill" in report
    assert "slippage_anomaly" in report
    assert "decision_deviation" in report

    conn = sqlite3.connect(str(db_path))
    counts = {
        "signal_runs": conn.execute("SELECT COUNT(*) FROM signal_runs").fetchone()[0],
        "signal_items": conn.execute("SELECT COUNT(*) FROM signal_items").fetchone()[0],
        "decision_events": conn.execute("SELECT COUNT(*) FROM decision_events").fetchone()[0],
        "execution_orders": conn.execute("SELECT COUNT(*) FROM execution_orders").fetchone()[0],
        "execution_fills": conn.execute("SELECT COUNT(*) FROM execution_fills").fetchone()[0],
        "execution_attribution": conn.execute("SELECT COUNT(*) FROM execution_attribution").fetchone()[0],
        "release_events": conn.execute("SELECT COUNT(*) FROM release_events").fetchone()[0],
    }
    statuses = {
        row[0]: row[1]
        for row in conn.execute(
            "SELECT status, COUNT(*) FROM execution_orders GROUP BY status"
        ).fetchall()
    }
    miss_reasons = {
        row[0]
        for row in conn.execute(
            "SELECT miss_reason_code FROM execution_attribution WHERE COALESCE(miss_reason_code, '') != ''"
        ).fetchall()
    }
    partial_ratio = conn.execute(
        """
        SELECT a.fill_ratio
        FROM execution_attribution a
        JOIN execution_orders o ON o.order_id = a.order_id
        WHERE o.status = 'partial_fill'
        """
    ).fetchone()[0]
    max_slippage = conn.execute("SELECT MAX(slippage_bp) FROM execution_attribution").fetchone()[0]
    conn.close()
    assert counts == {
        "signal_runs": 1,
        "signal_items": 6,
        "decision_events": 1,
        "execution_orders": 6,
        "execution_fills": 3,
        "execution_attribution": 6,
        "release_events": 1,
    }
    assert statuses == {
        "cancelled": 1,
        "expired": 1,
        "filled": 2,
        "manual_override": 1,
        "partial_fill": 1,
    }
    assert {
        "no_fill_price_not_reached",
        "manual_cancel",
        "partial_liquidity",
        "slippage_anomaly",
        "decision_deviation",
    }.issubset(miss_reasons)
    assert partial_ratio == 0.4
    assert max_slippage == pytest.approx(200.0)


def test_build_release_dry_run_fixture_refuses_existing_db_without_overwrite(tmp_path):
    db_path = tmp_path / "fixture.db"
    db_path.write_text("exists", encoding="utf-8")

    with pytest.raises(FileExistsError):
        build_release_dry_run_fixture(
            db_path=db_path,
            code_root=tmp_path,
            report_path=tmp_path / "dry_run.md",
            payload_path=tmp_path / "dry_run.json",
        )
