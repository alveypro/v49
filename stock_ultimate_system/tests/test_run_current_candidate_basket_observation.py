import json
import sqlite3
from pathlib import Path

from scripts import run_current_candidate_basket_observation as runner
from scripts.run_current_candidate_basket_observation import run_current_candidate_basket_observation


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_current_candidate_basket_observation_waits_for_future_window(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    baskets_dir = tmp_path / "artifacts" / "primary_result_candidate_baskets"
    snapshot = baskets_dir / "history" / "basket-001.json"
    config = tmp_path / "config" / "settings.yaml"

    _write_json(
        exp_dir / "primary_result_observation_latest.json",
        {"observation_window": {"started_at": "2026-04-20T01:30:00Z"}},
    )
    _write_json(
        baskets_dir / "current.json",
        {"basket_id": "basket-001", "snapshot_path": str(snapshot), "status": "conditional"},
    )
    _write_json(
        snapshot,
        {
            "basket_version": "primary_result_candidate_basket.v1",
            "basket_id": "basket-001",
            "status": "conditional",
            "items": [{"ts_code": "000001.SZ", "weight": 1.0}],
        },
    )
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text(
        "data:\n  sqlite_db_path: /path/that/should/not/be/read.db\n  sqlite_table: daily_trading_data\n",
        encoding="utf-8",
    )

    exit_code, payload = run_current_candidate_basket_observation(
        config_path=config,
        exp_dir=exp_dir,
        baskets_dir=baskets_dir,
        window_end="2026-04-17",
        output_path=tmp_path / "artifacts" / "observation_latest.json",
    )

    assert exit_code == 0
    assert payload["status"] == "pending_window"
    assert payload["blocking_reasons"] == []
    assert "wait until" in payload["next_actions"][0]


def test_current_candidate_basket_observation_resolves_relative_sqlite_path(tmp_path, monkeypatch):
    project_root = tmp_path
    exp_dir = project_root / "data" / "experiments"
    baskets_dir = project_root / "artifacts" / "primary_result_candidate_baskets"
    snapshot = baskets_dir / "history" / "basket-001.json"
    config = project_root / "config" / "settings.yaml"
    db_path = project_root / "db" / "market.db"

    def fake_resolve(path):
        candidate = Path(path)
        return candidate if candidate.is_absolute() else project_root / candidate

    monkeypatch.setattr(runner, "resolve_project_path", fake_resolve)

    _write_json(
        exp_dir / "primary_result_observation_latest.json",
        {"observation_window": {"started_at": "2026-04-17"}},
    )
    _write_json(
        baskets_dir / "current.json",
        {"basket_id": "basket-001", "snapshot_path": str(snapshot), "status": "conditional"},
    )
    _write_json(
        snapshot,
        {
            "basket_version": "primary_result_candidate_basket.v1",
            "basket_id": "basket-001",
            "status": "conditional",
            "items": [{"ts_code": "000001.SZ", "stock_name": "Ping An", "industry": "bank", "weight": 1.0}],
        },
    )
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text(
        "data:\n  sqlite_db_path: db/market.db\n  sqlite_table: daily_trading_data\n  benchmark_indices: [000001.SH]\n",
        encoding="utf-8",
    )
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE daily_trading_data (ts_code TEXT, trade_date TEXT, close_price REAL)")
        conn.executemany(
            "INSERT INTO daily_trading_data VALUES (?, ?, ?)",
            [
                ("000001.SZ", "20260417", 10.0),
                ("000001.SZ", "20260418", 11.0),
                ("000001.SH", "20260417", 100.0),
                ("000001.SH", "20260418", 101.0),
            ],
        )

    exit_code, payload = run_current_candidate_basket_observation(
        config_path=config,
        exp_dir=exp_dir,
        baskets_dir=baskets_dir,
        window_end="2026-04-18",
        price_history_csv=project_root / "data" / "experiments" / "current_candidate_basket_price_history_latest.csv",
        output_path=project_root / "artifacts" / "primary_result_candidate_baskets" / "observation_latest.json",
        ledger_jsonl=project_root / "artifacts" / "primary_result_candidate_baskets" / "performance_ledger.jsonl",
        summary_json=project_root / "artifacts" / "primary_result_candidate_baskets" / "performance_summary.json",
    )

    assert exit_code == 0
    assert payload["status"] == "completed"
    assert payload["price_history_extract"]["row_count"] == 4
    assert payload["feedback_status"] == "completed"
    assert payload["feedback_output_path"].endswith("feedback_latest.json")
    feedback = json.loads(
        (project_root / "artifacts" / "primary_result_candidate_baskets" / "feedback_latest.json").read_text(encoding="utf-8")
    )
    assert feedback["feedback_version"] == "primary_result_candidate_basket_feedback.v1"
    assert feedback["window_label"] == "5D"
