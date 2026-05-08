from pathlib import Path
import sys
import subprocess
import sqlite3
import time

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import run_update_database
from run_update_database import (
    _candidate_retry_universe_sizes,
    _formal_first_candidate_attempts,
    _resolve_online_post_universe_size,
    _benchmark_index_codes,
    _classify_tushare_error,
    _format_tushare_init_error,
    _db_latest_date_for_code,
    _ensure_schema,
    _tushare_error_priority,
    _update_benchmark_indices,
    _upsert_index_rows,
    run_post_candidate_artifacts,
    run_post_candidate_data_quality_report,
    run_post_candidate_basket_snapshot,
    run_post_buylist_snapshot,
)


def _create_daily_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE daily_trading_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code TEXT,
            trade_date TEXT,
            open_price REAL,
            high_price REAL,
            low_price REAL,
            close_price REAL,
            pre_close REAL,
            change_amount REAL,
            pct_chg REAL,
            vol REAL,
            amount REAL,
            turnover_rate REAL,
            created_at TEXT
        )
        """
    )
    _ensure_schema(conn, "daily_trading_data")


class FakeBenchmarkPro:
    def trade_cal(self, exchange, start_date, end_date):
        return pd.DataFrame(
            [
                {"cal_date": "20260415", "is_open": 1},
                {"cal_date": "20260416", "is_open": 1},
            ]
        )

    def index_daily(self, ts_code, start_date, end_date):
        assert ts_code == "000001.SH"
        assert start_date == "20260415"
        assert end_date == "20260416"
        return pd.DataFrame(
            [
                {
                    "ts_code": ts_code,
                    "trade_date": "20260415",
                    "open": 3000.0,
                    "high": 3020.0,
                    "low": 2990.0,
                    "close": 3010.0,
                    "pre_close": 2995.0,
                    "change": 15.0,
                    "pct_chg": 0.5,
                    "vol": 1000000.0,
                    "amount": 2000000.0,
                },
                {
                    "ts_code": ts_code,
                    "trade_date": "20260416",
                    "open": 3010.0,
                    "high": 3035.0,
                    "low": 3001.0,
                    "close": 3030.0,
                    "pre_close": 3010.0,
                    "change": 20.0,
                    "pct_chg": 0.66,
                    "vol": 1100000.0,
                    "amount": 2200000.0,
                },
            ]
        )


def test_classify_tushare_permission_error():
    exc = Exception("抱歉，您没有接口访问权限，权限的具体详情访问：https://tushare.pro/document/1?doc_id=108。")
    assert _classify_tushare_error(exc) == "permission_denied"
    assert "账号无当前接口访问权限" in _format_tushare_init_error(exc)


def test_classify_tushare_invalid_token_error():
    exc = Exception("您的token不对，请确认。")
    assert _classify_tushare_error(exc) == "invalid_token"
    assert "token 无效或未生效" in _format_tushare_init_error(exc)


def test_tushare_error_priority_prefers_permission_denied():
    permission = Exception("抱歉，您没有接口访问权限。")
    invalid = Exception("您的token不对，请确认。")
    assert _tushare_error_priority(permission) > _tushare_error_priority(invalid)


def test_candidate_retry_universe_sizes_deduplicates_and_preserves_order():
    sizes = _candidate_retry_universe_sizes(300, {"candidate_retry_universe_sizes": [300, 120, 40, 40, 15]})
    assert sizes == [300, 120, 40, 15]


def test_candidate_retry_universe_sizes_defaults_to_fastest_safe_fallback_first():
    sizes = _candidate_retry_universe_sizes(0, {})
    assert sizes == [15, 40, 120]


def test_formal_first_candidate_attempts_prioritizes_smaller_formal_runs_before_requested_size():
    attempts = _formal_first_candidate_attempts([300, 15, 40, 120], 300)
    assert attempts == [15, 40, 120, 300]


def test_resolve_online_post_universe_size_prefers_lightweight_default_until_explicit_full_market():
    assert _resolve_online_post_universe_size(0, {"candidate_online_universe_size": 120}) == (120, "online_default")
    assert _resolve_online_post_universe_size(
        0,
        {"candidate_online_universe_size": 120},
        allow_full_market=True,
    ) == (0, "full_market_allowed")
    assert _resolve_online_post_universe_size(80, {"candidate_online_universe_size": 120}) == (80, "requested")


def test_benchmark_index_codes_defaults_and_deduplicates():
    assert _benchmark_index_codes({}) == ["000001.SH"]
    assert _benchmark_index_codes({"benchmark_indices": ["000001.SH", "000001.SH", "399001.SZ"]}) == [
        "000001.SH",
        "399001.SZ",
    ]
    assert _benchmark_index_codes({"benchmark_indices": "000300.SH"}) == ["000300.SH"]


def test_upsert_index_rows_uses_daily_trading_schema(tmp_path):
    conn = sqlite3.connect(tmp_path / "stock.db")
    _create_daily_table(conn)
    frame = pd.DataFrame(
        [
            {
                "ts_code": "000001.SH",
                "trade_date": "20260416",
                "open": 3010.0,
                "high": 3035.0,
                "low": 3001.0,
                "close": 3030.0,
                "pre_close": 3010.0,
                "change": 20.0,
                "pct_chg": 0.66,
                "vol": 1100000.0,
                "amount": 2200000.0,
            }
        ]
    )

    assert _upsert_index_rows(conn, "daily_trading_data", frame) == 1
    row = conn.execute(
        "SELECT ts_code, trade_date, close_price, turnover_rate FROM daily_trading_data"
    ).fetchone()
    conn.close()
    assert row == ("000001.SH", "20260416", 3030.0, 0.0)


def test_update_benchmark_indices_uses_per_code_latest_date(tmp_path):
    conn = sqlite3.connect(tmp_path / "stock.db")
    _create_daily_table(conn)

    summary = _update_benchmark_indices(
        conn,
        "daily_trading_data",
        FakeBenchmarkPro(),
        ["000001.SH"],
        "20260416",
    )

    assert summary["status"] == "completed"
    assert summary["processed_codes"] == ["000001.SH"]
    assert summary["written_rows"] == 2
    assert summary["latest_before"] == {"000001.SH": "20200101"}
    assert summary["latest_after"] == {"000001.SH": "20260416"}
    assert _db_latest_date_for_code(conn, "daily_trading_data", "000001.SH") == "20260416"
    conn.close()


def test_run_post_candidates_retries_after_timeout(tmp_path, monkeypatch):
    project_root = tmp_path
    (project_root / "config").mkdir(parents=True, exist_ok=True)
    (project_root / "config" / "settings.yaml").write_text(
        "runtime:\n  candidate_timeout_sec: 1\n  candidate_retry_universe_sizes: [120, 15]\n",
        encoding="utf-8",
    )
    exp_dir = project_root / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)

    calls = []

    class Result:
        def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    def fake_run(cmd, cwd=None, capture_output=None, text=None, check=None, timeout=None):
        calls.append((cmd, timeout))
        universe_size = cmd[cmd.index("--universe-size") + 1]
        if universe_size == "120":
            raise subprocess.TimeoutExpired(cmd=cmd, timeout=timeout)
        (exp_dir / "candidates_top_latest.csv").write_text("ts_code\n000001.SZ\n", encoding="utf-8")
        (exp_dir / "candidates_top_latest.md").write_text("# ok\n", encoding="utf-8")
        (exp_dir / "candidates_basket_summary_latest.json").write_text(
            '{"generation_degraded": false, "guardrail_mode": "validation_skipped", "generation_reason": "", "candidate_count": 1}',
            encoding="utf-8",
        )
        future = time.time() + 1.0
        (exp_dir / "candidates_top_latest.csv").touch()
        (exp_dir / "candidates_top_latest.md").touch()
        import os

        os.utime(exp_dir / "candidates_top_latest.csv", (future, future))
        os.utime(exp_dir / "candidates_top_latest.md", (future, future))
        os.utime(exp_dir / "candidates_basket_summary_latest.json", (future, future))
        return Result(0, stdout="done")

    monkeypatch.setattr(run_update_database.subprocess, "run", fake_run)

    ok, detail = run_update_database.run_post_candidates(project_root, "config", 120, 10)
    assert ok is True
    assert "universe_size=15" in detail
    assert len(calls) == 1
    assert calls[0][0][calls[0][0].index("--universe-size") + 1] == "15"


def test_run_post_candidates_does_not_accept_interim_output_as_formal_latest(tmp_path, monkeypatch):
    project_root = tmp_path
    (project_root / "config").mkdir(parents=True, exist_ok=True)
    (project_root / "config" / "settings.yaml").write_text(
        "runtime:\n  candidate_timeout_sec: 1\n  candidate_retry_universe_sizes: [120, 15]\n",
        encoding="utf-8",
    )
    exp_dir = project_root / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)

    calls = []

    class Result:
        def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    def _touch(path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
        future = time.time() + 1.0
        path.touch()
        import os

        os.utime(path, (future, future))

    def fake_run(cmd, cwd=None, capture_output=None, text=None, check=None, timeout=None):
        calls.append((cmd, timeout))
        universe_size = cmd[cmd.index("--universe-size") + 1]
        if universe_size == "120":
            _touch(exp_dir / "candidates_top_interim_latest.csv")
            _touch(exp_dir / "candidates_top_interim_latest.md")
            _touch(exp_dir / "candidates_basket_summary_interim_latest.json")
            raise subprocess.TimeoutExpired(cmd=cmd, timeout=timeout)

        (exp_dir / "candidates_top_latest.csv").write_text("ts_code\n000001.SZ\n", encoding="utf-8")
        (exp_dir / "candidates_top_latest.md").write_text("# ok\n", encoding="utf-8")
        (exp_dir / "candidates_basket_summary_latest.json").write_text(
            '{"generation_degraded": false, "guardrail_mode": "validation_skipped", "generation_reason": "", "candidate_count": 1}',
            encoding="utf-8",
        )
        future = time.time() + 1.0
        import os

        os.utime(exp_dir / "candidates_top_latest.csv", (future, future))
        os.utime(exp_dir / "candidates_top_latest.md", (future, future))
        os.utime(exp_dir / "candidates_basket_summary_latest.json", (future, future))
        return Result(0, stdout="done")

    monkeypatch.setattr(run_update_database.subprocess, "run", fake_run)

    ok, detail, meta = run_update_database.run_post_candidates(project_root, "config", 120, 10, return_meta=True)

    assert ok is True
    assert "universe_size=15" in detail
    assert meta["used_attempt"] == 15
    assert len(calls) == 1
    assert calls[0][0][calls[0][0].index("--universe-size") + 1] == "15"


def test_run_post_candidates_retries_when_small_pool_returns_zero_candidates(tmp_path, monkeypatch):
    project_root = tmp_path
    (project_root / "config").mkdir(parents=True, exist_ok=True)
    (project_root / "config" / "settings.yaml").write_text(
        "runtime:\n  candidate_timeout_sec: 1\n  candidate_retry_universe_sizes: [15, 40, 120]\n",
        encoding="utf-8",
    )
    exp_dir = project_root / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)

    calls = []

    class Result:
        returncode = 0
        stdout = "done"
        stderr = ""

    def fake_run(cmd, cwd=None, capture_output=None, text=None, check=None, timeout=None):
        calls.append((cmd, timeout))
        universe_size = int(cmd[cmd.index("--universe-size") + 1])
        candidate_count = 0 if universe_size == 15 else 3
        (exp_dir / "candidates_top_latest.csv").write_text("ts_code\n000001.SZ\n", encoding="utf-8")
        (exp_dir / "candidates_top_latest.md").write_text("# ok\n", encoding="utf-8")
        (exp_dir / "candidates_basket_summary_latest.json").write_text(
            (
                '{"generation_degraded": false, "guardrail_mode": "validation_skipped", '
                f'"generation_reason": "", "candidate_count": {candidate_count}' + "}"
            ),
            encoding="utf-8",
        )
        future = time.time() + 1.0
        import os

        os.utime(exp_dir / "candidates_top_latest.csv", (future, future))
        os.utime(exp_dir / "candidates_top_latest.md", (future, future))
        os.utime(exp_dir / "candidates_basket_summary_latest.json", (future, future))
        return Result()

    monkeypatch.setattr(run_update_database.subprocess, "run", fake_run)

    ok, detail, meta = run_update_database.run_post_candidates(project_root, "config", 120, 10, return_meta=True)

    assert ok is True
    assert "universe_size=40" in detail
    assert meta["used_attempt"] == 40
    assert [call[0][call[0].index("--universe-size") + 1] for call in calls] == ["15", "40"]


def test_run_post_candidate_data_quality_report_passes_expected_latest_trade_date(tmp_path, monkeypatch):
    project_root = tmp_path
    config_dir = project_root / "config"
    config_dir.mkdir(parents=True)
    db_path = project_root / "stock.db"
    (config_dir / "settings.yaml").write_text(
        f"data:\n  sqlite_db_path: {db_path}\n  sqlite_table: daily_trading_data\n",
        encoding="utf-8",
    )
    conn = sqlite3.connect(db_path)
    _create_daily_table(conn)
    conn.execute(
        "INSERT INTO daily_trading_data (ts_code, trade_date, close_price, amount) VALUES (?, ?, ?, ?)",
        ("000001.SZ", "20260507", 10.0, 1000000.0),
    )
    conn.commit()
    conn.close()

    calls = []

    class Result:
        returncode = 0
        stdout = "report=passed"
        stderr = ""

    def fake_run(cmd, cwd=None, capture_output=None, text=None, check=None):
        calls.append((cmd, cwd))
        return Result()

    monkeypatch.setattr(run_update_database.subprocess, "run", fake_run)

    ok, detail = run_post_candidate_data_quality_report(project_root, "config")

    assert ok is True
    assert "data_as_of=20260507" in detail
    cmd, cwd = calls[0]
    assert cwd == str(project_root)
    assert "--expected-latest-trade-date" in cmd
    assert cmd[cmd.index("--expected-latest-trade-date") + 1] == "20260507"


def test_run_post_buylist_snapshot_invokes_snapshot_builder(tmp_path, monkeypatch):
    project_root = tmp_path
    calls = []

    class Result:
        returncode = 0
        stdout = "data/experiments/buylist_latest.json"
        stderr = ""

    def fake_run(cmd, cwd=None, capture_output=None, text=None, check=None):
        calls.append((cmd, cwd))
        return Result()

    monkeypatch.setattr(run_update_database.subprocess, "run", fake_run)

    ok, detail = run_post_buylist_snapshot(project_root, 7)

    assert ok is True
    assert detail == "buylist snapshot refreshed"
    cmd, cwd = calls[0]
    assert cwd == str(project_root)
    assert str(project_root / "run_buylist_snapshot.py") in cmd
    assert cmd[cmd.index("--target-count") + 1] == "7"


def test_run_post_candidate_basket_snapshot_invokes_registry_cli(tmp_path, monkeypatch):
    project_root = tmp_path
    calls = []

    class Result:
        returncode = 0
        stdout = '{"status":"approved"}'
        stderr = ""

    def fake_run(cmd, cwd=None, capture_output=None, text=None, check=None):
        calls.append((cmd, cwd))
        return Result()

    monkeypatch.setattr(run_update_database.subprocess, "run", fake_run)

    ok, detail, meta = run_post_candidate_basket_snapshot(project_root, 10)

    assert ok is True
    assert detail == "candidate basket snapshot registered"
    assert meta["summary_json_path"] == "data/experiments/candidates_basket_summary_latest.json"
    assert meta["snapshot_output_path"] == "artifacts/primary_result_candidate_baskets/latest_attempt.json"
    assert meta["status"] == "approved"
    cmd, cwd = calls[0]
    assert cwd == str(project_root)
    assert str(project_root / "scripts" / "register_primary_result_candidate_basket.py") in cmd
    assert cmd[cmd.index("--summary-json") + 1] == "data/experiments/candidates_basket_summary_latest.json"
    assert cmd[cmd.index("--top-n") + 1] == "10"
    assert cmd[cmd.index("--baskets-dir") + 1] == "artifacts/primary_result_candidate_baskets"
    assert cmd[cmd.index("--snapshot-output") + 1] == "artifacts/primary_result_candidate_baskets/latest_attempt.json"


def test_run_post_candidate_artifacts_reports_partial_failure(tmp_path, monkeypatch):
    calls = []

    def fake_run(cmd, cwd=None, capture_output=None, text=None, check=None):
        calls.append(cmd)

        class Result:
            stdout = "ok"
            stderr = ""

            @property
            def returncode(self):
                return 1 if "register_primary_result_candidate_basket.py" in " ".join(cmd) else 0

        return Result()

    monkeypatch.setattr(run_update_database.subprocess, "run", fake_run)

    failures, artifacts = run_post_candidate_artifacts(tmp_path, 5)

    assert failures == 1
    assert artifacts["buylist_snapshot"]["ok"] is True
    assert artifacts["candidate_basket_snapshot"]["ok"] is False
    assert artifacts["candidate_basket_snapshot"]["summary_json_path"] == "data/experiments/candidates_basket_summary_latest.json"
    assert len(calls) == 2
