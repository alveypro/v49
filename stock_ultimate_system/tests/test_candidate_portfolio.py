import json
import sqlite3
import subprocess
import sys
from pathlib import Path

from src.candidate_quality.portfolio import CandidatePortfolioConfig, build_candidate_portfolio
from src.candidate_quality.portfolio import build_candidate_portfolio_quality, build_portfolio_capacity_report


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _write_snapshot(tmp_path: Path, items: list[dict[str, object]]) -> Path:
    path = tmp_path / "candidate_observation_snapshot_latest.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "candidate_observation_snapshot.v1",
                "status": "frozen",
                "snapshot_date": "20260506",
                "lineage_run_id": "candidate-test",
                "items": items,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


def _write_candidates_csv(tmp_path: Path, rows: list[tuple[str, str, str, float]]) -> Path:
    path = tmp_path / "candidates_top_latest.csv"
    lines = ["rank,ts_code,stock_name,industry,final_score,portfolio_weight_after_risk"]
    for idx, (code, name, industry, weight) in enumerate(rows, start=1):
        lines.append(f"{idx},{code},{name},{industry},{100 - idx},{weight}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _build_db(
    db_path: Path,
    codes: list[str],
    *,
    high_corr: bool = False,
    days: int = 70,
    amount: float = 10_000_000.0,
) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE daily_trading_data (ts_code TEXT, trade_date TEXT, close_price REAL)")
    rows = []
    for day in range(days):
        date = f"2026{3 + (day // 28):02d}{1 + (day % 28):02d}"
        for idx, code in enumerate(codes):
            if high_corr:
                close = 10.0 + day * (1.0 + idx * 0.02)
            else:
                close = 10.0 + day * (idx + 1) + ((-1) ** day) * idx
            rows.append((code, date, close, 1_000_000.0, amount))
    rows.append((codes[0], "20260506", 100.0, 1_000_000.0, amount))
    if len(codes) > 1:
        for code in codes[1:]:
            rows.append((code, "20260506", 100.0, 1_000_000.0, amount))
    conn.execute("DROP TABLE daily_trading_data")
    conn.execute("CREATE TABLE daily_trading_data (ts_code TEXT, trade_date TEXT, close_price REAL, vol REAL, amount REAL)")
    conn.executemany("INSERT INTO daily_trading_data VALUES (?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()


def test_candidate_portfolio_outputs_normalized_weights_and_exposure(tmp_path):
    snapshot = _write_snapshot(
        tmp_path,
        [
            {"observation_id": "20260506:AAA", "ts_code": "AAA", "stock_name": "A", "industry": "Tech", "rank": 1},
            {"observation_id": "20260506:BBB", "ts_code": "BBB", "stock_name": "B", "industry": "Finance", "rank": 2},
        ],
    )
    csv_path = _write_candidates_csv(tmp_path, [("AAA", "A", "Tech", 0.8), ("BBB", "B", "Finance", 0.2)])
    db_path = tmp_path / "prices.db"
    _build_db(db_path, ["AAA", "BBB"])

    portfolio, exposure = build_candidate_portfolio(
        snapshot_path=snapshot,
        candidates_csv_path=csv_path,
        sqlite_db_path=db_path,
        config=CandidatePortfolioConfig(max_single_weight=0.7),
    )

    assert portfolio["summary"]["weight_sum"] == 1.0
    assert portfolio["summary"]["max_single_weight"] <= 0.7
    assert exposure["industry_exposure"]["industry_weights"]["Tech"] > 0


def test_candidate_portfolio_trims_industry_over_hard_cap(tmp_path):
    snapshot = _write_snapshot(
        tmp_path,
        [
            {"observation_id": "20260506:AAA", "ts_code": "AAA", "stock_name": "A", "industry": "Tech", "rank": 1},
            {"observation_id": "20260506:BBB", "ts_code": "BBB", "stock_name": "B", "industry": "Tech", "rank": 2},
            {"observation_id": "20260506:CCC", "ts_code": "CCC", "stock_name": "C", "industry": "Bank", "rank": 3},
        ],
    )
    csv_path = _write_candidates_csv(
        tmp_path,
        [("AAA", "A", "Tech", 0.45), ("BBB", "B", "Tech", 0.45), ("CCC", "C", "Bank", 0.10)],
    )
    db_path = tmp_path / "prices.db"
    _build_db(db_path, ["AAA", "BBB", "CCC"])

    portfolio, _ = build_candidate_portfolio(
        snapshot_path=snapshot,
        candidates_csv_path=csv_path,
        sqlite_db_path=db_path,
        config=CandidatePortfolioConfig(max_single_weight=0.6, hard_max_industry_weight=0.65),
    )

    assert portfolio["summary"]["industry_weights"]["Tech"] <= 0.65
    assert "industry_hard_cap_applied:Tech" in portfolio["summary"]["industry_adjustments"]


def test_candidate_portfolio_marks_correlation_data_gap(tmp_path):
    snapshot = _write_snapshot(
        tmp_path,
        [
            {"observation_id": "20260506:AAA", "ts_code": "AAA", "stock_name": "A", "industry": "Tech", "rank": 1},
            {"observation_id": "20260506:BBB", "ts_code": "BBB", "stock_name": "B", "industry": "Bank", "rank": 2},
        ],
    )
    db_path = tmp_path / "prices.db"
    _build_db(db_path, ["AAA", "BBB"], days=5)

    portfolio, exposure = build_candidate_portfolio(
        snapshot_path=snapshot,
        sqlite_db_path=db_path,
        config=CandidatePortfolioConfig(max_single_weight=0.7, min_correlation_points=20),
    )

    assert portfolio["status"] == "review"
    assert "correlation_data_insufficient" in portfolio["review_reasons"]
    assert exposure["correlation_exposure"]["pairs"][0]["status"] == "data_gap"


def test_candidate_portfolio_trims_high_correlation_pair(tmp_path):
    snapshot = _write_snapshot(
        tmp_path,
        [
            {"observation_id": "20260506:AAA", "ts_code": "AAA", "stock_name": "A", "industry": "Tech", "rank": 1},
            {"observation_id": "20260506:BBB", "ts_code": "BBB", "stock_name": "B", "industry": "Bank", "rank": 2},
        ],
    )
    csv_path = _write_candidates_csv(tmp_path, [("AAA", "A", "Tech", 0.5), ("BBB", "B", "Bank", 0.5)])
    db_path = tmp_path / "prices.db"
    _build_db(db_path, ["AAA", "BBB"], high_corr=True)

    portfolio, exposure = build_candidate_portfolio(
        snapshot_path=snapshot,
        candidates_csv_path=csv_path,
        sqlite_db_path=db_path,
        config=CandidatePortfolioConfig(max_single_weight=0.7, high_correlation_threshold=0.8),
    )

    assert "high_correlation_detected" in portfolio["review_reasons"]
    assert exposure["correlation_exposure"]["high_correlation_pairs"]
    assert portfolio["summary"]["correlation_adjustments"]


def test_candidate_portfolio_cli_writes_outputs(tmp_path):
    snapshot = _write_snapshot(
        tmp_path,
        [
            {"observation_id": "20260506:AAA", "ts_code": "AAA", "stock_name": "A", "industry": "Tech", "rank": 1},
            {"observation_id": "20260506:BBB", "ts_code": "BBB", "stock_name": "B", "industry": "Bank", "rank": 2},
        ],
    )
    db_path = tmp_path / "prices.db"
    _build_db(db_path, ["AAA", "BBB"])

    result = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "build_candidate_portfolio.py"),
            "--exp-dir",
            str(tmp_path),
            "--snapshot",
            str(snapshot),
            "--db-path",
            str(db_path),
            "--json",
        ],
        cwd=str(PROJECT_ROOT),
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["candidate_count"] == 2
    assert (tmp_path / "candidate_portfolio_latest.json").exists()
    assert (tmp_path / "portfolio_exposure_report_latest.json").exists()
    assert (tmp_path / "portfolio_capacity_report_latest.json").exists()
    assert (tmp_path / "candidate_portfolio_quality_latest.json").exists()


def test_portfolio_capacity_report_blocks_low_capacity(tmp_path):
    snapshot = _write_snapshot(
        tmp_path,
        [
            {"observation_id": "20260506:AAA", "ts_code": "AAA", "stock_name": "A", "industry": "Tech", "rank": 1},
            {"observation_id": "20260506:BBB", "ts_code": "BBB", "stock_name": "B", "industry": "Bank", "rank": 2},
        ],
    )
    db_path = tmp_path / "prices.db"
    _build_db(db_path, ["AAA", "BBB"], amount=100_000.0)
    portfolio, _ = build_candidate_portfolio(
        snapshot_path=snapshot,
        sqlite_db_path=db_path,
        config=CandidatePortfolioConfig(max_single_weight=0.7),
    )

    capacity = build_portfolio_capacity_report(
        portfolio=portfolio,
        sqlite_db_path=db_path,
        config=CandidatePortfolioConfig(max_single_weight=0.7, min_amount=300_000.0),
    )

    assert capacity["status"] == "blocked"
    assert "capacity_amount_below_minimum" in capacity["blocking_reasons"]


def test_candidate_portfolio_quality_blocks_when_capacity_fails(tmp_path):
    portfolio = {
        "schema_version": "candidate_portfolio.v1",
        "status": "passed",
        "snapshot_date": "20260506",
        "summary": {"top_industry_weight": 0.4, "concentration_hhi": 0.25},
    }
    exposure = {"status": "passed"}
    capacity = {
        "schema_version": "portfolio_capacity_report.v1",
        "status": "blocked",
        "blocking_reasons": ["capacity_participation_exceeds_block_limit"],
        "summary": {"worst_participation_rate": 0.2, "estimated_impact_cost_bps": 8.0},
    }

    quality = build_candidate_portfolio_quality(
        portfolio=portfolio,
        exposure_report=exposure,
        capacity_report=capacity,
        transaction_cost_report={"status": "passed", "summary": {"total_transaction_cost": 100.0}},
        realistic_backtest={"status": "passed", "summary": {"blocked_count": 0, "return_decay": 0.0}},
    )

    assert quality["status"] == "blocked"
    assert "portfolio_capacity_blocked" in quality["blocking_reasons"]
