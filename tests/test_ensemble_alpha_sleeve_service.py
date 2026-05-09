from __future__ import annotations

import json
import sqlite3

from openclaw.services.ensemble_alpha_sleeve_service import build_ensemble_alpha_sleeve_fact_chain


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE signal_runs (
            run_id TEXT PRIMARY KEY,
            run_type TEXT NOT NULL,
            strategy TEXT NOT NULL,
            trade_date TEXT NOT NULL DEFAULT '',
            data_version TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'created',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE signal_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            score REAL NOT NULL DEFAULT 0,
            rank_idx INTEGER NOT NULL DEFAULT 0,
            reason_codes TEXT,
            raw_payload_json TEXT
        );
        CREATE TABLE daily_trading_data (
            ts_code TEXT,
            trade_date TEXT,
            close_price REAL,
            pct_chg REAL,
            amount REAL,
            turnover_rate REAL
        );
        """
    )


def test_ensemble_alpha_sleeve_fact_chain_builds_replayable_research_facts():
    conn = sqlite3.connect(":memory:")
    _init_schema(conn)
    conn.execute(
        "INSERT INTO signal_runs(run_id, run_type, strategy, trade_date, status) VALUES (?, ?, ?, ?, ?)",
        ("scan_v5", "scan", "v5", "20260102", "success"),
    )
    conn.executemany(
        """
        INSERT INTO signal_items(run_id, ts_code, score, rank_idx, reason_codes, raw_payload_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "scan_v5",
                "000001.SZ",
                90.0,
                1,
                json.dumps(["v5_strength", "breakout", "money_flow"], ensure_ascii=False),
                json.dumps({"industry": "电子", "notes": "主力资金 趋势"}, ensure_ascii=False),
            ),
            (
                "scan_v5",
                "000002.SZ",
                80.0,
                2,
                json.dumps(["bottom", "sector_rotation"], ensure_ascii=False),
                json.dumps({"notes": "底部 行业"}, ensure_ascii=False),
            ),
            (
                "scan_v5",
                "000003.SZ",
                70.0,
                3,
                json.dumps(["stable", "quality", "event"], ensure_ascii=False),
                json.dumps({"notes": "低波 事件"}, ensure_ascii=False),
            ),
        ],
    )
    conn.executemany(
        "INSERT INTO daily_trading_data(ts_code, trade_date, close_price) VALUES (?, ?, ?)",
        [
            ("000001.SZ", "20260102", 10.0),
            ("000001.SZ", "20260103", 11.0),
            ("000002.SZ", "20260102", 20.0),
            ("000002.SZ", "20260103", 19.0),
            ("000003.SZ", "20260102", 30.0),
            ("000003.SZ", "20260103", 30.6),
        ],
    )

    review = build_ensemble_alpha_sleeve_fact_chain(
        conn,
        as_of_date="2026-01-02",
        strategies=["v5"],
        holding_days=1,
        top_n_per_strategy=5,
    )

    assert review["research_only"] is True
    assert review["signal_count"] == 3
    assert review["replayable_signal_count"] == 3
    assert review["sleeves"]["momentum"]["active_signal_count"] >= 1
    assert review["sleeves"]["money_flow"]["active_signal_count"] >= 1
    assert review["sleeves"]["sector_rotation"]["active_signal_count"] >= 1
    assert review["sleeves"]["quality_low_vol"]["active_signal_count"] >= 1
    assert review["sleeves"]["event_risk"]["active_signal_count"] >= 1
    assert "do_not_emit_portfolio_weights_from_fact_chain" in review["hard_boundaries"]
    assert "portfolio_weights" not in review


def test_ensemble_alpha_sleeve_fact_chain_blocks_when_no_source_runs():
    conn = sqlite3.connect(":memory:")
    _init_schema(conn)

    review = build_ensemble_alpha_sleeve_fact_chain(
        conn,
        as_of_date="20260102",
        strategies=["v5"],
        holding_days=1,
    )

    assert review["research_only"] is True
    assert "missing_source_scan_runs:v5" in review["blocking_reasons"]
    assert "missing_signal_items_for_alpha_sleeves" in review["blocking_reasons"]


def test_ensemble_alpha_sleeve_fact_chain_uses_data_version_for_pit_visibility():
    conn = sqlite3.connect(":memory:")
    _init_schema(conn)
    conn.execute(
        """
        INSERT INTO signal_runs(run_id, run_type, strategy, trade_date, data_version, status)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("scan_v5", "scan", "v5", "2026-05-06", "trade_date:20260402|max_daily_trading_data=20260402", "success"),
    )
    conn.execute(
        """
        INSERT INTO signal_items(run_id, ts_code, score, rank_idx, reason_codes, raw_payload_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            "scan_v5",
            "000001.SZ",
            90.0,
            1,
            json.dumps(["breakout"], ensure_ascii=False),
            "{}",
        ),
    )
    conn.executemany(
        "INSERT INTO daily_trading_data(ts_code, trade_date, close_price) VALUES (?, ?, ?)",
        [
            ("000001.SZ", "20260402", 10.0),
            ("000001.SZ", "20260403", 10.5),
        ],
    )

    review = build_ensemble_alpha_sleeve_fact_chain(
        conn,
        as_of_date="20260402",
        strategies=["v5"],
        holding_days=1,
    )

    assert review["run_count"] == 1
    assert review["signal_count"] == 1
    assert review["sample_facts"][0]["run_trade_date"] == "2026-05-06"
    assert review["sample_facts"][0]["visible_as_of_date"] == "20260402"
    assert review["sample_facts"][0]["visibility_source"] == "data_version"


def test_ensemble_alpha_sleeve_fact_chain_uses_tushare_pro_pit_features_before_keyword_proxy():
    conn = sqlite3.connect(":memory:")
    _init_schema(conn)
    conn.executescript(
        """
        CREATE TABLE moneyflow_daily (
            ts_code TEXT,
            trade_date TEXT,
            buy_lg_amount REAL,
            sell_lg_amount REAL,
            buy_elg_amount REAL,
            sell_elg_amount REAL,
            net_mf_amount REAL
        );
        CREATE TABLE stock_basic (ts_code TEXT, industry TEXT, circ_mv REAL);
        CREATE TABLE moneyflow_ind_ths (trade_date TEXT, industry TEXT, pct_change REAL, net_amount REAL);
        CREATE TABLE top_list (trade_date TEXT, ts_code TEXT, net_amount REAL);
        CREATE TABLE stk_factor_pro_daily (
            ts_code TEXT,
            trade_date TEXT,
            ma_qfq_5 REAL,
            ma_qfq_20 REAL,
            pe_ttm REAL,
            pb REAL,
            circ_mv REAL
        );
        CREATE TABLE cyq_perf_daily (ts_code TEXT, trade_date TEXT, winner_rate REAL);
        """
    )
    conn.execute(
        "INSERT INTO signal_runs(run_id, run_type, strategy, trade_date, status) VALUES (?, ?, ?, ?, ?)",
        ("scan_v5", "scan", "v5", "20260122", "success"),
    )
    conn.execute(
        """
        INSERT INTO signal_items(run_id, ts_code, score, rank_idx, reason_codes, raw_payload_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("scan_v5", "000001.SZ", 80.0, 1, "[]", "{}"),
    )
    close = 10.0
    rows = []
    for idx in range(23):
        close += 0.08
        rows.append(("000001.SZ", f"202601{idx + 1:02d}", close, 0.7, 120000.0, 1.0))
    conn.executemany(
        "INSERT INTO daily_trading_data(ts_code, trade_date, close_price, pct_chg, amount, turnover_rate) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.execute(
        "INSERT INTO moneyflow_daily VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("000001.SZ", "20260122", 40000.0, 10000.0, 30000.0, 8000.0, 50000.0),
    )
    conn.execute("INSERT INTO stock_basic VALUES (?, ?, ?)", ("000001.SZ", "电子", 2_000_000.0))
    conn.execute("INSERT INTO moneyflow_ind_ths VALUES (?, ?, ?, ?)", ("20260122", "电子", 2.0, 80000.0))
    conn.execute("INSERT INTO top_list VALUES (?, ?, ?)", ("20260122", "000001.SZ", 20000.0))
    conn.execute(
        "INSERT INTO stk_factor_pro_daily VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("000001.SZ", "20260122", 11.4, 10.6, 20.0, 2.0, 2_000_000.0),
    )
    conn.execute("INSERT INTO cyq_perf_daily VALUES (?, ?, ?)", ("000001.SZ", "20260122", 22.0))

    review = build_ensemble_alpha_sleeve_fact_chain(
        conn,
        as_of_date="2026-01-22",
        strategies=["v5"],
        holding_days=1,
    )

    sample = review["sample_facts"][0]
    assert sample["sleeve_scores"]["momentum"]["mapping_source"] == "tushare_pro_pit_feature"
    assert sample["sleeve_scores"]["quality_low_vol"]["mapping_source"] == "tushare_pro_pit_feature"
    assert sample["sleeve_scores"]["event_risk"]["mapping_source"] == "tushare_pro_pit_feature"
    assert sample["tushare_pro_alpha_features"]["feature_version"] == "tushare_pro_alpha_features.v1"
    assert review["sleeves"]["momentum"]["active_signal_count"] == 1
    assert review["sleeves"]["quality_low_vol"]["active_signal_count"] == 1
    assert review["sleeves"]["event_risk"]["active_signal_count"] == 1


def test_ensemble_alpha_sleeve_fact_chain_reports_multi_horizon_decay_and_usage_policy():
    conn = sqlite3.connect(":memory:")
    _init_schema(conn)
    conn.execute(
        "INSERT INTO signal_runs(run_id, run_type, strategy, trade_date, status) VALUES (?, ?, ?, ?, ?)",
        ("scan_v5", "scan", "v5", "20260101", "success"),
    )
    conn.executemany(
        """
        INSERT INTO signal_items(run_id, ts_code, score, rank_idx, reason_codes, raw_payload_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            ("scan_v5", "000001.SZ", 90.0, 1, json.dumps(["breakout"]), "{}"),
            ("scan_v5", "000002.SZ", 70.0, 2, json.dumps(["breakout"]), "{}"),
            ("scan_v5", "000003.SZ", 50.0, 3, json.dumps(["breakout"]), "{}"),
        ],
    )
    price_rows = []
    for code, drift in [("000001.SZ", 0.03), ("000002.SZ", 0.015), ("000003.SZ", -0.005)]:
        close = 10.0
        for idx in range(22):
            close *= 1.0 + drift
            price_rows.append((code, f"202601{idx + 1:02d}", close, drift * 100.0, 100000.0, 1.0))
    conn.executemany(
        "INSERT INTO daily_trading_data(ts_code, trade_date, close_price, pct_chg, amount, turnover_rate) VALUES (?, ?, ?, ?, ?, ?)",
        price_rows,
    )

    review = build_ensemble_alpha_sleeve_fact_chain(
        conn,
        as_of_date="2026-01-01",
        strategies=["v5"],
        holding_days=5,
        top_n_per_strategy=3,
    )

    momentum = review["sleeves"]["momentum"]
    assert set(momentum["multi_horizon_attribution"]["horizons"]) == {"1", "3", "5", "10", "20"}
    assert momentum["multi_horizon_attribution"]["decay_available"] is True
    assert momentum["multi_horizon_attribution"]["horizons"]["5"]["rank_ic"] is not None
    assert momentum["recommended_use"] == "positive_alpha_candidate"
    assert review["multi_horizon_decay"]["momentum"]["positive_horizon_count"] >= 2
    assert review["multi_horizon_decay"]["momentum"]["positive_rank_horizon_count"] >= 2
    assert review["sleeve_use_policy"]["momentum"] == "positive_alpha_candidate"
    assert review["sleeve_policy_audit"]["passed"] is True
    assert review["sleeve_policy_audit"]["alpha_candidate_sleeves"] == ["momentum"]
    assert "do_not_use_negative_ic_sleeves_as_positive_alpha" in review["hard_boundaries"]


def test_ensemble_alpha_sleeve_policy_keeps_low_vol_as_filter_and_blocks_unsupported_alpha():
    conn = sqlite3.connect(":memory:")
    _init_schema(conn)
    conn.execute(
        "INSERT INTO signal_runs(run_id, run_type, strategy, trade_date, status) VALUES (?, ?, ?, ?, ?)",
        ("scan_v5", "scan", "v5", "20260101", "success"),
    )
    conn.executemany(
        """
        INSERT INTO signal_items(run_id, ts_code, score, rank_idx, reason_codes, raw_payload_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            ("scan_v5", "000001.SZ", 90.0, 1, json.dumps(["money_flow", "stable"]), "{}"),
            ("scan_v5", "000002.SZ", 70.0, 2, json.dumps(["money_flow", "stable"]), "{}"),
            ("scan_v5", "000003.SZ", 50.0, 3, json.dumps(["money_flow", "stable"]), "{}"),
        ],
    )
    price_rows = []
    for code, drift in [("000001.SZ", 0.01), ("000002.SZ", 0.03), ("000003.SZ", 0.05)]:
        close = 10.0
        for idx in range(22):
            close *= 1.0 + drift
            price_rows.append((code, f"202601{idx + 1:02d}", close, drift * 100.0, 100000.0, 1.0))
    conn.executemany(
        "INSERT INTO daily_trading_data(ts_code, trade_date, close_price, pct_chg, amount, turnover_rate) VALUES (?, ?, ?, ?, ?, ?)",
        price_rows,
    )

    review = build_ensemble_alpha_sleeve_fact_chain(
        conn,
        as_of_date="2026-01-01",
        strategies=["v5"],
        holding_days=5,
        top_n_per_strategy=3,
    )

    assert review["sleeve_use_policy"]["quality_low_vol"] == "risk_filter_candidate"
    assert review["sleeve_use_policy"]["money_flow"] == "research_blocked_negative_ic"
    assert review["sleeve_policy_audit"]["passed"] is True
    assert "quality_low_vol" in review["sleeve_policy_audit"]["risk_filter_sleeves"]
    assert "money_flow" in review["sleeve_policy_audit"]["blocked_sleeves"]
    assert "sleeve_not_positive_alpha:risk_filter_candidate" in review["sleeves"]["quality_low_vol"]["blocking_reasons"]
    assert "sleeve_not_positive_alpha:research_blocked_negative_ic" in review["sleeves"]["money_flow"]["blocking_reasons"]
