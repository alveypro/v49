from __future__ import annotations

import sqlite3

from openclaw.services.tushare_pro_alpha_feature_service import build_tushare_pro_alpha_features


def test_tushare_pro_alpha_features_build_explicit_pit_sleeve_scores():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE daily_trading_data (
            ts_code TEXT,
            trade_date TEXT,
            close_price REAL,
            pct_chg REAL,
            amount REAL,
            turnover_rate REAL
        );
        CREATE TABLE moneyflow_daily (
            ts_code TEXT,
            trade_date TEXT,
            buy_lg_amount REAL,
            sell_lg_amount REAL,
            buy_elg_amount REAL,
            sell_elg_amount REAL,
            net_mf_amount REAL
        );
        CREATE TABLE stock_basic (
            ts_code TEXT,
            industry TEXT,
            circ_mv REAL
        );
        CREATE TABLE moneyflow_ind_ths (
            trade_date TEXT,
            industry TEXT,
            pct_change REAL,
            net_amount REAL
        );
        CREATE TABLE top_list (
            trade_date TEXT,
            ts_code TEXT,
            net_amount REAL,
            reason TEXT
        );
        CREATE TABLE top_inst (
            trade_date TEXT,
            ts_code TEXT,
            net_buy REAL,
            reason TEXT
        );
        CREATE TABLE hm_detail_daily (
            trade_date TEXT,
            ts_code TEXT,
            net_amount REAL,
            hm_name TEXT
        );
        CREATE TABLE repurchase_events (
            ts_code TEXT,
            ann_date TEXT,
            amount REAL,
            proc TEXT
        );
        CREATE TABLE share_float_events (
            ts_code TEXT,
            ann_date TEXT,
            float_ratio REAL,
            share_type TEXT
        );
        CREATE TABLE stk_surv_events (
            ts_code TEXT,
            surv_date TEXT,
            fund_visitors REAL,
            rece_mode TEXT
        );
        CREATE TABLE margin_detail (
            trade_date TEXT,
            ts_code TEXT,
            rzye REAL,
            rqye REAL
        );
        CREATE TABLE stk_auction_daily (
            ts_code TEXT,
            trade_date TEXT,
            vol REAL,
            price REAL,
            amount REAL,
            pre_close REAL,
            turnover_rate REAL,
            volume_ratio REAL,
            float_share REAL
        );
        CREATE TABLE stk_factor_pro_daily (
            ts_code TEXT,
            trade_date TEXT,
            ma_qfq_5 REAL,
            ma_qfq_20 REAL,
            pe_ttm REAL,
            pb REAL,
            circ_mv REAL
        );
        CREATE TABLE cyq_perf_daily (
            ts_code TEXT,
            trade_date TEXT,
            winner_rate REAL
        );
        """
    )
    prices = []
    close = 10.0
    for idx in range(22):
        close += 0.08
        prices.append(("000001.SZ", f"202601{idx + 1:02d}", close, 0.8, 120000.0, 1.1))
    conn.executemany(
        "INSERT INTO daily_trading_data VALUES (?, ?, ?, ?, ?, ?)",
        prices,
    )
    conn.execute(
        "INSERT INTO moneyflow_daily VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("000001.SZ", "20260122", 45000.0, 12000.0, 30000.0, 8000.0, 52000.0),
    )
    conn.execute("INSERT INTO stock_basic VALUES (?, ?, ?)", ("000001.SZ", "电子", 2_000_000.0))
    conn.execute("INSERT INTO moneyflow_ind_ths VALUES (?, ?, ?, ?)", ("20260122", "电子", 2.5, 90000.0))
    conn.execute("INSERT INTO top_list VALUES (?, ?, ?, ?)", ("20260122", "000001.SZ", 20000.0, "日涨幅偏离值"))
    conn.execute("INSERT INTO top_inst VALUES (?, ?, ?, ?)", ("20260122", "000001.SZ", 12000.0, "机构买入"))
    conn.execute("INSERT INTO hm_detail_daily VALUES (?, ?, ?, ?)", ("20260121", "000001.SZ", 8000.0, "活跃营业部"))
    conn.execute("INSERT INTO repurchase_events VALUES (?, ?, ?, ?)", ("000001.SZ", "20260120", 5000.0, "实施"))
    conn.execute("INSERT INTO share_float_events VALUES (?, ?, ?, ?)", ("000001.SZ", "20260119", 2.5, "首发限售"))
    conn.execute("INSERT INTO stk_surv_events VALUES (?, ?, ?, ?)", ("000001.SZ", "20260118", 14.0, "现场调研"))
    conn.executemany(
        "INSERT INTO margin_detail VALUES (?, ?, ?, ?)",
        [
            ("20260122", "000001.SZ", 120000.0, 2000.0),
            ("20260121", "000001.SZ", 110000.0, 1000.0),
        ],
    )
    conn.execute(
        "INSERT INTO stk_auction_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("000001.SZ", "20260122", 10000.0, 11.8, 118000.0, 11.6, 0.8, 1.3, 1000000.0),
    )
    conn.execute(
        "INSERT INTO stk_factor_pro_daily VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("000001.SZ", "20260122", 11.5, 10.5, 18.0, 2.1, 2_000_000.0),
    )
    conn.execute("INSERT INTO cyq_perf_daily VALUES (?, ?, ?)", ("000001.SZ", "20260122", 20.0))

    review = build_tushare_pro_alpha_features(conn, ts_code="000001.SZ", as_of_date="2026-01-22")

    assert review["research_only"] is True
    assert "price_volume" in review["pit_inputs"]
    assert "money_flow" in review["pit_inputs"]
    assert "sector_heat" in review["pit_inputs"]
    assert review["scores"]["momentum"] > 0
    assert review["scores"]["money_flow"] > 0
    assert review["scores"]["sector_rotation"] > 0
    assert review["scores"]["quality_low_vol"] > 0
    assert review["scores"]["event_risk"] > 0
    assert review["scores"]["hard_event_alpha"] > 0
    assert review["evidence"]["event_count"] >= 6
    assert set(review["evidence"]["event_types"]) >= {
        "top_list",
        "top_inst",
        "hot_money",
        "repurchase",
        "share_float_unlock",
        "institution_survey",
    }
    assert review["evidence"]["event_facts"][0]["visible_as_of_date"] <= "20260122"
    hard_alpha = review["evidence"]["hard_alpha"]
    assert hard_alpha["money_flow_persistence"]["positive_net_days"] == 1
    assert hard_alpha["dragon_tiger_seat_quality"]["institution_positive_rows"] == 1
    assert hard_alpha["capacity_liquidity"]["auction_trade_date"] == "20260122"
    assert hard_alpha["margin_pressure"]["financing_balance_delta"] > 0
    assert hard_alpha["announcement_availability"]["blocking_reasons"] == [
        "missing_announcement_or_earnings_forecast_tables"
    ]
    assert review["blocking_reasons"] == []
    assert "do_not_fetch_live_tushare_inside_backtest" in review["hard_boundaries"]
    assert "do_not_use_unscored_event_presence_as_trade_signal" in review["hard_boundaries"]
    assert "do_not_use_missing_announcement_tables_as_synthetic_event_alpha" in review["hard_boundaries"]
