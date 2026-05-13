from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd

from openclaw.services import top5_trader_brief_rebuild_service as svc


def _seed_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE stock_basic (ts_code TEXT PRIMARY KEY, name TEXT)")
        conn.executemany(
            "INSERT INTO stock_basic (ts_code, name) VALUES (?, ?)",
            [
                ("600001.SH", "高权重"),
                ("600002.SH", "低流动"),
                ("600003.SH", "中权重"),
            ],
        )
        conn.execute(
            "CREATE TABLE daily_trading_data (ts_code TEXT, trade_date TEXT, close_price REAL)"
        )
        conn.executemany(
            "INSERT INTO daily_trading_data (ts_code, trade_date, close_price) VALUES (?, ?, ?)",
            [
                ("600001.SH", "20260510", 10.0),
                ("600002.SH", "20260510", 20.0),
                ("600003.SH", "20260510", 30.0),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def test_top5_trader_brief_exports_trading_desk_controls(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.db"
    _seed_db(db_path)
    artifact = tmp_path / "strategy_competition_portfolio_audit_fixture.json"
    artifact.write_text(
        json.dumps(
            {
                "artifact_version": "strategy_competition_portfolio_audit.v1",
                "competition_run_id": "comp_fixture",
                "trade_date": "2026-05-10",
                "audit_mode": "strict",
                "top5_portfolio_audit": [
                    {
                        "ts_code": "600001.SH",
                        "weight": 0.25,
                        "source": {
                            "final_stock_score": 81.2,
                            "signal_refs": [{"strategy": "v9", "strategy_tier": "canary"}],
                        },
                        "risk": {
                            "industry": "测试",
                            "liquidity_amount": 900000,
                            "risk_contribution_share": 0.18,
                            "pct_chg": 0.3,
                        },
                        "cost": {"estimated_cost_bps": 18.0},
                    },
                    {
                        "ts_code": "600002.SH",
                        "weight": 0.23,
                        "source": {
                            "final_stock_score": 80.0,
                            "signal_refs": [{"strategy": "v9", "strategy_tier": "canary"}],
                        },
                        "risk": {
                            "industry": "测试",
                            "liquidity_amount": 450000,
                            "risk_contribution_share": 0.22,
                            "pct_chg": -0.6,
                        },
                        "cost": {"estimated_cost_bps": 28.0},
                    },
                    {
                        "ts_code": "600003.SH",
                        "weight": 0.12,
                        "source": {
                            "final_stock_score": 78.0,
                            "signal_refs": [{"strategy": "v9", "strategy_tier": "canary"}],
                        },
                        "risk": {
                            "industry": "测试",
                            "liquidity_amount": 1200000,
                            "risk_contribution_share": 0.12,
                            "pct_chg": -1.2,
                        },
                        "cost": {"estimated_cost_bps": 16.0},
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    ctx = svc.Top5RepoContext(
        repo_root=tmp_path,
        permanent_db_path=str(db_path),
        sim_db_path=str(tmp_path / "sim.db"),
        assistant_db_path=str(tmp_path / "assistant.db"),
    )

    built = svc._run_with_ctx(ctx, svc._build_top5_trader_brief_from_artifact, artifact)

    df = pd.read_csv(built["csv"])
    assert Path(built["csv"]).name.startswith("top5_trader_brief_20260510_")
    assert list(df["执行优先级"]) == ["P1", "P3", "P2"]
    assert "委托方式" in df.columns
    assert "首波上限" in df.columns
    assert "交易台硬门禁" in df.columns
    assert "清单状态" in df.columns
    assert df.loc[1, "风险标签"] == "流动性敏感、交易成本偏高"
    assert df.loc[1, "委托方式"] == "被动限价为主；盘口恢复前不主动扫单"
    md_text = Path(built["markdown"]).read_text(encoding="utf-8")
    assert "未完成成交回报与盘后归因前，不得把 Top5 表现用于生产晋级" in md_text
    assert "硬门禁=竞价流动性低于快照30%" in md_text


def test_latest_valid_top5_audit_artifact_skips_empty_newer_file(tmp_path: Path) -> None:
    older = tmp_path / "strategy_competition_portfolio_audit_old.json"
    older.write_text(json.dumps({"top5_portfolio_audit": [{"ts_code": "600001.SH"}]}), encoding="utf-8")
    newer = tmp_path / "strategy_competition_portfolio_audit_new.json"
    newer.write_text(json.dumps({"top5_portfolio_audit": []}), encoding="utf-8")

    assert svc._latest_valid_top5_audit_artifact(tmp_path) == older


def test_resolve_top5_trade_compact_falls_back_to_run_id(tmp_path: Path) -> None:
    artifact = tmp_path / "strategy_competition_portfolio_audit_comp_20260510_094330.json"

    assert svc._resolve_top5_trade_compact({"trade_date": ""}, "comp_20260510_094330", artifact) == "20260510"
