CREATE TABLE IF NOT EXISTS signal_runs (
    run_id TEXT PRIMARY KEY,
    run_type TEXT NOT NULL,
    strategy TEXT NOT NULL,
    trade_date TEXT NOT NULL DEFAULT '',
    data_version TEXT NOT NULL DEFAULT '',
    code_version TEXT NOT NULL DEFAULT '',
    param_version TEXT NOT NULL DEFAULT '',
    parent_run_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'created',
    artifact_path TEXT NOT NULL DEFAULT '',
    summary_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_signal_runs_type_strategy_created
ON signal_runs (run_type, strategy, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_signal_runs_trade_date
ON signal_runs (trade_date, created_at DESC);

CREATE TABLE IF NOT EXISTS signal_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    ts_code TEXT NOT NULL,
    score REAL NOT NULL DEFAULT 0,
    rank_idx INTEGER NOT NULL DEFAULT 0,
    reason_codes TEXT NOT NULL DEFAULT '[]',
    raw_payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES signal_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_signal_items_run_rank
ON signal_items (run_id, rank_idx, score DESC);

CREATE INDEX IF NOT EXISTS idx_signal_items_ts_code
ON signal_items (ts_code, created_at DESC);
