CREATE TABLE IF NOT EXISTS decision_events (
    decision_id TEXT PRIMARY KEY,
    decision_type TEXT NOT NULL,
    based_on_run_id TEXT NOT NULL DEFAULT '',
    risk_gate_state TEXT NOT NULL DEFAULT '{}',
    release_gate_state TEXT NOT NULL DEFAULT '{}',
    approval_reason_codes TEXT NOT NULL DEFAULT '[]',
    approval_note TEXT NOT NULL DEFAULT '',
    operator_name TEXT NOT NULL DEFAULT '',
    decision_payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_decision_events_type_created
ON decision_events (decision_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_decision_events_run_id
ON decision_events (based_on_run_id, created_at DESC);

CREATE TABLE IF NOT EXISTS decision_snapshot (
    decision_id TEXT PRIMARY KEY,
    decision_status TEXT NOT NULL DEFAULT 'created',
    effective_trade_date TEXT NOT NULL DEFAULT '',
    selected_count INTEGER NOT NULL DEFAULT 0,
    active_flag INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (decision_id) REFERENCES decision_events(decision_id)
);

CREATE INDEX IF NOT EXISTS idx_decision_snapshot_effective
ON decision_snapshot (effective_trade_date, active_flag, updated_at DESC);
