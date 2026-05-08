CREATE TABLE IF NOT EXISTS execution_orders (
    order_id TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL,
    ts_code TEXT NOT NULL,
    side TEXT NOT NULL DEFAULT 'buy',
    target_qty INTEGER NOT NULL DEFAULT 0,
    decision_price REAL NOT NULL DEFAULT 0,
    submitted_price REAL NOT NULL DEFAULT 0,
    submitted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'created',
    cancel_reason TEXT NOT NULL DEFAULT '',
    broker_ref TEXT NOT NULL DEFAULT '',
    source_type TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (decision_id) REFERENCES decision_events(decision_id)
);

CREATE INDEX IF NOT EXISTS idx_execution_orders_decision
ON execution_orders (decision_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_execution_orders_status
ON execution_orders (status, submitted_at DESC);

CREATE TABLE IF NOT EXISTS execution_fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    fill_price REAL NOT NULL DEFAULT 0,
    fill_qty INTEGER NOT NULL DEFAULT 0,
    fill_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    fill_fee REAL NOT NULL DEFAULT 0,
    fill_slippage_bp REAL NOT NULL DEFAULT 0,
    venue TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES execution_orders(order_id)
);

CREATE INDEX IF NOT EXISTS idx_execution_fills_order
ON execution_fills (order_id, fill_time DESC);

CREATE TABLE IF NOT EXISTS execution_attribution (
    order_id TEXT PRIMARY KEY,
    decision_price REAL NOT NULL DEFAULT 0,
    submit_price REAL NOT NULL DEFAULT 0,
    avg_fill_price REAL NOT NULL DEFAULT 0,
    close_price REAL NOT NULL DEFAULT 0,
    delay_sec REAL NOT NULL DEFAULT 0,
    fill_ratio REAL NOT NULL DEFAULT 0,
    slippage_bp REAL NOT NULL DEFAULT 0,
    miss_reason_code TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES execution_orders(order_id)
);
