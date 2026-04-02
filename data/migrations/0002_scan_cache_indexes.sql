CREATE TABLE IF NOT EXISTS scan_cache_v5 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_date TEXT NOT NULL,
    ts_code TEXT NOT NULL,
    stock_name TEXT,
    industry TEXT,
    latest_price REAL,
    circ_mv REAL,
    final_score REAL,
    dim_scores TEXT,
    scan_params TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(scan_date, ts_code, scan_params)
);

CREATE TABLE IF NOT EXISTS scan_cache_v6 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_date TEXT NOT NULL,
    ts_code TEXT NOT NULL,
    stock_name TEXT,
    industry TEXT,
    latest_price REAL,
    circ_mv REAL,
    final_score REAL,
    dim_scores TEXT,
    scan_params TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(scan_date, ts_code, scan_params)
);

CREATE INDEX IF NOT EXISTS idx_scan_cache_v5_date
ON scan_cache_v5(scan_date, scan_params);

CREATE INDEX IF NOT EXISTS idx_scan_cache_v6_date
ON scan_cache_v6(scan_date, scan_params);
