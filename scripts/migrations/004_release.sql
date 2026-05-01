CREATE TABLE IF NOT EXISTS release_events (
    release_id TEXT PRIMARY KEY,
    release_type TEXT NOT NULL,
    code_version TEXT NOT NULL DEFAULT '',
    config_version TEXT NOT NULL DEFAULT '',
    operator_name TEXT NOT NULL DEFAULT '',
    gate_result TEXT NOT NULL DEFAULT '{}',
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_release_events_type_created
ON release_events (release_type, created_at DESC);

CREATE TABLE IF NOT EXISTS release_validations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    release_id TEXT NOT NULL,
    validation_type TEXT NOT NULL,
    validation_status TEXT NOT NULL,
    validation_output_path TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (release_id) REFERENCES release_events(release_id)
);

CREATE INDEX IF NOT EXISTS idx_release_validations_release
ON release_validations (release_id, created_at DESC);
