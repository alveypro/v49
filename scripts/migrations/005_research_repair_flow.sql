CREATE TABLE IF NOT EXISTS research_repair_iteration_flow (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    flow_id TEXT NOT NULL UNIQUE,
    strategy TEXT NOT NULL,
    candidate TEXT NOT NULL,
    parent_flow_id TEXT NOT NULL DEFAULT '',
    attempt_no INTEGER NOT NULL,
    rule_version TEXT NOT NULL,
    rule_hash TEXT NOT NULL,
    current_status TEXT NOT NULL,
    current_pool TEXT NOT NULL DEFAULT 'research_blocked',
    repair_objective TEXT NOT NULL,
    forbidden_objectives TEXT NOT NULL DEFAULT '[]',
    predeclared_rules_json TEXT NOT NULL DEFAULT '{}',
    fixed_window_set_json TEXT NOT NULL DEFAULT '[]',
    benchmark_config_json TEXT NOT NULL DEFAULT '{}',
    data_snapshot TEXT NOT NULL DEFAULT '',
    unthrottled_artifact_path TEXT NOT NULL DEFAULT '',
    throttled_artifact_path TEXT NOT NULL DEFAULT '',
    attribution_artifact_path TEXT NOT NULL DEFAULT '',
    monitor_artifact_path TEXT NOT NULL DEFAULT '',
    repair_review_artifact_path TEXT NOT NULL DEFAULT '',
    unthrottled_summary_json TEXT NOT NULL DEFAULT '{}',
    throttled_summary_json TEXT NOT NULL DEFAULT '{}',
    regime_summary_json TEXT NOT NULL DEFAULT '{}',
    blocking_reasons_json TEXT NOT NULL DEFAULT '[]',
    warning_reasons_json TEXT NOT NULL DEFAULT '[]',
    next_allowed_actions_json TEXT NOT NULL DEFAULT '[]',
    prohibited_actions_json TEXT NOT NULL DEFAULT '[]',
    operator_name TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS research_rule_freeze_registry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_version TEXT NOT NULL UNIQUE,
    candidate TEXT NOT NULL,
    strategy TEXT NOT NULL,
    rule_hash TEXT NOT NULL,
    rule_spec_json TEXT NOT NULL DEFAULT '{}',
    activation_regime TEXT NOT NULL DEFAULT '',
    predeclared INTEGER NOT NULL DEFAULT 1,
    frozen_at TEXT NOT NULL,
    operator_name TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS research_artifact_registry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    flow_id TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    artifact_path TEXT NOT NULL,
    artifact_hash TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    summary_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS observation_watch_risk_register (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    flow_id TEXT NOT NULL,
    strategy TEXT NOT NULL,
    candidate TEXT NOT NULL,
    risk_code TEXT NOT NULL,
    risk_level TEXT NOT NULL,
    risk_description TEXT NOT NULL,
    metric_name TEXT NOT NULL DEFAULT '',
    metric_value REAL,
    threshold_value REAL,
    required_monitoring TEXT NOT NULL,
    exit_condition TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_research_repair_flow_candidate
ON research_repair_iteration_flow(strategy, candidate, current_status);

CREATE INDEX IF NOT EXISTS idx_research_artifact_flow
ON research_artifact_registry(flow_id, artifact_type);

CREATE INDEX IF NOT EXISTS idx_observation_watch_risk_flow
ON observation_watch_risk_register(flow_id, status);
