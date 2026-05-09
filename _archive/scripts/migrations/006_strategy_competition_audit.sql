CREATE TABLE IF NOT EXISTS strategy_competition_registry (
    registry_id TEXT PRIMARY KEY,
    strategy TEXT NOT NULL,
    alpha_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'candidate',
    model_card_json TEXT NOT NULL DEFAULT '{}',
    hypothesis TEXT NOT NULL DEFAULT '',
    rule_hash TEXT NOT NULL DEFAULT '',
    data_hash TEXT NOT NULL DEFAULT '',
    code_hash TEXT NOT NULL DEFAULT '',
    evidence_manifest TEXT NOT NULL DEFAULT '',
    eligible_for_competition INTEGER NOT NULL DEFAULT 0,
    blocking_reasons_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_strategy_competition_registry_strategy_created
ON strategy_competition_registry (strategy, created_at DESC);

CREATE TABLE IF NOT EXISTS portfolio_competition_runs (
    competition_run_id TEXT PRIMARY KEY,
    trade_date TEXT NOT NULL DEFAULT '',
    fixed_candidate_pool_json TEXT NOT NULL DEFAULT '[]',
    ranking_method_hash TEXT NOT NULL DEFAULT '',
    recommendation_artifact_json TEXT NOT NULL DEFAULT '{}',
    top5_symbols_json TEXT NOT NULL DEFAULT '[]',
    portfolio_constraints_json TEXT NOT NULL DEFAULT '{}',
    result_status TEXT NOT NULL DEFAULT 'created',
    blocking_reasons_json TEXT NOT NULL DEFAULT '[]',
    artifact_path TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_portfolio_competition_runs_trade_date
ON portfolio_competition_runs (trade_date, created_at DESC);

CREATE TABLE IF NOT EXISTS portfolio_construction_audits (
    audit_id TEXT PRIMARY KEY,
    competition_run_id TEXT NOT NULL,
    top5_audit_json TEXT NOT NULL DEFAULT '[]',
    risk_summary_json TEXT NOT NULL DEFAULT '{}',
    cost_summary_json TEXT NOT NULL DEFAULT '{}',
    constraints_passed INTEGER NOT NULL DEFAULT 0,
    blocking_reasons_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_portfolio_construction_audits_run
ON portfolio_construction_audits (competition_run_id, created_at DESC);

CREATE TABLE IF NOT EXISTS independent_validation_decisions (
    validation_id TEXT PRIMARY KEY,
    competition_run_id TEXT NOT NULL,
    validator_name TEXT NOT NULL DEFAULT '',
    validator_role TEXT NOT NULL DEFAULT '',
    decision TEXT NOT NULL DEFAULT 'pending',
    conflict_of_interest_attested INTEGER NOT NULL DEFAULT 0,
    reviewed_artifacts_json TEXT NOT NULL DEFAULT '[]',
    blocking_reasons_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_independent_validation_decisions_run
ON independent_validation_decisions (competition_run_id, created_at DESC);
