import json
from pathlib import Path

from src import dashboard_context
from src.current_result_pointer import CurrentResultPointerStore
from src.first_place_evidence_cockpit import (
    WAIT_STATUS_COMMAND,
    build_first_place_evidence_cockpit_view_model,
    render_first_place_evidence_cockpit,
)
from src.result_registry import ResultRegistry
from run_dashboard import _render_dashboard
from src.run_registry import RunRegistry


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_cockpit_artifacts(tmp_path: Path) -> Path:
    artifacts_dir = tmp_path / "artifacts"
    RunRegistry(runs_dir=artifacts_dir / "run_registry").register(
        run_id="run-001",
        run_type="daily_research",
        producer="test",
        config_hash="cfg-001",
        data_snapshot_id="data-001",
        code_revision="rev-001",
        make_current=True,
    )
    ResultRegistry(results_dir=artifacts_dir / "result_registry").register(
        record_id="result-record-001",
        result_id="primary:002463.SZ",
        run_id="run-001",
        ts_code="002463.SZ",
        stock_name="沪电股份",
        lifecycle_stage="L2",
        artifact_ids=["artifact:a"],
        make_current=True,
    )
    CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer").point_to(
        pointer_snapshot_id="pointer-001",
        result_id="primary:002463.SZ",
        run_id="run-001",
        lifecycle_id="lifecycle-001",
        artifact_ids=["artifact:a"],
        as_of_date="2026-04-18",
    )
    _write_json(
        artifacts_dir / "primary_result_observation_wait_status_latest.json",
        {
            "status": "pending_window",
            "generated_at": "2026-04-18T03:32:19+00:00",
            "result_id": "primary:002463.SZ",
            "ts_code": "002463.SZ",
            "stock_name": "沪电股份",
            "current_date": "2026-04-18",
            "observation_window": {
                "started_at": "2026-04-20T01:30:00Z",
                "has_started": False,
            },
            "blocking_reasons": [
                "current date must be on or after observation window start before closure checks"
            ],
        },
    )
    _write_json(
        artifacts_dir / "primary_result_performance_evidence_latest.json",
        {
            "generated_at": "2026-04-18T03:09:10+00:00",
            "streams": [
                {
                    "stream_id": "primary_result",
                    "entry_total": 1,
                    "windows": [{"blocking_reasons": ["requires at least 20 ledger entries"]}],
                },
                {
                    "stream_id": "candidate_basket",
                    "entry_total": 0,
                    "windows": [{"blocking_reasons": ["requires at least 20 ledger entries"]}],
                },
            ]
        },
    )
    _write_json(
        artifacts_dir / "primary_result_promotion_readiness_gate_latest.json",
        {
            "generated_at": "2026-04-18T03:09:10+00:00",
            "decision": "blocked",
            "blocking_reasons": ["performance evidence must be ready before promotion review"],
        },
    )
    _write_json(
        artifacts_dir / "primary_result_daily_operations_scoreboard_latest.json",
        {"generated_at": "2026-04-18T03:09:10+00:00", "overall_status": "yellow", "score": 79},
    )
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        exp_dir / "primary_result_daily_closure_latest.json",
        {
            "status": "pending_window",
            "generated_at": "2026-04-26T23:28:32+08:00",
            "window_start": "2026-04-27T01:30:00Z",
            "window_end": "2026-04-26",
            "next_actions": ["wait until the primary result observation window starts before running daily closure"],
        },
    )
    _write_json(
        exp_dir / "primary_result_lifecycle_evidence_latest.json",
        {
            "status": "passed",
            "result_id": "primary:002463.SZ",
            "run_id": "run-001",
            "lifecycle_id": "lifecycle-001",
            "final_payload": {
                "result_id": "primary:002463.SZ",
                "run_id": "run-001",
                "lifecycle_id": "lifecycle-001",
                "artifact_ids": ["artifact:a"],
                "as_of_date": "2026-04-18",
            },
            "steps": [
                {
                    "step": "current_result_pointer",
                    "exists": True,
                    "path": str(artifacts_dir / "current_result_pointer" / "current.json"),
                }
            ],
        },
    )
    return artifacts_dir


def _prepare_dashboard_artifacts(tmp_path: Path) -> tuple[Path, Path]:
    exp_dir = tmp_path / "data" / "experiments"
    rep_dir = tmp_path / "data" / "reports"
    exp_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "daily_research_latest.md").write_text(
        "- score: 90.00/100\n- success_rate: 95.00%\n- failure_penalty: 1.0\n- category_penalty: 0.0\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_health_trend_latest.csv").write_text(
        "generated_at,score\n2026-04-05 08:00:00,90\n",
        encoding="utf-8",
    )
    (exp_dir / "backtest_leaderboard.csv").write_text(
        "run_id,source_type,stock_pool,total_return,sharpe_ratio,max_drawdown,win_rate,total_trades\n"
        "r1,official_research,000001.SZ|600036.SH,0.05,1.0,-0.04,0.55,24\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "002463.SZ,沪电股份,元器件,strong_buy,medium,120\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.md").write_text("# Candidates\n", encoding="utf-8")
    (exp_dir / "candidates_basket_summary_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "candidates_basket_validation_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"002463.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_cycle_latest.json").write_text(
        '{"cycle_state":"observe_only","recommended_action":"hold_observation","operator_message":"仅观察。","release_readiness":{"ready_for_release":false}}',
        encoding="utf-8",
    )
    (exp_dir / "t12_alert_center_latest.json").write_text('{"summary":{"overall_status":"pass"}}', encoding="utf-8")
    (exp_dir / "t12_review_checklist_latest.json").write_text('{"overall_status":"pass"}', encoding="utf-8")
    (exp_dir / "t12_threshold_control_latest.json").write_text('{"applied": true}', encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text(
        '{"mode":"off","triggered":false,"would_rollback_apply":false}',
        encoding="utf-8",
    )
    (exp_dir / "t12_threshold_rollback_events.json").write_text('{"items":[]}', encoding="utf-8")
    (exp_dir / "t12_threshold_stable_snapshot.json").write_text('{"thresholds":{}}', encoding="utf-8")
    (rep_dir / "backtest_report_20260405_080000.md").write_text("# report\n", encoding="utf-8")
    _write_cockpit_artifacts(tmp_path)
    return exp_dir, rep_dir


def test_first_place_evidence_cockpit_reads_wait_and_evidence_state(tmp_path):
    artifacts_dir = _write_cockpit_artifacts(tmp_path)

    payload = build_first_place_evidence_cockpit_view_model(
        artifacts_dir=artifacts_dir,
        exp_dir=tmp_path / "data" / "experiments",
    )
    html_text = render_first_place_evidence_cockpit(payload)

    assert payload["status"] == "pending_window"
    assert payload["decision_status"] == "受控等待"
    assert payload["decision_reason"] == "观察窗口尚未开始"
    assert payload["fact_source_role"] == "evidence_display_only"
    assert payload["current_result_pointer_result_id"] == "primary:002463.SZ"
    assert payload["next_check"] == "2026-04-27"
    assert payload["decision_summary"] == "所有样本闭合动作已锁定"
    assert payload["primary_progress"] == "1/20"
    assert payload["basket_progress"] == "0/20"
    assert payload["primary_needed"] == 19
    assert payload["basket_needed"] == 20
    assert payload["promotion_decision"] == "blocked"
    assert payload["promotion_decision_label"] == "晋级锁定"
    assert payload["action_locks"][0]["state"] == "已锁定"
    assert payload["evidence_sources"][0]["artifact"] == "current_result_pointer/current.json"
    assert payload["evidence_sources"][1]["artifact"] == "primary_result_daily_closure_latest.json"
    assert WAIT_STATUS_COMMAND in html_text
    assert "证据路径" in html_text
    assert "判断、证据、边界、下一步" in html_text
    assert "当前状态" in html_text
    assert "受控等待" in html_text
    assert "观察窗口尚未开始" in html_text
    assert "下一次检查" in html_text
    assert "2026-04-27" in html_text
    assert "所有样本闭合动作已锁定" in html_text
    assert "系统状态" in html_text
    assert "晋级锁定" in html_text
    assert "行动锁矩阵" in html_text
    assert "闭合 primary_result" in html_text
    assert "写入 performance ledger" in html_text
    assert "证据来源" in html_text
    assert "主结果证据还差 19 个干净样本" in html_text


def test_dashboard_overview_places_evidence_cockpit_before_primary_result(tmp_path, monkeypatch):
    exp_dir, rep_dir = _prepare_dashboard_artifacts(tmp_path)
    monkeypatch.setattr(
        dashboard_context,
        "resolve_experiments_path",
        lambda path=None: exp_dir if path in {None, ".", "", "data/experiments"} else exp_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_reports_path",
        lambda path=None: rep_dir if path in {None, ".", "", "data/reports"} else rep_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_artifacts_path",
        lambda path=None: (tmp_path / "artifacts") if path in {None, ".", "", "artifacts"} else (tmp_path / "artifacts" / Path(path)),
    )
    import run_dashboard as dashboard_module

    monkeypatch.setattr(
        dashboard_module,
        "resolve_experiments_path",
        lambda path=None: exp_dir if path in {None, ".", "", "data/experiments"} else exp_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_module,
        "resolve_artifacts_path",
        lambda path=None: (tmp_path / "artifacts") if path in {None, ".", "", "artifacts"} else (tmp_path / "artifacts" / Path(path)),
    )

    html_text = _render_dashboard(
        tmp_path,
        current_view="overview",
        candidate_index=0,
        current_report="research",
        base_path="/stock",
    )

    external_spine_index = html_text.index('id="external-decision-spine"')
    control_strip_index = html_text.index('class="control-strip"')
    cockpit_index = html_text.index('id="first-place-evidence-cockpit"')
    primary_result_index = html_text.index('id="stock-primary-result"')
    kpi_index = html_text.index('class="grid-kpi"')
    assert external_spine_index < control_strip_index
    assert cockpit_index < primary_result_index
    assert primary_result_index < kpi_index
    assert "对外结论" in html_text
    assert "先读这里" in html_text
    assert "当前结论：" in html_text
    assert "主结果对象：" in html_text
    assert "候选篮第一：" in html_text
    assert "风险状态：" in html_text
    assert "受控等待" in html_text
    assert "观察窗口尚未开始" in html_text
    assert "所有样本闭合动作已锁定" in html_text
    assert "1/20" in html_text
    assert "0/20" in html_text
    assert "晋级锁定" in html_text
    assert "行动锁矩阵" in html_text
    assert "证据来源" in html_text
    assert "主结果已闭合" in html_text
    assert "篮子已闭合</span><strong>0</strong>" in html_text
    assert "不决定主结果真相，不闭合样本、不写 ledger、不交易、不促晋级" in html_text


def test_dashboard_core_blocks_share_external_display_contract(tmp_path, monkeypatch):
    exp_dir, rep_dir = _prepare_dashboard_artifacts(tmp_path)
    monkeypatch.setattr(
        dashboard_context,
        "resolve_experiments_path",
        lambda path=None: exp_dir if path in {None, ".", "", "data/experiments"} else exp_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_reports_path",
        lambda path=None: rep_dir if path in {None, ".", "", "data/reports"} else rep_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_artifacts_path",
        lambda path=None: (tmp_path / "artifacts") if path in {None, ".", "", "artifacts"} else (tmp_path / "artifacts" / Path(path)),
    )
    import run_dashboard as dashboard_module

    monkeypatch.setattr(
        dashboard_module,
        "resolve_experiments_path",
        lambda path=None: exp_dir if path in {None, ".", "", "data/experiments"} else exp_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_module,
        "resolve_artifacts_path",
        lambda path=None: (tmp_path / "artifacts") if path in {None, ".", "", "artifacts"} else (tmp_path / "artifacts" / Path(path)),
    )

    html_text = _render_dashboard(
        tmp_path,
        current_view="overview",
        candidate_index=0,
        current_report="research",
        base_path="/stock",
    )

    summary_contract_index = html_text.index('data-display-contract="summary"')
    primary_contract_index = html_text.index('data-display-contract="primary-result"')
    prefilter_contract_index = html_text.index('data-display-contract="prefilter"')
    assert summary_contract_index < primary_contract_index < prefilter_contract_index
    assert html_text.count('class="display-contract') >= 3
    assert html_text.count("<span>状态</span>") >= 3
    assert html_text.count("<span>证据</span>") >= 3
    assert html_text.count("<span>边界</span>") >= 3
    assert html_text.count("<span>下一步</span>") >= 3
    assert "先确认行动锁和数据门，再讨论样本闭合" in html_text
    assert "不替代主结论，不触发样本闭合、不写 ledger" in html_text


def test_dashboard_secondary_blocks_share_external_display_contract(tmp_path, monkeypatch):
    exp_dir, rep_dir = _prepare_dashboard_artifacts(tmp_path)
    monkeypatch.setattr(
        dashboard_context,
        "resolve_experiments_path",
        lambda path=None: exp_dir if path in {None, ".", "", "data/experiments"} else exp_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_reports_path",
        lambda path=None: rep_dir if path in {None, ".", "", "data/reports"} else rep_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_artifacts_path",
        lambda path=None: (tmp_path / "artifacts") if path in {None, ".", "", "artifacts"} else (tmp_path / "artifacts" / Path(path)),
    )
    import run_dashboard as dashboard_module

    monkeypatch.setattr(
        dashboard_module,
        "resolve_experiments_path",
        lambda path=None: exp_dir if path in {None, ".", "", "data/experiments"} else exp_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_module,
        "resolve_artifacts_path",
        lambda path=None: (tmp_path / "artifacts") if path in {None, ".", "", "artifacts"} else (tmp_path / "artifacts" / Path(path)),
    )

    html_text = _render_dashboard(
        tmp_path,
        current_view="overview",
        candidate_index=0,
        current_report="research",
        base_path="/stock",
    )

    assert 'data-display-contract="opportunities"' in html_text
    assert 'data-display-contract="selection-funnel"' in html_text
    assert 'data-display-contract="diagnosis"' in html_text
    assert "候选卡只用于下钻顺序，不代表可交易或可闭合样本" in html_text
    assert "解释筛选收缩，不替代主结果和证据驾驶舱" in html_text
    assert "诊断只评估策略状态，不解除样本等待和行动锁" in html_text
    assert "等待真实数据门后再复核闭合证据" in html_text


def test_dashboard_overview_collapses_internal_noise_by_default(tmp_path, monkeypatch):
    exp_dir, rep_dir = _prepare_dashboard_artifacts(tmp_path)
    monkeypatch.setattr(
        dashboard_context,
        "resolve_experiments_path",
        lambda path=None: exp_dir if path in {None, ".", "", "data/experiments"} else exp_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_reports_path",
        lambda path=None: rep_dir if path in {None, ".", "", "data/reports"} else rep_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_artifacts_path",
        lambda path=None: (tmp_path / "artifacts") if path in {None, ".", "", "artifacts"} else (tmp_path / "artifacts" / Path(path)),
    )
    import run_dashboard as dashboard_module

    monkeypatch.setattr(
        dashboard_module,
        "resolve_experiments_path",
        lambda path=None: exp_dir if path in {None, ".", "", "data/experiments"} else exp_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_module,
        "resolve_artifacts_path",
        lambda path=None: (tmp_path / "artifacts") if path in {None, ".", "", "artifacts"} else (tmp_path / "artifacts" / Path(path)),
    )

    html_text = _render_dashboard(
        tmp_path,
        current_view="overview",
        candidate_index=0,
        current_report="research",
        base_path="/stock",
    )

    assert 'id="overview-visuals-disclosure"' in html_text
    assert 'id="overview-operations-disclosure"' in html_text
    assert "关键研究图形已收起" in html_text
    assert "运维控制台已收起" in html_text
    visuals_summary_index = html_text.index('id="overview-visuals-disclosure"')
    visuals_card_index = html_text.index('id="overview-visuals"')
    operations_summary_index = html_text.index('id="overview-operations-disclosure"')
    operations_card_index = html_text.index('id="ops"')
    assert visuals_summary_index < visuals_card_index
    assert operations_summary_index < operations_card_index


def test_dashboard_overview_first_screen_is_visually_converged(tmp_path, monkeypatch):
    exp_dir, rep_dir = _prepare_dashboard_artifacts(tmp_path)
    monkeypatch.setattr(
        dashboard_context,
        "resolve_experiments_path",
        lambda path=None: exp_dir if path in {None, ".", "", "data/experiments"} else exp_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_reports_path",
        lambda path=None: rep_dir if path in {None, ".", "", "data/reports"} else rep_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_artifacts_path",
        lambda path=None: (tmp_path / "artifacts") if path in {None, ".", "", "artifacts"} else (tmp_path / "artifacts" / Path(path)),
    )
    import run_dashboard as dashboard_module

    monkeypatch.setattr(
        dashboard_module,
        "resolve_experiments_path",
        lambda path=None: exp_dir if path in {None, ".", "", "data/experiments"} else exp_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_module,
        "resolve_artifacts_path",
        lambda path=None: (tmp_path / "artifacts") if path in {None, ".", "", "artifacts"} else (tmp_path / "artifacts" / Path(path)),
    )

    html_text = _render_dashboard(
        tmp_path,
        current_view="overview",
        candidate_index=0,
        current_report="research",
        base_path="/stock",
    )

    assert 'id="external-decision-spine"' in html_text
    assert 'id="external-system-summary"' in html_text
    assert "运行、候选、预筛、治理指标已下沉" in html_text
    assert "主结果证据" in html_text
    assert "候选篮证据" in html_text
    assert "晋级锁定" in html_text
    assert 'class="view-banner"' not in html_text
    assert 'class="headline-bar"' not in html_text
    assert 'class="today-brief"' not in html_text
    assert 'class="hero"' not in html_text
    assert 'class="command-deck"' not in html_text
    assert ".container { order: 1; }" in html_text
    assert ".sidebar { order: 2;" in html_text
    assert ".topbar-title span { display: none; }" in html_text


def test_dashboard_prefilter_is_external_evidence_not_primary_decision(tmp_path, monkeypatch):
    exp_dir, rep_dir = _prepare_dashboard_artifacts(tmp_path)
    _write_json(
        exp_dir / "candidate_prefilter_universe_latest.json",
        {
            "generated_at": "2026-01-30 10:34:00",
            "trade_date": "20260130",
            "row_count": 2,
            "market_symbol_count": 2,
            "excluded_count": 0,
            "top_candidates": [
                {
                    "ts_code": "UPTREND.SZ",
                    "prefilter_score": 0.7802422637119506,
                    "prefilter_reason": "流动性质量高，趋势结构较强，量能稳定",
                },
                {
                    "ts_code": "DOWNTREND.SZ",
                    "prefilter_score": 0.596424009447636,
                    "prefilter_reason": "流动性质量高，量能稳定",
                },
            ],
        },
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_experiments_path",
        lambda path=None: exp_dir if path in {None, ".", "", "data/experiments"} else exp_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_reports_path",
        lambda path=None: rep_dir if path in {None, ".", "", "data/reports"} else rep_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_context,
        "resolve_artifacts_path",
        lambda path=None: (tmp_path / "artifacts") if path in {None, ".", "", "artifacts"} else (tmp_path / "artifacts" / Path(path)),
    )
    import run_dashboard as dashboard_module

    monkeypatch.setattr(
        dashboard_module,
        "resolve_experiments_path",
        lambda path=None: exp_dir if path in {None, ".", "", "data/experiments"} else exp_dir / Path(path),
    )
    monkeypatch.setattr(
        dashboard_module,
        "resolve_artifacts_path",
        lambda path=None: (tmp_path / "artifacts") if path in {None, ".", "", "artifacts"} else (tmp_path / "artifacts" / Path(path)),
    )

    html_text = _render_dashboard(
        tmp_path,
        current_view="overview",
        candidate_index=0,
        current_report="research",
        base_path="/stock",
    )

    cockpit_index = html_text.index('id="first-place-evidence-cockpit"')
    prefilter_index = html_text.index('id="prefilter"')
    assert cockpit_index < prefilter_index
    assert "专家证据区" in html_text
    assert "对外结论以第一屏受控等待驾驶舱为准" in html_text
    assert "本区只解释全市场预筛过程，不触发样本闭合、不写 ledger" in html_text
    assert "诊断样本，不作为对外结论" in html_text
    assert "诊断样本，非正式A股代码" in html_text
    assert "78.0" in html_text
    assert "0.7802422637119506" not in html_text
