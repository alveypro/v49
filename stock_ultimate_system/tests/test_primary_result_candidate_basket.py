import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.primary_result_candidate_basket import (
    PrimaryResultCandidateBasketRegistry,
    build_primary_result_candidate_basket_snapshot,
)


def _write_candidates(path: Path, *, concentrated: bool = False, high_risk: bool = False) -> Path:
    risk = "high" if high_risk else "medium"
    weight_a = 0.7 if concentrated else 0.34
    weight_b = 0.2 if concentrated else 0.33
    weight_c = 0.1 if concentrated else 0.33
    second_industry = "银行" if concentrated else "医药"
    path.write_text(
        "rank,ts_code,stock_name,industry,signal,risk_level,final_score,portfolio_weight_after_risk,basket_role,basket_risk_flag\n"
        f"1,000001.SZ,平安银行,银行,strong_buy,{risk},155,{weight_a},core,ok\n"
        f"2,600036.SH,招商银行,{second_industry},buy,medium,150,{weight_b},satellite,ok\n"
        f"3,300383.SZ,光环新网,通信,buy,medium,145,{weight_c},satellite,ok\n",
        encoding="utf-8",
    )
    return path


def test_primary_result_candidate_basket_snapshot_approves_weighted_basket(tmp_path):
    candidates = _write_candidates(tmp_path / "candidates.csv")
    summary = tmp_path / "summary.json"
    summary.write_text('{"expected_basket_return": 0.03}', encoding="utf-8")
    output = tmp_path / "basket.json"

    exit_code, payload = build_primary_result_candidate_basket_snapshot(
        candidates_csv_path=candidates,
        summary_json_path=summary,
        basket_id="basket-001",
        top_n=3,
        output_path=output,
    )

    assert exit_code == 0
    assert payload["basket_version"] == "primary_result_candidate_basket.v1"
    assert payload["status"] == "approved"
    assert payload["source_candidates_csv_hash"]
    assert payload["risk_budget"]["weight_sum"] == 1.0
    assert payload["risk_budget"]["max_single_weight"] <= 0.35
    assert len(payload["items"]) == 3
    assert output.exists()


def test_primary_result_candidate_basket_snapshot_marks_conditional_for_industry_over_target_band(tmp_path):
    candidates = tmp_path / "candidates.csv"
    candidates.write_text(
        "rank,ts_code,stock_name,industry,signal,risk_level,final_score,portfolio_weight_after_risk,basket_role,basket_risk_flag\n"
        "1,000001.SZ,平安银行,银行,strong_buy,medium,155,0.34,core,ok\n"
        "2,600036.SH,招商银行,银行,buy,medium,150,0.18,satellite,ok\n"
        "3,300383.SZ,光环新网,通信,buy,medium,145,0.24,satellite,ok\n"
        "4,600050.SH,中国联通,通信,buy,medium,140,0.24,satellite,ok\n",
        encoding="utf-8",
    )

    exit_code, payload = build_primary_result_candidate_basket_snapshot(
        candidates_csv_path=candidates,
        basket_id="basket-conditional",
        top_n=4,
    )

    assert exit_code == 0
    assert payload["status"] == "conditional"
    assert payload["blocking_reasons"] == []
    assert "top industry weight exceeds target operating band" in payload["conditional_reasons"]
    assert payload["policy"]["target_max_industry_weight"] == 0.5
    assert payload["policy"]["max_industry_weight"] == 0.65


def test_primary_result_candidate_basket_blocks_concentrated_weight(tmp_path):
    candidates = _write_candidates(tmp_path / "candidates.csv", concentrated=True)

    exit_code, payload = build_primary_result_candidate_basket_snapshot(
        candidates_csv_path=candidates,
        basket_id="basket-concentrated",
        top_n=3,
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert "single-name weight must stay within policy" in payload["blocking_reasons"]
    assert "top industry weight must stay within hard policy limit" in payload["blocking_reasons"]


def test_primary_result_candidate_basket_blocks_missing_identity_fields(tmp_path):
    candidates = tmp_path / "candidates.csv"
    candidates.write_text(
        "rank,ts_code,stock_name,industry,signal,risk_level,final_score,portfolio_weight_after_risk,basket_role,basket_risk_flag\n"
        "1,000001.SH,,,buy,medium,106,1.0,core,ok\n",
        encoding="utf-8",
    )

    exit_code, payload = build_primary_result_candidate_basket_snapshot(
        candidates_csv_path=candidates,
        basket_id="basket-missing-identity",
        top_n=1,
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert "candidate basket items must include stock name and industry" in payload["blocking_reasons"]
    assert payload["identity_quality"]["missing_name_codes"] == ["000001.SH"]


def test_primary_result_candidate_basket_blocks_degraded_generation_for_formal_release(tmp_path):
    candidates = _write_candidates(tmp_path / "candidates.csv")
    summary = tmp_path / "summary.json"
    summary.write_text(
        '{"generation_degraded":true,"generation_reason":"batch_prediction_timeout(12s)","guardrail_mode":"defensive"}',
        encoding="utf-8",
    )

    exit_code, payload = build_primary_result_candidate_basket_snapshot(
        candidates_csv_path=candidates,
        summary_json_path=summary,
        basket_id="basket-degraded",
        top_n=3,
        formal_release=True,
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert "candidate generation must complete without degradation" in payload["blocking_reasons"]
    assert "candidate basket guardrail mode must not be defensive for formal release" in payload["blocking_reasons"]


def test_primary_result_candidate_basket_blocks_thin_validation_sample_for_formal_release(tmp_path):
    candidates = _write_candidates(tmp_path / "candidates.csv")
    summary = tmp_path / "summary.json"
    summary.write_text('{"generation_degraded":false,"guardrail_mode":"normal"}', encoding="utf-8")
    validation = tmp_path / "validation.json"
    validation.write_text('{"summary":{"rebalance_dates":2}}', encoding="utf-8")

    exit_code, payload = build_primary_result_candidate_basket_snapshot(
        candidates_csv_path=candidates,
        summary_json_path=summary,
        validation_json_path=validation,
        basket_id="basket-thin-validation",
        top_n=3,
        formal_release=True,
        min_validation_rebalance_dates=20,
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert "candidate basket validation sample count must meet formal release threshold" in payload["blocking_reasons"]
    assert payload["validation_quality"]["rebalance_dates"] == 2


def test_candidate_basket_registry_preserves_history_and_switches_current(tmp_path):
    registry = PrimaryResultCandidateBasketRegistry(baskets_dir=tmp_path / "baskets")
    first = {
        "basket_version": "primary_result_candidate_basket.v1",
        "basket_id": "basket-001",
        "status": "approved",
    }
    second = {
        "basket_version": "primary_result_candidate_basket.v1",
        "basket_id": "basket-002",
        "status": "approved",
    }

    first_pointer = registry.register_snapshot(first)
    second_pointer = registry.register_snapshot(second)

    assert first_pointer["basket_id"] == "basket-001"
    assert second_pointer["basket_id"] == "basket-002"
    assert registry.current()["basket_id"] == "basket-002"
    assert (tmp_path / "baskets" / "history" / "basket-001.json").exists()
    assert (tmp_path / "baskets" / "history" / "basket-002.json").exists()
    with pytest.raises(FileExistsError):
        registry.register_snapshot(first)

    rollback_pointer = registry.rollback_current("basket-001")
    assert rollback_pointer["basket_id"] == "basket-001"
    assert rollback_pointer["rollback"] is True
    assert registry.current()["basket_id"] == "basket-001"


def test_register_primary_result_candidate_basket_cli(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script = project_root / "scripts" / "register_primary_result_candidate_basket.py"
    candidates = _write_candidates(tmp_path / "candidates.csv")
    baskets_dir = tmp_path / "baskets"

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--candidates-csv",
            str(candidates),
            "--summary-json",
            str(tmp_path / "missing_summary.json"),
            "--baskets-dir",
            str(baskets_dir),
            "--basket-id",
            "basket-cli",
            "--top-n",
            "3",
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "approved"
    assert payload["registered"] is True
    assert json.loads((baskets_dir / "current.json").read_text(encoding="utf-8"))["basket_id"] == "basket-cli"


def test_register_primary_result_candidate_basket_cli_defaults_to_basket_summary_path(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script = project_root / "scripts" / "register_primary_result_candidate_basket.py"
    candidates = _write_candidates(tmp_path / "candidates.csv")
    baskets_dir = tmp_path / "baskets"

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--candidates-csv",
            str(candidates),
            "--baskets-dir",
            str(baskets_dir),
            "--basket-id",
            "basket-default-summary",
            "--top-n",
            "3",
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "approved"
    assert payload["snapshot"]["source_summary_json_path"].endswith("data/experiments/candidates_basket_summary_latest.json")


def test_candidate_basket_registry_registers_conditional_snapshot(tmp_path):
    registry = PrimaryResultCandidateBasketRegistry(baskets_dir=tmp_path / "baskets")
    snapshot = {
        "basket_version": "primary_result_candidate_basket.v1",
        "basket_id": "basket-conditional",
        "status": "conditional",
    }

    pointer = registry.register_snapshot(snapshot)

    assert pointer["basket_id"] == "basket-conditional"
    assert pointer["status"] == "conditional"
    assert registry.current()["status"] == "conditional"
