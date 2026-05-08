from pathlib import Path

import pandas as pd

from src.candidate_quality.lineage import (
    build_candidate_lineage,
    validate_candidate_lineage,
    write_candidate_lineage,
)


def test_candidate_lineage_binds_run_data_sources_and_quality(tmp_path):
    candidate_csv = tmp_path / "candidates_top_latest.csv"
    candidate_md = tmp_path / "candidates_top_latest.md"
    summary = tmp_path / "candidates_basket_summary_latest.json"
    validation = tmp_path / "candidates_basket_validation_latest.json"
    audit = tmp_path / "candidates_audit_latest.json"
    gate = tmp_path / "candidate_data_quality_gate_latest.json"
    report = tmp_path / "data_quality_report_latest.json"
    for path in [candidate_csv, candidate_md, summary, validation, audit, gate, report]:
        path.write_text("{}", encoding="utf-8")

    frame = pd.DataFrame(
        [
            {
                "rank": 1,
                "ts_code": "000001.SZ",
                "stock_name": "平安银行",
                "industry": "银行",
                "data_quality_level": "pass",
                "data_quality_score": 98.0,
            }
        ]
    )

    lineage = build_candidate_lineage(
        candidate_frame=frame,
        run_id="candidate-run-001",
        output_paths={
            "latest_csv": str(candidate_csv),
            "latest_md": str(candidate_md),
            "latest_summary": str(summary),
            "latest_validation": str(validation),
            "latest_audit": str(audit),
            "latest_data_quality_gate": str(gate),
            "data_quality_report": str(report),
        },
        data_quality_report={"schema_version": "candidate_data_quality_report.v1", "expected_latest_trade_date": "20260506"},
        data_quality_gate={
            "status": "passed",
            "candidates": [
                {
                    "ts_code": "000001.SZ",
                    "quality_level": "pass",
                    "quality_score": 98.0,
                    "blocking_reasons": [],
                }
            ],
        },
        generation_meta={"champion_version": "champion-v1"},
        guardrail={"mode": "normal", "reasons": []},
        validation_result={"summary": {"rebalance_dates": 3}},
    )

    assert lineage["status"] == "passed"
    assert lineage["run_id"] == "candidate-run-001"
    assert lineage["data_as_of"] == "20260506"
    assert lineage["model_version"] == "champion-v1"
    assert lineage["candidates"][0]["lineage_hash"]
    assert lineage["candidates"][0]["input_files"]

    path = write_candidate_lineage(lineage, output_dir=tmp_path)
    assert Path(path).exists()


def test_candidate_lineage_validation_blocks_missing_required_fields():
    result = validate_candidate_lineage(
        {
            "run_id": "",
            "data_as_of": "",
            "source_files": [],
            "candidates": [{"ts_code": "000001.SZ", "run_id": "", "data_as_of": "", "input_files": []}],
        }
    )

    assert result["status"] == "failed"
    assert "missing_run_id" in result["blocking_reasons"]
    assert "missing_data_as_of" in result["blocking_reasons"]
    assert "missing_source_files" in result["blocking_reasons"]
