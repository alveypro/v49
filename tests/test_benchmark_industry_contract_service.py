from __future__ import annotations

from openclaw.services.benchmark_industry_contract_service import build_benchmark_industry_contract


def test_benchmark_industry_contract_builds_deterministic_hashes():
    contract = build_benchmark_industry_contract(
        benchmark_trade_date="20260502",
        source="external_index_contract",
        provider_batch_id="batch_20260502",
        provider_snapshot_id="snapshot_20260502",
        approved_by="independent_reviewer",
        approved_at="2026-05-02 09:30:00",
        approval_signature="sig:benchmark:20260502",
        industry_weights={"bank": 40.0, "tech": 60.0},
        constituents=[
            {"ts_code": "000002.SZ", "industry": "tech", "weight": 0.6},
            {"ts_code": "000001.SZ", "industry": "bank", "weight": 0.4},
        ],
    )

    assert contract["artifact_version"] == "benchmark_industry_contract.v1"
    assert contract["industry_weights"] == {"bank": 0.4, "tech": 0.6}
    assert contract["constituents"][0]["ts_code"] == "000001.SZ"
    assert contract["provider_batch_id"] == "batch_20260502"
    assert contract["provider_snapshot_id"] == "snapshot_20260502"
    assert contract["approval_signature"] == "sig:benchmark:20260502"
    assert contract["approval_signature_algo"] == "sha256_secret_v1"
    assert contract["approval_key_id"] == "benchmark_default_key"
    assert contract["industry_weights_hash"]
    assert contract["constituent_hash"]
    assert contract["contract_hash"]


def test_benchmark_industry_contract_filters_empty_fields():
    contract = build_benchmark_industry_contract(
        benchmark_trade_date="20260502",
        source="external_index_contract",
        industry_weights={"": 10.0, "bank": 0.0},
        constituents=[{"ts_code": "", "industry": "bank", "weight": 0.1}],
    )

    assert contract["industry_weights"] == {}
    assert contract["constituents"] == []
