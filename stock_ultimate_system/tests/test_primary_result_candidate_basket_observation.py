import json
import subprocess
import sys
from pathlib import Path

import pytest

from src.primary_result_candidate_basket_observation import (
    PrimaryResultCandidateBasketPerformanceLedger,
    build_primary_result_candidate_basket_observation,
)


def _write_basket(path: Path) -> Path:
    payload = {
        "basket_version": "primary_result_candidate_basket.v1",
        "basket_id": "basket-001",
        "status": "approved",
        "items": [
            {"ts_code": "000001.SZ", "stock_name": "平安银行", "industry": "银行", "weight": 0.6},
            {"ts_code": "300383.SZ", "stock_name": "光环新网", "industry": "通信", "weight": 0.4},
        ],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_price_history(path: Path, *, missing_second: bool = False, failed: bool = False) -> Path:
    second_a = 9.8 if failed else 11.0
    rows = [
        "ts_code,trade_date,close",
        "000001.SZ,2026-04-15,10.0",
        "300383.SZ,2026-04-15,20.0",
        "BENCHMARK,2026-04-15,100.0",
    ]
    if not missing_second:
        rows.extend(
            [
                f"000001.SZ,2026-04-20,{second_a}",
                "300383.SZ,2026-04-20,22.0",
                "BENCHMARK,2026-04-20,101.0",
            ]
        )
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


def test_candidate_basket_observation_calculates_weighted_return_and_contributions(tmp_path):
    basket = _write_basket(tmp_path / "basket.json")
    price_history = _write_price_history(tmp_path / "price_history.csv")
    output = tmp_path / "observation.json"

    exit_code, payload = build_primary_result_candidate_basket_observation(
        basket_snapshot_path=basket,
        price_history_path=price_history,
        benchmark_ts_code="BENCHMARK",
        window_start="2026-04-15",
        window_end="2026-04-20",
        output_path=output,
    )

    assert exit_code == 0
    assert payload["observation_version"] == "primary_result_candidate_basket_observation.v1"
    assert payload["status"] == "completed"
    assert payload["metrics"]["basket_return"] == 0.1
    assert payload["metrics"]["benchmark_return"] == 0.01
    assert payload["metrics"]["excess_return"] == 0.09
    assert payload["industry_contributions"] == {"银行": 0.06, "通信": 0.04}
    assert output.exists()


def test_candidate_basket_observation_blocks_missing_price_points(tmp_path):
    basket = _write_basket(tmp_path / "basket.json")
    price_history = _write_price_history(tmp_path / "price_history.csv", missing_second=True)

    with pytest.raises(ValueError, match="at least two points"):
        build_primary_result_candidate_basket_observation(
            basket_snapshot_path=basket,
            price_history_path=price_history,
            benchmark_ts_code="BENCHMARK",
            window_start="2026-04-15",
            window_end="2026-04-20",
        )


def test_candidate_basket_observation_accepts_conditional_snapshot(tmp_path):
    basket = _write_basket(tmp_path / "basket.json")
    payload = json.loads(basket.read_text(encoding="utf-8"))
    payload["status"] = "conditional"
    basket.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    price_history = _write_price_history(tmp_path / "price_history.csv")

    exit_code, observation = build_primary_result_candidate_basket_observation(
        basket_snapshot_path=basket,
        price_history_path=price_history,
        benchmark_ts_code="BENCHMARK",
        window_start="2026-04-15",
        window_end="2026-04-20",
    )

    assert exit_code == 0
    assert observation["status"] == "completed"


def test_candidate_basket_performance_ledger_appends_observation_summary(tmp_path):
    basket = _write_basket(tmp_path / "basket.json")
    price_history = _write_price_history(tmp_path / "price_history.csv")
    observation = tmp_path / "observation.json"
    build_primary_result_candidate_basket_observation(
        basket_snapshot_path=basket,
        price_history_path=price_history,
        benchmark_ts_code="BENCHMARK",
        window_start="2026-04-15",
        window_end="2026-04-20",
        output_path=observation,
    )
    ledger = PrimaryResultCandidateBasketPerformanceLedger(
        ledger_path=tmp_path / "ledger.jsonl",
        summary_path=tmp_path / "summary.json",
    )

    entry = ledger.append_observation(observation_path=observation)

    assert entry["outcome"] == "success"
    assert entry["basket_return"] == 0.1
    summary = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert summary["entry_total"] == 1
    assert summary["average_excess_return"] == 0.09
    with pytest.raises(FileExistsError):
        ledger.append_observation(observation_path=observation)


def test_candidate_basket_observation_cli_registers_ledger(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script = project_root / "scripts" / "run_primary_result_candidate_basket_observation.py"
    basket = _write_basket(tmp_path / "basket.json")
    price_history = _write_price_history(tmp_path / "price_history.csv")

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--basket-snapshot",
            str(basket),
            "--price-history-csv",
            str(price_history),
            "--benchmark-ts-code",
            "BENCHMARK",
            "--window-start",
            "2026-04-15",
            "--window-end",
            "2026-04-20",
            "--output",
            str(tmp_path / "observation.json"),
            "--ledger-jsonl",
            str(tmp_path / "ledger.jsonl"),
            "--summary-json",
            str(tmp_path / "summary.json"),
            "--register-ledger",
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "completed"
    assert payload["ledger_registered"] is True
