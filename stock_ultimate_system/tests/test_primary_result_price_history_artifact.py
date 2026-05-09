import json
import subprocess
import sys
from pathlib import Path

from src.primary_result_price_history_artifact import build_primary_result_price_history_artifact


def _write_price_history(path: Path, *, missing_close: bool = False, insufficient: bool = False) -> Path:
    if missing_close:
        path.write_text(
            "ts_code,trade_date\n"
            "000001.SZ,2026-04-15\n"
            "BENCHMARK,2026-04-15\n",
            encoding="utf-8",
        )
        return path
    rows = [
        "ts_code,trade_date,close",
        "000001.SZ,2026-04-15,10.00",
        "BENCHMARK,2026-04-15,100.00",
    ]
    if not insufficient:
        rows.extend(
            [
                "000001.SZ,2026-04-20,11.00",
                "BENCHMARK,2026-04-20,102.00",
            ]
        )
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


def test_primary_result_price_history_artifact_validates_local_csv(tmp_path):
    price_history = _write_price_history(tmp_path / "price_history.csv")

    exit_code, payload = build_primary_result_price_history_artifact(
        price_history_path=price_history,
        ts_code="000001.SZ",
        benchmark_ts_code="BENCHMARK",
        window_start="2026-04-15T09:30:00Z",
        window_end="2026-04-20T15:00:00Z",
    )

    assert exit_code == 0
    assert payload["artifact_version"] == "primary_result_price_history_artifact.v1"
    assert payload["status"] == "valid"
    assert payload["source_price_history_hash"]
    assert payload["row_counts"]["observed"] == 2
    assert payload["row_counts"]["benchmark"] == 2
    assert payload["metrics"]["observed_return"] == 0.1
    assert payload["blocking_reasons"] == []


def test_primary_result_price_history_artifact_rejects_missing_columns(tmp_path):
    price_history = _write_price_history(tmp_path / "price_history.csv", missing_close=True)

    exit_code, payload = build_primary_result_price_history_artifact(
        price_history_path=price_history,
        ts_code="000001.SZ",
        benchmark_ts_code="BENCHMARK",
        window_start="2026-04-15T09:30:00Z",
        window_end="2026-04-20T15:00:00Z",
    )

    assert exit_code == 1
    assert payload["status"] == "invalid"
    missing_columns = next(check for check in payload["checks"] if check["name"] == "required_columns_present")
    assert missing_columns["details"]["missing_columns"] == ["close"]


def test_primary_result_price_history_artifact_rejects_insufficient_window_points(tmp_path):
    price_history = _write_price_history(tmp_path / "price_history.csv", insufficient=True)

    exit_code, payload = build_primary_result_price_history_artifact(
        price_history_path=price_history,
        ts_code="000001.SZ",
        benchmark_ts_code="BENCHMARK",
        window_start="2026-04-15T09:30:00Z",
        window_end="2026-04-20T15:00:00Z",
    )

    assert exit_code == 1
    assert payload["status"] == "invalid"
    assert any("observed price history" in reason for reason in payload["blocking_reasons"])


def test_validate_primary_result_price_history_cli_writes_manifest(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "validate_primary_result_price_history.py"
    price_history = _write_price_history(tmp_path / "price_history.csv")
    output_path = tmp_path / "price_history_manifest.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--price-history-csv",
            str(price_history),
            "--ts-code",
            "000001.SZ",
            "--benchmark-ts-code",
            "BENCHMARK",
            "--window-start",
            "2026-04-15T09:30:00Z",
            "--window-end",
            "2026-04-20T15:00:00Z",
            "--output",
            str(output_path),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "valid"
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "valid"
