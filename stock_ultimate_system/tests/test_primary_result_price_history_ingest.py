import json
import subprocess
import sys
from pathlib import Path

from src.primary_result_price_history_ingest import import_primary_result_price_history


def _write_source_csv(path: Path, *, insufficient: bool = False) -> Path:
    rows = [
        "ts_code,trade_date,close",
        "300757.SZ,2026-04-15,100.00",
        "BENCHMARK,2026-04-15,1000.00",
        "000001.SZ,2026-04-15,10.00",
    ]
    if not insufficient:
        rows.extend(
            [
                "300757.SZ,2026-04-16,106.00",
                "BENCHMARK,2026-04-16,1010.00",
                "300757.SZ,2026-04-20,108.00",
                "BENCHMARK,2026-04-20,1020.00",
            ]
        )
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


def test_primary_result_price_history_ingest_writes_canonical_csv_and_manifest(tmp_path):
    source_csv = _write_source_csv(tmp_path / "source.csv")
    output_csv = tmp_path / "primary_result_price_history_latest.csv"
    manifest = tmp_path / "primary_result_price_history_ingest_latest.json"

    exit_code, payload = import_primary_result_price_history(
        source_csv_path=source_csv,
        output_csv_path=output_csv,
        manifest_output_path=manifest,
        ts_code="300757.SZ",
        benchmark_ts_code="BENCHMARK",
        window_start="2026-04-15T10:05:00Z",
        window_end="2026-04-16T15:00:00Z",
        source_label="manual_local_csv",
    )

    assert exit_code == 0
    assert payload["ingest_version"] == "primary_result_price_history_ingest.v1"
    assert payload["status"] == "imported"
    assert payload["source_csv_hash"]
    assert payload["output_csv_hash"]
    assert payload["row_counts"] == {"output_total": 4, "observed": 2, "benchmark": 2}
    assert output_csv.exists()
    assert manifest.exists()
    assert "000001.SZ" not in output_csv.read_text(encoding="utf-8")


def test_primary_result_price_history_ingest_blocks_insufficient_window_rows(tmp_path):
    source_csv = _write_source_csv(tmp_path / "source.csv", insufficient=True)
    output_csv = tmp_path / "primary_result_price_history_latest.csv"

    exit_code, payload = import_primary_result_price_history(
        source_csv_path=source_csv,
        output_csv_path=output_csv,
        ts_code="300757.SZ",
        benchmark_ts_code="BENCHMARK",
        window_start="2026-04-15T10:05:00Z",
        window_end="2026-04-16T15:00:00Z",
        source_label="manual_local_csv",
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert "observed ts_code must have at least two rows in window" in payload["blocking_reasons"]
    assert not output_csv.exists()


def test_import_primary_result_price_history_cli(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "import_primary_result_price_history.py"
    source_csv = _write_source_csv(tmp_path / "source.csv")
    output_csv = tmp_path / "primary_result_price_history_latest.csv"
    manifest = tmp_path / "primary_result_price_history_ingest_latest.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--source-csv",
            str(source_csv),
            "--output-csv",
            str(output_csv),
            "--manifest-output",
            str(manifest),
            "--ts-code",
            "300757.SZ",
            "--benchmark-ts-code",
            "BENCHMARK",
            "--window-start",
            "2026-04-15T10:05:00Z",
            "--window-end",
            "2026-04-16T15:00:00Z",
            "--source-label",
            "manual_local_csv",
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "imported"
    assert json.loads(manifest.read_text(encoding="utf-8"))["status"] == "imported"
