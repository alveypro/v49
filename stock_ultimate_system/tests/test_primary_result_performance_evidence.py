import json
import subprocess
import sys
from pathlib import Path

from src.primary_result_performance_evidence import build_primary_result_performance_evidence


def _write_jsonl(path: Path, entries: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(entry, ensure_ascii=False) + "\n" for entry in entries), encoding="utf-8")


def _primary_entries(total: int, *, failed: bool = False) -> list[dict[str, object]]:
    entries = []
    for index in range(total):
        bad = failed and index >= total // 2
        entries.append(
            {
                "entry_id": f"primary-{index}",
                "recorded_at": f"2026-04-{index + 1:02d}T00:00:00Z",
                "result_id": f"primary:{index:06d}.SZ",
                "outcome": "failed" if bad else "success",
                "window_ended_at": f"2026-04-{index + 1:02d}",
                "observed_return": -0.02 if bad else 0.03,
                "benchmark_return": 0.01,
                "excess_return": -0.03 if bad else 0.02,
                "max_drawdown": -0.12 if bad else -0.03,
            }
        )
    return entries


def _basket_entries(total: int, *, failed: bool = False) -> list[dict[str, object]]:
    entries = []
    for index in range(total):
        bad = failed and index >= total // 2
        entries.append(
            {
                "entry_id": f"basket-{index}",
                "recorded_at": f"2026-05-{index + 1:02d}T00:00:00Z",
                "basket_id": "basket",
                "outcome": "failed" if bad else "success",
                "window_ended_at": f"2026-05-{index + 1:02d}",
                "basket_return": -0.01 if bad else 0.025,
                "benchmark_return": 0.005,
                "excess_return": -0.015 if bad else 0.02,
                "max_drawdown": -0.1 if bad else -0.02,
            }
        )
    return entries


def test_performance_evidence_accumulates_until_first_floor(tmp_path):
    primary = tmp_path / "primary.jsonl"
    basket = tmp_path / "basket.jsonl"
    _write_jsonl(primary, _primary_entries(3))
    _write_jsonl(basket, _basket_entries(1))

    exit_code, payload = build_primary_result_performance_evidence(
        primary_ledger_jsonl=primary,
        basket_ledger_jsonl=basket,
        output_path=tmp_path / "evidence.json",
    )

    assert exit_code == 0
    assert payload["evidence_version"] == "primary_result_performance_evidence.v1"
    assert payload["status"] == "accumulating"
    assert "primary_result needs 17 more ledger entries" in payload["next_actions"][0]
    assert (tmp_path / "evidence.json").exists()


def test_performance_evidence_ready_when_primary_and_basket_pass_first_floor(tmp_path):
    primary = tmp_path / "primary.jsonl"
    basket = tmp_path / "basket.jsonl"
    _write_jsonl(primary, _primary_entries(20))
    _write_jsonl(basket, _basket_entries(20))

    exit_code, payload = build_primary_result_performance_evidence(primary_ledger_jsonl=primary, basket_ledger_jsonl=basket)

    assert exit_code == 0
    assert payload["status"] == "ready"
    assert {stream["status"] for stream in payload["streams"]} == {"evidence_ready"}
    assert payload["streams"][0]["windows"][0]["status"] == "passed"


def test_performance_evidence_fails_when_first_floor_metrics_fail(tmp_path):
    primary = tmp_path / "primary.jsonl"
    basket = tmp_path / "basket.jsonl"
    _write_jsonl(primary, _primary_entries(20, failed=True))
    _write_jsonl(basket, _basket_entries(20))

    exit_code, payload = build_primary_result_performance_evidence(primary_ledger_jsonl=primary, basket_ledger_jsonl=basket)

    assert exit_code == 1
    assert payload["status"] == "failed"
    primary_stream = next(stream for stream in payload["streams"] if stream["stream_id"] == "primary_result")
    assert primary_stream["status"] == "evidence_failed"
    assert any("route to review" in action for action in payload["next_actions"])


def test_performance_evidence_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script = project_root / "scripts" / "build_primary_result_performance_evidence.py"
    primary = tmp_path / "primary.jsonl"
    basket = tmp_path / "basket.jsonl"
    _write_jsonl(primary, _primary_entries(20))
    _write_jsonl(basket, _basket_entries(20))

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--primary-ledger-jsonl",
            str(primary),
            "--basket-ledger-jsonl",
            str(basket),
            "--output",
            str(tmp_path / "evidence.json"),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "ready"
    assert json.loads((tmp_path / "evidence.json").read_text(encoding="utf-8"))["status"] == "ready"


def test_performance_evidence_rejects_pytest_derived_source_paths(tmp_path):
    primary = tmp_path / "primary.jsonl"
    basket = tmp_path / "basket.jsonl"
    polluted_entry = _primary_entries(1)[0]
    polluted_entry["source_observation_path"] = "/private/var/folders/x/pytest-of-mac/pytest-1/test_case/data/experiments/primary_result_observation_latest.json"
    _write_jsonl(primary, [polluted_entry])
    _write_jsonl(basket, _basket_entries(1))

    try:
        build_primary_result_performance_evidence(primary_ledger_jsonl=primary, basket_ledger_jsonl=basket)
    except ValueError as exc:
        assert "source_observation_path" in str(exc)
    else:
        raise AssertionError("expected ValueError for pytest-derived source path")
