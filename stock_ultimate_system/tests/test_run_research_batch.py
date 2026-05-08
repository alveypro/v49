import json
import sys
from pathlib import Path

import run_research_batch as batch


def test_research_batch_supports_skip_flags(monkeypatch, tmp_path):
    project_root = tmp_path / "project"
    config_dir = project_root / "config" / "server"
    out_path = project_root / "data" / "experiments" / "research_batch_latest.json"
    config_dir.mkdir(parents=True, exist_ok=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    (config_dir / "settings.yaml").write_text("data: {}\n", encoding="utf-8")

    def fake_resolve_project_path(value="."):
        if str(value) == "config/server":
            return config_dir
        if str(value) == str(out_path):
            return out_path
        return project_root

    monkeypatch.setattr(batch, "resolve_project_path", fake_resolve_project_path)
    monkeypatch.setattr(
        batch,
        "resolve_research_pool_with_meta",
        lambda config_dir, size_override=None: (
            ["000001.SZ", "600036.SH"],
            {"liquidity_min_turnover": 1_000_000, "liquidity_filtered_out": 3},
        ),
    )

    calls: list[list[str]] = []

    def fake_run_step(cmd: list[str], cwd: Path) -> dict[str, object]:
        calls.append(cmd)
        return {
            "cmd": cmd,
            "started_at": "2026-03-22T00:00:00",
            "ended_at": "2026-03-22T00:00:01",
            "returncode": 0,
            "stdout_tail": "",
            "stderr_tail": "",
        }

    monkeypatch.setattr(batch, "_run_step", fake_run_step)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_research_batch.py",
            "--config-dir",
            "config/server",
            "--skip-candidates",
            "--skip-daily-research",
            "--out",
            str(out_path),
        ],
    )

    batch.main()

    assert len(calls) == 1
    assert calls[0][1].endswith("run_grid_backtest.py")
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["skip_candidates"] is True
    assert payload["skip_daily_research"] is True
    assert payload["skip_grid_backtest"] is False
    assert payload["research_pool_meta"]["liquidity_filtered_out"] == 3
    assert list(payload["steps"].keys()) == ["grid_backtest"]
