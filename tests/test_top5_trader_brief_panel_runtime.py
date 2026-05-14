from pathlib import Path

from openclaw.runtime.top5_trader_brief_panel import latest_top5_trader_brief_exports


def test_latest_top5_trader_brief_exports_prefers_manifest(tmp_path: Path) -> None:
    exports_dir = tmp_path / "exports"
    exports_dir.mkdir()
    md_path = exports_dir / "top5_trader_brief_20260514_run.md"
    csv_path = exports_dir / "top5_trader_brief_20260514_run.csv"
    md_path.write_text("# brief\n", encoding="utf-8")
    csv_path.write_text("序号,股票代码\n1,000001.SZ\n", encoding="utf-8")
    (exports_dir / "top5_trader_brief_latest_manifest.json").write_text(
        (
            "{\n"
            f'  "markdown": "{md_path}",\n'
            f'  "csv": "{csv_path}"\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    assert latest_top5_trader_brief_exports(tmp_path) == {
        "markdown": str(md_path),
        "csv": str(csv_path),
    }


def test_v49_app_does_not_own_top5_rebuild_logic() -> None:
    app_text = Path("v49_app.py").read_text(encoding="utf-8")
    forbidden = [
        "def _ensure_top5_execution_sync_tables(",
        "def _compute_top5_advice_accuracy_payload(",
        "def rebuild_top5_trader_brief_from_latest_audit_artifact(",
        "TOP5_ADVICE_STATE_PATH",
        "TOP5_EXECUTION_UNIFIED_TABLE",
    ]
    for token in forbidden:
        assert token not in app_text
