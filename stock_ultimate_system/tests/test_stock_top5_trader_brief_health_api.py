import json
from pathlib import Path

from src.stock_top5_trader_brief_health_api import build_top5_trader_brief_health_body


def test_build_top5_trader_brief_health_body_returns_stable_contract(tmp_path: Path):
    root = tmp_path / "stock_ultimate_system"
    root.mkdir()

    payload = json.loads(build_top5_trader_brief_health_body(root).decode("utf-8"))

    assert payload["contract_version"] == "top5_trader_brief_health.v1"
    assert payload["generator"] == "stock_ultimate_system.stock_top5_trader_brief_health_api"
    assert payload["manifest_found"] is False
    assert payload["reference_kind"] == "missing_manifest"
    assert payload["resolved_exports_dir"] == str((root / "exports").resolve())
