from __future__ import annotations

import json
import sys
from pathlib import Path


def ensure_repo_on_sys_path_for_openclaw(*, dashboard_file: Path | None = None) -> Path:
    """The stock dashboard runs inside stock_ultimate_system/; openclaw/ is the repo parent."""
    anchor = Path(dashboard_file).resolve() if dashboard_file else Path(__file__).resolve()
    repo_root = anchor.parent.parent if anchor.name == "run_dashboard.py" else anchor.parents[2]
    root_str = str(repo_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    return repo_root


def build_top5_trader_brief_health_body(root_dir: Path, *, dashboard_file: Path | None = None) -> bytes:
    ensure_repo_on_sys_path_for_openclaw(dashboard_file=dashboard_file)
    try:
        from openclaw.services.top5_brief_manifest_freshness_service import (
            build_top5_manifest_health_payload,
            default_exports_dir_for_monorepo,
        )
    except Exception as exc:
        err_payload = {
            "contract_version": "top5_trader_brief_health.v1",
            "openclaw_import_error": repr(exc),
            "message_zh": "无法加载 openclaw 清单健康模块；请确认从仓库根运行或 PYTHONPATH 包含 Airivo 根目录。",
        }
        return json.dumps(err_payload, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")

    resolved_exports = default_exports_dir_for_monorepo(dashboard_root=Path(root_dir))
    payload = build_top5_manifest_health_payload(exports_dir=resolved_exports)
    payload["resolved_exports_dir"] = str(Path(resolved_exports).resolve())
    payload["generator"] = "stock_ultimate_system.stock_top5_trader_brief_health_api"
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
