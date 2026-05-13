#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从最新 strategy_competition_portfolio_audit_*.json 重建 Top5 交易员清单（md/csv/manifest）。

设计意图（可与投研/资管运维对齐）：
- 单一入口：调度器、CI、手工剧本只调用本脚本，不直接 import Streamlit。
- 可观测：写入 exports/top5_trader_brief_last_rebuild.json 供监控与对账。
- 无 UI：导入前设置 V49_HEADLESS=1，避免 st.set_page_config 等非会话调用。

环境：须在仓库根目录执行；依赖与 v49_app 一致（数据库、策略包等）。
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _write_rebuild_record(*, ok: bool, message: str) -> None:
    manifest_path = ROOT / "exports" / "top5_trader_brief_latest_manifest.json"
    record: dict = {
        "ok": ok,
        "message": (message or "")[:8000],
        "finished_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "entry": "tools/rebuild_top5_trader_brief_exports.py",
    }
    if manifest_path.is_file():
        try:
            man = json.loads(manifest_path.read_text(encoding="utf-8"))
            if isinstance(man, dict):
                record["artifact_sha256"] = man.get("artifact_sha256")
                record["artifact_path"] = man.get("artifact_path")
                record["trade_date_compact"] = man.get("trade_date_compact")
                record["competition_run_id"] = man.get("competition_run_id")
        except Exception:
            record["manifest_read_error"] = True
    out_path = ROOT / "exports" / "top5_trader_brief_last_rebuild.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    os.environ.setdefault("V49_HEADLESS", "1")
    sys.path.insert(0, str(ROOT))

    try:
        from openclaw.services.top5_trader_brief_rebuild_service import (  # noqa: E402
            rebuild_top5_trader_brief_exports,
        )
    except Exception as exc:
        err = f"无法加载 Top5 无头重建服务: {exc}"
        print(err, file=sys.stderr)
        _write_rebuild_record(ok=False, message=err)
        return 1

    ok, msg = rebuild_top5_trader_brief_exports(repo_root=ROOT)
    _write_rebuild_record(ok=ok, message=msg)
    print(msg)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
