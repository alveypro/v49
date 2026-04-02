#!/usr/bin/env python3

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.assistant.agent_mesh import AGENT_VERSION, count_agents
from openclaw.assistant.stock_qa import OpenClawStockAssistant


def _questions() -> List[str]:
    return [
        "你是谁，现在具备什么能力？",
        "你会哪些skills，能联网学习安装吗？",
        "600519现在是什么状态，给触发和失效条件",
        "v6今天风险如何，为什么是orange？",
        "给我Top20牛股候选并说明理由",
        "我的系统如何做回测参数优化？",
        "量化交易的哲学本质是什么？",
        "如果市场突发利空，仓位该怎么调整？",
        "从估值+基本面分析宁德时代",
        "总结一下你今天的判断并给本周动作",
    ]


def _check_round(idx: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    answer = str(payload.get("answer") or "")
    route = str(payload.get("route") or "")
    hits = payload.get("agent_hits") or []
    confidence = float(payload.get("confidence") or 0.0)
    ok = True
    reasons: List[str] = []
    if not route:
        ok = False
        reasons.append("empty_route")
    if len(answer) < 80:
        ok = False
        reasons.append("short_answer")
    if not isinstance(hits, list) or len(hits) < 3:
        ok = False
        reasons.append("agent_hits_too_few")
    if payload.get("agent_version") != AGENT_VERSION:
        ok = False
        reasons.append("bad_agent_version")
    if int(payload.get("agent_count") or 0) != 50:
        ok = False
        reasons.append("bad_agent_count")
    return {
        "round": idx,
        "ok": ok,
        "route": route,
        "confidence": confidence,
        "hit_count": len(hits) if isinstance(hits, list) else 0,
        "reasons": reasons,
    }


def main() -> int:
    qa = OpenClawStockAssistant(
        log_dir="logs/openclaw",
        db_path="/Users/mac/2026Qlin/permanent_stock_database.db",
    )
    if count_agents() != 50:
        print(json.dumps({"ok": False, "error": f"agent registry count={count_agents()}, expect=50"}, ensure_ascii=False))
        return 2

    results: List[Dict[str, Any]] = []
    seen_hits = set()
    rounds = _questions()
    for idx, q in enumerate(rounds, 1):
        out = qa.answer(q, history=[])
        c = _check_round(idx, out)
        results.append(c)
        for x in out.get("agent_hits") or []:
            seen_hits.add(str(x))

    pass_count = sum(1 for r in results if r["ok"])
    report = {
        "ok": pass_count == len(results),
        "agent_version": AGENT_VERSION,
        "agent_registry_count": count_agents(),
        "rounds": len(results),
        "pass_count": pass_count,
        "coverage_hits": len(seen_hits),
        "results": results,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    out_dir = Path("logs/openclaw")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"agent50_selftest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in report.items() if k != "results"}, ensure_ascii=False))
    print(str(out_file))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
