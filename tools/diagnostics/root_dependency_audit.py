#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

MAINLINE_SCAN_PATHS = [
    "v49_app.py",
    "trading_assistant.py",
    "scheduler_service.py",
    "auto_evolve.py",
    "main.py",
    "openclaw",
    "ui",
]

ROOT_DEPENDENCY_GROUPS: dict[str, list[str]] = {
    "legacy_evaluators": [
        "comprehensive_stock_evaluator_v3.py",
        "comprehensive_stock_evaluator_v4.py",
        "comprehensive_stock_evaluator_v5.py",
        "comprehensive_stock_evaluator_v6.py",
        "comprehensive_stock_evaluator_v6_ultimate.py",
        "comprehensive_stock_evaluator_v7_ultimate.py",
        "comprehensive_stock_evaluator_v8_ultimate.py",
    ],
    "v6_supporting_data": [
        "v6_data_provider.py",
        "v6_data_provider_optimized.py",
        "v6_leader_analyzer.py",
    ],
    "strategy_and_backtest_helpers": [
        "stable_uptrend_strategy.py",
        "optimized_backtest_strategy_v49.py",
        "backtest_v6_ultra_short.py",
    ],
    "runtime_services": [
        "notification_service.py",
        "dynamic_rebalance_manager.py",
        "kelly_position_manager.py",
        "risk_params.py",
    ],
    "operations": [
        "auto_evolve.py",
    ],
}


def build_patterns(files: list[str]) -> dict[str, re.Pattern[str]]:
    patterns: dict[str, re.Pattern[str]] = {}
    for filename in files:
        module = filename[:-3]
        patterns[filename] = re.compile(
            rf"(?m)^\s*(from\s+{re.escape(module)}\s+import\b|import\s+{re.escape(module)}(?:\s|$|,))"
        )
    return patterns


def collect_scan_files() -> list[Path]:
    files: list[Path] = []
    for rel in MAINLINE_SCAN_PATHS:
        path = ROOT / rel
        if path.is_file():
            files.append(path)
            continue
        if path.is_dir():
            files.extend(sorted(p for p in path.rglob("*.py") if p.is_file()))
    return files


def audit() -> dict[str, dict[str, list[str]]]:
    all_files = [item for group in ROOT_DEPENDENCY_GROUPS.values() for item in group]
    patterns = build_patterns(all_files)
    results: dict[str, dict[str, list[str]]] = {name: {} for name in ROOT_DEPENDENCY_GROUPS}

    for scan_file in collect_scan_files():
        rel = scan_file.relative_to(ROOT).as_posix()
        try:
            text = scan_file.read_text(encoding="utf-8")
        except Exception:
            continue
        for group_name, candidates in ROOT_DEPENDENCY_GROUPS.items():
            for filename in candidates:
                if patterns[filename].search(text):
                    results[group_name].setdefault(filename, []).append(rel)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit mainline dependencies still anchored at repo root.")
    parser.parse_args()

    results = audit()
    print("[root-audit] mainline scan paths:")
    for item in MAINLINE_SCAN_PATHS:
        print(f"  - {item}")

    for group_name, mapping in results.items():
        print(f"\n[{group_name}]")
        if not mapping:
            print("  (no references found)")
            continue
        for filename, refs in sorted(mapping.items()):
            print(f"  {filename}")
            for ref in refs:
                print(f"    - {ref}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
