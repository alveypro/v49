from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.adapters import V49Adapter


def demo_scan_handler(params):
    return {
        "picks": [
            {"ts_code": "600000.SH", "score": 86.5, "strategy": "v6", "reason": "consensus"},
            {"ts_code": "000001.SZ", "score": 82.1, "strategy": "v6", "reason": "breakout"},
        ],
        "metrics": {"count": 2},
        "params": params,
    }


def demo_backtest_handler(params):
    return {
        "summary": {
            "win_rate": 0.51,
            "max_drawdown": 0.09,
            "signal_density": 0.03,
        },
        "params": params,
    }


def main():
    adapter = V49Adapter(Path("v49_app.py"))
    adapter.register_scan_handler("v6", demo_scan_handler)
    adapter.register_backtest_handler("v6", demo_backtest_handler)

    scan = adapter.run_scan("v6", {"score_threshold": 85})
    picks = scan.get("result", {}).get("picks", [])

    merged = adapter.merge_signals(picks, {"v6": 1.2})

    backtest = adapter.run_backtest("v6", "2025-01-01", "2026-01-31", {"holding_days": 3})
    stats = backtest.get("result", {}).get("summary", {})

    risk = adapter.risk_check(stats, {"win_rate_min": 0.45, "max_drawdown_max": 0.12, "signal_density_min": 0.02})

    report = adapter.generate_report(
        "daily_brief",
        {
            "summary": {"scan": scan.get("status"), "backtest": backtest.get("status"), "risk": risk["risk_level"]},
            "opportunities": merged.get("ranked_list", []),
        },
    )

    print("scan status:", scan.get("status"))
    print("backtest status:", backtest.get("status"))
    print("risk level:", risk.get("risk_level"))
    print("report:", report.get("markdown"))


if __name__ == "__main__":
    main()
