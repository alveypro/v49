import argparse
import json

from src.utils.governance_audit import run_governance_audit


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run governance readiness audit for stock system")
    parser.add_argument("--config-dir", default="config", help="Config directory")
    parser.add_argument("--output-dir", default="data/experiments", help="Experiment output directory")
    parser.add_argument("--max-status-age-hours", type=float, default=36.0, help="Max allowed age for update status")
    parser.add_argument("--max-candidate-age-hours", type=float, default=36.0, help="Max allowed age for candidate output")
    parser.add_argument("--max-research-age-hours", type=float, default=36.0, help="Max allowed age for daily research output")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = run_governance_audit(
        config_dir=args.config_dir,
        output_dir=args.output_dir,
        max_status_age_hours=args.max_status_age_hours,
        max_candidate_age_hours=args.max_candidate_age_hours,
        max_research_age_hours=args.max_research_age_hours,
    )
    print(json.dumps(payload["summary"], ensure_ascii=False))
    print("Generated:")
    print(payload["outputs"]["latest_json"])
    print(payload["outputs"]["latest_md"])


if __name__ == "__main__":
    main()
