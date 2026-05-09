import argparse
import json

from src.utils.experiment_artifact_bundle import build_experiment_artifact_bundle


def main() -> None:
    parser = argparse.ArgumentParser(description="Build required governance artifact bundle")
    parser.add_argument("--output-dir", default="data/experiments", help="Experiment output directory")
    args = parser.parse_args()

    outputs = build_experiment_artifact_bundle(output_dir=args.output_dir)
    print("Generated:")
    for _, path in outputs.items():
        print(path)
    print(json.dumps({"generated_count": len(outputs)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
