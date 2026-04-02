#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

echo "[overnight] openclaw full run"
python3 openclaw/run_daily.py --strategy v6 --offline-stock-limit 300 --sample-size 400 --score-threshold 75 --output-dir logs/openclaw/overnight

echo "[overnight] done"
