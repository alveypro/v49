#!/usr/bin/env bash
set -euo pipefail
cd /opt/airivo/app
mkdir -p evolution
/opt/airivo/app/.venv/bin/python /opt/airivo/app/auto_evolve.py >> /opt/airivo/logs/auto_evolve.log 2>&1
