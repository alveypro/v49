#!/usr/bin/env bash
set -euo pipefail
cd /opt/airivo/app
mkdir -p evolution
export TZ=Asia/Shanghai
export AUTO_PUSH=1
export UPDATE_DAYS=30
export FUND_PORTFOLIO_FUNDS="110022.OF,110011.OF,005911.OF,161039.OF,009265.OF"
/opt/airivo/app/.venv/bin/python /opt/airivo/app/auto_evolve.py >> /opt/airivo/logs/auto_evolve.log 2>&1
