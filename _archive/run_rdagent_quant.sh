#!/usr/bin/env bash
set -euo pipefail
cd /opt/rd-agent
export QLIB_QUANT_TRAIN_START=2020-09-01
export QLIB_QUANT_TRAIN_END=2024-12-31
export QLIB_QUANT_VALID_START=2025-01-01
export QLIB_QUANT_VALID_END=2025-12-31
export QLIB_QUANT_TEST_START=2026-01-01
export QLIB_QUANT_TEST_END=2026-02-05
export QLIB_QUANT_EVOLVING_N=3
/opt/rd-agent/.venv/bin/rdagent fin_quant --loop-n 1 --step-n 2 >> /opt/airivo/logs/rdagent_quant.log 2>&1
