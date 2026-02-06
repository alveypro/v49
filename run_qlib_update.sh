#!/usr/bin/env bash
set -euo pipefail
cd /opt/airivo/app
/opt/rd-agent/.venv/bin/python /opt/airivo/qlib_src/export_to_qlib_csv.py >> /opt/airivo/logs/qlib_export.log 2>&1
/opt/rd-agent/.venv/bin/python /opt/qlib/scripts/dump_bin.py dump_update \
  --data_path /opt/airivo/qlib_src \
  --qlib_dir /root/.qlib/qlib_data/cn_data \
  --max_workers 4 \
  --date_field_name date \
  --include_fields open,high,low,close,volume,amount \
  >> /opt/airivo/logs/qlib_dump_update.log 2>&1
