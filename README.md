# v49 Streamlit App

This repo contains the Streamlit app for the v49.0 long-term stable stock selection system.

## Single Source Of Truth

- Canonical main file: `终极量价暴涨系统_v49.0_长期稳健版.py` (repo root)
- `v49/终极量价暴涨系统_v49.0_长期稳健版.py` is a compatibility launcher that forwards to the root file.
- Please edit only the root main file.

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Optional env vars
export TUSHARE_TOKEN="your_token"
export PERMANENT_DB_PATH="/path/to/permanent_stock_database.db"

streamlit run 终极量价暴涨系统_v49.0_长期稳健版.py --server.port 8501
```

## Config

If you prefer config file, copy `config.json.example` to `config.json` and fill in values.

## Notes

- Do NOT commit database files or tokens to Git.
- Set `TUSHARE_TOKEN` in env or `config.json` on the server.

## Connection Stability (airivo.online)

- Streamlit stability flags are already baked into:
  - `start_v49_streamlit.sh`
  - `start_v49_full.sh`
  - `.streamlit/config.toml`
- Nginx reverse proxy reference (WebSocket + long timeout):
  - `deploy/nginx/airivo.online.streamlit.conf`
- Run Streamlit as launchd service (auto-restart + logs):

```bash
./tools/install_v49_streamlit_launchd.sh
```
