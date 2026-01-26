# v49 Streamlit App

This repo contains the Streamlit app for the v49.0 long-term stable stock selection system.

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
