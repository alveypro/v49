# Regression Tiers

- `smoke` (~2 min): syntax + migrations + demo orchestration + safety check
- `integration` (~10-20 min): migrations + manual/comprehensive checks + real-lite run
- `overnight` (long): larger sample full run

## Usage

```bash
bash scripts/regression/run_smoke.sh
bash scripts/regression/run_integration.sh
bash scripts/regression/run_overnight.sh
```
