#!/usr/bin/env bash
set -euo pipefail

: "${VAULT_ADDR:?set VAULT_ADDR}"
: "${VAULT_TOKEN:?set VAULT_TOKEN}"

# Usage:
#   export VAULT_ADDR=http://127.0.0.1:8200
#   export VAULT_TOKEN=aurum-dev-token
#   export EIA_API_KEY=...
#   export FRED_API_KEY=...
#   export NOAA_GHCND_TOKEN=...
#   ./scripts/secrets/seed_vault_dev.sh

python3 scripts/secrets/push_vault_env.py \
  --mapping EIA_API_KEY=secret/data/aurum/eia:api_key \
  --mapping FRED_API_KEY=secret/data/aurum/fred:api_key \
  --mapping NOAA_GHCND_TOKEN=secret/data/aurum/noaa:token || true

# Optional ISO creds/examples (set envs before running)
python3 scripts/secrets/push_vault_env.py \
  --mapping PJM_API_KEY=secret/data/aurum/pjm:token \
  --mapping ISONE_USERNAME=secret/data/aurum/isone:username \
  --mapping ISONE_PASSWORD=secret/data/aurum/isone:password || true
