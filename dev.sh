#!/usr/bin/env bash
set -Eeuo pipefail

BACKEND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKENDS_DIR="$(dirname "$BACKEND_DIR")"
VENV="$BACKEND_DIR/venv"

ENV_FILE="${1:-$BACKEND_DIR/.env.local}"

if [[ -f "$ENV_FILE" ]]; then
  echo "▶ Loading env from $ENV_FILE"
  set -o allexport
  source "$ENV_FILE"
  set +o allexport
else
  echo "⚠ Env file not found: $ENV_FILE"
  echo "   Copy .env.example to .env.local and fill in the values"
  exit 1
fi

if [[ ! -d "$VENV" ]]; then
  echo "❌ venv not found at $VENV"
  echo "   Run: python3 -m venv venv && venv/bin/pip install -r requirements.txt"
  exit 1
fi

lsof -ti :5000 | xargs kill -9 2>/dev/null || true

source "$VENV/bin/activate"
cd "$BACKENDS_DIR"
python3 -m msb_zuv_input_data_backend.msb_zuv_input_data_app
