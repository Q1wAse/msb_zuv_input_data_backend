#!/usr/bin/env bash
set -Eeuo pipefail

BACKEND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKENDS_DIR="$(dirname "$BACKEND_DIR")"
VENV="$BACKEND_DIR/venv"

export DATABASE_URL="${DATABASE_URL:-postgresql://postgres:postgres@127.0.0.1:5432/msb_zuv_input_data_tables}"

if [[ ! -d "$VENV" ]]; then
  echo "❌ venv not found at $VENV"
  echo "   Run: python3 -m venv venv && venv/bin/pip install -r requirements.txt"
  exit 1
fi

echo "▶ Releasing port 5000..."
lsof -ti :5000 | xargs kill -9 2>/dev/null && echo "   killed previous instance" || echo "   port is free"

source "$VENV/bin/activate"
cd "$BACKENDS_DIR"
python3 -m msb_zuv_input_data_backend.msb_zuv_input_data_app
