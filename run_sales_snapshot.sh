#!/bin/bash
set -euo pipefail

ROOT="/Users/mune/Desktop/Cursor/inventory-automation"
VENV="$ROOT/.venv"

cd "$ROOT"

if [ -x "$VENV/bin/python" ]; then
  PY="$VENV/bin/python"
else
  PY="$(command -v python3)"
fi

if [ -z "$PY" ]; then
  echo "python3 not found" >&2
  exit 1
fi

"$PY" "$ROOT/scripts/sales_snapshot.py"
