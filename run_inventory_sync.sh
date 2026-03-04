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

# Prefer system Chrome if installed (more stable on macOS GUI sessions)
if [ -d "/Applications/Google Chrome.app" ]; then
  export EZADMIN_CHROME_CHANNEL=chrome
fi
export EZADMIN_BROWSER=chromium
export EZADMIN_ALLOW_FALLBACK=0
# Optional: set to 1 for headless mode (may break some logins)
# export EZADMIN_HEADLESS=1

"$PY" "$ROOT/scripts/ezadmin_stock_sync.py"
