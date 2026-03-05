#!/bin/bash
set -euo pipefail

ROOT="/Users/mune/Workspace/inventory-automation"
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

# Force Playwright Chromium to avoid Chrome permission issues
unset EZADMIN_CHROME_CHANNEL
export EZADMIN_BROWSER=chromium
export EZADMIN_ALLOW_FALLBACK=0
# Optional: set to 1 for headless mode (may break some logins)
# export EZADMIN_HEADLESS=1

"$PY" "$ROOT/scripts/ezadmin_stock_sync.py"
