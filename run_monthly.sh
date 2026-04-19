#!/bin/bash
# Monthly fundamentals scraper — activated by launchd on the 1st of each month at 7am.
# Activates the virtualenv, then runs the monthly pipeline.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$SCRIPT_DIR/.venv/bin/activate"
LOG_DIR="$SCRIPT_DIR/logs"

mkdir -p "$LOG_DIR"

if [ ! -f "$VENV" ]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: virtualenv not found at $VENV" >&2
  echo "Run: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

source "$VENV"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting monthly pipeline (fundamentals + infra + council + outputs)"
cd "$SCRIPT_DIR"
python main.py --run monthly
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Monthly pipeline complete"
