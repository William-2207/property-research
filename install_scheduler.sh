#!/bin/bash
# Registers both property agent launchd jobs with launchctl.
# Run once after cloning / setting up the project.
# Safe to re-run — unloads first if already registered.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"
PLIST_DIR="$SCRIPT_DIR/launchd"

WEEKLY_LABEL="com.propertyagent.weekly"
MONTHLY_LABEL="com.propertyagent.monthly"
WEEKLY_PLIST="$PLIST_DIR/$WEEKLY_LABEL.plist"
MONTHLY_PLIST="$PLIST_DIR/$MONTHLY_LABEL.plist"

# ── Preflight checks ──────────────────────────────────────────────────────────

if [ ! -f "$WEEKLY_PLIST" ] || [ ! -f "$MONTHLY_PLIST" ]; then
  echo "ERROR: plist files not found in $PLIST_DIR" >&2
  exit 1
fi

if [ ! -f "$SCRIPT_DIR/.venv/bin/activate" ]; then
  echo "ERROR: virtualenv not found. Run:" >&2
  echo "  python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

# Ensure Playwright's Chromium browser is installed
echo "Checking Playwright browser installation..."
"$SCRIPT_DIR/.venv/bin/playwright" install chromium --with-deps 2>/dev/null || \
  "$SCRIPT_DIR/.venv/bin/python" -m playwright install chromium
echo "  ✓ Playwright Chromium ready"

mkdir -p "$LAUNCH_AGENTS"
mkdir -p "$SCRIPT_DIR/logs"

# ── Install function ──────────────────────────────────────────────────────────

install_plist() {
  local label="$1"
  local src="$2"
  local dest="$LAUNCH_AGENTS/$label.plist"

  # Unload if already registered (ignore errors — not loaded is fine)
  launchctl unload "$dest" 2>/dev/null || true

  cp "$src" "$dest"
  # launchd requires the plist to be owned by root or the user, and not group/world writable
  chmod 644 "$dest"

  launchctl load "$dest"
  echo "  ✓ Registered: $label"
  echo "    Plist:  $dest"
}

# ── Register jobs ─────────────────────────────────────────────────────────────

echo "Installing property agent launchd jobs..."
echo ""

install_plist "$WEEKLY_LABEL"  "$WEEKLY_PLIST"
install_plist "$MONTHLY_LABEL" "$MONTHLY_PLIST"

# ── Verify ────────────────────────────────────────────────────────────────────

echo ""
echo "Registered jobs:"
launchctl list | grep "com.propertyagent" || echo "  (none visible yet — normal if launchd is still loading)"

echo ""
echo "Schedule:"
echo "  Weekly  — every Monday at 07:00  → logs/weekly.stdout.log"
echo "  Monthly — 1st of month at 07:00  → logs/monthly.stdout.log"
echo ""
echo "To check status:    launchctl list | grep propertyagent"
echo "To run now (test):  launchctl start $WEEKLY_LABEL"
echo "To remove:          ./uninstall_scheduler.sh"
