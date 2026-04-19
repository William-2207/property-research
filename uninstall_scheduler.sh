#!/bin/bash
# Unregisters both property agent launchd jobs and removes the plists
# from ~/Library/LaunchAgents/. Does NOT delete logs or project files.

set -euo pipefail

LAUNCH_AGENTS="$HOME/Library/LaunchAgents"

WEEKLY_LABEL="com.propertyagent.weekly"
MONTHLY_LABEL="com.propertyagent.monthly"

uninstall_plist() {
  local label="$1"
  local dest="$LAUNCH_AGENTS/$label.plist"

  if [ -f "$dest" ]; then
    launchctl unload "$dest" 2>/dev/null && echo "  ✓ Unloaded: $label" || echo "  ~ Not loaded: $label"
    rm -f "$dest"
    echo "  ✓ Removed:  $dest"
  else
    echo "  ~ Not found: $dest (already removed)"
  fi
}

echo "Removing property agent launchd jobs..."
echo ""

uninstall_plist "$WEEKLY_LABEL"
uninstall_plist "$MONTHLY_LABEL"

echo ""
echo "Done. Logs are preserved in ./logs/"
echo "To reinstall: ./install_scheduler.sh"
