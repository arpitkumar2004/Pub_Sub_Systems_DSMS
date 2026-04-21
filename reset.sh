#!/usr/bin/env bash
# =============================================================================
# reset.sh
#
# Linux/macOS equivalent of reset.ps1.
# One-shot cleanup script to wipe all broker state so the cluster starts from
# a completely fresh slate.  Run this BEFORE the demo on every machine that
# hosts a broker.
#
# Usage (from the dsms/ folder):
#   chmod +x reset.sh        # only needed once
#   ./reset.sh
#
# What it cleans:
#   1. Any lingering python broker / publisher / subscriber processes.
#   2. The entire broker_data/ directory (RAFT meta, RAFT log, topic files).
#   3. The entire subscriber_output/ directory (per-pattern .log files,
#      .state_<name>.json manifests, and .state_<name>.lock lockfiles —
#      aka everything that makes a subscriber "remember" its previous run).
#   4. Python __pycache__ folders so no stale .pyc files are loaded.
# =============================================================================

set -u

# Colors (gracefully degrade if stdout is not a tty)
if [ -t 1 ]; then
    CYAN="\033[36m"
    GREEN="\033[32m"
    YELLOW="\033[33m"
    DIM="\033[2m"
    RESET="\033[0m"
else
    CYAN="" ; GREEN="" ; YELLOW="" ; DIM="" ; RESET=""
fi

# cd into the script's own directory so paths are always correct
here="$(cd "$(dirname "$0")" && pwd)"
cd "$here"

echo -e "${CYAN}=== DSMS Reset ===${RESET}"
echo    "Working directory : $here"
echo

# -----------------------------------------------------------------------------
# Step 1: kill leftover broker / publisher / subscriber processes
# -----------------------------------------------------------------------------
echo "[1/4] Killing any leftover python broker/publisher/subscriber processes..."

# Match any python process whose command line references one of our scripts.
# -f matches the full argument list, not just the executable name.
patterns=(
    "broker/broker.py"
    "publisher/dsms_pub.py"
    "subscriber/dsms_sub.py"
)

killed=0
for pat in "${patterns[@]}"; do
    # pgrep returns one pid per line; ignore when nothing matches.
    pids="$(pgrep -f "$pat" || true)"
    for pid in $pids; do
        # Skip our own shell and any parent shell that happens to match.
        if [ "$pid" = "$$" ] || [ "$pid" = "$PPID" ]; then
            continue
        fi
        if kill -TERM "$pid" 2>/dev/null; then
            echo -e "    ${DIM}killed PID $pid  ($pat)${RESET}"
            killed=$((killed + 1))
        fi
    done
done

if [ "$killed" -eq 0 ]; then
    echo -e "    ${DIM}no leftover processes found${RESET}"
fi

# -----------------------------------------------------------------------------
# Step 2: wipe broker_data directory (RAFT state + topic log files)
# -----------------------------------------------------------------------------
echo
echo "[2/4] Removing broker_data/ ..."
if [ -d "./broker_data" ]; then
    rm -rf ./broker_data
    echo -e "    ${GREEN}broker_data/ deleted${RESET}"
else
    echo -e "    ${DIM}broker_data/ did not exist${RESET}"
fi

# -----------------------------------------------------------------------------
# Step 3: wipe subscriber_output directory
#   - *.log            per-pattern output files written by each subscription
#   - .state_*.json    the persistence manifest(s) (one per --name)
#   - .state_*.lock    PID lockfile(s); stale locks would block restart
# -----------------------------------------------------------------------------
echo
echo "[3/4] Removing subscriber_output/ ..."
if [ -d "./subscriber_output" ]; then
    rm -rf ./subscriber_output
    echo -e "    ${GREEN}subscriber_output/ deleted${RESET}"
else
    echo -e "    ${DIM}subscriber_output/ did not exist${RESET}"
fi

# -----------------------------------------------------------------------------
# Step 4: clear python bytecode caches so no stale code sneaks in
# -----------------------------------------------------------------------------
echo
echo "[4/4] Removing __pycache__ folders..."
count="$(find . -type d -name "__pycache__" 2>/dev/null | wc -l | tr -d ' ')"
if [ "$count" -gt 0 ]; then
    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
    echo -e "    ${GREEN}removed $count __pycache__ folder(s)${RESET}"
else
    echo -e "    ${DIM}no __pycache__ folders found${RESET}"
fi

# -----------------------------------------------------------------------------
# Next-step reminder
# -----------------------------------------------------------------------------
echo
echo -e "${CYAN}=== Reset complete ===${RESET}"
echo
echo -e "${YELLOW}Next steps (run each command in its OWN terminal):${RESET}"
echo
echo "  Terminal 1 (this machine's broker):"
echo "    python3 broker/broker.py --id <your_node_id>"
echo
echo "  Terminal 2 (publisher for this machine):"
echo "    python3 publisher/dsms_pub.py --apps publisher/apps.json"
echo
echo "  Terminal 3 (subscriber for this machine):"
echo "    python3 subscriber/dsms_sub.py"
echo
echo "Remember: run ./reset.sh on every teammate's laptop too, so all"
echo "three nodes start with an empty broker_data/ folder."
echo
