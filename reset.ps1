# =============================================================================
# reset.ps1
#
# One-shot cleanup script to wipe all broker state so the cluster starts from
# a completely fresh slate.  Run this BEFORE the demo on every machine that
# hosts a broker (your laptop + the two teammates' laptops).
#
# Usage (from the dsms/ folder):
#   powershell -ExecutionPolicy Bypass -File .\reset.ps1
#
# Or if the execution policy is already unrestricted, just:
#   .\reset.ps1
#
# What it cleans:
#   1. Any lingering python broker / publisher / subscriber processes.
#   2. The entire broker_data\ directory (RAFT meta, RAFT log, topic files).
#   3. The entire subscriber_output\ directory (per-pattern .log files,
#      .state_<name>.json manifests, and .state_<name>.lock lockfiles —
#      aka everything that makes a subscriber 'remember' its previous run).
#   4. Python __pycache__ folders so no stale .pyc files are loaded.
# =============================================================================

$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $here

Write-Host "=== DSMS Reset ===" -ForegroundColor Cyan
Write-Host "Working directory : $here"
Write-Host ""

# -----------------------------------------------------------------------------
# Step 1: kill leftover broker / publisher / subscriber processes
# -----------------------------------------------------------------------------
Write-Host "[1/4] Killing any leftover python broker/publisher/subscriber processes..."
$killed = 0
Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" |
    Where-Object {
        $_.CommandLine -match "broker\\broker\.py" -or
        $_.CommandLine -match "publisher\\dsms_pub\.py" -or
        $_.CommandLine -match "subscriber\\dsms_sub\.py"
    } |
    ForEach-Object {
        try {
            Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop
            Write-Host "    killed PID $($_.ProcessId)  ->  $($_.CommandLine)" -ForegroundColor DarkGray
            $killed++
        } catch {
            Write-Host "    could not kill PID $($_.ProcessId): $_" -ForegroundColor DarkYellow
        }
    }
if ($killed -eq 0) {
    Write-Host "    no leftover processes found" -ForegroundColor DarkGray
}

# -----------------------------------------------------------------------------
# Step 2: wipe broker_data directory (RAFT state + topic log files)
# -----------------------------------------------------------------------------
Write-Host ""
Write-Host "[2/4] Removing broker_data\ ..."
if (Test-Path ".\broker_data") {
    Remove-Item -Recurse -Force ".\broker_data"
    Write-Host "    broker_data\ deleted" -ForegroundColor Green
} else {
    Write-Host "    broker_data\ did not exist" -ForegroundColor DarkGray
}

# -----------------------------------------------------------------------------
# Step 3: wipe subscriber_output directory
#   - *.log            per-pattern output files written by each subscription
#   - .state_*.json    the persistence manifest(s) (one per --name)
#   - .state_*.lock    PID lockfile(s); stale locks would block restart
# -----------------------------------------------------------------------------
Write-Host ""
Write-Host "[3/4] Removing subscriber_output\ ..."
if (Test-Path ".\subscriber_output") {
    Remove-Item -Recurse -Force ".\subscriber_output"
    Write-Host "    subscriber_output\ deleted" -ForegroundColor Green
} else {
    Write-Host "    subscriber_output\ did not exist" -ForegroundColor DarkGray
}

# -----------------------------------------------------------------------------
# Step 4: clear python bytecode caches so no stale code sneaks in
# -----------------------------------------------------------------------------
Write-Host ""
Write-Host "[4/4] Removing __pycache__ folders..."
$caches = Get-ChildItem -Path . -Recurse -Directory -Filter "__pycache__" -ErrorAction SilentlyContinue
if ($caches) {
    foreach ($c in $caches) {
        Remove-Item -Recurse -Force $c.FullName
    }
    Write-Host "    removed $($caches.Count) __pycache__ folder(s)" -ForegroundColor Green
} else {
    Write-Host "    no __pycache__ folders found" -ForegroundColor DarkGray
}

# -----------------------------------------------------------------------------
# Next-step reminder
# -----------------------------------------------------------------------------
Write-Host ""
Write-Host "=== Reset complete ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps (run each command in its OWN terminal):" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Terminal 1 (this machine's broker):"
Write-Host "    python broker/broker.py --id <your_node_id>"
Write-Host ""
Write-Host "  Terminal 2 (publisher for this machine):"
Write-Host "    python publisher/dsms_pub.py --apps publisher/apps.json"
Write-Host ""
Write-Host "  Terminal 3 (subscriber for this machine):"
Write-Host "    python subscriber/dsms_sub.py"
Write-Host ""
Write-Host "Remember: run reset.ps1 on every teammate's laptop too, so all"
Write-Host "three nodes start with an empty broker_data/ folder."
Write-Host ""
