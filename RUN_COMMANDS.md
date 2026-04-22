# DSMS — Run Commands Cheat Sheet

Every command is given **twice**: once for **Windows PowerShell** and once for
**Linux / macOS (bash / zsh)**. Pick the block that matches your operating system.

Before running any commands, open a terminal in the root repository folder:

```powershell
# Windows PowerShell
cd "D:\Pub_Sub_Systems_DSMS-main"
```

```bash
# Linux / macOS
cd /path/to/Pub_Sub_Systems_DSMS-main
```

---

## 1. One-Time Setup

### 1a. Install Python Dependencies

```powershell
# Windows PowerShell
python -m pip install -r requirements.txt
```

```bash
# Linux / macOS
python3 -m pip install -r requirements.txt
```

### 1b. Configure Cluster Endpoints in `common/config.py`

Edit the `BROKERS` list in `common/config.py` so each entry has the host IP and port. For local testing, use `127.0.0.1`. For a multi-machine LAN setup, assign the real IPv4 address of each machine. Every machine in the cluster must have the **exact same** `common/config.py`.

```python
BROKERS = [
    {"id": 0, "host": "10.145.99.48", "port": 5000},
    {"id": 1, "host": "10.145.99.43", "port": 5001},
    {"id": 2, "host": "10.145.27.212", "port": 5002},
]
```

To see this machine's IPv4 address:

```powershell
# Windows PowerShell
ipconfig | Select-String "IPv4"
```

```bash
# Linux / macOS
hostname -I 2>/dev/null || ifconfig | grep "inet "
```

---

## 2. Firewall Rules (Open Once, Per Machine)

Each broker must accept incoming TCP on its assigned port (5000, 5001, 5002, etc.).
The subscriber also exposes an HTTP REST API (default port `8080`).

### Windows (PowerShell as Administrator)

```powershell
# Replace <BROKER_PORT> with this machine's port (5000, 5001, 5002, etc.)
New-NetFirewallRule -DisplayName "DSMS Broker <BROKER_PORT>" `
    -Direction Inbound -Protocol TCP -LocalPort <BROKER_PORT> -Action Allow

# Inbound rule for the subscriber HTTP REST API
New-NetFirewallRule -DisplayName "DSMS Subscriber 8080" `
    -Direction Inbound -Protocol TCP -LocalPort 8080 -Action Allow
```

To remove the firewall rules later:

```powershell
Remove-NetFirewallRule -DisplayName "DSMS Broker <BROKER_PORT>"
Remove-NetFirewallRule -DisplayName "DSMS Subscriber 8080"
```

### Linux (ufw — Ubuntu / Debian)

```bash
sudo ufw allow <BROKER_PORT>/tcp comment "DSMS broker"
sudo ufw allow 8080/tcp          comment "DSMS subscriber HTTP API"
sudo ufw reload
```

### Linux (firewalld — RHEL / Fedora / CentOS)

```bash
sudo firewall-cmd --permanent --add-port=<BROKER_PORT>/tcp
sudo firewall-cmd --permanent --add-port=8080/tcp
sudo firewall-cmd --reload
```

---

## 3. Reset the Node (Wipe All State)

Run this **before every fresh demo or test run** on every machine in the cluster:

```powershell
# Windows PowerShell
powershell -ExecutionPolicy Bypass -File .\reset.ps1
```

```bash
# Linux / macOS
chmod +x reset.sh   # first time only
./reset.sh
```

**What it wipes:**
- Leftover python processes (`broker.py`, `dsms_pub.py`, `dsms_sub.py`).
- `broker_data/` (RAFT state `raft_meta.json`, logs `raft_log.jsonl`, and topic `.log` files).
- `subscriber_output/` (streaming `.log` files, `.state_*.json` manifests, and `.state_*.lock` PID lockfiles).
- Python `__pycache__/` folders.

---

## 4. Start a Broker Node

Run one broker per machine (or per terminal window on localhost). Pass this node's integer ID:

```powershell
# Windows PowerShell (in its own terminal window)
python broker/broker.py --id 0
```

```bash
# Linux / macOS (in its own terminal window)
python3 broker/broker.py --id 0
```

Repeat for node 1 and node 2 in separate terminals:
```bash
python broker/broker.py --id 1
python broker/broker.py --id 2
```

*(Alternatively, to launch all 3 brokers automatically on localhost: `python start_cluster.py`)*

Wait ~4 seconds for election timers to fire. You will observe:
```
[RAFT 0] Started as FOLLOWER | term=0 | peers=[1, 2]
[RAFT 0] Starting ELECTION | term=1
[RAFT 0] *** BECAME LEADER | term=1 ***
```

---

## 5. Start the Publisher

The publisher monitors OS processes listed in `publisher/apps.json` and streams metrics.

Edit `publisher/apps.json` if needed:
```json
{
    "server_id": "server0",
    "apps": ["brave", "whatsapp", "msedge"]
}
```

Start the publisher:

```powershell
# Windows PowerShell (in its own terminal)
python publisher/dsms_pub.py --apps publisher/apps.json
```

```bash
# Linux / macOS (in its own terminal)
python3 publisher/dsms_pub.py --apps publisher/apps.json
```

Output:
```
[PUB] Server ID: server0
[PUB] Apps: ['brave', 'whatsapp', 'msedge']
[PUB] Topics: ['brave.server0', 'whatsapp.server0', 'msedge.server0']
[PUB] Creating topic 'brave.server0' (attempt 1/10)
[PUB] Topic 'brave.server0' created successfully.
[PUB] Sending METRIC | topic=brave.server0 | cpu=12.3 memory=1.45
```

---

## 6. Start the Subscriber Service

The subscriber service maintains persistent file streaming, independent poller threads per subscription, atomic manifests, and an HTTP REST API.

```powershell
# Windows PowerShell (in its own terminal)
python subscriber/dsms_sub.py --port 8080 --name sub1
```

```bash
# Linux / macOS (in its own terminal)
python3 subscriber/dsms_sub.py --port 8080 --name sub1
```

Options:
- `--port <PORT>`: HTTP REST API port (default: `8080`).
- `--name <NAME>`: Persistent identity for this subscriber (default: `port<PORT>`). Prevents two processes from clobbering the same manifest.

---

## 7. Interacting with the Subscriber (HTTP REST API & Files)

Replace `localhost` with the subscriber machine's LAN IP if querying from another machine.

### 7a. Subscribe to a Topic or Pattern

Creates an independent worker thread and dedicated output file in `subscriber_output/`.

```powershell
# Windows PowerShell
Invoke-WebRequest -Method POST -Uri "http://localhost:8080/subscribe?topic=brave.server0"
# Or:
curl.exe -X POST "http://localhost:8080/subscribe?topic=brave.server0"
```

```bash
# Linux / macOS
curl -X POST "http://localhost:8080/subscribe?topic=brave.server0"
```

You can also subscribe to app prefixes or server suffixes (dynamic topic resolution):
```bash
curl -X POST "http://localhost:8080/subscribe?topic=server0"
curl -X POST "http://localhost:8080/subscribe?topic=brave"
```

### 7b. Check Active Subscriptions & Health

```powershell
Invoke-WebRequest -Uri "http://localhost:8080/status"
```

```bash
curl "http://localhost:8080/status"
```

### 7c. Query Logs via REST API

Returns all collected metrics matching an exact topic, app prefix, or server suffix:

```powershell
# Exact topic
Invoke-WebRequest -Uri "http://localhost:8080/logs?topic=brave.server0"

# Suffix query (all apps on server0)
Invoke-WebRequest -Uri "http://localhost:8080/logs?topic=server0"

# Prefix query (all servers running brave)
Invoke-WebRequest -Uri "http://localhost:8080/logs?topic=brave"
```

```bash
# Exact topic
curl "http://localhost:8080/logs?topic=brave.server0"

# Suffix query
curl "http://localhost:8080/logs?topic=server0"

# Prefix query
curl "http://localhost:8080/logs?topic=brave"
```

### 7d. Tail Streaming Logs Directly on Disk

Each subscription continuously appends records to `subscriber_output/<pattern>_<timestamp>_sub<id>.log`:

```powershell
# Windows PowerShell
Get-Content -Wait -Tail 15 subscriber_output\brave.server0_*.log
```

```bash
# Linux / macOS
tail -f subscriber_output/brave.server0_*.log
```

### 7e. Unsubscribe

```powershell
# By subscription ID:
Invoke-WebRequest -Method DELETE -Uri "http://localhost:8080/subscribe?id=1"

# By topic pattern:
Invoke-WebRequest -Method DELETE -Uri "http://localhost:8080/subscribe?topic=brave.server0"
```

```bash
# By subscription ID:
curl -X DELETE "http://localhost:8080/subscribe?id=1"

# By topic pattern:
curl -X DELETE "http://localhost:8080/subscribe?topic=brave.server0"
```

---

## 8. Run Automated End-to-End Tests

To run the complete automated test suite (verifying connectivity, leader election, NACK redirection, topic creation, metric publishing, byte-offset reading, prefix aggregation, and log replication):

```powershell
# Windows PowerShell
python tests/test_dsms.py
```

```bash
# Linux / macOS
python3 tests/test_dsms.py
```

Expected summary:
```
  Automated tests : 10
  Passed          : 10
  Failed          : 0

  All automated tests passed! 🎉
```

---

## 9. Diagnostic & Troubleshooting Commands

### Check if a Broker Port is Open & Listening

```powershell
# Windows PowerShell — replace <PORT> with 5000, 5001, 5002, or 8080
netstat -ano | findstr :<PORT>
```

```bash
# Linux / macOS
ss -tlnp | grep :<PORT>        # Linux
lsof -i :<PORT>                # macOS / Linux
```

### Test Network Reachability to Another Node

```powershell
# Windows PowerShell
Test-NetConnection -ComputerName <REMOTE_IP> -Port <PORT>
```

```bash
# Linux / macOS
nc -zv <REMOTE_IP> <PORT>
```

### Verify Replicated Log Files on Disk

```powershell
# Windows PowerShell
Get-ChildItem .\broker_data\node*\*.log | Select-Object FullName, Length
```

```bash
# Linux / macOS
ls -lh broker_data/node*/*.log
```

---

## 10. Fault Tolerance & Failover Demonstration

1. **Start the Cluster**: Ensure brokers 0, 1, 2, publisher, and subscriber are running.
2. **Identify Leader**: Note which broker printed `*** BECAME LEADER ***` (or check `curl http://localhost:8080/status`).
3. **Kill Leader**: Press `Ctrl+C` in the leader's terminal.
4. **Observe Failover**:
   - The remaining two brokers detect missing heartbeats after ~6–15s.
   - One follower starts an election, gathers 2/2 votes, and becomes the new leader.
   - The publisher receives a connection error, queries another broker, receives `NACK <new_port>`, and resumes publication with **zero data loss**.
   - The subscriber reconnects to the new leader and resumes pulling from its last acknowledged byte offset.
5. **Recover Crashed Node**: Restart the killed broker (`python broker/broker.py --id <ID>`). It loads its persistent state from `raft_meta.json` and `raft_log.jsonl`, catches up via leader replication, and resumes follower duties.
