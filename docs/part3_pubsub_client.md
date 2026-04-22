# Part 3 — Publisher & Persistent Subscriber (Client Layer)
### Subsystem: Publisher & Persistent Subscriber (Client Layer)
### Files: `publisher/dsms_pub.py` · `publisher/metrics.py` · `publisher/apps.json` · `subscriber/dsms_sub.py` · `subscriber/log_manager.py`

---

## 1. Responsibilities & Role in DSMS

Part 3 forms the **application and client layer** of the DSMS pub-sub architecture.
While Parts 1 and 2 handle cluster coordination and durable log storage, Part 3 generates the data stream and delivers it to downstream operators, monitoring dashboards, and alert pipelines.

This subsystem provides:
1. **Publisher (`DSMS_PUB`)**: Monitors active OS processes using `psutil`, extracts real-time CPU and memory usage, formats structured metric entries, and reliably pushes them to the broker cluster with automatic leader discovery and failover redirection.
2. **Subscriber Service (`DSMS_SUB`)**: A multi-threaded, persistent client service that polls the broker cluster using byte offsets, dynamically resolves topic patterns (exact, prefix, suffix), streams tagged metrics to disk files, preserves state across restarts using an atomic manifest, and exposes an HTTP REST API for queries and subscription control.

---

## 2. Architecture & Interaction Flow

```
┌─────────────────────────────────────────────────────────────┐
│                 DSMS_PUB (Publisher)                        │
│   ├── reads publisher/apps.json                             │
│   ├── collects OS metrics via psutil (publisher/metrics.py) │
│   ├── leader caching & automatic failover discovery         │
│   └── sends: CREATE_TOPIC & METRIC via TCP                  │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│             Broker Cluster (Parts 1 & 2)                    │
│   Leader commits data to disk logs using RAFT consensus     │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                 DSMS_SUB (Subscriber)                       │
│   ├── HTTP REST API Server (:8080)                          │
│   │     POST /subscribe, DELETE /subscribe                  │
│   │     GET /status, GET /logs?topic=...                    │
│   ├── Worker Thread Pool (One Subscription Thread/File)     │
│   │     Dynamic discovery: LIST_TOPICS <pattern>            │
│   │     Pull protocol: SUBSCRIBE <topic> <byte_offset>      │
│   ├── Disk Output: subscriber_output/<pattern>_<ts>_sub.log │
│   ├── State Manifest: subscriber_output/.state_<name>.json   │
│   └── Process Lockfile: subscriber_output/.state_<name>.lock│
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Detailed Component Breakdown

### 3.1 Publisher Subsystem

#### 3.1.1 Configuration (`publisher/apps.json`)
Declares which server machine this publisher runs on and which application processes it monitors:
```json
{
    "server_id": "server0",
    "apps": [
        "brave",
        "whatsapp",
        "msedge"
    ]
}
```
Topics are automatically formatted as `{app_name}.{server_id}` (e.g. `brave.server0`, `whatsapp.server0`, `msedge.server0`).

#### 3.1.2 Process Metrics Collector (`publisher/metrics.py`)
- **`get_metrics_for_app(app_name)`**: Iterates over running processes via `psutil.process_iter()`. Finds the target process, samples CPU utilization over a 100ms interval (`proc.cpu_percent(interval=0.1)`), calculates memory percentage (`proc.memory_percent()`), and formats the metric string:
  `"cpu=12.3 memory=1.45"`
- **`get_system_metrics()`**: If an application is not currently active, the collector seamlessly falls back to system-wide metrics:
  `cpu = psutil.cpu_percent(interval=0.1)`
  `mem = psutil.virtual_memory().percent`
- **`list_running_processes()`**: Helper utility returning sorted `(pid, name)` pairs to assist in debugging and configuring `apps.json`.

#### 3.1.3 Publisher Loop & Dynamic Leader Failover (`publisher/dsms_pub.py`)
- **Topic Initialization:** On boot, the publisher broadcasts `CREATE_TOPIC` for each configured topic with retry logic (`MAX_RETRIES=10`).
- **Leader Discovery & Redirection:**
  1. The publisher maintains a per-topic leader cache (`self.topic_leader[topic]`).
  2. If it contacts a follower, the broker responds `NACK <leader_port>`. The publisher records the leader port and immediately reconnects directly to the leader.
  3. If the leader crashes, the socket connection fails (`ConnectionRefusedError` or timeout). The publisher invalidates its cache, marks the dead port, scans remaining brokers, receives the new leader's port via `NACK`, and resumes streaming with zero data loss.
- **Metric Broadcast:** Streams `METRIC <topic> <data>` every `PUBLISH_INTERVAL` (5 seconds).

---

### 3.2 Persistent Subscriber Service (`subscriber/dsms_sub.py`)

The subscriber service is designed for production reliability: it combines continuous background disk streaming with a RESTful control and query plane.

#### 3.2.1 The `Subscription` Worker Class
Each `POST /subscribe?topic=<pattern>` allocates an independent `Subscription` instance:
- **Dedicated Thread:** Runs a continuous polling loop with its own `threading.Event` stop flag.
- **Dedicated Output Log:** Creates an append-only file:
  `subscriber_output/<sanitized_pattern>_<timestamp>_sub<id>.log`
- **Topic Resolution:** Queries the broker leader via `LIST_TOPICS <pattern>`. If the pattern is a suffix (`server0`) or prefix (`brave`), the broker resolves all matching concrete topics (e.g., `brave.server0`, `whatsapp.server0`). Newly created topics matching the pattern are discovered automatically on subsequent cycles and tracked from `offset=0`.
- **Incremental Byte Polling:** For each tracked topic, sends `SUBSCRIBE <topic> <byte_offset>`, receives `DATA <new_offset>`, tags each JSON entry with its topic:
  `{"topic": "brave.server0", "ts": "2026-04-05 12:00:00", "data": "cpu=12.3 memory=1.45"}`
  and appends it to its dedicated output file.

#### 3.2.2 Crash Resilience via Atomic State Manifests
- **State File:** `subscriber_output/.state_<name>.json`
- **Atomic Writes:** Written via temporary file replacement (`_atomic_write_json`) with `fsync` and retry handling for Windows file locks (`WinError 5`).
- **Manifest Shape:**
  ```json
  {
    "version": 1,
    "name": "sub1",
    "next_sub_id": 2,
    "saved_at": "2026-04-05 12:05:00",
    "subscriptions": [
      {
        "id": 1,
        "pattern": "brave.server0",
        "created_at": "2026-04-05 12:00:00",
        "file": "D:\\Pub_Sub_Systems_DSMS\\subscriber_output\\brave.server0_2026-04-05_12-00-00_sub1.log",
        "offsets": {
          "brave.server0": 420
        }
      }
    ]
  }
  ```
- **Rehydration:** If the subscriber process or host restarts, `_restore_from_manifest()` automatically restarts worker threads, reopens the existing log files in append mode (`"ab"`), and resumes pulling from the stored offsets without duplicating previously collected records.

#### 3.2.3 Concurrency Protection: PID Lockfiles
To prevent two subscriber processes with the same identity from clobbering identical state files:
- Acquires `subscriber_output/.state_<name>.lock` containing the active PID.
- Uses `_pid_alive(pid)` (via `psutil.pid_exists` or `os.kill`) to differentiate between a genuinely running process (which causes startup rejection) and a dead PID from a prior crash (which is safely reclaimed).

#### 3.2.4 HTTP REST API
Exposed via Flask on `SUBSCRIBER_HTTP_PORT` (default `8080`):
- **`POST /subscribe?topic=<pattern>`**: Allocates subscription, launches worker thread, creates log file, saves manifest.
- **`DELETE /subscribe?id=<id>` or `?topic=<pattern>`**: Stops targeted subscription threads, closes log handles, updates manifest.
- **`GET /status`**: Returns health, leader port, manifest path, active subscriptions, and per-topic offsets.
- **`GET /logs?topic=<pattern>`**: Gathers, deduplicates, and sorts entries matching exact topic, app prefix, or server suffix across all subscriber log files.

---

### 3.3 `subscriber/log_manager.py` — In-Memory Log Cache Utility

Maintained for in-memory buffering and standalone testing:
- **`subscribe(topic)` / `unsubscribe(topic)`**: Registers or deletes in-memory subscription maps.
- **`add_raw_bytes(topic, raw_bytes)`**: Decodes JSON lines from raw broker byte streams and appends them to in-memory arrays.
- **`get_entries(topic_or_prefix)`**: Aggregates records across topics matching an exact string, prefix (`prefix.`), or suffix (`.suffix`), sorting the merged result chronologically.

---

## 4. How to Demo Part 3 Independently

You can demonstrate publishing and subscribing independently once the broker cluster is running.

### 4.1 Publisher Demonstration

**Step 1 — Start the Publisher:**
```bash
python publisher/dsms_pub.py --apps publisher/apps.json
```

**Expected Console Output:**
```
[PUB] Server ID: server0
[PUB] Apps: ['brave', 'whatsapp', 'msedge']
[PUB] Topics: ['brave.server0', 'whatsapp.server0', 'msedge.server0']
[PUB] Creating topic 'brave.server0' (attempt 1/10)
[PUB] Topic 'brave.server0' created successfully.
[PUB] Sending METRIC | topic=brave.server0 | cpu=14.2 memory=3.20
```

### 4.2 Subscriber Demonstration

**Step 1 — Start the Subscriber Service:**
```bash
python subscriber/dsms_sub.py --port 8080 --name sub1
```

**Step 2 — Subscribe to Topics via HTTP:**
```bash
# Exact topic subscription:
curl -X POST "http://localhost:8080/subscribe?topic=brave.server0"

# Suffix pattern subscription (all apps on server0):
curl -X POST "http://localhost:8080/subscribe?topic=server0"
```

**Step 3 — Inspect Streaming Output on Disk:**
```bash
# Windows PowerShell
Get-Content -Wait -Tail 10 subscriber_output\brave.server0_*.log

# Linux / macOS
tail -f subscriber_output/brave.server0_*.log
```

**Step 4 — Query Logs via HTTP API:**
```bash
# Suffix aggregation query:
curl "http://localhost:8080/logs?topic=server0"

# Status query:
curl "http://localhost:8080/status"
```

### 4.3 Failover & Crash Recovery Test

1. **Kill Broker Leader:** While publisher and subscriber are actively streaming, press `Ctrl+C` in the leader broker's terminal.
2. **Publisher Behavior:** Logs a connection error, contacts another broker, gets `NACK <new_leader_port>`, and redirects smoothly without crashing.
3. **Subscriber Behavior:** Detects broken socket, switches to the new leader on the next poll cycle, and continues reading from its saved byte offset.
4. **Kill Subscriber:** Press `Ctrl+C` in the subscriber terminal. Wait 15 seconds while the publisher continues streaming. Restart the subscriber:
   ```bash
   python subscriber/dsms_sub.py --port 8080 --name sub1
   ```
   Notice that the subscriber outputs:
   ```
   [SUB] Resuming 2 subscription(s) from '.../.state_sub1.json'
   [SUB] Resumed sub#1 pattern='brave.server0' offsets={'brave.server0': 350}
   ```
   It immediately resumes pulling from offset 350, catching up on all metrics published while it was down without duplicating data.

---

## 5. Integration Points Summary

| Caller / Callee | Method / Interaction | Purpose |
|---|---|---|
| **Publisher → Broker** | `CREATE_TOPIC <topic>` | Ensures topic log exists on the cluster before metrics are sent. |
| **Publisher → Broker** | `METRIC <topic> <data>` | Pushes process metrics to the leader. |
| **Publisher → OS** | `psutil.process_iter()` | Samples real CPU% and memory% from active OS processes. |
| **Subscriber → Broker** | `LIST_TOPICS <pattern>` | Resolves prefixes and suffixes to concrete topics. |
| **Subscriber → Broker** | `SUBSCRIBE <topic> <offset>` | Streams committed metric records starting from byte offset. |
| **Subscriber → Disk** | `subscriber_output/*.log` | Persists streaming metrics tagged by source topic. |
| **Subscriber → Manifest** | `.state_<name>.json` | Persists subscription IDs, patterns, and byte offsets. |
| **Subscriber → Lockfile** | `.state_<name>.lock` | Prevents concurrent instances from clobbering state. |
| **External Clients → Subscriber** | HTTP REST API (`:8080`) | Manages subscriptions and queries aggregated logs. |
