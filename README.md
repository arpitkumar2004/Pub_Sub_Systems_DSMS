# DSMS — Distributed Server Monitoring System
---

A high-performance, fault-tolerant, distributed **publish-subscribe (pub-sub) system** built from scratch in Python. DSMS continuously monitors OS processes across distributed servers and reliably streams real-time CPU and memory metrics through a clustered broker architecture coordinated by the **RAFT Consensus Algorithm**.

The system is designed with real-world distributed systems principles: strict leader consensus, append-only disk storage, offset-based streaming, automated leader discovery with failover redirection, persistent state rehydration across machine restarts, and RESTful query APIs.

---

## Table of Contents

1. [System Architecture](#1-system-architecture)
2. [Key Design Choices & Principles](#2-key-design-choices--principles)
3. [Component Deep-Dive](#3-component-deep-dive)
   - [3.1 Publisher (`DSMS_PUB`)](#31-publisher-dsms_pub)
   - [3.2 Broker Network Server (`broker.py`)](#32-broker-network-server-brokerpy)
   - [3.3 Consensus Engine (`raft.py` & `raft_persister.py`)](#33-consensus-engine-raftpy--raft_persisterpy)
   - [3.4 Append-Only Storage (`log_store.py`)](#34-append-only-storage-log_storepy)
   - [3.5 Persistent Subscriber Service (`DSMS_SUB`)](#35-persistent-subscriber-service-dsmssub)
4. [Step-by-Step Data Flow](#4-step-by-step-data-flow)
5. [Message & Wire Protocol](#5-message--wire-protocol)
6. [RAFT Consensus Implementation](#6-raft-consensus-implementation)
7. [Fault Tolerance & Crash Recovery Scenarios](#7-fault-tolerance--crash-recovery-scenarios)
8. [Directory Structure](#8-directory-structure)
9. [Installation & Setup](#9-installation--setup)
10. [Running the System](#10-running-the-system)
11. [HTTP REST API Reference](#11-http-rest-api-reference)
12. [Automated & Manual Testing](#12-automated--manual-testing)
13. [Configuration Reference](#13-configuration-reference)

---

## 1. System Architecture

```
   ┌────────────────────────────────────────────────────────┐
   │            Publisher Host (DSMS_PUB)                   │
   │  ┌──────────────┐   reads   ┌───────────────────────┐  │
   │  │  apps.json   │ ────────► │ psutil Metric Sampler │  │
   │  └──────────────┘           └──────────┬────────────┘  │
   │                                        │ METRIC /      │
   │                                        │ CREATE_TOPIC  │
   └────────────────────────────────────────┼───────────────┘
                                            │ (TCP)
                                            ▼
   ┌─────────────────────────────────────────────────────────────────────────┐
   │                       Broker Cluster (RAFT)                             │
   │                                                                         │
   │   ┌───────────────────┐    REPLICATE / VOTE    ┌────────────────────┐   │
   │   │  Broker Node 0    │ ◄────────────────────► │  Broker Node 1     │   │
   │   │  [LEADER]         │                        │  [FOLLOWER]        │   │
   │   │  ├── RaftNode     │                        │  ├── RaftNode      │   │
   │   │  ├── RaftPersister│                        │  ├── RaftPersister │   │
   │   │  └── LogStore     │                        │  └── LogStore      │   │
   │   └─────────┬─────────┘                        └─────────┬──────────┘   │
   │             ▲              REPLICATE / VOTE              ▲              │
   │             └──────────────────────┬─────────────────────┘              │
   │                                    ▼                                    │
   │                        ┌───────────────────────┐                        │
   │                        │  Broker Node 2        │                        │
   │                        │  [FOLLOWER]           │                        │
   │                        │  ├── RaftNode         │                        │
   │                        │  ├── RaftPersister    │                        │
   │                        │  └── LogStore         │                        │
   │                        └───────────┬───────────┘                        │
   └────────────────────────────────────┼────────────────────────────────────┘
                                        │ SUBSCRIBE <topic> <offset> /
                                        │ LIST_TOPICS <pattern> (TCP)
                                        ▼
   ┌─────────────────────────────────────────────────────────────────────────┐
   │                  Subscriber Host (DSMS_SUB)                             │
   │                                                                         │
   │   ┌─────────────────────────────────────────────────────────────────┐   │
   │   │  Worker Pool: One Subscription Thread + File per Pattern        │   │
   │   │  ├── Thread #1: polls brave.server0 ──► subscriber_output/*.log │   │
   │   │  └── Thread #2: polls server0 (all) ──► subscriber_output/*.log │   │
   │   └────────────────────────────────┬────────────────────────────────┘   │
   │                                    │ Atomic state sync                  │
   │   ┌────────────────────────────────┴────────────────────────────────┐   │
   │   │  Persistence: .state_<name>.json  |  PID Lock: .state_<name>.lock│   │
   │   └────────────────────────────────┬────────────────────────────────┘   │
   │                                    │ REST API (:8080)                   │
   │   ┌────────────────────────────────▼────────────────────────────────┐   │
   │   │  Endpoints: POST /subscribe | DELETE /subscribe | GET /status   │   │
   │   │             GET /logs?topic=<pattern>                           │   │
   │   └─────────────────────────────────────────────────────────────────┘   │
   └─────────────────────────────────────────────────────────────────────────┘
                                        ▲
                                        │ HTTP Queries (curl, browser, UI)
                               ┌────────┴────────┐
                               │ External Client │
                               └─────────────────┘
```

---

## 2. Key Design Choices & Principles

| Concern | Architectural Choice | Rationale & Practical Benefit |
|---|---|---|
| **Subscriber Model** | **Pull-Based Polling** | Subscribers fetch data at their own pace (`POLL_INTERVAL=3s`), eliminating message loss from subscriber buffer saturation and keeping brokers stateless with respect to subscriber clients. |
| **Log Format & Reading** | **Append-Only with Byte Offsets** | Follows the Apache Kafka storage model. Subscribers request data starting at `offset=N`; the broker seeks directly without scanning, returning only new bytes and advancing the offset. |
| **Cluster Consensus** | **Full RAFT Implementation** | Guarantees strong consistency (linearizable writes), deterministic state machine replication, single-leader election, and seamless failover even under node crashes. |
| **Crash Durability** | **Double-Layer Persistence** | (1) RAFT state (`current_term`, `voted_for`, `last_applied`, log entries) is fsynced to `raft_meta.json` and `raft_log.jsonl` via `RaftPersister`.<br>(2) Committed records are appended to per-topic logs on disk (`broker_data/node<N>/<topic>.log`). |
| **Subscriber Reliability** | **Persistent Thread-Per-Subscription** | Subscriptions run on independent threads writing to dedicated files (`subscriber_output/`). State manifests (`.state_<name>.json`) rehydrate upon subscriber restart so no duplicate data is fetched. |
| **Process Exclusivity** | **Cross-Platform PID Locks** | `.state_<name>.lock` ensures two concurrent subscriber processes cannot corrupt identical state manifests, using active PID validation. |
| **Topic Aggregation** | **Prefix & Suffix Pattern Matching** | Topics follow `<app_name>.<server_id>`. Subscribers can query an exact topic (`brave.server0`), an app prefix (`brave` across all servers), or a server suffix (`server0` for all apps on a host). |

---

## 3. Component Deep-Dive

### 3.1 Publisher (`DSMS_PUB`)
*Files: [`publisher/dsms_pub.py`](file:///d:/Pub_Sub_Systems_DSMS-main/publisher/dsms_pub.py), [`publisher/metrics.py`](file:///d:/Pub_Sub_Systems_DSMS-main/publisher/metrics.py), [`publisher/apps.json`](file:///d:/Pub_Sub_Systems_DSMS-main/publisher/apps.json)*

- **Configuration:** Reads `apps.json` containing `server_id` (e.g., `"server0"`) and targeted process names (`apps: ["brave", "whatsapp", "msedge"]`). Topics are automatically constructed as `{app}.{server_id}`.
- **Metric Collection:** Utilizes `psutil` to inspect active processes, extracting process-level CPU percentage and RSS memory. If an application is not currently active, it seamlessly falls back to system-wide CPU and memory metrics.
- **Durable Registration:** Sends `CREATE_TOPIC <topic>` for each topic at boot with exponential retry logic (`MAX_RETRIES=10`).
- **Leader Caching & Failover:** Caches the broker leader port. If a follower responds with `NACK <leader_port>`, the publisher redirects immediately to the leader. If the current connection terminates, the publisher marks the socket dead and rediscovers the newly elected leader.

### 3.2 Broker Network Server (`broker.py`)
*File: [`broker/broker.py`](file:///d:/Pub_Sub_Systems_DSMS-main/broker/broker.py)*

- **Multi-Threaded TCP Server:** Binds to the node's configured host and port, spawning dedicated worker threads per incoming connection for client requests and inter-broker RAFT communication.
- **Request Routing:**
  - `CREATE_TOPIC <topic>`: Proposes topic creation to the RAFT state machine. Waits for majority commit before returning `ACK`.
  - `METRIC <topic> <data>`: Validates topic existence, proposes the metric to RAFT, waits for consensus commit, and returns `ACK`.
  - `SUBSCRIBE <topic> <offset>`: Reads committed data from the local `LogStore` at the specified byte offset. Returns `DATA <new_offset>\n` followed by the raw bytes.
  - `LIST_TOPICS <pattern>`: Evaluates registered topic files against pattern matching rules (exact, prefix, or suffix) and returns matching concrete topics to subscribers.
  - RAFT RPCs (`VOTE_REQUEST`, `VOTE_GRANTED`, `VOTE_DENIED`, `REPLICATE`, `REPLICATE_ACK`): Dispatched to the local `RaftNode`.
- **Follower Redirection:** Followers reject client writes with `NACK <leader_port>`, informing clients of the current leader's network location.

### 3.3 Consensus Engine (`raft.py` & `raft_persister.py`)
*Files: [`broker/raft.py`](file:///d:/Pub_Sub_Systems_DSMS-main/broker/raft.py), [`broker/raft_persister.py`](file:///d:/Pub_Sub_Systems_DSMS-main/broker/raft_persister.py)*

- **State Machine:** Nodes operate as `FOLLOWER`, `CANDIDATE`, or `LEADER`.
- **Randomized Timers:** Follower election timeouts are randomized between `6.0s` and `15.0s` (`ELECTION_TIMEOUT_MIN` / `MAX`), well above the leader heartbeat interval (`HEARTBEAT_INTERVAL=0.75s`) to prevent split votes.
- **Log Commit Semantics:** An entry is committed when a quorum (majority) of nodes confirms replication. Leader commit advancement strictly follows RAFT Section 5.4.2 by appending a `NO_OP` upon election victory.
- **Non-Blocking RPC Queues:** Dedicated per-peer worker threads (`_peer_send_worker`) isolate network latency or dead peers from blocking the core RAFT state machine.
- **State Persistence (`RaftPersister`):** Persists `current_term`, `voted_for`, `last_applied`, and the RAFT log entries (`raft_meta.json` and `raft_log.jsonl`) with atomic file replacements and fsync, including Windows file-locking retry backoff (`WinError 5`).

### 3.4 Append-Only Storage (`log_store.py`)
*File: [`broker/log_store.py`](file:///d:/Pub_Sub_Systems_DSMS-main/broker/log_store.py)*

- **Disk Structure:** Maintains isolated append-only log files under `broker_data/node<id>/<sanitized_topic>.log`.
- **Deterministic Timestamps:** Metric timestamps are assigned by the RAFT leader when an entry is proposed and replicated across the cluster, ensuring log files across all broker nodes are byte-for-byte identical.
- **Zero-Copy Seeking:** `read(topic, byte_offset)` seeks directly to the target byte offset and streams out content up to the current file boundary, returning the advanced byte offset.

### 3.5 Persistent Subscriber Service (`DSMS_SUB`)
*Files: [`subscriber/dsms_sub.py`](file:///d:/Pub_Sub_Systems_DSMS-main/subscriber/dsms_sub.py), [`subscriber/log_manager.py`](file:///d:/Pub_Sub_Systems_DSMS-main/subscriber/log_manager.py)*

- **Dedicated Subscription Workers:** Each `/subscribe` invocation creates an independent `Subscription` instance with its own worker thread, polling loop, and offset map.
- **Dynamic Topic Discovery:** Uses `LIST_TOPICS` against the broker leader to automatically detect new matching topics created on the fly.
- **Disk Streaming:** All received entries are stamped with their concrete topic name and appended sequentially to dedicated log files:
  `subscriber_output/<pattern>_<timestamp>_sub<id>.log`
- **Crash Rehydration:** State manifests (`subscriber_output/.state_<name>.json`) track active subscriptions and their offsets. On service restart, previous subscriptions are resumed without duplicating log data.
- **Lockfile Protection:** Enforces single-instance safety via `.state_<name>.lock`.
- **Dual Consumption:** Provides both continuous file-based log consumption and an integrated Flask HTTP API with endpoints for subscription control and query aggregation (`GET /logs`, `GET /status`).

---

## 4. Step-by-Step Data Flow

```
Step 1: Cluster Formation & Leader Election
  1. Brokers 0, 1, 2 start as FOLLOWERS with randomized election timers.
  2. First timer expires (e.g., Node 0) → increments term, becomes CANDIDATE, broadcasts VOTE_REQUEST.
  3. Peers grant votes (candidate log is up to date, term is valid).
  4. Node 0 secures majority (2/3 votes) → becomes LEADER, appends NO_OP, broadcasts heartbeats.

Step 2: Topic Creation
  1. DSMS_PUB starts, reads apps.json → topics: ["brave.server0", "whatsapp.server0"].
  2. Publisher sends CREATE_TOPIC brave.server0 to broker cluster.
  3. If received by a follower: follower replies NACK <leader_port>; publisher retries leader.
  4. Leader proposes CREATE_TOPIC entry in RAFT log, writes to raft_log.jsonl, broadcasts REPLICATE.
  5. Followers append entry and reply REPLICATE_ACK.
  6. Quorum reached → Leader advances commit_index → Apply threads create brave_server0.log on disk.
  7. Leader sends ACK to publisher.

Step 3: Metric Publication (Every 5 Seconds)
  1. Publisher queries psutil for process metrics → "cpu=14.2 memory=3.10".
  2. Publisher sends: METRIC brave.server0 cpu=14.2 memory=3.10 to leader.
  3. Leader proposes entry with deterministic timestamp ts="2026-04-05 12:00:00".
  4. Leader replicates to followers → receives majority REPLICATE_ACK → advances commit_index.
  5. Apply threads write {"ts": "2026-04-05 12:00:00", "data": "cpu=14.2 memory=3.10"} to disk.
  6. Leader returns ACK to publisher.

Step 4: Subscription & Streaming
  1. Client sends POST /subscribe?topic=server0 to DSMS_SUB.
  2. Subscriber queries broker leader with LIST_TOPICS server0.
  3. Leader resolves pattern to ["brave.server0", "whatsapp.server0"].
  4. Subscription worker initializes offsets to 0, opens subscriber_output/server0_..._sub1.log.
  5. Background poller sends SUBSCRIBE brave.server0 0 to leader.
  6. Leader reads brave_server0.log from offset 0, replies:
     DATA 84\n{"ts":"2026-04-05 12:00:00","data":"cpu=14.2 memory=3.10"}\n
  7. Subscriber tags entry with source topic, appends to subscriber output file, updates offset to 84.
  8. Next poll sends SUBSCRIBE brave.server0 84 → receives only incremental new data.
```

---

## 5. Message & Wire Protocol

All network communication uses newline-terminated (`\n`) UTF-8 strings over persistent TCP sockets.

### Client ↔ Broker Protocol

| Command | Direction | Format | Description |
|---|---|---|---|
| `CREATE_TOPIC` | Client → Broker | `CREATE_TOPIC <topic>\n` | Request creation of a new topic |
| `METRIC` | Client → Broker | `METRIC <topic> <data>\n` | Publish metric data to a topic |
| `ACK` | Broker → Client | `ACK\n` | Operation committed successfully |
| `NACK` | Broker → Client | `NACK <leader_port>\n` | Node is follower; redirect to leader port |
| `SUBSCRIBE` | Client → Broker | `SUBSCRIBE <topic> <offset>\n` | Request log bytes starting from byte offset |
| `DATA` | Broker → Client | `DATA <new_offset>\n<raw_bytes>` | Header with advanced offset followed by payload |
| `LIST_TOPICS` | Client → Broker | `LIST_TOPICS <pattern>\n` | Query topics matching exact, prefix, or suffix |
| `TOPICS` | Broker → Client | `TOPICS ["topic1", "topic2"]\n` | JSON list of matching topic names |

### Inter-Broker RAFT Protocol

| Message | Wire Representation | Description |
|---|---|---|
| `VOTE_REQUEST` | `VOTE_REQUEST {"term": T, "candidate_id": ID, "last_log_index": I, "last_log_term": LT}\n` | Candidate solicits election vote |
| `VOTE_GRANTED` | `VOTE_GRANTED {"term": T, "voter_id": ID}\n` | Peer grants vote to candidate |
| `VOTE_DENIED` | `VOTE_DENIED {"term": T}\n` | Peer rejects vote |
| `REPLICATE` | `REPLICATE {"term": T, "leader_id": ID, "prev_log_index": PI, "prev_log_term": PT, "entries": [...], "commit_index": CI}\n` | Log replication payload or empty heartbeat |
| `REPLICATE_ACK` | `REPLICATE_ACK {"term": T, "follower_id": ID, "match_index": MI, "success": true/false}\n` | Replication acknowledgment |

---

## 6. RAFT Consensus Implementation

### 6.1 State Machine Lifecycle
```
                 ┌──────────────────────────────────────────────┐
                 │                                              │
                 ▼                                              │
          ┌──────────────┐   Election Timeout    ┌──────────────┴─┐
          │   FOLLOWER   ├──────────────────────►│   CANDIDATE    │
          └──────┬───────┘   (No Leader Ping)    └──────┬─────────┘
                 ▲                                      │
                 │ Higher Term Seen                     │ Majority Votes
                 │                                      ▼
                 └───────────────────────────────┌────────────────┐
                                                 │     LEADER     │
                                                 └────────────────┘
```

- **Follower:** Listens for `REPLICATE` messages. If `_timer_deadline` passes without a valid heartbeat, transitions to `CANDIDATE`.
- **Candidate:** Increments `current_term`, votes for itself, writes state to `raft_meta.json`, and sends `VOTE_REQUEST` to peers. Upon receiving `(cluster_size // 2) + 1` votes, transitions to `LEADER`.
- **Leader:** Immediately appends a `NO_OP` log entry to flush prior-term commits, starts the periodic heartbeat loop (`HEARTBEAT_INTERVAL=0.75s`), and initializes `next_index` and `match_index` for each peer.
- **Step Down:** If any node receives a message with `term > current_term`, it immediately reverts to `FOLLOWER`, clears `voted_for`, persists its updated term, and resets its election timer.

### 6.2 Safe Log Replication & Applying
1. Client requests arrive at the leader's `propose()` method.
2. Leader appends the entry to its in-memory log, stamps it with the current timestamp, fsyncs it to `raft_log.jsonl`, and broadcasts `REPLICATE`.
3. Followers verify log continuity (`prev_log_index` and `prev_log_term`). If mismatched, the follower rejects the replication, prompting the leader to decrement `next_index` until log consistency is restored.
4. Once a majority of followers confirm the entry, the leader advances its `commit_index`.
5. Background `_apply_loop` threads on all nodes monitor `commit_index > last_applied`, sequentially applying committed entries to `LogStore` topic files and persisting `last_applied` to disk.

---

## 7. Fault Tolerance & Crash Recovery Scenarios

| Failure Scenario | System Handling & Recovery Guarantee |
|---|---|
| **Broker Leader Crashes** | Followers detect missing heartbeats after election timeout (6–15s). A follower initiates an election and acquires quorum. The publisher receives connection errors, queries remaining brokers, receives `NACK <new_port>`, and seamlessly resumes publication with **zero data loss**. |
| **Follower Broker Crashes** | The 2 remaining nodes maintain a strict majority (2 out of 3). Writes and commits proceed uninterrupted. When the follower restarts, it reloads `raft_meta.json` and `raft_log.jsonl`, rejoins the cluster, and catches up via leader replication. |
| **Network Partition (2 vs 1)** | The minority partition (1 node) cannot achieve quorum and rejects writes. The majority partition (2 nodes) continues electing a leader and committing entries. When the partition heals, the isolated node steps down upon seeing the higher term and updates its log. |
| **Subscriber Service Restarts** | `dsms_sub.py` loads `.state_<name>.json` containing previous topic offsets. Resumed worker threads reopen output files in append mode (`"ab"`) and poll brokers from their last acknowledged byte offset, preventing duplicated entries. |
| **Broker Process Crash & Reboot** | `RaftPersister` reloads `current_term`, `voted_for`, and `last_applied` alongside `raft_log.jsonl`. Because `last_applied` is preserved, already-committed log entries are never re-applied to topic files on reboot. |
| **Concurrent Process Conflict** | `.state_<name>.lock` performs active PID validation (`psutil`/`os.kill`), preventing accidental duplicate subscribers from clobbering identical state files. |

---

## 8. Directory Structure

```
Pub_Sub_Systems_DSMS/
├── common/
│   ├── __init__.py
│   ├── config.py                 # Shared cluster topology, timeouts, ports
│   └── protocol.py               # TCP message encoding, decoding & socket helpers
├── broker/
│   ├── __init__.py
│   ├── broker.py                 # Broker TCP server, request routing, connection handler
│   ├── log_store.py              # Per-topic append-only log store & byte-offset reader
│   ├── raft.py                   # RAFT consensus state machine (leader election & replication)
│   └── raft_persister.py         # Atomic disk persistence for RAFT metadata & logs
├── publisher/
│   ├── __init__.py
│   ├── apps.json                 # Publisher application monitoring configuration
│   ├── dsms_pub.py               # Main publisher process with leader caching & retry loops
│   └── metrics.py                # psutil CPU/memory metrics collection & fallback
├── subscriber/
│   ├── __init__.py
│   ├── dsms_sub.py               # Multi-threaded subscriber, file streaming, REST API, state manifest
│   └── log_manager.py            # In-memory log cache and prefix/suffix aggregation utility
├── broker_data/                  # Runtime broker data directories (per node)
│   ├── node0/                    # Node 0 topic logs (.log), raft_meta.json, raft_log.jsonl
│   ├── node1/                    # Node 1 topic logs (.log), raft_meta.json, raft_log.jsonl
│   └── node2/                    # Node 2 topic logs (.log), raft_meta.json, raft_log.jsonl
├── subscriber_output/            # Runtime subscriber output directory
│   ├── .state_<name>.json        # Subscriber persistent manifest (subscriptions & offsets)
│   ├── .state_<name>.lock        # PID lockfile preventing concurrent subscriber collisions
│   └── <topic>_<ts>_sub<id>.log  # Dedicated streaming log files per subscription
├── tests/
│   └── test_dsms.py              # End-to-end automated validation and test suite
├── docs/                         # Component architectural documentation
│   ├── part1_raft_consensus.md   # Part 1 deep-dive (RAFT consensus & protocol)
│   ├── part2_broker_storage.md   # Part 2 deep-dive (Broker server & log storage)
│   └── part3_pubsub_client.md    # Part 3 deep-dive (Publisher & persistent subscriber)
├── reset.ps1                     # Windows cleanup script (wipes state, kills stale processes)
├── reset.sh                      # Linux/macOS cleanup script (wipes state, kills stale processes)
├── start_cluster.py              # Helper to launch all 3 broker nodes
├── RUN_COMMANDS.md               # Step-by-step CLI execution cheat sheet
├── requirements.txt              # Project dependencies (psutil, flask)
└── README.md                     # Comprehensive project documentation (this file)
```

---

## 9. Installation & Setup

### 9.1 Prerequisites
- Python 3.8+ installed on all machines.
- Network connectivity between machines (for multi-host deployments).

### 9.2 Clone & Install Dependencies

```bash
# Clone the repository
git clone <repo-url>
cd Pub_Sub_Systems_DSMS

# Install required Python packages
python -m pip install -r requirements.txt
```

*Requirements:*
- `psutil` (process inspection and system metrics)
- `flask` (subscriber HTTP REST API)

### 9.3 Cluster Topology Configuration
Edit [`common/config.py`](file:///d:/Pub_Sub_Systems_DSMS-main/common/config.py) to declare the broker nodes. For local multi-terminal testing, use `127.0.0.1`. For multi-machine deployment, specify the respective LAN IP addresses:

```python
BROKERS = [
    {"id": 0, "host": "10.145.99.48", "port": 5000},
    {"id": 1, "host": "10.145.99.43", "port": 5001},
    {"id": 2, "host": "10.145.27.212", "port": 5002},
]
```

---

## 10. Running the System

### 10.1 Fresh Reset
Before running a fresh demo or test run, wipe prior test logs and stale processes:

**Windows PowerShell:**
```powershell
powershell -ExecutionPolicy Bypass -File .\reset.ps1
```

**Linux / macOS:**
```bash
chmod +x reset.sh
./reset.sh
```

### 10.2 Starting the Broker Cluster

Run each broker in its own terminal:

**Terminal 1 (Broker Node 0):**
```bash
python broker/broker.py --id 0
```

**Terminal 2 (Broker Node 1):**
```bash
python broker/broker.py --id 1
```

**Terminal 3 (Broker Node 2):**
```bash
python broker/broker.py --id 2
```

*(Alternatively, run `python start_cluster.py` to start all three brokers automatically).*

Wait ~4 seconds for election timers to fire and a leader to be established.

### 10.3 Starting the Publisher

Configure the applications to monitor in [`publisher/apps.json`](file:///d:/Pub_Sub_Systems_DSMS-main/publisher/apps.json):
```json
{
  "server_id": "server0",
  "apps": ["brave", "whatsapp", "msedge"]
}
```

**Terminal 4:**
```bash
python publisher/dsms_pub.py --apps publisher/apps.json
```

The publisher registers topics and streams CPU/memory metrics every 5 seconds.

### 10.4 Starting the Subscriber

**Terminal 5:**
```bash
python subscriber/dsms_sub.py --port 8080 --name sub_client1
```

---

## 11. HTTP REST API Reference

Base URL: `http://localhost:8080` (or the configured `SUBSCRIBER_HTTP_PORT`).

### 1. Subscribe to a Topic or Pattern
**Endpoint:** `POST /subscribe?topic=<pattern>`

Creates an independent background poller worker and output file for the pattern. Supports exact topics (`brave.server0`), app prefixes (`brave`), or server suffixes (`server0`).

```bash
curl -X POST "http://localhost:8080/subscribe?topic=brave.server0"
```

**Response (200 OK):**
```json
{
  "status": "subscribed",
  "subscription_id": 1,
  "pattern": "brave.server0",
  "file": "D:\\Pub_Sub_Systems_DSMS\\subscriber_output\\brave.server0_2026-04-05_12-00-00_sub1.log",
  "created_at": "2026-04-05 12:00:00"
}
```

### 2. Check Subscription Status
**Endpoint:** `GET /status`

Returns active subscriptions, monitored topics, current byte offsets, and leader information.

```bash
curl "http://localhost:8080/status"
```

**Response (200 OK):**
```json
{
  "name": "sub_client1",
  "leader_port": 5000,
  "state_file": "D:\\Pub_Sub_Systems_DSMS\\subscriber_output\\.state_sub_client1.json",
  "subscriptions": [
    {
      "id": 1,
      "pattern": "brave.server0",
      "created_at": "2026-04-05 12:00:00",
      "file": "D:\\Pub_Sub_Systems_DSMS\\subscriber_output\\brave.server0_2026-04-05_12-00-00_sub1.log",
      "alive": true,
      "topics": {
        "brave.server0": 248
      }
    }
  ]
}
```

### 3. Query Log Entries
**Endpoint:** `GET /logs?topic=<pattern>`

Returns all collected metric records matching an exact topic name, prefix, or suffix.

```bash
# Query exact topic:
curl "http://localhost:8080/logs?topic=brave.server0"

# Suffix aggregation (all apps on server0):
curl "http://localhost:8080/logs?topic=server0"

# Prefix aggregation (all servers running brave):
curl "http://localhost:8080/logs?topic=brave"
```

**Response (200 OK):**
```json
{
  "topic": "server0",
  "entry_count": 2,
  "entries": [
    {
      "topic": "brave.server0",
      "ts": "2026-04-05 12:00:01",
      "data": "cpu=12.4 memory=4.20"
    },
    {
      "topic": "msedge.server0",
      "ts": "2026-04-05 12:00:01",
      "data": "cpu=8.1 memory=2.90"
    }
  ]
}
```

### 4. Unsubscribe
**Endpoint:** `DELETE /subscribe?id=<sub_id>` or `DELETE /subscribe?topic=<pattern>`

Stops the subscription worker thread and flushes its output log file.

```bash
# By subscription ID:
curl -X DELETE "http://localhost:8080/subscribe?id=1"

# By pattern:
curl -X DELETE "http://localhost:8080/subscribe?topic=server0"
```

**Response (200 OK):**
```json
{
  "status": "unsubscribed",
  "stopped": [1]
}
```

### 5. Inspecting Streaming Logs on Disk
Because each subscription outputs directly to disk, you can monitor metrics in real time using native CLI tools:

**Windows PowerShell:**
```powershell
Get-Content -Wait -Tail 10 subscriber_output\brave.server0_*.log
```

**Linux / macOS:**
```bash
tail -f subscriber_output/brave.server0_*.log
```

---

## 12. Automated & Manual Testing

### 12.1 Automated End-to-End Test Suite
The repository includes a comprehensive automated test suite in [`tests/test_dsms.py`](file:///d:/Pub_Sub_Systems_DSMS-main/tests/test_dsms.py).

**How to run:**
1. Start the 3 broker nodes.
2. (Optional) Start `dsms_sub.py` on port 8080 if running the HTTP prefix aggregation test.
3. Run the test suite:

```bash
python tests/test_dsms.py
```

**Test Coverage:**
- **Test 1 — Broker Connectivity:** Verifies all 3 brokers are reachable via TCP.
- **Test 2 — Leader Detection:** Verifies exactly one leader is elected and followers recognize it.
- **Test 3 — NACK Redirection:** Validates followers reply with `NACK` containing the leader's port.
- **Test 4 — Topic Creation:** Validates topic creation via RAFT consensus and commit ACK.
- **Test 5 — Metric Publication:** Verifies publication of metric entries through the leader.
- **Test 6 — Baseline Subscription:** Validates `SUBSCRIBE` from offset 0 returns initial records.
- **Test 7 — Offset-Based Polling:** Verifies reading at `offset=N` returns only newly published data.
- **Test 8 — Multi-Topic Isolation:** Ensures distinct topics maintain independent offsets and logs.
- **Test 9 — Prefix Aggregation:** Validates multi-topic prefix and suffix queries through the subscriber HTTP API.
- **Test 10 — Replication Integrity:** Verifies disk logs across all broker nodes are byte-for-byte identical.

---

## 13. Configuration Reference

All settings reside in [`common/config.py`](file:///d:/Pub_Sub_Systems_DSMS-main/common/config.py):

| Setting | Default | Description |
|---|---|---|
| `BROKERS` | Ports 5000, 5001, 5002 | Cluster network topology (node IDs, hosts, ports) |
| `ELECTION_TIMEOUT_MIN` | `6.0` s | Minimum follower election timeout |
| `ELECTION_TIMEOUT_MAX` | `15.0` s | Maximum follower election timeout |
| `HEARTBEAT_INTERVAL` | `0.75` s | Leader heartbeat frequency |
| `PUBLISH_INTERVAL` | `5` s | Publisher metric collection and broadcast frequency |
| `MAX_RETRIES` | `10` | Maximum retry attempts for operations and leader discovery |
| `RETRY_DELAY` | `1.0` s | Delay between retry attempts |
| `POLL_INTERVAL` | `3` s | Subscriber broker polling frequency |
| `SUBSCRIBER_HTTP_PORT` | `8080` | Default HTTP REST API port |

