# Part 2 — Broker Network Server & Append-Only Log Storage
### Subsystem: Broker Network Server & Append-Only Log Storage
### Files: `broker/broker.py` · `broker/log_store.py` · `common/config.py` · `start_cluster.py`

---

## 1. Responsibilities & Role in DSMS

Part 2 represents the core **network infrastructure and storage engine** of each broker node in the cluster.
It acts as the central coordinator between clients (publishers and subscribers) and the consensus layer (RAFT):

- **Network Gateway:** Listens for incoming TCP connections from publishers, subscribers, and peer brokers, spawning dedicated threads to handle concurrent requests without blocking.
- **Request Routing:** Dispatches write operations (`CREATE_TOPIC`, `METRIC`) to the RAFT consensus engine, handles subscriber read requests (`SUBSCRIBE`, `LIST_TOPICS`), and routes RAFT internal RPCs.
- **Follower Redirection:** Enforces single-leader write authority by rejecting writes on follower nodes with `NACK <leader_port>`, redirecting clients to the active leader.
- **Append-Only Disk Storage (`LogStore`):** Manages per-topic append-only log files on disk, ensuring fast sequential I/O, crash safety, and efficient zero-copy byte-offset seeking.
- **Cluster Topology Management (`config.py`):** Defines the cluster structure (hosts, ports, node IDs) and operational parameters.

---

## 2. Architecture & Interaction Flow

```
                      ┌──────────────────────────────────────────────┐
                      │              broker.py (THIS PART)           │
Publisher   ──TCP──►  │  ├── TCP Server Socket (bind/listen)        │
Subscriber  ──TCP──►  │  ├── Multi-Threaded Handler (_handle_conn)   │
Peer Broker ──TCP──►  │  │                                           │
                      │  ├── Routes writes ────────────────────────► │ ──► RAFT (Part 1)
                      │  ├── Routes RAFT RPCs ─────────────────────► │
                      │  │                                           │
                      │  ├── Serves SUBSCRIBE ──► LogStore.read()   │
                      │  └── Serves LIST_TOPICS ─► LogStore.topics() │
                      └──────────────────────┬───────────────────────┘
                                             │
                                             ▼
                      ┌──────────────────────────────────────────────┐
                      │             log_store.py (THIS PART)         │
                      │  ├── broker_data/node<N>/<topic>.log         │
                      │  ├── Thread-safe append() (sequential write) │
                      │  └── Fast seek(byte_offset) & read()         │
                      └──────────────────────────────────────────────┘
```

---

## 3. Detailed Component Breakdown

### 3.1 `broker/broker.py` — The Broker Process

#### Multi-Threaded TCP Server Architecture
```python
class Broker:
    def __init__(self, node_id: int):
        self.node_id = node_id
        node_cfg = next(b for b in config.BROKERS if b["id"] == node_id)
        self.host = node_cfg["host"]
        self.port = node_cfg["port"]
        
        self.log_store = LogStore(base_dir=f"broker_data/node{node_id}")
        self.raft = RaftNode(node_id, self.log_store, self._send_to_peer)
        
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(50)
```

Each broker process accepts connections in an infinite loop (`serve_forever()`), dispatching each accepted socket to a daemon thread (`_handle_connection`). This ensures high concurrency: slow or polling subscribers cannot starve publishers or inter-broker RAFT heartbeats.

#### Message Dispatching & Handling

1. **`CREATE_TOPIC <topic>`:**
   - Checks `raft.is_leader()`. If false, returns `NACK <leader_port>`.
   - Proposes `"CREATE_TOPIC"` to the RAFT state machine: `idx = self.raft.propose("CREATE_TOPIC", topic)`.
   - Blocks on `self.raft.wait_for_commit(idx, timeout=6.0)`.
   - Upon majority commit, returns `ACK\n` to the client.

2. **`METRIC <topic> <data>`:**
   - Checks `raft.is_leader()`. If false, returns `NACK <leader_port>`.
   - Verifies the topic exists via `self.log_store.topic_exists(topic)`. If not, returns `ERROR topic does not exist\n`.
   - Proposes `"METRIC"` to RAFT and waits for majority commit.
   - Upon commit, returns `ACK\n`.

3. **`SUBSCRIBE <topic> <offset>`:**
   - Checks `raft.is_leader()`. If false, redirects with `NACK <leader_port>` so subscribers always pull from the leader containing the latest committed log tip.
   - Reads bytes from disk: `content_bytes, new_offset = self.log_store.read(topic, byte_offset)`.
   - Replies with header `DATA <new_offset>\n` followed immediately by the raw payload bytes.

4. **`LIST_TOPICS <pattern>` (Dynamic Pattern Matching):**
   - Enables subscribers to discover concrete topics without manual configuration.
   - Evaluates all existing `.log` files in `LogStore`:
     ```python
     for t in self.log_store.all_topics():
         if t == pattern or t.startswith(pattern + ".") or t.endswith("." + pattern):
             matching.append(t)
     ```
   - Matches:
     - Exact topic: `"brave.server0"` → `["brave.server0"]`
     - App prefix: `"brave"` → `["brave.server0", "brave.server1", ...]`
     - Server suffix: `"server0"` → `["brave.server0", "whatsapp.server0", ...]`
   - Returns `TOPICS <json_list>\n`.

5. **RAFT RPC Routing:**
   - Dispatches `VOTE_REQUEST`, `VOTE_GRANTED`, `VOTE_DENIED`, `REPLICATE`, and `REPLICATE_ACK` directly to the `RaftNode`.

---

### 3.2 `broker/log_store.py` — Append-Only Disk Storage

Borrowed from the architecture of modern streaming engines like Apache Kafka, `LogStore` avoids costly random database updates in favor of linear sequential disk appends.

#### Path Sanitization
Topics contain dots (`.`) to separate application and server identifiers. `LogStore` maps topic strings to safe filesystem names:
```python
def _get_path(self, topic: str) -> str:
    safe_name = topic.replace(".", "_")
    return os.path.join(self.base_dir, f"{safe_name}.log")
```
*Example:* `brave.server0` → `broker_data/node0/brave_server0.log`.

#### Thread-Safe Sequential Writes: `append(topic, data, ts)`
```python
entry = json.dumps({"ts": ts, "data": data}) + "\n"
with self._lock:
    with open(path, "a") as f:
        f.write(entry)
```
- Each record is a self-contained JSON line.
- When invoked by RAFT's apply loop, the `ts` timestamp was assigned by the leader upon entry proposal. Because all nodes write the leader's timestamp, the `.log` files on all 3 brokers are identical byte-for-byte.

#### Zero-Copy Offset Seeking: `read(topic, byte_offset)`
```python
def read(self, topic: str, byte_offset: int = 0):
    path = self._get_path(topic)
    if not os.path.exists(path):
        return b"", 0
    with self._lock:
        with open(path, "rb") as f:
            f.seek(byte_offset)
            content = f.read()
        new_offset = byte_offset + len(content)
    return content, new_offset
```
- The subscriber passes its last known `byte_offset`.
- The broker seeks directly to that position, reads all newly appended bytes, and computes `new_offset`.
- If no new data has been written since the last poll, `read()` returns empty bytes (`b""`) and the same offset immediately.

---

### 3.3 `common/config.py` — Shared Cluster Topology

Centralized configuration file imported across all brokers, publishers, and subscribers:

```python
# Cluster topology (hosts and ports for each broker ID)
BROKERS = [
    {"id": 0, "host": "10.145.99.48", "port": 5000},
    {"id": 1, "host": "10.145.99.43", "port": 5001},
    {"id": 2, "host": "10.145.27.212", "port": 5002},
]

# RAFT timing values in seconds
ELECTION_TIMEOUT_MIN = 6      # Minimum follower timeout
ELECTION_TIMEOUT_MAX = 15     # Maximum follower timeout
HEARTBEAT_INTERVAL = 0.75     # Frequency of leader heartbeats

# Publisher behavior
PUBLISH_INTERVAL = 5          # Publish period
MAX_RETRIES = 10              # Retries for topic creation & publishing
RETRY_DELAY = 1.0

# Subscriber behavior
POLL_INTERVAL = 3             # Subscriber poll cycle
SUBSCRIBER_HTTP_PORT = 8080   # HTTP REST API port
```

---

### 3.4 `start_cluster.py` — Local Development Helper

A utility script that starts all 3 broker nodes concurrently using `subprocess`:
- **On Windows:** Opens 3 separate visible command terminal windows (`cmd /k`).
- **On Linux / macOS:** Launches background processes redirecting output to `broker_node0.log`, `broker_node1.log`, `broker_node2.log`.
- Provides clean shutdown handling with `python start_cluster.py --stop`.

---

## 4. How to Demo Part 2 Independently

You can demonstrate broker serving, topic creation, metric commits, offset-based reads, and dynamic topic resolution without running publishers or subscribers.

### Step 1: Start 3 Broker Nodes
```bash
python broker/broker.py --id 0    # Terminal 1
python broker/broker.py --id 1    # Terminal 2
python broker/broker.py --id 2    # Terminal 3
```
Wait ~4 seconds for the leader election to complete.

### Step 2: Test Leader Detection & Write via Raw TCP (Python Interactive Shell)
```python
import socket

# Connect to Node 0 (e.g. port 5000)
s = socket.create_connection(("127.0.0.1", 5000))
s.sendall(b"CREATE_TOPIC database.server0\n")
resp = s.recv(1024).decode()
print("Response:", resp)
s.close()
```
- If Node 0 is LEADER: returns `ACK\n`.
- If Node 0 is FOLLOWER: returns `NACK <leader_port>\n`. Reconnect to that leader port to complete the write.

### Step 3: Publish a Metric Entry
```python
s = socket.create_connection(("127.0.0.1", <LEADER_PORT>))
s.sendall(b"METRIC database.server0 cpu=45.2 memory=60.1\n")
print(s.recv(1024).decode())  # prints: ACK
s.close()
```

### Step 4: Verify Offset-Based Reading
```python
# First read from offset 0:
s = socket.create_connection(("127.0.0.1", <LEADER_PORT>))
s.sendall(b"SUBSCRIBE database.server0 0\n")
header = s.recv(20).decode()
data = s.recv(1024).decode()
print("Header:", header)  # e.g. DATA 75
print("Data:", data)      # contains the JSON line
s.close()

# Second read from offset 75:
s = socket.create_connection(("127.0.0.1", <LEADER_PORT>))
s.sendall(b"SUBSCRIBE database.server0 75\n")
print(s.recv(1024).decode())  # prints: DATA 75 (no new bytes!)
s.close()
```

### Step 5: Test Pattern Topic Listing
```python
s = socket.create_connection(("127.0.0.1", <LEADER_PORT>))
s.sendall(b"LIST_TOPICS server0\n")
print(s.recv(1024).decode())  # prints: TOPICS ["database.server0"]
s.close()
```

### Step 6: Verify Log Replication Across All Nodes
Check the filesystem across all 3 nodes:
```bash
# Each node has the identical file:
cat broker_data/node0/database_server0.log
cat broker_data/node1/database_server0.log
cat broker_data/node2/database_server0.log
```
All files contain the exact same timestamp and metric string.

---

## 5. Integration Points Summary

| Caller / Callee | Method / Interaction | Purpose |
|---|---|---|
| **Publisher → Broker** | `CREATE_TOPIC <topic>` | Registers a new topic on the cluster. |
| **Publisher → Broker** | `METRIC <topic> <data>` | Streams process CPU and memory metrics. |
| **Subscriber → Broker** | `SUBSCRIBE <topic> <offset>` | Fetches new log bytes starting at byte offset. |
| **Subscriber → Broker** | `LIST_TOPICS <pattern>` | Dynamically resolves topics by prefix/suffix. |
| **Broker → RaftNode** | `propose(type, topic, data)` | Submits client requests to RAFT consensus. |
| **Broker → RaftNode** | `wait_for_commit(index)` | Waits for quorum commit before acknowledging client. |
| **RaftNode → LogStore** | `create_topic(topic)` | Creates empty `.log` file on disk upon commit. |
| **RaftNode → LogStore** | `append(topic, data, ts)` | Flushes committed metric to disk with replicated timestamp. |
| **Broker → LogStore** | `read(topic, byte_offset)` | Performs byte-offset seeks for subscribers. |
