# Part 1 — RAFT Consensus & Wire Protocol
### Subsystem: Consensus & Wire Protocol
### Files: `broker/raft.py` · `broker/raft_persister.py` · `common/protocol.py`

---

## 1. Responsibilities & Role in DSMS

Part 1 is the core consensus engine and communication foundation of the entire distributed system.
Without this subsystem, brokers cannot agree on state transitions, leader status, or log entry ordering across independent machines.

RAFT answers the fundamental distributed systems problem:
> *"When multiple broker nodes receive concurrent write requests and network or machine failures occur, how do we guarantee that all functioning replicas store the exact same entries in the exact same order with zero data loss or split-brain divergence?"*

This subsystem implements:
1. **The RAFT Consensus Algorithm**: Leader election, randomized timers, heartbeat broadcasting, log replication, and safe commit progression.
2. **State Persistence (`RaftPersister`)**: Crash durability for terms, granted votes, applied indices, and uncommitted/committed RAFT logs.
3. **Dedicated Inter-Node RPC Workers**: Queued, non-blocking network transmission that isolates dead or lagging peers.
4. **The Wire Protocol (`protocol.py`)**: Serialization, deserialization, and socket streaming helpers for all client-broker and peer-peer messages.

---

## 2. Architecture & Interaction Flow

```
   Client (PUB / SUB)
           │
           │ TCP Requests (CREATE_TOPIC, METRIC, SUBSCRIBE, LIST_TOPICS)
           ▼
┌─────────────────────────────────────────────────────────────┐
│  Broker Server (Part 2 — Storage & Network)                 │
│    ├── Calls raft.is_leader()                               │
│    ├── Calls raft.propose(type, topic, data)                │
│    └── Calls raft.wait_for_commit(index)                    │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│  RAFT Consensus Engine (Part 1 — THIS SUBSYSTEM)            │
│  ├── RaftNode: State machine (FOLLOWER/CANDIDATE/LEADER)    │
│  ├── RaftPersister: Durability (raft_meta.json, raft_log)   │
│  ├── Peer Queues: Dedicated sender threads per peer         │
│  └── Apply Loop: Drives committed records to LogStore       │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼ protocol.py messages over TCP
┌─────────────────────────────────────────────────────────────┐
│  Peer Brokers (Node 1, Node 2)                              │
│  ├── VOTE_REQUEST / VOTE_GRANTED / VOTE_DENIED              │
│  └── REPLICATE / REPLICATE_ACK                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Detailed Component Breakdown

### 3.1 `common/protocol.py` — The Wire Protocol

All messages are plain-text lines terminated by `\n` using UTF-8 encoding.

#### Client ↔ Broker Messages
| Command | Format | Semantics |
|---|---|---|
| `CREATE_TOPIC` | `CREATE_TOPIC <topic>\n` | Client requests creation of a topic log. |
| `METRIC` | `METRIC <topic> <data>\n` | Client publishes metric data. |
| `ACK` | `ACK\n` | Broker confirms durability/commit of write. |
| `NACK` | `NACK <leader_port>\n` | Sent by follower; redirects client to known leader. |
| `SUBSCRIBE` | `SUBSCRIBE <topic> <offset>\n` | Subscriber requests data starting at byte offset. |
| `DATA` | `DATA <new_offset>\n<raw_bytes>` | Header containing advanced offset followed by log bytes. |
| `LIST_TOPICS` | `LIST_TOPICS <pattern>\n` | Subscriber requests matching topic names. |
| `TOPICS` | `TOPICS <json_list>\n` | Broker leader returns matching topic array. |

#### RAFT Internal RPCs
- **`VOTE_REQUEST`**:
  `VOTE_REQUEST {"term": T, "candidate_id": ID, "last_log_index": I, "last_log_term": LT}\n`
- **`VOTE_GRANTED`**:
  `VOTE_GRANTED {"term": T, "voter_id": ID}\n`
- **`VOTE_DENIED`**:
  `VOTE_DENIED {"term": T}\n`
- **`REPLICATE`** (Also acts as periodic heartbeat when `entries=[]`):
  `REPLICATE {"term": T, "leader_id": ID, "prev_log_index": PI, "prev_log_term": PT, "entries": [...], "commit_index": CI}\n`
- **`REPLICATE_ACK`**:
  `REPLICATE_ACK {"term": T, "follower_id": ID, "match_index": MI, "success": true/false}\n`

#### Critical Network Helper: `recv_line(sock)`
Reads from the TCP stream one byte at a time until encountering `\n`. This ensures that variable-length text commands are framed accurately without consuming trailing bytes belonging to payload data or future pipelined messages.

---

### 3.2 `broker/raft_persister.py` — Crash-Proof Persistence

Per Figure 2 of the RAFT specification (Ongaro & Ousterhout), certain node states must survive machine reboot or process crashes to prevent duplicate votes or term confusion:

1. `current_term`: Latest term server has seen.
2. `voted_for`: Candidate ID that received vote in current term.
3. `raft_log`: Every log entry proposed or replicated.

#### DSMS Enhancement: Persisting `last_applied`
In addition to standard RAFT volatile state, `RaftPersister` persists `last_applied`. In DSMS, the underlying `LogStore` is strictly append-only. If a restarted broker re-applied already applied entries from index 0 upon reboot, topic `.log` files would contain duplicated lines. Persisting `last_applied` prevents replaying committed entries into the topic logs.

#### Files Maintained:
- `broker_data/node<id>/raft_meta.json`: Stores `{"current_term": T, "voted_for": V, "last_applied": A}`.
- `broker_data/node<id>/raft_log.jsonl`: Append-only JSON Lines, one entry per line.

#### Atomic Replacements & Windows Locking:
```python
def _atomic_replace_with_retry(src: str, dst: str, attempts: int = 25, delay: float = 0.04):
    for attempt in range(attempts):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            if attempt == attempts - 1:
                raise
            time.sleep(delay)
```
On Windows, `os.replace` can trigger `PermissionError` (WinError 5) if another thread or anti-virus momentarily holds an open read handle on the destination file. A retry loop with backoff ensures atomic file replacements succeed reliably without crashing the broker.

---

### 3.3 `broker/raft.py` — The RAFT State Machine

#### The Three Node States:
```
           ┌──────────────┐   Election Timeout    ┌──────────────┐
           │   FOLLOWER   ├──────────────────────►│  CANDIDATE   │
           └──────┬───────┘   (No Heartbeats)     └──────┬───────┘
                  ▲                                      │
                  │ Higher Term Seen                     │ Majority Votes
                  │                                      ▼
                  └───────────────────────────────┌──────────────┐
                                                  │    LEADER    │
                                                  └──────────────┘
```

1. **FOLLOWER**:
   - Passive listener.
   - Resets election timer whenever a valid `REPLICATE` (or heartbeat) is received from the leader.
   - If timer expires (`6.0s` – `15.0s`), transitions to `CANDIDATE`.

2. **CANDIDATE**:
   - Increments `current_term` and votes for itself.
   - Saves term and vote to disk via `_persister.save_meta()`.
   - Broadcasts `VOTE_REQUEST` to all peers.
   - If `votes_received > cluster_size // 2`, transitions to `LEADER`.

3. **LEADER**:
   - Sole node permitted to propose new log entries.
   - Broadcasts `REPLICATE` heartbeats every `0.75s` (`HEARTBEAT_INTERVAL`).
   - Maintains per-peer `next_index` and `match_index`.
   - Commits entries once a majority of peers acknowledge.

#### Critical RAFT Cornerstones in DSMS:

##### 1. The NO_OP Log Entry Fix (Section 5.4.2)
Upon winning an election, the new leader immediately appends a `NO_OP` log entry and replicates it.
- **Why?** RAFT forbids a leader from directly committing log entries from older terms by counting replicas. An entry from an older term is only committed when an entry from the current term is committed.
- **Result:** The `NO_OP` entry ensures that older, pending entries are immediately committed across the cluster, preventing disk log divergence and ensuring byte offsets remain stable across failovers.

##### 2. Deterministic Leader Timestamps
When `propose()` is called on the leader, the entry is stamped with:
```python
"ts": time.strftime("%Y-%m-%d %H:%M:%S")
```
This timestamp is replicated inside the RAFT log entry to all followers. When nodes apply the entry to disk, all broker nodes write the exact same timestamp string, guaranteeing byte-for-byte identical topic logs.

##### 3. Dedicated Per-Peer Worker Queues
```python
self._peer_queues = {}
for peer_id in self.peers:
    q = queue.Queue()
    self._peer_queues[peer_id] = q
    threading.Thread(target=self._peer_send_worker, args=(peer_id, q), daemon=True).start()
```
Network I/O to peers is completely decoupled from the main RAFT lock. If one follower crashes or stalls on network I/O, its dedicated sender worker thread absorbs the delay without blocking heartbeats or replications to healthy nodes.

---

## 4. How to Demo Part 1 Independently

You can validate RAFT consensus and election without starting publishers or subscribers.

### Step 1: Launch 3 Brokers in Separate Terminals
```bash
python broker/broker.py --id 0    # Terminal 1
python broker/broker.py --id 1    # Terminal 2
python broker/broker.py --id 2    # Terminal 3
```

### Step 2: Observe Election & Leader Heartbeats
Within ~6–15 seconds, one node wins the election:
```
[RAFT 0] Started as FOLLOWER | term=0 | peers=[1, 2]
[RAFT 0] Starting ELECTION | term=1
[RAFT 0] Vote from Node 1 | total=2/3
[RAFT 0] *** BECAME LEADER | term=1 ***
[RAFT 0] Appended NO_OP at index 0 (term=1)
```

### Step 3: Test Dynamic Leader Failover
1. Identify the leader terminal (e.g., Node 0) and press `Ctrl+C`.
2. Observe Nodes 1 and 2 detect heartbeat loss:
```
[RAFT 1] Starting ELECTION | term=2
[RAFT 1] Vote from Node 2 | total=2/3
[RAFT 1] *** BECAME LEADER | term=2 ***
```
3. A new leader is elected with no split-brain.

### Step 4: Reconnect the Dead Node
Restart Node 0:
```bash
python broker/broker.py --id 0
```
Node 0 loads its persistent state from `raft_meta.json` and `raft_log.jsonl`, discovers the higher term (term 2), steps down to FOLLOWER, and catches up on missing entries via leader replication.

---

## 5. Integration Points Summary

| Caller / Callee | Method / Interaction | Purpose |
|---|---|---|
| **Broker → RaftNode** | `raft.is_leader()` | Verifies whether this node can accept client writes. |
| **Broker → RaftNode** | `raft.get_leader_port()` | Obtains the leader's TCP port for `NACK` redirection. |
| **Broker → RaftNode** | `raft.propose(type, topic, data)` | Submits a `CREATE_TOPIC` or `METRIC` operation to the cluster. |
| **Broker → RaftNode** | `raft.wait_for_commit(index)` | Blocks until the proposed index achieves majority commit. |
| **Broker → RaftNode** | `raft.handle_vote_request(...)` | Dispatches incoming candidate vote requests. |
| **Broker → RaftNode** | `raft.handle_replicate(...)` | Dispatches incoming replication packets and heartbeats. |
| **RaftNode → LogStore** | `log_store.create_topic(topic)` | Invoked by `_apply_entry()` when topic creation commits. |
| **RaftNode → LogStore** | `log_store.append(topic, data, ts)` | Invoked by `_apply_entry()` when metric commits. |
| **RaftNode → RaftPersister** | `_persister.save_meta(...)` | Persists updated term, vote, and last applied index. |
| **RaftNode → RaftPersister** | `_persister.append_entry(entry)` | Persists newly proposed or replicated log entries. |
