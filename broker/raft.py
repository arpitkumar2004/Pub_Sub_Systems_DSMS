"""RAFT consensus node implementation used by the broker cluster."""

import os
import queue
import random
import sys
import threading
import time

# Allow imports from the parent 'dsms' package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from broker.raft_persister import RaftPersister
from common import config, protocol


class RaftNode:
    """
    RAFT consensus node, with one instance per broker process.
    """

    def __init__(self, node_id: int, log_store, send_to_peer_fn):
        """
        node_id: integer ID of this broker and it must match config.BROKERS
        log_store: LogStore instance used when applying committed entries
        send_to_peer_fn: callable(peer_id, message_string) that delivers a RAFT
                         message to another broker and is provided by broker.py
        """
        self.node_id = node_id
        self.log_store = log_store
        self._send_raw = send_to_peer_fn

        self.peers = [b["id"] for b in config.BROKERS if b["id"] != node_id]
        self.cluster_size = len(config.BROKERS)

        # Reuse log_store's data directory so all persistent node state stays
        # under broker_data/nodeX and restart recovery can reload it together.
        self._persister = RaftPersister(log_store.base_dir)
        (
            self.current_term,
            self.voted_for,
            loaded_last_applied,
            self.raft_log,
        ) = self._persister.load()
        if self.raft_log:
            print(
                f"[RAFT {node_id}] Recovered from disk | term={self.current_term} "
                f"voted_for={self.voted_for} log_len={len(self.raft_log)} "
                f"last_applied={loaded_last_applied}"
            )

        # Anything already applied to topic logs is effectively committed, so
        # commit_index starts from persisted last_applied to avoid duplicates.
        self.last_applied = loaded_last_applied
        self.commit_index = loaded_last_applied

        # These maps are used only when the node is leader.
        self.next_index = {}
        self.match_index = {}

        self.state = "FOLLOWER"
        self.leader_id = None
        self.votes_received = set()

        self._lock = threading.Lock()

        # One queue and sender thread per peer keeps a dead peer isolated.
        self._peer_queues = {}
        for peer_id in self.peers:
            q = queue.Queue()
            self._peer_queues[peer_id] = q
            threading.Thread(
                target=self._peer_send_worker,
                args=(peer_id, q),
                daemon=True,
                name=f"raft-sender-{peer_id}",
            ).start()

        self._timer_deadline = 0.0
        self._reset_timer_locked()

        threading.Thread(
            target=self._election_timer_loop,
            daemon=True,
            name="raft-election-timer",
        ).start()
        threading.Thread(
            target=self._apply_loop,
            daemon=True,
            name="raft-apply",
        ).start()

        print(f"[RAFT {self.node_id}] Started as FOLLOWER | term={self.current_term} | peers={self.peers}")

    def is_leader(self) -> bool:
        """Return True if this node is currently the RAFT leader."""
        with self._lock:
            return self.state == "LEADER"

    def get_leader_port(self):
        """
        Return the TCP port of the current known leader, or -1 if unknown.
        The publisher/subscriber uses this to redirect its request.
        """
        with self._lock:
            if self.leader_id is None:
                return -1
            for b in config.BROKERS:
                if b["id"] == self.leader_id:
                    return b["port"]
            return -1

    def propose(self, entry_type: str, topic: str, data: str = "") -> int:
        """
        Submit a new operation to the RAFT log, and return the index or -1 if this node is not the leader.
        """
        with self._lock:
            if self.state != "LEADER":
                return -1

            new_index = len(self.raft_log)
            entry = {
                "index": new_index,
                "term": self.current_term,
                "type": entry_type,
                "topic": topic,
                "data": data,
                # The leader timestamp is replicated to keep disk output identical across nodes.
                "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            self.raft_log.append(entry)
            # Flush the new entry to disk before replicating it, so even if the
            # leader crashes right after returning the index to the caller, the
            # entry will still be there on restart.
            self._persister.append_entry(entry)
            print(f"[RAFT {self.node_id}] Proposed [{entry_type}] '{topic}' at index {new_index}")

            self._queue_replicate_all()
            return new_index

    def wait_for_commit(self, index: int, timeout: float = 6.0) -> bool:
        """
        Block (spin) until the entry at 'index' is committed or timeout expires.
        Returns True if committed in time, False on timeout.

        Called by broker.py after propose() to know when to send ACK to client.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                if self.commit_index >= index:
                    return True
            time.sleep(0.05)
        return False

    def handle_vote_request(
        self,
        term: int,
        candidate_id: int,
        last_log_index: int,
        last_log_term: int,
    ):
        """
        Process a vote request and grant it only when term and log conditions are valid.
        """
        with self._lock:
            if term > self.current_term:
                self._step_down(term)

            grant = False
            if (term == self.current_term
                    and (self.voted_for is None or self.voted_for == candidate_id)
                    and self._log_is_ok(last_log_index, last_log_term)):

                self.voted_for = candidate_id
                # Persist the vote before replying, so a crash cannot erase a
                # vote that was already granted in this term.
                self._persister.save_meta(self.current_term, self.voted_for, self.last_applied)
                grant = True
                self._reset_timer_locked()
                print(f"[RAFT {self.node_id}] GRANTED vote to Node {candidate_id} (term {term})")
            else:
                reason = []
                if term < self.current_term:
                    reason.append("stale term")
                if self.voted_for not in (None, candidate_id):
                    reason.append(f"already voted for {self.voted_for}")
                if not self._log_is_ok(last_log_index, last_log_term):
                    reason.append("candidate log behind ours")
                print(f"[RAFT {self.node_id}] DENIED vote to Node {candidate_id}: {', '.join(reason)}")

            if grant:
                self._queue_send(
                    candidate_id,
                    protocol.encode_vote_granted(self.current_term, self.node_id),
                )
            else:
                self._queue_send(
                    candidate_id,
                    protocol.encode_vote_denied(self.current_term),
                )

    def handle_vote_granted(self, term: int, voter_id: int):
        """
        Count a granted vote and become leader after reaching majority.
        """
        with self._lock:
            if term > self.current_term:
                self._step_down(term)
                return
            if self.state != "CANDIDATE" or term != self.current_term:
                return

            self.votes_received.add(voter_id)
            total = len(self.votes_received)
            print(f"[RAFT {self.node_id}] Vote from Node {voter_id} | total={total}/{self.cluster_size}")

            if total > self.cluster_size // 2:
                self._become_leader()

    def handle_vote_denied(self, term: int):
        """
        A peer refused to vote for us.  If it revealed a higher term, step down.
        """
        with self._lock:
            if term > self.current_term:
                self._step_down(term)

    def handle_replicate(
        self,
        term: int,
        leader_id: int,
        prev_log_index: int,
        prev_log_term: int,
        entries: list,
        commit_index: int,
    ):
        """
        Process a replicate or heartbeat message from the leader.
        """
        with self._lock:
            if term > self.current_term:
                self._step_down(term)

            if term < self.current_term:
                self._queue_send(
                    leader_id,
                    protocol.encode_replicate_ack(self.current_term, self.node_id, -1, False),
                )
                return

            self.leader_id = leader_id
            if self.state != "FOLLOWER":
                self._step_down(term)
            self._reset_timer_locked()

            # Track whether this call changed the log so we only rewrite when
            # necessary.
            log_dirty = False

            if prev_log_index >= 0:
                if prev_log_index >= len(self.raft_log):
                    self._queue_send(
                        leader_id,
                        protocol.encode_replicate_ack(self.current_term, self.node_id, len(self.raft_log) - 1, False),
                    )
                    return
                if self.raft_log[prev_log_index]["term"] != prev_log_term:
                    self.raft_log = self.raft_log[:prev_log_index]
                    # Persist truncation before ACK so stale tail entries do
                    # not reappear after restart.
                    self._persister.rewrite_log(self.raft_log)
                    self._queue_send(
                        leader_id,
                        protocol.encode_replicate_ack(self.current_term, self.node_id, len(self.raft_log) - 1, False),
                    )
                    return

            if entries:
                first_new_idx = entries[0]["index"]
                if first_new_idx < len(self.raft_log):
                    if self.raft_log[first_new_idx]["term"] != entries[0]["term"]:
                        self.raft_log = self.raft_log[:first_new_idx]
                        log_dirty = True
            else:
                cutoff = prev_log_index + 1
                if len(self.raft_log) > cutoff:
                    self.raft_log = self.raft_log[:cutoff]
                    log_dirty = True

            for entry in entries:
                idx = entry["index"]
                if idx < len(self.raft_log):
                    if self.raft_log[idx]["term"] != entry["term"]:
                        self.raft_log = self.raft_log[:idx]
                        self.raft_log.append(entry)
                        log_dirty = True
                else:
                    self.raft_log.append(entry)
                    log_dirty = True

            # If log content changed, rewrite once before ACK.
            if log_dirty:
                self._persister.rewrite_log(self.raft_log)

            if commit_index > self.commit_index:
                new_commit = min(commit_index, len(self.raft_log) - 1)
                if new_commit > self.commit_index:
                    print(f"[RAFT {self.node_id}] FOLLOWER commit_index advanced {self.commit_index} -> {new_commit} (leader={leader_id})")
                self.commit_index = new_commit

            match_index = len(self.raft_log) - 1
            self._queue_send(
                leader_id,
                protocol.encode_replicate_ack(self.current_term, self.node_id, match_index, True),
            )

    def handle_replicate_ack(
        self,
        term: int,
        follower_id: int,
        match_index: int,
        success: bool,
    ):
        """
        Process replicate acknowledgement from a follower.
        """
        with self._lock:
            if term > self.current_term:
                self._step_down(term)
                return
            if self.state != "LEADER":
                return

            if success:
                prev = self.match_index.get(follower_id, -1)
                self.match_index[follower_id] = max(prev, match_index)
                self.next_index[follower_id] = self.match_index[follower_id] + 1
                self._try_advance_commit()
            else:
                ni = self.next_index.get(follower_id, len(self.raft_log))
                self.next_index[follower_id] = max(0, ni - 1)
                self._queue_replicate_to(follower_id)

    def _step_down(self, new_term: int):
        """
        Revert to follower state when a higher term is observed.
        """
        print(f"[RAFT {self.node_id}] Stepping down to FOLLOWER | term {self.current_term} -> {new_term}")
        self.state = "FOLLOWER"
        self.current_term = new_term
        self.voted_for = None
        # Persist term and cleared vote before continuing so step-down cannot
        # be undone by a crash.
        self._persister.save_meta(self.current_term, self.voted_for, self.last_applied)
        self._reset_timer_locked()

    def _start_election(self):
        """
        Transition to candidate state and broadcast vote requests to peers.
        """
        self.state = "CANDIDATE"
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self.leader_id = None
        # Persist term and vote before sending requests so restart cannot cause
        # a double vote in the same term.
        self._persister.save_meta(self.current_term, self.voted_for, self.last_applied)
        self._reset_timer_locked()

        last_log_index = len(self.raft_log) - 1
        last_log_term = self.raft_log[-1]["term"] if self.raft_log else 0

        msg = protocol.encode_vote_request(
            self.current_term,
            self.node_id,
            last_log_index,
            last_log_term,
        )

        print(f"[RAFT {self.node_id}] Starting ELECTION | term={self.current_term}")
        for peer_id in self.peers:
            self._queue_send(peer_id, msg)

    def _become_leader(self):
        """
        Transition to leader state, initialize replication state, and start heartbeats.
        """
        print(f"[RAFT {self.node_id}] *** BECAME LEADER | term={self.current_term} ***")
        self.state = "LEADER"
        self.leader_id = self.node_id

        log_len = len(self.raft_log)
        for peer_id in self.peers:
            self.next_index[peer_id] = log_len
            self.match_index[peer_id] = -1

        threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name="raft-heartbeat",
        ).start()

        # Append a no-op entry immediately after leadership change so commit
        # progression stays consistent.
        no_op = {
            "index": len(self.raft_log),
            "term": self.current_term,
            "type": "NO_OP",
            "topic": "",
            "data": "",
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        self.raft_log.append(no_op)
        # Same reasoning as in propose(): persist before replicating.
        self._persister.append_entry(no_op)
        print(f"[RAFT {self.node_id}] Appended NO_OP at index {no_op['index']} (term={self.current_term})")
        self._queue_replicate_all()

    def _election_timer_loop(self):
        """
        Start a new election when the timeout expires without a leader heartbeat.
        """
        while True:
            time.sleep(0.1)
            with self._lock:
                if self.state == "LEADER":
                    self._reset_timer_locked()
                    continue
                if time.time() >= self._timer_deadline:
                    self._start_election()

    def _heartbeat_loop(self):
        """
        Send heartbeats and pending entries while this node remains leader.
        """
        while True:
            time.sleep(config.HEARTBEAT_INTERVAL)
            with self._lock:
                if self.state != "LEADER":
                    return
                self._queue_replicate_all()

    def _apply_loop(self):
        """
        Apply committed entries to LogStore in index order.
        """
        # Keep this loop resilient so one transient I/O failure does not stop
        # all future apply progress.
        while True:
            try:
                time.sleep(0.05)
                with self._lock:
                    applied_any = False
                    while self.last_applied < self.commit_index:
                        self.last_applied += 1
                        entry = self.raft_log[self.last_applied]
                        self._apply_entry(entry)
                        applied_any = True
                    # Persist last_applied once per batch so restart does not
                    # reapply entries that are already on disk.
                    if applied_any:
                        self._persister.save_meta(
                            self.current_term, self.voted_for, self.last_applied
                        )
            except Exception as exc:
                print(f"[RAFT {self.node_id}] apply-loop error (continuing): {exc}")

    def _peer_send_worker(self, peer_id: int, q: "queue.Queue"):
        """
        Send messages to one peer and collapse queue backlog so only the latest pending message is transmitted.
        """
        while True:
            peer_id_x, message = q.get()
            while True:
                try:
                    peer_id_x, message = q.get_nowait()
                except queue.Empty:
                    break
            try:
                self._send_raw(peer_id, message)
            except Exception:
                pass

    def _reset_timer_locked(self):
        """
        Set a random election deadline while holding the lock.
        """
        timeout = random.uniform(
            config.ELECTION_TIMEOUT_MIN, config.ELECTION_TIMEOUT_MAX)
        self._timer_deadline = time.time() + timeout

    def _queue_send(self, peer_id: int, message: str):
        """
        Enqueue a message for the peer-specific sender queue.
        """
        q = self._peer_queues.get(peer_id)
        if q is not None:
            q.put((peer_id, message))

    def _queue_replicate_all(self):
        """Send REPLICATE to every follower. Called with lock held."""
        for peer_id in self.peers:
            self._queue_replicate_to(peer_id)

    def _queue_replicate_to(self, peer_id: int):
        """
        Build and enqueue a REPLICATE message for one follower.
        """
        next_idx = self.next_index.get(peer_id, len(self.raft_log))

        entries_to_send = self.raft_log[next_idx:]

        prev_idx = next_idx - 1
        prev_term = (
            self.raft_log[prev_idx]["term"]
            if 0 <= prev_idx < len(self.raft_log)
            else 0
        )

        msg = protocol.encode_replicate(
            self.current_term,
            self.node_id,
            prev_idx,
            prev_term,
            entries_to_send,
            self.commit_index,
        )
        self._queue_send(peer_id, msg)

    def _try_advance_commit(self):
        """
        Advance commit_index when a majority has replicated a current-term entry.
        """
        if not self.raft_log:
            return

        for n in range(len(self.raft_log) - 1, self.commit_index, -1):
            if self.raft_log[n]["term"] != self.current_term:
                continue

            count = 1
            for peer_id in self.peers:
                if self.match_index.get(peer_id, -1) >= n:
                    count += 1

            if count > self.cluster_size // 2:
                print(f"[RAFT {self.node_id}] COMMITTED up to index {n}")
                self.commit_index = n
                break

    def _log_is_ok(self, candidate_last_index: int, candidate_last_term: int) -> bool:
        """
        Return True when the candidate log is at least as up-to-date as this node log.
        """
        our_last_term = self.raft_log[-1]["term"] if self.raft_log else 0
        our_last_index = len(self.raft_log) - 1

        if candidate_last_term != our_last_term:
            return candidate_last_term > our_last_term
        return candidate_last_index >= our_last_index

    def _apply_entry(self, entry: dict):
        """
        Apply one committed RAFT entry to LogStore.
        """
        entry_type = entry.get("type")
        topic = entry.get("topic", "")
        data = entry.get("data", "")

        if entry_type == "NO_OP":
            return

        if entry_type == "CREATE_TOPIC":
            created = self.log_store.create_topic(topic)
            if created:
                print(f"[RAFT {self.node_id}] Applied CREATE_TOPIC -> '{topic}'")

        elif entry_type == "METRIC":
            # Create the topic on demand if local files were removed while the
            # committed log still references it.
            if not self.log_store.topic_exists(topic):
                self.log_store.create_topic(topic)
                print(f"[RAFT {self.node_id}] Auto-created missing topic '{topic}' before applying METRIC")
            # Reuse the leader timestamp so all nodes write the same bytes.
            ts = entry.get("ts")
            self.log_store.append(topic, data, ts=ts)
