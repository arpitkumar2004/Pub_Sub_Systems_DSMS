"""Persistent storage for the three RAFT fields that must survive a crash."""

import json
import os
import threading
import time


class RaftPersister:
    """
    Save and load current_term, voted_for, and raft_log so a node can restart
    without losing RAFT state. These fields are the persistent state listed in
    Figure 2 of the RAFT paper.

    Two files are kept:
      - raft_meta.json   : {"current_term": N, "voted_for": X}
      - raft_log.jsonl   : one JSON object per line, one line per log entry
    """

    def __init__(self, base_dir: str):
        """Create the data directory and remember the two file paths."""
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)
        self._meta_path = os.path.join(base_dir, "raft_meta.json")
        self._log_path = os.path.join(base_dir, "raft_log.jsonl")
        self._lock = threading.Lock()

    def load(self):
        """
        Load persistent fields from disk. If this is a fresh node, return the
        same defaults used by RaftNode on clean startup.

        last_applied is persisted even though RAFT classifies it as volatile.
        In this project, LogStore appends blindly, so replaying already-applied
        entries after restart would duplicate topic-log lines.
        """
        current_term = 0
        voted_for = None
        last_applied = -1

        if os.path.exists(self._meta_path):
            with open(self._meta_path, "r") as f:
                meta = json.load(f)
            current_term = meta.get("current_term", 0)
            voted_for = meta.get("voted_for")
            last_applied = meta.get("last_applied", -1)

        raft_log = []
        if os.path.exists(self._log_path):
            with open(self._log_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        raft_log.append(json.loads(line))

        return current_term, voted_for, last_applied, raft_log

    def save_meta(self, current_term: int, voted_for, last_applied: int = -1):
        """
        Rewrite meta atomically: write temp, fsync, then replace.
        """
        with self._lock:
            tmp_path = self._meta_path + ".tmp"
            with open(tmp_path, "w") as f:
                json.dump(
                    {
                        "current_term": current_term,
                        "voted_for": voted_for,
                        "last_applied": last_applied,
                    },
                    f,
                )
                f.flush()
                os.fsync(f.fileno())
            _atomic_replace_with_retry(tmp_path, self._meta_path)

    def append_entry(self, entry: dict):
        """Append one log entry as a single JSON line and fsync it."""
        with self._lock:
            with open(self._log_path, "a") as f:
                f.write(json.dumps(entry) + "\n")
                f.flush()
                os.fsync(f.fileno())

    def rewrite_log(self, raft_log: list):
        """
        Rewrite the full log when RAFT truncates conflicting tail entries,
        because append-only files cannot remove bytes from the end.
        """
        with self._lock:
            tmp_path = self._log_path + ".tmp"
            with open(tmp_path, "w") as f:
                for entry in raft_log:
                    f.write(json.dumps(entry) + "\n")
                f.flush()
                os.fsync(f.fileno())
            _atomic_replace_with_retry(tmp_path, self._log_path)


def _atomic_replace_with_retry(src: str, dst: str, attempts: int = 25, delay: float = 0.04):
    """
    os.replace is atomic on POSIX, but on Windows it can fail with
    PermissionError (WinError 5) when another process briefly holds dst.
    Retry with short backoff so transient locks do not fail the caller.

    25 attempts x 40 ms gives about 1 second total retry budget.
    """
    for attempt in range(attempts):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            if attempt == attempts - 1:
                raise
            time.sleep(delay)
