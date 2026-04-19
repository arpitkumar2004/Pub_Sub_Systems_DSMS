"""DSMS subscriber service with one worker thread and one output file per subscription.

Each POST /subscribe call creates an independent Subscription with its own
offset map and stop flag. State is persisted in a manifest keyed by subscriber
identity, and a PID lockfile prevents two running processes from sharing the
same identity at the same time.
"""

import argparse
import atexit
import json
import os
import re
import socket
import sys
import tempfile
import threading
import time
from datetime import datetime

from flask import Flask, jsonify, request

try:
    import psutil  # Used only for cross-platform lockfile liveness checks.
except ImportError:
    psutil = None

# Allow imports from the parent 'dsms' package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common import config, protocol


# Output directory and filename helpers.

OUTPUT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "subscriber_output")
)


def _sanitize_pattern(pattern: str) -> str:
    """Turn a pattern into a safe filesystem fragment."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", pattern)


def _build_output_filename(sub_id: int, pattern: str, created_at: datetime) -> str:
    """
    Produce a unique filename per subscription.
    Example: brave.server0_2026-04-04_12-34-56_sub3.log
    """
    ts = created_at.strftime("%Y-%m-%d_%H-%M-%S")
    return f"{_sanitize_pattern(pattern)}_{ts}_sub{sub_id}.log"


# Lockfile and atomic state writer helpers.

def _pid_alive(pid: int) -> bool:
    """Cross-platform 'is this PID still running?' check."""
    if pid <= 0:
        return False
    if psutil is not None:
        try:
            return psutil.pid_exists(pid)
        except Exception:
            return False
    # Fallback: works on POSIX. On Windows, invalid-signal cases raise and are treated as not alive.
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _acquire_lockfile(lock_path: str) -> bool:
    """
    Try to claim a PID lockfile and return True on success.

    If the file already exists and the PID inside belongs to a live
    process, startup is refused so two subscribers cannot silently
    clobber each other's manifest. A stale lock (dead PID) is
    reclaimed automatically.
    """
    if os.path.exists(lock_path):
        try:
            with open(lock_path, "r") as f:
                other_pid = int((f.read() or "0").strip())
        except (OSError, ValueError):
            other_pid = 0
        if _pid_alive(other_pid) and other_pid != os.getpid():
            print(f"[SUB] Another subscriber (PID {other_pid}) already owns "
                  f"the lock '{lock_path}'. Use a different --name or --port, "
                  f"or stop the other process first.")
            return False
    try:
        with open(lock_path, "w") as f:
            f.write(str(os.getpid()))
        return True
    except OSError as e:
        print(f"[SUB] Could not write lockfile '{lock_path}': {e}")
        return False


def _release_lockfile(lock_path: str):
    """Remove the lockfile if it still belongs to this process. Idempotent."""
    try:
        if not os.path.exists(lock_path):
            return
        with open(lock_path, "r") as f:
            pid_in_file = int((f.read() or "0").strip())
        if pid_in_file == os.getpid():
            os.remove(lock_path)
    except (OSError, ValueError):
        pass


def _atomic_write_json(path: str, data: dict, retries: int = 10, delay_sec: float = 0.04):
    """
    Rewrite path atomically: write temp in the same directory, fsync, then replace.
    A short retry loop tolerates transient Windows file locks.
    """
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        last_err = None
        for _ in range(retries):
            try:
                os.replace(tmp_path, path)
                return
            except PermissionError as e:
                last_err = e
                time.sleep(delay_sec)
        raise last_err if last_err else OSError("atomic rename failed")
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


# Subscription implementation: one thread, one file, one offsets map.

class Subscription:
    """
    Background worker created from one /subscribe request.

    Each instance is fully self-contained, so stopping one does not affect any
    other subscription, even another one with the exact same pattern.
    """

    def __init__(
        self,
        sub_id: int,
        pattern: str,
        subscriber: "Subscriber",
        *,
        resume_state: dict = None,
    ):
        """
        Create a fresh subscription when resume_state is None, otherwise resume one.

        resume_state shape (as stored in the manifest):
            {
              "created_at": "YYYY-MM-DD HH:MM:SS",
              "file":       "<absolute path>",
              "offsets":    {topic: byte_offset, ...}
            }
        """
        self.id = sub_id
        self.pattern = pattern
        self._subscriber = subscriber  # Back-reference used for network and persistence.

        if resume_state:
            # Restore creation time so filename conventions remain stable.
            try:
                self.created_at = datetime.strptime(
                    resume_state["created_at"], "%Y-%m-%d %H:%M:%S"
                )
            except (KeyError, ValueError):
                self.created_at = datetime.now()
            self.file_path = resume_state.get("file") or os.path.join(
                OUTPUT_DIR,
                _build_output_filename(self.id, self.pattern, self.created_at),
            )
            self._offsets = dict(resume_state.get("offsets", {}))
            file_mode = "ab"  # Append to existing file; do not truncate.
        else:
            self.created_at = datetime.now()
            self.file_path = os.path.join(
                OUTPUT_DIR,
                _build_output_filename(self.id, self.pattern, self.created_at),
            )
            self._offsets = {}
            file_mode = "wb"  # Fresh file.

        self._offsets_lock = threading.Lock()

        self._fh = open(self.file_path, file_mode)
        self._file_lock = threading.Lock()

        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"sub{self.id}-{pattern}",
        )
        self._thread.start()

    # Control methods.

    def stop(self):
        """Signal the thread to exit; safe to call multiple times."""
        self._stop_event.set()

    def alive(self) -> bool:
        return self._thread.is_alive()

    # Status helpers.

    def snapshot(self) -> dict:
        """Return a JSON-safe description used by /status."""
        with self._offsets_lock:
            offsets = dict(self._offsets)
        return {
            "id":         self.id,
            "pattern":    self.pattern,
            "created_at": self.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "file":       self.file_path,
            "alive":      self.alive(),
            "topics": offsets,  # {concrete_topic: last_known_offset}
        }

    def persist_snapshot(self) -> dict:
        """
        Minimal JSON-safe dict used by the persistence manifest.
        Only the fields needed to reconstruct the subscription on restart.
        """
        with self._offsets_lock:
            offsets = dict(self._offsets)
        return {
            "id":         self.id,
            "pattern":    self.pattern,
            "created_at": self.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "file":       self.file_path,
            "offsets":    offsets,
        }

    # Main polling loop.

    def _run(self):
        """
          Poll cycle: discover topics, register new ones at offset zero, fetch new
          data per topic, append to file, then sleep until the next interval.
        """
        print(f"[SUB#{self.id}] Started | pattern='{self.pattern}' "
              f"| file='{self.file_path}'")

        while not self._stop_event.is_set():
            try:
                self._poll_once()
                # Persist offsets after each successful cycle.
                self._subscriber.request_state_save()
            except Exception as e:
                # Keep running after transient network or parsing failures.
                print(f"[SUB#{self.id}] Poll error (continuing): {e}")

            # _stop_event.wait returns True when stop is requested.
            if self._stop_event.wait(timeout=config.POLL_INTERVAL):
                break

        # Flush and close file on shutdown.
        try:
            with self._file_lock:
                self._fh.flush()
                self._fh.close()
        except OSError:
            pass
        print(f"[SUB#{self.id}] Stopped")

    def _poll_once(self):
        """One iteration of the loop: discover + poll + write."""
        # Discover concrete topics that currently match the pattern.
        concrete = self._subscriber.list_topics_from_broker(self.pattern)

        with self._offsets_lock:
            # Add newly discovered topics at offset zero.
            for t in concrete:
                if t not in self._offsets:
                    self._offsets[t] = 0
                    print(f"[SUB#{self.id}] Tracking new topic '{t}' from offset 0")
            # Snapshot topics for this cycle.
            topics_to_poll = list(self._offsets.items())

        # Pull bytes for each tracked topic.
        for topic, offset in topics_to_poll:
            if self._stop_event.is_set():
                return
            self._fetch_topic(topic, offset)

    def _fetch_topic(self, topic: str, start_offset: int):
        """
        SUBSCRIBE a single concrete topic, write whatever new bytes the
        broker returned to this subscription's file, then advance the
        offset so the next cycle picks up from where this one ended.
        """
        result = self._subscriber.fetch_topic_bytes(topic, start_offset)
        if result is None:
            return  # Broker unreachable in this cycle; retry later.
        new_offset, raw_content = result

        if raw_content:
            self._write_raw_to_file(topic, raw_content)

        with self._offsets_lock:
            self._offsets[topic] = new_offset

    def _write_raw_to_file(self, topic: str, raw_bytes: bytes):
        """
        Tag each JSON line with its source topic and append the block in one write.
        """
        tagged_lines = []
        for line in raw_bytes.decode("utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            tagged = {"topic": topic, **entry}
            tagged_lines.append(json.dumps(tagged))

        if not tagged_lines:
            return
        payload = ("\n".join(tagged_lines) + "\n").encode("utf-8")

        with self._file_lock:
            if self._fh.closed:
                return
            try:
                self._fh.write(payload)
                self._fh.flush()
            except OSError as e:
                print(f"[SUB#{self.id}] Write error: {e}")


# Subscriber: HTTP layer, leader discovery, and subscription registry.

class Subscriber:
    """
    HTTP frontend that owns all Subscription instances.

    Persistent identity comes from `name` (e.g. "port8080" or a user-supplied
    string). Two processes with the same name cannot run concurrently;
    the lockfile enforces this.
    """

    def __init__(
        self,
        http_port: int = config.SUBSCRIBER_HTTP_PORT,
        name: str = None,
    ):
        self.http_port = http_port
        self.name = name or f"port{http_port}"

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Persistence paths are derived from the identity so multiple
        # subscribers on the same machine get independent manifests.
        safe_name = _sanitize_pattern(self.name)
        self._state_path = os.path.join(OUTPUT_DIR, f".state_{safe_name}.json")
        self._lock_path = os.path.join(OUTPUT_DIR, f".state_{safe_name}.lock")

        self._subs: dict = {}  # sub_id -> Subscription
        self._subs_lock = threading.Lock()
        self._next_sub_id = 1

        self._leader_port = None
        self._leader_lock = threading.Lock()

        # Serialize manifest writes so concurrent poll callbacks do not interleave on disk.
        self._state_save_lock = threading.Lock()

        # Lockfile: refuse to start a second process with the same name.
        if not _acquire_lockfile(self._lock_path):
            raise SystemExit(1)
        # Release lock on normal interpreter exit; stale locks are reclaimed on next run.
        atexit.register(_release_lockfile, self._lock_path)
        atexit.register(self._save_state_now)

        self.app = Flask(__name__)
        self._register_routes()

        # Rehydrate previously-saved subscriptions and start their threads.
        self._restore_from_manifest()

    # HTTP routes.

    def _register_routes(self):
        app = self.app

        # POST /subscribe?topic=<pattern>
        @app.route("/subscribe", methods=["POST"])
        def subscribe():
            pattern = request.args.get("topic", "").strip()
            if not pattern:
                return jsonify({"error": "Missing 'topic' query parameter"}), 400

            sub = self._spawn_subscription(pattern)
            self._save_state_now()
            return jsonify({
                "status":          "subscribed",
                "subscription_id": sub.id,
                "pattern":         sub.pattern,
                "file":            sub.file_path,
                "created_at":      sub.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            }), 200

        # DELETE /subscribe?id=<id> or DELETE /subscribe?topic=<pattern>
        @app.route("/subscribe", methods=["DELETE"])
        def unsubscribe():
            raw_id = request.args.get("id", "").strip()
            pattern = request.args.get("topic", "").strip()

            if not raw_id and not pattern:
                return jsonify({
                    "error": "Provide either 'id' (one subscription) "
                             "or 'topic' (every subscription for that pattern)",
                }), 400

            stopped = []

            if raw_id:
                try:
                    sid = int(raw_id)
                except ValueError:
                    return jsonify({"error": f"Invalid id '{raw_id}'"}), 400
                with self._subs_lock:
                    sub = self._subs.pop(sid, None)
                if sub is None:
                    return jsonify({"error": f"No subscription with id={sid}"}), 404
                sub.stop()
                stopped.append(sub.id)
            else:
                with self._subs_lock:
                    ids = [sid for sid, s in self._subs.items() if s.pattern == pattern]
                    subs = [self._subs.pop(sid) for sid in ids]
                for s in subs:
                    s.stop()
                    stopped.append(s.id)

            if stopped:
                self._save_state_now()

            return jsonify({
                "status":  "unsubscribed",
                "stopped": stopped,
            }), 200

        # GET /status
        @app.route("/status", methods=["GET"])
        def status():
            with self._subs_lock:
                subs = [s.snapshot() for s in self._subs.values()]
            return jsonify({
                "name":          self.name,
                "state_file":    self._state_path,
                "leader_port":   self._leader_port,
                "subscriptions": subs,
            }), 200

        # GET /logs?topic=<pattern>
        @app.route("/logs", methods=["GET"])
        def get_logs():
            pattern = request.args.get("topic", "").strip()
            if not pattern:
                return jsonify({"error": "Missing 'topic' query parameter"}), 400

            matched_entries = []
            seen_files = set()
            with self._subs_lock:
                for sub in self._subs.values():
                    seen_files.add(sub.file_path)

            if os.path.exists(OUTPUT_DIR):
                for fname in os.listdir(OUTPUT_DIR):
                    if fname.endswith(".log"):
                        seen_files.add(os.path.join(OUTPUT_DIR, fname))

            for fpath in seen_files:
                if not os.path.exists(fpath):
                    continue
                try:
                    with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                entry = json.loads(line)
                                t = entry.get("topic", "")
                                if (
                                    t == pattern
                                    or t.startswith(pattern + ".")
                                    or t.endswith("." + pattern)
                                    or pattern in t
                                ):
                                    matched_entries.append(entry)
                            except json.JSONDecodeError:
                                continue
                except OSError:
                    continue

            # Deduplicate if multiple files captured the same entry
            unique = []
            seen_keys = set()
            for e in matched_entries:
                key = (e.get("topic"), e.get("ts"), e.get("data"))
                if key not in seen_keys:
                    seen_keys.add(key)
                    unique.append(e)

            unique.sort(key=lambda x: x.get("ts", ""))
            return jsonify({
                "topic": pattern,
                "entry_count": len(unique),
                "entries": unique,
            }), 200


    # Subscription management.

    def _spawn_subscription(self, pattern: str) -> Subscription:
        """Allocate an id and launch a new Subscription thread."""
        with self._subs_lock:
            sub_id = self._next_sub_id
            self._next_sub_id += 1

        # Subscription construction opens the file and starts the thread.
        sub = Subscription(sub_id, pattern, self)

        with self._subs_lock:
            self._subs[sub_id] = sub

        print(f"[SUB] Created subscription #{sub.id} pattern='{pattern}' "
              f"file='{sub.file_path}'")
        return sub

    # Persistence operations.

    def request_state_save(self):
        """
        Called by each Subscription after successful polling to keep the
        manifest aligned with current offsets.
        """
        self._save_state_now()

    def _save_state_now(self):
        """Rewrite the on-disk manifest atomically; best effort."""
        try:
            with self._state_save_lock:
                with self._subs_lock:
                    subs_snapshot = [s.persist_snapshot() for s in self._subs.values()]
                    next_sub_id = self._next_sub_id
                payload = {
                    "version":       1,
                    "name":          self.name,
                    "next_sub_id":   next_sub_id,
                    "saved_at":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "subscriptions": subs_snapshot,
                }
                _atomic_write_json(self._state_path, payload)
        except Exception as e:
            # Persistence failures must not terminate poller threads.
            print(f"[SUB] State save failed ({self._state_path}): {e}")

    def _restore_from_manifest(self):
        """
        Load the manifest (if any) and re-spawn every saved subscription.
        Output files are reopened in append mode and offsets are restored so
        the poll picks up exactly where the previous process stopped.
        """
        if not os.path.exists(self._state_path):
            print(f"[SUB] No prior state at '{self._state_path}' (fresh start)")
            return

        try:
            with open(self._state_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            print(f"[SUB] Could not load state file: {e}. Starting fresh.")
            return

        saved_subs = data.get("subscriptions", [])
        if not saved_subs:
            print(f"[SUB] State file empty (no subscriptions to resume)")
            return

        # Preserve id space so resumed subscriptions keep their ids.
        max_saved_id = max((s.get("id", 0) for s in saved_subs), default=0)
        self._next_sub_id = max(data.get("next_sub_id", max_saved_id + 1),
                                max_saved_id + 1)

        print(f"[SUB] Resuming {len(saved_subs)} subscription(s) from "
              f"'{self._state_path}'")
        for s in saved_subs:
            try:
                sub_id = int(s["id"])
                pattern = s["pattern"]
            except (KeyError, TypeError, ValueError):
                print(f"[SUB] Skipping malformed entry in manifest: {s}")
                continue
            try:
                sub = Subscription(sub_id, pattern, self, resume_state=s)
            except OSError as e:
                # File may have been deleted between runs; fall back to fresh start.
                print(f"[SUB] Could not reopen file for sub#{sub_id} "
                      f"('{s.get('file')}'): {e}.  Re-creating from scratch.")
                sub = Subscription(sub_id, pattern, self)
            with self._subs_lock:
                self._subs[sub.id] = sub
            print(f"[SUB] Resumed sub#{sub.id} pattern='{pattern}' "
                  f"offsets={s.get('offsets', {})}")

    # Broker network helpers shared across all Subscription instances.

    def list_topics_from_broker(self, pattern: str) -> list:
        """
        Ask the RAFT leader for concrete topics matching a pattern.
        Return an empty list if no broker is reachable or nothing matches.
        """
        message = protocol.encode_list_topics(pattern)
        for host, port in self._broker_candidates():
            try:
                with socket.create_connection((host, port), timeout=5.0) as s:
                    s.sendall(message.encode())
                    line = protocol.recv_line(s)
                if line is None:
                    continue
                msg_type, payload = protocol.decode_message(line)

                if msg_type == "NACK":
                    leader_port = payload.get("leader_port", -1)
                    if leader_port > 0:
                        with self._leader_lock:
                            self._leader_port = leader_port
                    continue  # Retry at hinted leader.

                if msg_type == "TOPICS":
                    with self._leader_lock:
                        self._leader_port = port
                    return payload.get("topics", [])

                print(f"[SUB] Unexpected LIST_TOPICS response: {msg_type}")
                return []
            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                print(f"[SUB] LIST_TOPICS cannot reach {host}:{port} - {e}")
                with self._leader_lock:
                    if self._leader_port == port:
                        self._leader_port = None
        return []

    def fetch_topic_bytes(self, topic: str, byte_offset: int):
        """
        Send SUBSCRIBE <topic> <byte_offset> and return (new_offset, raw_bytes).
        Return None when no broker can serve this cycle.
        """
        message = protocol.encode_subscribe(topic, byte_offset)
        for host, port in self._broker_candidates():
            try:
                with socket.create_connection((host, port), timeout=5.0) as s:
                    s.sendall(message.encode())
                    header_line = protocol.recv_line(s)
                    if header_line is None:
                        continue

                    msg_type, payload = protocol.decode_message(header_line)

                    if msg_type == "NACK":
                        leader_port = payload.get("leader_port", -1)
                        if leader_port > 0:
                            with self._leader_lock:
                                self._leader_port = leader_port
                        continue  # Retry at hinted leader.

                    if msg_type != "DATA":
                        print(f"[SUB] Unexpected response type: {msg_type}")
                        continue

                    new_offset = payload["new_offset"]
                    raw_content = b""
                    while True:
                        chunk = s.recv(4096)
                        if not chunk:
                            break
                        raw_content += chunk

                    with self._leader_lock:
                        self._leader_port = port
                    return new_offset, raw_content
            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                print(f"[SUB] Fetch cannot reach {host}:{port} - {e}")
                with self._leader_lock:
                    if self._leader_port == port:
                        self._leader_port = None
        return None

    def _broker_candidates(self) -> list:
        """Known leader first, then the rest of the brokers as fallback."""
        with self._leader_lock:
            lp = self._leader_port

        ordered = []
        if lp:
            host = self._port_to_host(lp)
            ordered.append((host, lp))
        for b in config.BROKERS:
            if b["port"] != lp:
                ordered.append((b["host"], b["port"]))
        return ordered

    def _port_to_host(self, port: int) -> str:
        for b in config.BROKERS:
            if b["port"] == port:
                return b["host"]
        return config.BROKERS[0]["host"]

    # Runtime entrypoint.

    def run(self):
        """Start the Flask HTTP server (blocking)."""
        print(f"[SUB] identity='{self.name}' state='{self._state_path}'")
        print(f"[SUB] HTTP API running at http://127.0.0.1:{self.http_port}")
        print(f"[SUB] Output directory: {OUTPUT_DIR}")
        print(f"[SUB] Subscribe (each call spawns its own thread + file):")
        print(f"       curl -X POST 'http://localhost:{self.http_port}/subscribe?topic=brave.server0'")
        print(f"       curl -X POST 'http://localhost:{self.http_port}/subscribe?topic=server0'")
        print(f"       curl -X POST 'http://localhost:{self.http_port}/subscribe?topic=brave'")
        print(f"[SUB] Unsubscribe by id or by pattern:")
        print(f"       curl -X DELETE 'http://localhost:{self.http_port}/subscribe?id=1'")
        print(f"       curl -X DELETE 'http://localhost:{self.http_port}/subscribe?topic=server0'")
        self.app.run(host="0.0.0.0", port=self.http_port,
                     debug=False, use_reloader=False)


# Entry point.

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DSMS Subscriber - per-subscribe thread + file, persistent across restarts.")
    parser.add_argument(
        "--port", type=int, default=config.SUBSCRIBER_HTTP_PORT,
        help=f"HTTP port for the REST API (default: {config.SUBSCRIBER_HTTP_PORT})",
    )
    parser.add_argument(
        "--name", type=str, default=None,
        help="Persistent identity for this subscriber.  Two processes "
             "with the same --name cannot run concurrently.  Defaults "
             "to 'port<N>' so different ports get independent state.",
    )
    args = parser.parse_args()

    sub = Subscriber(http_port=args.port, name=args.name)
    sub.run()
