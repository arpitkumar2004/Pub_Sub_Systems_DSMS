"""In-memory cache for subscriber topic data and offsets."""

import json
import threading


class LogManager:
    """Store subscription state, offsets, and parsed entries in a thread-safe structure."""

    def __init__(self):
        self.subscriptions: dict = {}
        self._lock = threading.Lock()

    def subscribe(self, topic: str):
        """Create subscription state for a topic if it is not already present."""
        with self._lock:
            if topic not in self.subscriptions:
                self.subscriptions[topic] = {"byte_offset": 0, "entries": []}
                print(f"[LogManager] Subscribed to '{topic}'")

    def unsubscribe(self, topic: str):
        """Remove a topic subscription and its cached entries."""
        with self._lock:
            if topic in self.subscriptions:
                del self.subscriptions[topic]
                print(f"[LogManager] Unsubscribed from '{topic}'")

    def is_subscribed(self, topic: str) -> bool:
        """Return True when topic is currently subscribed."""
        with self._lock:
            return topic in self.subscriptions

    def all_subscribed_topics(self) -> list:
        """Return a list of subscribed topic names."""
        with self._lock:
            return list(self.subscriptions.keys())

    def get_byte_offset(self, topic: str) -> int:
        """Return the stored byte offset for topic, defaulting to zero."""
        with self._lock:
            return self.subscriptions.get(topic, {}).get("byte_offset", 0)

    def update_offset(self, topic: str, new_offset: int):
        """Update the byte offset for a subscribed topic."""
        with self._lock:
            if topic in self.subscriptions:
                self.subscriptions[topic]["byte_offset"] = new_offset

    def add_raw_bytes(self, topic: str, raw_bytes: bytes):
        """Parse JSON lines from broker bytes and append decoded entries for topic."""
        if not raw_bytes:
            return

        lines = raw_bytes.decode("utf-8", errors="replace").splitlines()
        new_entries = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                entry["topic"] = topic
                new_entries.append(entry)
            except json.JSONDecodeError:
                continue

        with self._lock:
            if topic in self.subscriptions:
                self.subscriptions[topic]["entries"].extend(new_entries)

        if new_entries:
            print(f"[LogManager] Added {len(new_entries)} new entries for '{topic}'")

    def get_entries(self, topic_or_prefix: str) -> list:
        """Return entries for exact topic, app prefix, or server suffix, sorted by timestamp."""
        with self._lock:
            result = []
            for topic_name, state in self.subscriptions.items():
                match = (
                    topic_name == topic_or_prefix
                    or topic_name.startswith(topic_or_prefix + ".")
                    or topic_name.endswith("." + topic_or_prefix)
                )
                if match:
                    result.extend(state["entries"])

        result.sort(key=lambda entry: entry.get("ts", ""))
        return result

    def get_all_status(self) -> dict:
        """Return entry counts and byte offsets for each subscribed topic."""
        with self._lock:
            return {
                topic: {
                    "entry_count": len(state["entries"]),
                    "byte_offset": state["byte_offset"],
                }
                for topic, state in self.subscriptions.items()
            }
