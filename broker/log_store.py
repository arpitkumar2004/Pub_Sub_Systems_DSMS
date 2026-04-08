"""Append-only topic log storage for broker nodes."""

import json
import os
import threading
import time


class LogStore:
    """Manage per-topic append-only log files with thread-safe file access."""

    def __init__(self, base_dir: str = "logs"):
        """Initialize the storage directory and lock used for file operations."""
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)
        self._lock = threading.Lock()

    def _get_path(self, topic: str) -> str:
        """Return the file path for a topic by converting dots to underscores."""
        safe_name = topic.replace(".", "_")
        return os.path.join(self.base_dir, f"{safe_name}.log")

    def create_topic(self, topic: str) -> bool:
        """Create an empty topic file if it does not exist and return whether it was newly created."""
        path = self._get_path(topic)
        with self._lock:
            if not os.path.exists(path):
                open(path, "a").close()
                print(f"[LogStore] Created topic file: {path}")
                return True
        return False

    def topic_exists(self, topic: str) -> bool:
        """Return True if the topic file exists on this broker."""
        return os.path.exists(self._get_path(topic))

    def all_topics(self) -> list:
        """Return all known topics by scanning .log files in the storage directory."""
        topics = []
        for file_name in os.listdir(self.base_dir):
            if file_name.endswith(".log"):
                topic = file_name[:-4].replace("_", ".")
                topics.append(topic)
        return topics

    def append(self, topic: str, data: str, ts: str = None):
        """Append one JSON entry to a topic log and use the provided timestamp when available."""
        path = self._get_path(topic)
        if ts is None:
            ts = time.strftime("%Y-%m-%d %H:%M:%S")

        entry = json.dumps({"ts": ts, "data": data}) + "\n"

        with self._lock:
            with open(path, "a") as file_handle:
                file_handle.write(entry)

    def read(self, topic: str, byte_offset: int = 0):
        """Read bytes from a topic file starting at byte_offset and return content with the new offset."""
        path = self._get_path(topic)

        if not os.path.exists(path):
            return b"", 0

        with self._lock:
            with open(path, "rb") as file_handle:
                file_handle.seek(byte_offset)
                content = file_handle.read()
            new_offset = byte_offset + len(content)

        return content, new_offset
