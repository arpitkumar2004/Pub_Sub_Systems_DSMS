"""Publisher process that creates topics and publishes metrics to the broker cluster."""

import json
import os
import socket
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common import config, protocol
from publisher.metrics import get_metrics_for_app, get_system_metrics


class Publisher:
    """Read publisher configuration, create topics, and stream metric entries."""

    def __init__(self, apps_json_path: str):
        with open(apps_json_path, "r") as file_handle:
            cfg = json.load(file_handle)

        self.server_id = cfg["server_id"]
        self.apps = cfg["apps"]
        self.topics = [f"{app}.{self.server_id}" for app in self.apps]
        self.topic_leader: dict = {topic: None for topic in self.topics}

        print(f"[PUB] Server ID: {self.server_id}")
        print(f"[PUB] Apps: {self.apps}")
        print(f"[PUB] Topics: {self.topics}")

    def run(self):
        """Create all configured topics and start the publish loop."""
        print("[PUB] Creating topics on broker cluster...")
        self._create_all_topics()
        print("[PUB] All topics created. Starting metric loop...")
        self._publish_loop()

    def _create_all_topics(self):
        """Send CREATE_TOPIC for each configured topic with retries."""
        for topic in self.topics:
            success = False
            for attempt in range(1, config.MAX_RETRIES + 1):
                print(f"[PUB] Creating topic '{topic}' (attempt {attempt}/{config.MAX_RETRIES})")
                response = self._send_request(protocol.encode_create_topic(topic), topic)
                if response == "ACK":
                    print(f"[PUB] Topic '{topic}' created successfully.")
                    success = True
                    break
                if response is None:
                    print(f"[PUB] No response. Retrying in {config.RETRY_DELAY}s...")
                    time.sleep(config.RETRY_DELAY)
                else:
                    print(f"[PUB] Unexpected response: {response}. Retrying...")
                    time.sleep(config.RETRY_DELAY)

            if not success:
                print(f"[PUB] FATAL: Could not create topic '{topic}' after {config.MAX_RETRIES} attempts. Exiting.")
                sys.exit(1)

    def _publish_loop(self):
        """Collect and publish metrics for each configured app forever."""
        while True:
            for app, topic in zip(self.apps, self.topics):
                data = get_metrics_for_app(app)
                if data is None:
                    data = get_system_metrics()
                    print(f"[PUB] '{app}' not found in processes; using system metrics.")

                print(f"[PUB] Sending METRIC | topic={topic} | {data}")
                self._send_metric_with_retry(topic, data)

            time.sleep(config.PUBLISH_INTERVAL)

    def _send_metric_with_retry(self, topic: str, data: str):
        """Send METRIC with retries and leader rediscovery behavior."""
        for _ in range(config.MAX_RETRIES):
            response = self._send_request(protocol.encode_metric(topic, data), topic)
            if response == "ACK":
                return
            if response is None:
                self.topic_leader[topic] = None
                time.sleep(0.5)
            else:
                print(f"[PUB] Unexpected response for METRIC: {response}")
                time.sleep(0.5)

        print(f"[PUB] WARNING: Could not commit metric for '{topic}' after {config.MAX_RETRIES} attempts.")

    def _send_request(self, message: str, topic: str) -> str:
        """Send one request to brokers and return ACK, an error string, or None."""
        conn_timeout = 8.0
        dead_ports = set()
        candidates = self._get_broker_candidates(topic)

        for host, port in candidates:
            if port in dead_ports:
                continue

            try:
                with socket.create_connection((host, port), timeout=conn_timeout) as sock:
                    sock.sendall(message.encode())
                    response_line = protocol.recv_line(sock)

                if response_line is None:
                    continue

                msg_type, payload = protocol.decode_message(response_line)

                if msg_type == "ACK":
                    self.topic_leader[topic] = port
                    return "ACK"

                if msg_type == "NACK":
                    leader_port = payload.get("leader_port", -1)
                    if leader_port > 0 and leader_port not in dead_ports:
                        print(f"[PUB] NACK from port {port} -> leader is port {leader_port}")
                        self.topic_leader[topic] = leader_port
                        leader_host = self._port_to_host(leader_port)
                        try:
                            with socket.create_connection((leader_host, leader_port), timeout=conn_timeout) as redirected_sock:
                                redirected_sock.sendall(message.encode())
                                redirected_response = protocol.recv_line(redirected_sock)
                            if redirected_response:
                                redirected_type, _ = protocol.decode_message(redirected_response)
                                if redirected_type == "ACK":
                                    return "ACK"
                        except (ConnectionRefusedError, socket.timeout, OSError) as redirected_error:
                            print(f"[PUB] Cannot reach redirected leader {leader_host}:{leader_port} - {redirected_error}")
                            dead_ports.add(leader_port)
                            self.topic_leader[topic] = None
                    else:
                        print("[PUB] NACK: no leader elected yet. Waiting...")
                        time.sleep(config.RETRY_DELAY)
                    continue

                return response_line.strip() if response_line else None

            except (ConnectionRefusedError, socket.timeout, OSError) as error:
                print(f"[PUB] Cannot reach broker at {host}:{port} - {error}")
                dead_ports.add(port)
                if self.topic_leader.get(topic) == port:
                    self.topic_leader[topic] = None

        return None

    def _get_broker_candidates(self, topic: str) -> list:
        """Return broker candidates, preferring cached leader first."""
        known_leader_port = self.topic_leader.get(topic)
        candidates = []

        if known_leader_port:
            leader_host = self._port_to_host(known_leader_port)
            candidates.append((leader_host, known_leader_port))

        for broker in config.BROKERS:
            if broker["port"] != known_leader_port:
                candidates.append((broker["host"], broker["port"]))

        return candidates

    def _port_to_host(self, port: int) -> str:
        """Resolve broker host from a configured port."""
        for broker in config.BROKERS:
            if broker["port"] == port:
                return broker["host"]
        return config.BROKERS[0]["host"]


if __name__ == "__main__":
    default_apps_json = os.path.join(os.path.dirname(__file__), "apps.json")

    import argparse

    parser = argparse.ArgumentParser(description="DSMS Publisher that monitors processes and publishes metrics.")
    parser.add_argument(
        "--apps",
        default=default_apps_json,
        help="Path to apps.json configuration file.",
    )
    args = parser.parse_args()

    publisher = Publisher(apps_json_path=args.apps)
    publisher.run()
