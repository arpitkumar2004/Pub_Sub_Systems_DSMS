"""Main broker process for DSMS."""

import argparse
import os
import socket
import sys
import threading

# Allow imports from the parent package regardless of current working directory.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from broker.log_store import LogStore
from broker.raft import RaftNode
from common import config, protocol


class Broker:
    """One broker node that combines TCP serving, RAFT, and local storage."""

    def __init__(self, node_id: int):
        """Initialize broker network endpoints, storage, and RAFT node."""
        self.node_id = node_id

        node_cfg = next(b for b in config.BROKERS if b["id"] == node_id)
        self.host = node_cfg["host"]
        self.port = node_cfg["port"]

        data_dir = os.path.join(
            os.path.dirname(__file__),
            "..",
            "broker_data",
            f"node{node_id}",
        )
        self.log_store = LogStore(base_dir=data_dir)

        self.raft = RaftNode(
            node_id=node_id,
            log_store=self.log_store,
            send_to_peer_fn=self._send_to_peer,
        )

        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(50)
        print(f"[Broker {node_id}] Listening on {self.host}:{self.port}")

    def serve_forever(self):
        """Accept incoming connections and dispatch one handler thread per connection."""
        print(f"[Broker {self.node_id}] Ready to accept connections.")
        while True:
            try:
                conn, addr = self.server_sock.accept()
                thread = threading.Thread(
                    target=self._handle_connection,
                    args=(conn, addr),
                    daemon=True,
                )
                thread.start()
            except KeyboardInterrupt:
                print(f"\n[Broker {self.node_id}] Shutting down.")
                break
            except Exception as exc:
                print(f"[Broker {self.node_id}] Accept error: {exc}")

    def _handle_connection(self, conn: socket.socket, addr):
        """Read one message from a connection, route it, and return a response when needed."""
        try:
            conn.settimeout(5.0)
            line = protocol.recv_line(conn)
            if not line:
                return

            msg_type, payload = protocol.decode_message(line)
            if msg_type is None:
                return

            if msg_type == "CREATE_TOPIC":
                self._handle_create_topic(conn, payload["topic"])
            elif msg_type == "METRIC":
                self._handle_metric(conn, payload["topic"], payload["data"])
            elif msg_type == "SUBSCRIBE":
                self._handle_subscribe(conn, payload["topic"], payload["offset"])
            elif msg_type == "LIST_TOPICS":
                self._handle_list_topics(conn, payload["pattern"])
            elif msg_type == "VOTE_REQUEST":
                self.raft.handle_vote_request(
                    payload["term"],
                    payload["candidate_id"],
                    payload["last_log_index"],
                    payload["last_log_term"],
                )
            elif msg_type == "VOTE_GRANTED":
                self.raft.handle_vote_granted(payload["term"], payload["voter_id"])
            elif msg_type == "VOTE_DENIED":
                self.raft.handle_vote_denied(payload["term"])
            elif msg_type == "REPLICATE":
                self.raft.handle_replicate(
                    payload["term"],
                    payload["leader_id"],
                    payload["prev_log_index"],
                    payload["prev_log_term"],
                    payload["entries"],
                    payload["commit_index"],
                )
            elif msg_type == "REPLICATE_ACK":
                self.raft.handle_replicate_ack(
                    payload["term"],
                    payload["follower_id"],
                    payload["match_index"],
                    payload["success"],
                )
            else:
                conn.sendall(f"ERROR unknown message type: {msg_type}\n".encode())
        except Exception as exc:
            print(f"[Broker {self.node_id}] Error handling connection from {addr}: {exc}")
        finally:
            conn.close()

    def _handle_create_topic(self, conn: socket.socket, topic: str):
        """Handle CREATE_TOPIC by proposing through RAFT and replying after commit."""
        if not self.raft.is_leader():
            leader_port = self.raft.get_leader_port()
            conn.sendall(protocol.encode_nack(leader_port).encode())
            return

        # Topic creation is always routed through RAFT so all replicas receive the same command stream.
        idx = self.raft.propose("CREATE_TOPIC", topic)
        if idx < 0:
            conn.sendall(protocol.encode_nack(self.raft.get_leader_port()).encode())
            return

        committed = self.raft.wait_for_commit(idx)
        if committed:
            print(f"[Broker {self.node_id}] CREATE_TOPIC '{topic}' committed.")
            conn.sendall(protocol.encode_ack().encode())
        else:
            conn.sendall(b"ERROR topic creation timed out\n")

    def _handle_metric(self, conn: socket.socket, topic: str, data: str):
        """Handle METRIC by validating the topic, proposing to RAFT, and replying after commit."""
        if not self.raft.is_leader():
            leader_port = self.raft.get_leader_port()
            conn.sendall(protocol.encode_nack(leader_port).encode())
            return

        if not self.log_store.topic_exists(topic):
            conn.sendall(f"ERROR topic '{topic}' does not exist\n".encode())
            return

        idx = self.raft.propose("METRIC", topic, data)
        if idx < 0:
            conn.sendall(protocol.encode_nack(self.raft.get_leader_port()).encode())
            return

        committed = self.raft.wait_for_commit(idx)
        if committed:
            conn.sendall(protocol.encode_ack().encode())
        else:
            conn.sendall(b"ERROR metric commit timed out\n")

    def _handle_subscribe(self, conn: socket.socket, topic: str, byte_offset: int):
        """Handle SUBSCRIBE and return a DATA header followed by raw log bytes."""
        if not self.raft.is_leader():
            leader_port = self.raft.get_leader_port()
            conn.sendall(protocol.encode_nack(leader_port).encode())
            return

        content_bytes, new_offset = self.log_store.read(topic, byte_offset)
        conn.sendall(protocol.encode_data_header(new_offset))
        if content_bytes:
            conn.sendall(content_bytes)

    def _handle_list_topics(self, conn: socket.socket, pattern: str):
        """
        Resolve a pattern to the concrete topic files that match it.

        Matching rules (same as subscriber-side aggregation):
          - exact:  "brave.server0"  -> ["brave.server0"] if it exists
          - prefix: "brave"          -> all topics starting with "brave."
          - suffix: "server0"        -> all topics ending   with ".server0"

        Only the leader serves this so subscribers always see the newest
        committed topic set (followers lag a heartbeat behind).
        """
        if not self.raft.is_leader():
            leader_port = self.raft.get_leader_port()
            conn.sendall(protocol.encode_nack(leader_port).encode())
            return

        pattern = pattern.strip()
        matching = []
        for t in self.log_store.all_topics():
            if (
                t == pattern
                or t.startswith(pattern + ".")
                or t.endswith("." + pattern)
            ):
                matching.append(t)

        conn.sendall(protocol.encode_topics_response(matching).encode())

    def _send_to_peer(self, peer_id: int, message: str):
        """Send one message to a peer broker using a short-lived TCP connection."""
        peer_cfg = next((b for b in config.BROKERS if b["id"] == peer_id), None)
        if peer_cfg is None:
            return

        try:
            with socket.create_connection((peer_cfg["host"], peer_cfg["port"]), timeout=1.0) as sock:
                sock.sendall(message.encode())
        except (ConnectionRefusedError, socket.timeout, OSError):
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start a DSMS broker node.")
    parser.add_argument(
        "--id",
        type=int,
        required=True,
        help="Broker node ID that must exist in common/config.py BROKERS.",
    )
    args = parser.parse_args()

    if args.id not in [b["id"] for b in config.BROKERS]:
        print(f"ERROR: node id {args.id} not found in config.BROKERS")
        sys.exit(1)

    broker = Broker(node_id=args.id)
    broker.serve_forever()
