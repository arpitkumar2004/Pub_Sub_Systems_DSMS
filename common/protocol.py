"""Encoding and decoding utilities for DSMS TCP messages."""

import json


def encode_create_topic(topic: str) -> str:
    """Return a CREATE_TOPIC request."""
    return f"CREATE_TOPIC {topic}\n"


def encode_metric(topic: str, data: str) -> str:
    """Return a METRIC request."""
    return f"METRIC {topic} {data}\n"


def encode_ack() -> str:
    """Return an ACK response."""
    return "ACK\n"


def encode_nack(leader_port) -> str:
    """Return a NACK response and include the known leader port or -1."""
    return f"NACK {leader_port}\n"


def encode_subscribe(topic: str, byte_offset: int) -> str:
    """Return a SUBSCRIBE request with topic and byte offset."""
    return f"SUBSCRIBE {topic} {byte_offset}\n"


def encode_data_header(new_offset: int) -> bytes:
    """Return the DATA header that precedes raw payload bytes."""
    return f"DATA {new_offset}\n".encode()


def encode_list_topics(pattern: str) -> str:
    """
    Return a LIST_TOPICS request. The pattern may be an exact topic name
    (e.g. brave.server0), a bare app name (e.g. brave), or a bare server
    name (e.g. server0). The broker resolves it using the same
    exact/prefix/suffix rules the subscriber used before.
    """
    return f"LIST_TOPICS {pattern}\n"


def encode_topics_response(topics: list) -> str:
    """Return a TOPICS response carrying the broker's matching topic list."""
    return f"TOPICS {json.dumps(topics)}\n"


def encode_vote_request(
    term: int,
    candidate_id: int,
    last_log_index: int,
    last_log_term: int,
) -> str:
    """Return a VOTE_REQUEST message with candidate term and log tip."""
    payload = json.dumps({
        "term": term,
        "candidate_id": candidate_id,
        "last_log_index": last_log_index,
        "last_log_term": last_log_term,
    })
    return f"VOTE_REQUEST {payload}\n"


def encode_vote_granted(term: int, voter_id: int) -> str:
    """Return a VOTE_GRANTED message."""
    return f"VOTE_GRANTED {json.dumps({'term': term, 'voter_id': voter_id})}\n"


def encode_vote_denied(term: int) -> str:
    """Return a VOTE_DENIED message."""
    return f"VOTE_DENIED {json.dumps({'term': term})}\n"


def encode_replicate(
    term: int,
    leader_id: int,
    prev_log_index: int,
    prev_log_term: int,
    entries: list,
    commit_index: int,
) -> str:
    """Return a REPLICATE message and use an empty entries list for heartbeat traffic."""
    payload = json.dumps({
        "term": term,
        "leader_id": leader_id,
        "prev_log_index": prev_log_index,
        "prev_log_term": prev_log_term,
        "entries": entries,
        "commit_index": commit_index,
    })
    return f"REPLICATE {payload}\n"


def encode_replicate_ack(
    term: int,
    follower_id: int,
    match_index: int,
    success: bool,
) -> str:
    """Return a REPLICATE_ACK message from follower to leader."""
    payload = json.dumps({
        "term": term,
        "follower_id": follower_id,
        "match_index": match_index,
        "success": success,
    })
    return f"REPLICATE_ACK {payload}\n"


def decode_message(line: str):
    """Parse one newline-terminated message and return (msg_type, payload)."""
    if not line:
        return None, None

    line = line.strip()
    if not line:
        return None, None

    parts = line.split(" ", 1)
    msg_type = parts[0].upper()
    rest = parts[1] if len(parts) > 1 else ""

    if msg_type == "ACK":
        return "ACK", {}

    if msg_type == "NACK":
        try:
            return "NACK", {"leader_port": int(rest.strip())}
        except ValueError:
            return "NACK", {"leader_port": -1}

    if msg_type == "CREATE_TOPIC":
        return "CREATE_TOPIC", {"topic": rest.strip()}

    if msg_type == "METRIC":
        sub = rest.split(" ", 1)
        topic = sub[0]
        data = sub[1] if len(sub) > 1 else ""
        return "METRIC", {"topic": topic, "data": data}

    if msg_type == "SUBSCRIBE":
        sub = rest.split(" ", 1)
        topic = sub[0]
        offset = int(sub[1]) if len(sub) > 1 else 0
        return "SUBSCRIBE", {"topic": topic, "offset": offset}

    if msg_type == "DATA":
        try:
            return "DATA", {"new_offset": int(rest.strip())}
        except ValueError:
            return "DATA", {"new_offset": 0}

    if msg_type == "LIST_TOPICS":
        return "LIST_TOPICS", {"pattern": rest.strip()}

    if msg_type == "TOPICS":
        try:
            return "TOPICS", {"topics": json.loads(rest)}
        except json.JSONDecodeError:
            return "TOPICS", {"topics": []}

    raft_types = {
        "VOTE_REQUEST", "VOTE_GRANTED", "VOTE_DENIED",
        "REPLICATE", "REPLICATE_ACK",
    }
    if msg_type in raft_types:
        try:
            payload = json.loads(rest)
        except json.JSONDecodeError:
            payload = {}
        return msg_type, payload

    return msg_type, {"raw": rest}


def recv_line(sock) -> str:
    """Read from socket until newline and return decoded text, or None if closed."""
    buf = b""
    while True:
        try:
            byte = sock.recv(1)
        except OSError:
            return None
        if not byte:
            return None
        buf += byte
        if buf.endswith(b"\n"):
            return buf.decode("utf-8", errors="replace")
