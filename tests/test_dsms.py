# =============================================================================
# tests/test_dsms.py
#
# Automated end-to-end test suite for the DSMS pub-sub system.
#
# WHAT THIS FILE TESTS:
#   Test 1  — Broker Connectivity    : can we reach all 3 brokers?
#   Test 2  — Leader Detection       : exactly one broker is the RAFT leader
#   Test 3  — NACK Redirect          : follower correctly redirects us to leader
#   Test 4  — CREATE_TOPIC           : topic is created on leader, replicated to all
#   Test 5  — METRIC publish         : metric is committed and readable
#   Test 6  — SUBSCRIBE (offset=0)   : read back the metric we just published
#   Test 7  — Offset-based reading   : second read returns ONLY new entries
#   Test 8  — Multi-topic publish    : two topics, both readable independently
#   Test 9  — Prefix aggregation     : subscribing to "server1" returns data
#                                       from all "*.server1" topics combined
#   Test 10 — Replication check      : log files on disk exist on ALL 3 brokers
#
#   Manual tests (printed as instructions):
#   Test 11 — Fault tolerance        : kill leader, verify new leader elected
#   Test 12 — Subscriber reconnect   : restart DSMS_SUB, verify data re-read
#
# HOW TO RUN:
#   1. Start all 3 brokers in separate terminals:
#        python broker/broker.py --id 0
#        python broker/broker.py --id 1
#        python broker/broker.py --id 2
#   2. Wait ~4 seconds for leader election.
#   3. Run this file:
#        python tests/test_dsms.py
#
# No publisher or subscriber process needs to be running.
# This script talks directly to the brokers via TCP.
# =============================================================================

import socket
import json
import time
import sys
import os

# Allow imports from the parent 'dsms' package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common import config, protocol


# =============================================================================
# Helpers
# =============================================================================

# ANSI colours for pretty output (works on modern Windows terminals too)
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

# Track pass / fail counts
_passed = 0
_failed = 0


def ok(msg: str):
    global _passed
    _passed += 1
    print(f"  {GREEN}✓ PASS{RESET}  {msg}")


def fail(msg: str):
    global _failed
    _failed += 1
    print(f"  {RED}✗ FAIL{RESET}  {msg}")


def info(msg: str):
    print(f"  {CYAN}ℹ{RESET}  {msg}")


def section(title: str):
    print(f"\n{BOLD}{CYAN}{'─'*60}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'─'*60}{RESET}")


def send_and_receive(host: str, port: int, message: str, read_extra=False):
    """
    Open a TCP connection, send message, read response line, close.
    If read_extra=True also reads any remaining bytes after the first line.
    Returns (msg_type, payload, extra_bytes) or (None, None, b'') on error.
    """
    try:
        with socket.create_connection((host, port), timeout=4.0) as s:
            s.sendall(message.encode())
            line = protocol.recv_line(s)
            if not line:
                return None, None, b""
            msg_type, payload = protocol.decode_message(line)
            extra = b""
            if read_extra:
                while True:
                    chunk = s.recv(4096)
                    if not chunk:
                        break
                    extra += chunk
            return msg_type, payload, extra
    except Exception as e:
        return None, None, b""


def find_leader():
    """
    Try each broker in config.BROKERS.  Send a CREATE_TOPIC for a dummy topic.
    - If we get ACK   → this IS the leader.
    - If we get NACK  → the NACK carries the leader's port.
    Returns (leader_host, leader_port) or (None, None) if no leader found.
    """
    for b in config.BROKERS:
        msg = protocol.encode_create_topic("__probe__")
        mt, payload, _ = send_and_receive(b["host"], b["port"], msg)
        if mt == "ACK":
            return b["host"], b["port"]
        elif mt == "NACK":
            lp = payload.get("leader_port", -1)
            if lp > 0:
                lh = next((x["host"] for x in config.BROKERS if x["port"] == lp), "127.0.0.1")
                return lh, lp
    return None, None


# =============================================================================
# TEST 1  —  Broker Connectivity
# =============================================================================

def test_broker_connectivity():
    section("Test 1 — Broker Connectivity")
    print("  Checking that all brokers are reachable on their configured ports.\n")

    for b in config.BROKERS:
        try:
            with socket.create_connection((b["host"], b["port"]), timeout=2.0):
                ok(f"Broker {b['id']} reachable at {b['host']}:{b['port']}")
        except Exception:
            fail(f"Broker {b['id']} NOT reachable at {b['host']}:{b['port']} "
                 f"— is it running?")


# =============================================================================
# TEST 2  —  Leader Detection
# =============================================================================

def test_leader_detection():
    section("Test 2 — Leader Detection (RAFT)")
    print("  Sending a CREATE_TOPIC probe to each broker.\n"
          "  Leader should ACK; followers should NACK with leader's port.\n")

    leader_count = 0
    follower_count = 0
    detected_leader_port = None

    for b in config.BROKERS:
        msg = protocol.encode_create_topic("__probe__")
        mt, payload, _ = send_and_receive(b["host"], b["port"], msg)

        if mt == "ACK":
            leader_count += 1
            detected_leader_port = b["port"]
            ok(f"Broker {b['id']} (port {b['port']}) responded ACK → is LEADER")
        elif mt == "NACK":
            follower_count += 1
            lp = payload.get("leader_port", -1)
            ok(f"Broker {b['id']} (port {b['port']}) responded NACK → is FOLLOWER "
               f"(redirected to port {lp})")
        else:
            fail(f"Broker {b['id']} gave unexpected response: {mt}")

    print()
    if leader_count == 1:
        ok(f"Exactly ONE leader elected (port {detected_leader_port}) ✓")
    elif leader_count == 0:
        fail("No leader found — election may still be in progress.  "
             "Wait a few more seconds and retry.")
    else:
        fail(f"Multiple leaders detected ({leader_count}) — split-brain!")

    return detected_leader_port


# =============================================================================
# TEST 3  —  NACK Redirect
# =============================================================================

def test_nack_redirect(leader_port):
    section("Test 3 — NACK Redirect (Follower → Leader)")
    print("  Sending a request intentionally to a FOLLOWER.\n"
          "  Expect NACK containing the leader's port.\n")

    follower = next((b for b in config.BROKERS if b["port"] != leader_port), None)
    if follower is None:
        info("Only one broker found — skipping redirect test.")
        return

    msg = protocol.encode_metric("test.server1", "cpu=1.0 memory=1.0")
    mt, payload, _ = send_and_receive(follower["host"], follower["port"], msg)

    if mt == "NACK":
        redirected_to = payload.get("leader_port", -1)
        if redirected_to == leader_port:
            ok(f"Follower (port {follower['port']}) correctly returned "
               f"NACK with leader port {redirected_to}")
        else:
            fail(f"NACK had wrong leader port: got {redirected_to}, expected {leader_port}")
    else:
        fail(f"Expected NACK from follower but got: {mt}")


# =============================================================================
# TEST 4  —  CREATE_TOPIC
# =============================================================================

def test_create_topic(leader_host, leader_port, topic):
    section(f"Test 4 — CREATE_TOPIC '{topic}'")
    print(f"  Sending CREATE_TOPIC directly to the leader (port {leader_port}).\n"
          f"  Expect ACK after RAFT commits the entry on majority.\n")

    msg = protocol.encode_create_topic(topic)
    mt, _, _ = send_and_receive(leader_host, leader_port, msg)

    if mt == "ACK":
        ok(f"CREATE_TOPIC '{topic}' → ACK from leader")
    else:
        fail(f"CREATE_TOPIC '{topic}' → expected ACK, got {mt}")


# =============================================================================
# TEST 5  —  METRIC Publish
# =============================================================================

def test_publish_metric(leader_host, leader_port, topic, data):
    section(f"Test 5 — METRIC Publish to '{topic}'")
    print(f"  Sending METRIC '{data}' to the leader (port {leader_port}).\n"
          f"  Expect ACK after RAFT replication + commit.\n")

    msg = protocol.encode_metric(topic, data)
    mt, _, _ = send_and_receive(leader_host, leader_port, msg)

    if mt == "ACK":
        ok(f"METRIC '{data}' on topic '{topic}' → ACK")
        return True
    else:
        fail(f"METRIC → expected ACK, got {mt}")
        return False


# =============================================================================
# TEST 6  —  SUBSCRIBE (read from offset 0)
# =============================================================================

def test_subscribe_basic(leader_host, leader_port, topic, expected_data_substr):
    section(f"Test 6 — SUBSCRIBE (offset=0) on '{topic}'")
    print(f"  Reading from offset 0 — should return the metric we just published.\n")

    msg = protocol.encode_subscribe(topic, 0)
    mt, payload, raw_bytes = send_and_receive(
        leader_host, leader_port, msg, read_extra=True)

    if mt != "DATA":
        fail(f"Expected DATA response, got {mt}")
        return None

    new_offset = payload["new_offset"]
    content    = raw_bytes.decode("utf-8", errors="replace")

    info(f"new_offset = {new_offset}")
    info(f"content    = {content.strip()}")

    if expected_data_substr in content:
        ok(f"Response contains expected data '{expected_data_substr}'")
    else:
        fail(f"Expected '{expected_data_substr}' in response, but got: {content.strip()}")

    if new_offset > 0:
        ok(f"new_offset={new_offset} > 0 (log has data)")
    else:
        fail("new_offset is 0 — no data was written to disk")

    return new_offset


# =============================================================================
# TEST 7  —  Offset-based Reading (incremental poll)
# =============================================================================

def test_offset_based_reading(leader_host, leader_port, topic, first_offset):
    section(f"Test 7 — Offset-based Reading (incremental poll)")
    print(f"  First we publish a NEW metric.\n"
          f"  Then we read from offset={first_offset} — should get ONLY the new entry.\n")

    # Publish a second metric with a unique marker
    new_data = "cpu=99.9 memory=88.8"
    msg = protocol.encode_metric(topic, new_data)
    mt, _, _ = send_and_receive(leader_host, leader_port, msg)
    if mt != "ACK":
        fail(f"Could not publish second metric: {mt}")
        return

    # Small delay to let apply thread write to disk
    time.sleep(0.3)

    # Read from first_offset — should NOT contain the first metric's data
    msg = protocol.encode_subscribe(topic, first_offset)
    mt, payload, raw_bytes = send_and_receive(
        leader_host, leader_port, msg, read_extra=True)

    if mt != "DATA":
        fail(f"Expected DATA, got {mt}")
        return

    content = raw_bytes.decode("utf-8", errors="replace")
    info(f"Data at offset {first_offset}: {content.strip()}")

    if new_data in content:
        ok(f"Incremental read found the NEW entry '{new_data}'")
    else:
        fail(f"New entry '{new_data}' not found in incremental read")

    if payload["new_offset"] > first_offset:
        ok(f"new_offset advanced: {first_offset} → {payload['new_offset']}")
    else:
        fail(f"new_offset did not advance beyond {first_offset}")


# =============================================================================
# TEST 8  —  Multi-topic Publishing
# =============================================================================

def test_multi_topic(leader_host, leader_port):
    section("Test 8 — Multi-topic Publishing")
    print("  Create two topics, publish to each, read back from each separately.\n")

    topics = ["webapp.server1", "database.server1"]
    data   = {"webapp.server1": "cpu=30.0 memory=20.0",
               "database.server1": "cpu=55.0 memory=70.0"}

    for topic in topics:
        msg = protocol.encode_create_topic(topic)
        mt, _, _ = send_and_receive(leader_host, leader_port, msg)
        if mt == "ACK":
            ok(f"Created topic '{topic}'")
        else:
            fail(f"Could not create topic '{topic}': {mt}")
            return

    for topic, d in data.items():
        msg = protocol.encode_metric(topic, d)
        mt, _, _ = send_and_receive(leader_host, leader_port, msg)
        if mt == "ACK":
            ok(f"Published to '{topic}': {d}")
        else:
            fail(f"Could not publish to '{topic}': {mt}")

    time.sleep(0.3)

    print()
    for topic, expected in data.items():
        msg = protocol.encode_subscribe(topic, 0)
        mt, payload, raw = send_and_receive(leader_host, leader_port, msg, read_extra=True)
        content = raw.decode("utf-8", errors="replace")
        if mt == "DATA" and expected in content:
            ok(f"Read back '{expected}' from topic '{topic}'")
        else:
            fail(f"Could not read back data from '{topic}'. Got: {content.strip()}")


# =============================================================================
# TEST 9  —  Prefix Aggregation via DSMS_SUB HTTP API
# =============================================================================

def test_prefix_aggregation():
    section("Test 9 — Prefix Aggregation (DSMS_SUB HTTP API)")
    print("  This test requires DSMS_SUB to be running on port "
          f"{config.SUBSCRIBER_HTTP_PORT}.\n")

    try:
        import urllib.request
        import urllib.error

        base = f"http://localhost:{config.SUBSCRIBER_HTTP_PORT}"

        # Subscribe to two topics
        for topic in ["webapp.server1", "database.server1"]:
            req = urllib.request.Request(
                f"{base}/subscribe?topic={topic}", method="POST")
            try:
                urllib.request.urlopen(req, timeout=3)
                ok(f"Subscribed to '{topic}' via HTTP")
            except urllib.error.URLError as e:
                fail(f"Could not subscribe to '{topic}': {e}")
                return

        # Wait for at least one poll cycle
        info(f"Waiting {config.POLL_INTERVAL + 1}s for DSMS_SUB to poll broker...")
        time.sleep(config.POLL_INTERVAL + 1)

        # --- Suffix aggregation: "server1" → webapp.server1 + database.server1 ---
        try:
            url = f"{base}/logs?topic=server1"
            resp = urllib.request.urlopen(url, timeout=3)
            body = json.loads(resp.read())
            topics_seen = set(e.get("topic") for e in body.get("entries", []))
            entry_count = body.get("entry_count", 0)
            info(f"Suffix query 'server1' returned {entry_count} entries "
                 f"from topics: {topics_seen}")
            if entry_count >= 2:
                ok(f"Suffix aggregation ('server1') returned {entry_count} entries "
                   f"across {len(topics_seen)} topics")
            else:
                fail(f"Suffix aggregation returned only {entry_count} entries "
                     f"(expected >= 2 from webapp.server1 + database.server1)")
        except Exception as e:
            fail(f"HTTP GET /logs?topic=server1 failed: {e}")

        # --- Prefix aggregation demo: subscribe to two "webapp.*" topics ---
        for topic in ["webapp.server1"]:
            req = urllib.request.Request(
                f"{base}/subscribe?topic={topic}", method="POST")
            try:
                urllib.request.urlopen(req, timeout=3)
            except Exception:
                pass  # already subscribed

        info(f"Waiting {config.POLL_INTERVAL + 1}s for second poll cycle...")
        time.sleep(config.POLL_INTERVAL + 1)

        try:
            url = f"{base}/logs?topic=webapp"
            resp = urllib.request.urlopen(url, timeout=3)
            body = json.loads(resp.read())
            entry_count = body.get("entry_count", 0)
            info(f"Prefix query 'webapp' returned {entry_count} entries")
            if entry_count > 0:
                ok(f"Prefix aggregation ('webapp') returned {entry_count} entries")
            else:
                fail("Prefix aggregation ('webapp') returned 0 entries")
        except Exception as e:
            fail(f"HTTP GET /logs?topic=webapp failed: {e}")

    except ImportError:
        info("urllib not available — skipping HTTP test")


# =============================================================================
# TEST 10  —  Replication Check (log files on all 3 broker nodes)
# =============================================================================

def test_replication_check(topic):
    section("Test 10 — Replication Check (disk files on all 3 nodes)")
    print("  Every broker should have the same topic log file on disk\n"
          "  because RAFT replicates to ALL nodes.\n")

    # Convert topic name to file name
    safe_topic = topic.replace(".", "_")
    log_filename = f"{safe_topic}.log"

    dsms_root = os.path.join(os.path.dirname(__file__), "..")

    for b in config.BROKERS:
        log_path = os.path.join(dsms_root, "broker_data",
                                f"node{b['id']}", log_filename)
        log_path = os.path.normpath(log_path)

        if os.path.exists(log_path):
            size = os.path.getsize(log_path)
            ok(f"Broker {b['id']}: {log_path}  ({size} bytes)")
        else:
            fail(f"Broker {b['id']}: log file NOT found at {log_path}")

    # Also verify all files have the same content (strongest replication check)
    contents = []
    for b in config.BROKERS:
        log_path = os.path.normpath(os.path.join(
            dsms_root, "broker_data", f"node{b['id']}", log_filename))
        if os.path.exists(log_path):
            with open(log_path, "rb") as f:
                contents.append(f.read())
        else:
            contents.append(None)

    valid = [c for c in contents if c is not None]
    if len(valid) == len(config.BROKERS) and len(set(valid)) == 1:
        ok("All broker log files have IDENTICAL content (perfect replication)")
    elif len(valid) == len(config.BROKERS):
        fail("Log files exist on all nodes but content DIFFERS — "
             "timestamps may differ if nodes applied entries at different clock seconds.")
        for i, c in enumerate(contents):
            if c:
                # Show first line of each node's log to help diagnose
                first_line = c.split(b"\n")[0].decode("utf-8", errors="replace")
                info(f"  Node {i} first entry: {first_line}")
    else:
        fail(f"Only {len(valid)} of {len(config.BROKERS)} nodes have the log file")


# =============================================================================
# MANUAL TEST INSTRUCTIONS  —  Fault Tolerance
# =============================================================================

def print_manual_tests(leader_port):
    section("Tests 11 & 12 — Manual Fault Tolerance Tests")

    print(f"""
{YELLOW}These tests cannot be automated because they require killing a process.
Follow these steps carefully and watch the terminal output.{RESET}

{BOLD}Test 11 — Leader Failure & Re-election:{RESET}
  1. The current leader is on port {leader_port}.
     Close that terminal window (Ctrl+C or just close it).
  2. Watch the OTHER two broker terminals.
     Within {config.ELECTION_TIMEOUT_MAX:.0f} seconds you should see:
       [RAFT X] Starting ELECTION | term=2
       [RAFT X] *** BECAME LEADER | term=2 ***
  3. Re-run this test script.
     Test 2 should show a NEW leader on a different port.
  4. Start the publisher again:
       python publisher/dsms_pub.py
     It should automatically find the new leader (via NACK redirect).

{BOLD}Test 12 — Subscriber Reconnect:{RESET}
  1. Stop DSMS_SUB (Ctrl+C).
  2. Restart it:
       python subscriber/dsms_sub.py
  3. Re-subscribe to your topics:
       Invoke-WebRequest -Uri "http://localhost:8080/subscribe?topic=python.server1" -Method POST
  4. Wait {config.POLL_INTERVAL} seconds, then read logs:
       Invoke-WebRequest -Uri "http://localhost:8080/logs?topic=python.server1" | Select-Object -ExpandProperty Content
     You should see ALL entries from the beginning (offset=0 on fresh start).

{BOLD}Test 13 — Follower Crash (system stays up):{RESET}
  1. Close ONE follower broker terminal.
  2. Keep publishing metrics.
  3. The system should continue working — leader + 1 follower = 2 nodes = majority.
  4. Restart the killed follower.
  5. It will automatically catch up via REPLICATE messages from the leader.
""")


# =============================================================================
# MAIN  —  Run all automated tests
# =============================================================================

def cleanup_test_data():
    """
    Delete any log files left over from previous test runs so Test 10
    (replication check) always compares freshly-written, consistent data.

    Why is this needed?
        Before the timestamp fix was applied, each broker wrote its own
        wall-clock timestamp when applying an entry.  If those old files
        survive on disk, they contain entries with different timestamps
        across nodes → byte comparison fails even though RAFT is correct.
        Cleaning up ensures every run starts from an empty slate.
    """
    # Topics that this test suite creates
    test_topics = [
        "testapp.server1",
        "webapp.server1",
        "database.server1",
        "__probe__",
    ]
    dsms_root = os.path.join(os.path.dirname(__file__), "..")
    removed = 0
    for b in config.BROKERS:
        for topic in test_topics:
            safe = topic.replace(".", "_")
            path = os.path.normpath(os.path.join(
                dsms_root, "broker_data", f"node{b['id']}", f"{safe}.log"))
            if os.path.exists(path):
                os.remove(path)
                removed += 1
    if removed:
        print(f"  {CYAN}ℹ{RESET}  Cleaned up {removed} stale test log file(s) from previous runs.")


if __name__ == "__main__":
    print(f"\n{BOLD}{'='*60}")
    print("  DSMS End-to-End Test Suite")
    print(f"  Cluster: {[b['port'] for b in config.BROKERS]}")
    print(f"{'='*60}{RESET}\n")

    # Remove log files from previous test runs so we start with clean data
    cleanup_test_data()

    # --- Connectivity ---
    test_broker_connectivity()

    # --- Leader discovery ---
    leader_port = test_leader_detection()

    if leader_port is None:
        print(f"\n{RED}No leader found. Make sure all 3 brokers are running and "
              f"wait a few more seconds for election to complete.{RESET}")
        sys.exit(1)

    leader_host = next(
        (b["host"] for b in config.BROKERS if b["port"] == leader_port),
        "127.0.0.1"
    )

    # --- NACK redirect ---
    test_nack_redirect(leader_port)

    # --- CREATE_TOPIC ---
    TEST_TOPIC = "testapp.server1"
    test_create_topic(leader_host, leader_port, TEST_TOPIC)

    # Give the apply thread a moment to write to disk
    time.sleep(0.3)

    # --- METRIC publish ---
    TEST_DATA = "cpu=42.0 memory=55.5"
    published = test_publish_metric(leader_host, leader_port, TEST_TOPIC, TEST_DATA)

    time.sleep(0.3)

    # --- SUBSCRIBE ---
    first_offset = None
    if published:
        first_offset = test_subscribe_basic(
            leader_host, leader_port, TEST_TOPIC, "42.0")

    # --- Offset-based reading ---
    if first_offset is not None and first_offset > 0:
        test_offset_based_reading(leader_host, leader_port, TEST_TOPIC, first_offset)

    # --- Multi-topic ---
    test_multi_topic(leader_host, leader_port)

    time.sleep(0.3)

    # --- Prefix aggregation (needs DSMS_SUB running) ---
    test_prefix_aggregation()

    # --- Replication check ---
    # Wait 1.5s so all three nodes' apply-threads have flushed to disk.
    # (Leader applies first; followers apply after the next heartbeat arrives,
    #  which is up to HEARTBEAT_INTERVAL seconds later.)
    info("Waiting 1.5s for replication to flush to all broker disks...")
    time.sleep(1.5)
    test_replication_check(TEST_TOPIC)

    # --- Manual tests ---
    print_manual_tests(leader_port)

    # --- Summary ---
    section("Test Summary")
    total = _passed + _failed
    print(f"\n  Automated tests : {total}")
    print(f"  {GREEN}Passed{RESET}         : {_passed}")
    if _failed:
        print(f"  {RED}Failed{RESET}         : {_failed}")
    else:
        print(f"  {GREEN}Failed{RESET}         : 0")

    print()
    if _failed == 0:
        print(f"  {GREEN}{BOLD}All automated tests passed! 🎉{RESET}")
    else:
        print(f"  {RED}{BOLD}{_failed} test(s) failed.{RESET}")
    print()
