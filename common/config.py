"""Configuration values shared by broker, publisher, and subscriber."""

# Broker endpoints used by all components.
BROKERS = [
    {"id": 0, "host": "10.145.99.48", "port": 5000},
    {"id": 1, "host": "10.145.99.43", "port": 5001},
    {"id": 2, "host": "10.145.27.212", "port": 5002},
]

# RAFT timing values in seconds.
ELECTION_TIMEOUT_MIN = 6
ELECTION_TIMEOUT_MAX = 15
HEARTBEAT_INTERVAL = 0.75

# Publisher behavior.
PUBLISH_INTERVAL = 5
MAX_RETRIES = 10
RETRY_DELAY = 1.0

# Subscriber behavior.
POLL_INTERVAL = 3
SUBSCRIBER_HTTP_PORT = 8080
