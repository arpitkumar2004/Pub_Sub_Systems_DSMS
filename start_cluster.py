"""Utility script to start or stop broker processes for local development."""

import os
import platform
import subprocess
import sys
import time


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BROKER_SCRIPT = os.path.join(BASE_DIR, "broker", "broker.py")
NUM_BROKERS = 3


def start_brokers():
    """Start broker nodes as separate processes and return the process list."""
    processes = []
    print(f"[start_cluster] Starting {NUM_BROKERS} broker nodes...")

    for node_id in range(NUM_BROKERS):
        cmd = [sys.executable, BROKER_SCRIPT, "--id", str(node_id)]

        if platform.system() == "Windows":
            proc = subprocess.Popen(
                ["cmd", "/c", "start", "cmd", "/k"] + cmd,
                cwd=BASE_DIR,
            )
        else:
            log_path = os.path.join(BASE_DIR, f"broker_node{node_id}.log")
            with open(log_path, "w") as log_file:
                proc = subprocess.Popen(
                    cmd,
                    cwd=BASE_DIR,
                    stdout=log_file,
                    stderr=log_file,
                )
            print(f"Node {node_id} started with PID {proc.pid}, log file: {log_path}")

        processes.append(proc)

    print("[start_cluster] All brokers started.")
    print("Wait about 3 seconds for leader election, then run publisher and subscriber.")
    return processes


def main():
    """Parse arguments and start or stop local broker processes."""
    import argparse

    parser = argparse.ArgumentParser(description="Start or stop the DSMS broker cluster.")
    parser.add_argument(
        "--stop",
        action="store_true",
        help="Stop running broker processes on Linux or macOS.",
    )
    args = parser.parse_args()

    if args.stop:
        if platform.system() != "Windows":
            os.system("pkill -f 'broker.py'")
            print("[start_cluster] Sent kill signal to broker processes.")
        else:
            print("[start_cluster] On Windows, close the broker terminals manually.")
        return

    processes = start_brokers()

    if platform.system() != "Windows":
        print("[start_cluster] Cluster is running. Press Ctrl+C to stop all brokers.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("[start_cluster] Stopping all brokers...")
            for proc in processes:
                proc.terminate()
            print("[start_cluster] Done.")


if __name__ == "__main__":
    main()
