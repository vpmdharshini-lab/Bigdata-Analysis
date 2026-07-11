"""
log_server_simulator.py
------------------------
Pretends to be 3 "high-velocity" branch servers (Swiggy-Chennai,
Swiggy-Bangalore, Swiggy-Mumbai). Each one is a tiny TCP server that,
the moment a client (our harvester daemon) connects, starts firing
log lines continuously, forever, at random intervals.

Run this FIRST, in its own terminal, and leave it running.
"""

import socket
import threading
import random
import time
from datetime import datetime

# One (name, port) per simulated branch server
BRANCHES = [
    ("swiggy-chennai", 9001),
    ("swiggy-bangalore", 9002),
    ("swiggy-mumbai", 9003),
]

LEVELS = ["INFO", "WARNING", "ERROR", "DEBUG"]

# Sample message templates per level, to make the logs feel real
MESSAGE_TEMPLATES = {
    "INFO": [
        "Order#{oid} placed successfully",
        "Order#{oid} picked up by delivery partner",
        "Order#{oid} delivered to customer",
        "Restaurant#{oid} accepted the order",
    ],
    "WARNING": [
        "Order#{oid} delivery delayed by 10 minutes",
        "Restaurant#{oid} response time above threshold",
        "High order volume detected near Order#{oid}",
    ],
    "ERROR": [
        "Payment gateway timeout for Order#{oid}",
        "Order#{oid} cancelled due to restaurant unavailability",
        "Delivery partner GPS signal lost for Order#{oid}",
    ],
    "DEBUG": [
        "Cache miss while fetching menu for Order#{oid}",
        "Retrying DB write for Order#{oid}",
    ],
}


def build_log_line(branch_name):
    """Builds ONE well-formed log line as a plain text string."""
    level = random.choice(LEVELS)
    oid = random.randint(1000, 9999)
    message = random.choice(MESSAGE_TEMPLATES[level]).format(oid=oid)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Pipe-separated format -> this is exactly what our regex will validate later
    return f"{timestamp} | {level} | {branch_name} | {message}\n"


def handle_client(conn, branch_name):
    """Keeps sending log lines to whoever connected, until they disconnect."""
    print(f"[{branch_name}] harvester connected, streaming logs...")
    try:
        while True:
            line = build_log_line(branch_name)
            conn.sendall(line.encode("utf-8"))
            # Random delay simulates "high velocity" bursty traffic
            time.sleep(random.uniform(0.05, 0.4))

            # Occasionally send a corrupted/garbled line on purpose,
            # so your regex validator has something real to reject.
            if random.random() < 0.05:
                conn.sendall(b"CORRUPTED_LINE_NO_STRUCTURE_HERE\n")
    except (BrokenPipeError, ConnectionResetError):
        print(f"[{branch_name}] harvester disconnected.")
    finally:
        conn.close()


def run_branch_server(branch_name, port):
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("127.0.0.1", port))
    server_sock.listen(1)
    print(f"[{branch_name}] listening on port {port}...")

    while True:
        conn, addr = server_sock.accept()
        client_thread = threading.Thread(
            target=handle_client, args=(conn, branch_name), daemon=True
        )
        client_thread.start()


if __name__ == "__main__":
    threads = []
    for name, port in BRANCHES:
        t = threading.Thread(target=run_branch_server, args=(name, port), daemon=True)
        t.start()
        threads.append(t)

    print("\nAll simulated branch servers are up. Press Ctrl+C to stop.\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down simulator.")