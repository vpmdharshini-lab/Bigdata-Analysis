"""
log_harvester_daemon.py
------------------------
This is the actual assignment deliverable.

For each simulated branch server it:
  1. Opens a TCP socket connection to it (one thread per server)
  2. Reads raw bytes as they arrive and manually SLICES the stream
     into individual log lines (TCP has no idea what a "line" is --
     we have to find the '\n' boundaries ourselves)
  3. Validates each line with a regex
  4. Builds a clean structured payload (dict) from valid lines
  5. Dynamically partitions payloads into separate binary files,
     one file per (branch, severity level) combination -- a new
     partition file is created the first time that combination shows up
  6. Writes each payload as a compact custom binary record (not plain text)

Run log_server_simulator.py FIRST in another terminal, then run this.
"""

import socket
import threading
import re
import struct
import os
import time
from collections import defaultdict

BRANCHES = [
    ("swiggy-chennai", 9001),
    ("swiggy-bangalore", 9002),
    ("swiggy-mumbai", 9003),
]

HOST = "127.0.0.1"
PARTITION_DIR = "partitions"

# ---------------------------------------------------------------------------
# STEP 1: The regex that defines what a "valid" log line looks like.
# Format expected:  2026-07-08 14:32:10 | INFO | swiggy-chennai | some message
# ---------------------------------------------------------------------------
LOG_PATTERN = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*\|\s*"
    r"(?P<level>INFO|WARNING|ERROR|DEBUG)\s*\|\s*"
    r"(?P<service>[\w\-]+)\s*\|\s*"
    r"(?P<message>.+)$"
)

# Maps severity text to a single byte code, for compact binary storage
LEVEL_CODE = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3}
CODE_LEVEL = {v: k for k, v in LEVEL_CODE.items()}

# ---------------------------------------------------------------------------
# STEP 2: Partition management.
# partitions[(service, level)] -> open binary file handle
# Each (service, level) pair gets its OWN file, created the first time
# we see a log line for that combination ("dynamic" partitioning).
# A lock per file avoids two threads corrupting the same file if they
# both happen to write the same partition at once.
# ---------------------------------------------------------------------------
partition_files = {}
partition_locks = defaultdict(threading.Lock)
partitions_master_lock = threading.Lock()  # protects the dict itself

# Simple running stats, printed every few seconds so you can show
# "real-time" behaviour live during your demo.
stats_lock = threading.Lock()
stats = defaultdict(int)


def get_partition_file(service, level):
    """Returns the binary file handle for this (service, level),
    creating the file the first time this combination appears."""
    key = (service, level)
    with partitions_master_lock:
        if key not in partition_files:
            os.makedirs(PARTITION_DIR, exist_ok=True)
            filename = os.path.join(PARTITION_DIR, f"{service}_{level}.bin")
            partition_files[key] = open(filename, "ab")  # append, binary mode
            print(f"[partition] created new partition file: {filename}")
        return partition_files[key]


def encode_record(timestamp, level, service, message):
    """
    Builds ONE custom binary record. Layout:

      [19 bytes]  timestamp, fixed width, ascii  e.g. "2026-07-08 14:32:10"
      [1 byte]    level code (0-3)
      [2 bytes]   length of service name (unsigned short)
      [N bytes]   service name (utf-8)
      [2 bytes]   length of message (unsigned short)
      [M bytes]   message (utf-8)

    '!' = network byte order, '19s' = 19-byte string, 'B' = 1 byte,
    'H' = 2-byte unsigned short. This is the "raw binary buffer" format.
    """
    ts_bytes = timestamp.encode("ascii").ljust(19, b" ")[:19]
    level_byte = LEVEL_CODE[level]
    service_bytes = service.encode("utf-8")
    message_bytes = message.encode("utf-8")

    header = struct.pack(
        "!19sBH", ts_bytes, level_byte, len(service_bytes)
    )
    mid = struct.pack("!H", len(message_bytes))

    return header + service_bytes + mid + message_bytes


def write_payload(record):
    """Writes one structured payload dict to its correct partition file."""
    binary_record = encode_record(
        record["timestamp"], record["level"], record["service"], record["message"]
    )
    # Prefix the whole record with its total length (4 bytes) so the
    # reader can later slice the file back into records easily.
    length_prefix = struct.pack("!I", len(binary_record))

    key = (record["service"], record["level"])
    f = get_partition_file(record["service"], record["level"])
    with partition_locks[key]:
        f.write(length_prefix + binary_record)
        f.flush()


def process_line(raw_line, branch_name):
    """STEP 3 + 4: validate with regex, build structured payload, store it."""
    match = LOG_PATTERN.match(raw_line)
    if not match:
        with stats_lock:
            stats[(branch_name, "REJECTED")] += 1
        return  # corrupted/garbled line -> silently dropped (as a real daemon would)

    payload = {
        "timestamp": match.group("timestamp"),
        "level": match.group("level"),
        "service": match.group("service"),
        "message": match.group("message"),
    }

    write_payload(payload)

    with stats_lock:
        stats[(branch_name, payload["level"])] += 1


def harvest_from_branch(branch_name, port):
    """
    STEP 2 (the 'custom socket slicing' part):
    Opens a TCP socket to one branch server and continuously reads
    raw bytes. TCP only guarantees a stream of bytes -- it does NOT
    guarantee that one recv() call = one log line. A line can arrive
    split across two recv() calls, or two lines can arrive in one
    recv() call. So we keep a running byte buffer and manually slice
    it at every newline character.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, port))
    print(f"[{branch_name}] connected on port {port}")

    buffer = b""  # our rolling byte buffer -- this IS the "socket slicing"
    try:
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                print(f"[{branch_name}] server closed connection.")
                break

            buffer += chunk

            # Slice out every complete line currently sitting in the buffer
            while b"\n" in buffer:
                line_bytes, buffer = buffer.split(b"\n", 1)
                try:
                    line = line_bytes.decode("utf-8").strip()
                except UnicodeDecodeError:
                    continue
                if line:
                    process_line(line, branch_name)
            # Anything left in `buffer` is an incomplete line fragment --
            # we simply wait for more bytes to arrive and keep appending.
    finally:
        sock.close()


def print_stats_periodically():
    """Runs in the background, printing a live dashboard every 3 seconds."""
    while True:
        time.sleep(3)
        with stats_lock:
            if not stats:
                continue
            print("\n--- live ingestion stats (last snapshot) ---")
            for (branch, level), count in sorted(stats.items()):
                print(f"  {branch:20s} {level:10s} {count}")
            print("---------------------------------------------\n")


if __name__ == "__main__":
    threads = []

    # One dedicated thread per branch server -- this is the
    # "multi-threaded" requirement.
    for name, port in BRANCHES:
        t = threading.Thread(target=harvest_from_branch, args=(name, port), daemon=True)
        t.start()
        threads.append(t)

    stats_thread = threading.Thread(target=print_stats_periodically, daemon=True)
    stats_thread.start()

    print("Harvester daemon running. Press Ctrl+C to stop.\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down harvester. Closing partition files...")
        for f in partition_files.values():
            f.close()