
"""
read_binary_logs.py
--------------------
Reads back one binary partition file and decodes it into human-readable
log lines. Use this to PROVE to your faculty that the .bin files really
do contain structured, recoverable log data -- not just raw junk bytes.

Usage:
    python read_binary_logs.py partitions/swiggy-chennai_ERROR.bin
"""

import struct
import sys

LEVEL_CODE = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3}
CODE_LEVEL = {v: k for k, v in LEVEL_CODE.items()}


def read_records(filepath):
    with open(filepath, "rb") as f:
        data = f.read()

    offset = 0
    records = []
    while offset < len(data):
        # First 4 bytes = length of the record that follows
        (record_len,) = struct.unpack_from("!I", data, offset)
        offset += 4

        record_bytes = data[offset : offset + record_len]
        offset += record_len

        # Now decode the record itself: 19s (timestamp) + B (level) + H (service len)
        ts_bytes, level_byte, service_len = struct.unpack_from("!19sBH", record_bytes, 0)
        pos = 19 + 1 + 2

        service = record_bytes[pos : pos + service_len].decode("utf-8")
        pos += service_len

        (message_len,) = struct.unpack_from("!H", record_bytes, pos)
        pos += 2

        message = record_bytes[pos : pos + message_len].decode("utf-8")

        records.append(
            {
                "timestamp": ts_bytes.decode("ascii").strip(),
                "level": CODE_LEVEL[level_byte],
                "service": service,
                "message": message,
            }
        )

    return records


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python read_binary_logs.py <path-to-.bin-file>")
        sys.exit(1)

    filepath = sys.argv[1]
    records = read_records(filepath)
    print(f"Found {len(records)} records in {filepath}:\n")
    for r in records:
        print(f"{r['timestamp']} | {r['level']} | {r['service']} | {r['message']}")