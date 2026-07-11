# Real-Time Parallel Log Aggregation Engine with Custom Socket Slicing

## What this project actually does (plain English)

Imagine 3 Swiggy branch servers (Chennai, Bangalore, Mumbai) constantly
phoning in order updates: "order placed", "payment failed", "delivery
delayed", etc. Our job is to build a **daemon** (background program) that:

1. Calls all 3 branches **at the same time** (multi-threaded)
2. Listens to whatever they say, live, as it arrives
3. Chops the raw phone-call audio into individual sentences (this is the
   "socket slicing" part — TCP gives us a stream of bytes, not neat messages)
4. Checks each sentence follows the correct grammar (regex validation)
5. Sorts good sentences into labelled folders — one folder per
   branch+severity combination (dynamic partitioning)
6. Writes them down not as plain handwriting but as a compact barcode
   format (raw binary buffers) — smaller and faster to store/read at scale

## Files

- `log_server_simulator.py` — fakes 3 "high-velocity" servers sending logs
- `log_harvester_daemon.py` — **the actual assignment deliverable**: the
  multi-threaded daemon that connects, slices, validates, partitions, and
  writes binary
- `read_binary_logs.py` — proves the binary files are real structured data
  by decoding them back to text (great for your demo/viva)
- `partitions/` — output folder, created automatically, one `.bin` file
  per (branch, severity) combination

## How to run it (2 terminals)

**Terminal 1:**
```
python3 log_server_simulator.py
```
Leave this running — it's your fake data source.

**Terminal 2:**
```
python3 log_harvester_daemon.py
```
Watch the live stats print every 3 seconds. Let it run for 10-15 seconds,
then press Ctrl+C to stop it cleanly (this closes all files properly).

**To verify/inspect the output:**
```
python3 read_binary_logs.py partitions/swiggy-chennai_ERROR.bin
```
Try this on any file inside `partitions/` — it will print the decoded
log lines.

## How each requirement in the task description maps to the code

| Requirement | Where it lives |
|---|---|
| "multi-threaded log harvesting daemon" | `log_harvester_daemon.py` — one `threading.Thread` per branch server in the `if __name__ == "__main__"` block |
| "opens TCP sockets to monitor... server instances" | `harvest_from_branch()` — `socket.connect((HOST, port))` |
| "parse stream buffers in real-time" | The `buffer += chunk` / `while b"\n" in buffer` loop in `harvest_from_branch()` |
| "execute regular expression validations" | `LOG_PATTERN` regex + `process_line()` |
| "partition logs into dynamic structured payloads" | `payload = {...}` dict + `get_partition_file()`, which creates a new file the first time a (service, level) pair appears |
| "write to partitioned local raw binary buffers" | `encode_record()` (custom `struct.pack` binary layout) + `write_payload()` |

## Why this design (in case you're asked "why not just use JSON/CSV?")

- TCP is a **stream** protocol — it has no concept of "messages", only bytes.
  So we MUST manually buffer and slice by delimiter ourselves. That's the
  "custom socket slicing" the title refers to.
- Binary encoding (`struct.pack`) is smaller and faster to write/read at
  high volume than text formats like JSON — relevant when you're told the
  servers are "high-velocity".
- Partitioning by service+severity means you can later go straight to
  `swiggy-chennai_ERROR.bin` instead of scanning millions of mixed lines —
  this is the same idea Big Data systems like Hadoop/Spark use when they
  partition data by date or region.

## Things worth mentioning if faculty asks follow-up questions

- **Thread safety**: each partition file has its own `threading.Lock` so
  two threads writing to the same file at once can't corrupt it.
- **Malformed data handling**: the simulator deliberately sends broken
  lines 5% of the time; these fail the regex and get silently dropped and
  counted as "REJECTED" in the stats — this proves your validation works.
- **Extending it**: you could add a 4th server, or partition by hour
  instead of just severity, without changing the core logic — that's what
  "dynamic" partitioning means here (files are created on demand, not
  hardcoded upfront).
