# Bitcask — a crash-safe key-value store in Python

A single-process, log-structured KV store: append-only segments,
an in-memory KeyDir for O(1) lookups, CRC-validated crash recovery,
and space-reclaiming compaction. CLI-first, stdlib only, ~1k lines.

```
            ┌─────────────┐
 CLI ─────► │   Bitcask   │ ──► KeyDir (in-memory dict)
 (argparse) │ put/get/    │       key → (segment, offset, size, ts)
            │ delete/...  │
            └──────┬──────┘
                   │ append + flush + fsync (every write)
            ┌──────▼──────┐
            │ SegmentWriter│ ──► segments/seg_000001.log, ...
            └─────────────┘
```

## Record format

| magic (4B) | timestamp (8B) | key_len (4B) | val_len (4B) | crc (4B) | key | value |
|---|---|---|---|---|---|---|

`<--- header 24B --->`

`magic = b'BITC'`, big-endian `>4sQIII`, CRC32 over
header-without-crc + key + value. Deletes are tombstones
(`value=None`): the high bit of `val_len` (`TOMBSTONE_FLAG`) marks
them, so an empty value (`val_len = 0`, `value = b""`) is a real
stored value and not confused with a deletion.

## How recovery works

Sort segments by ID, replay in order, rebuild KeyDir; on CRC
mismatch or truncated record, truncate the file at the last good
offset and continue. Every put/delete flushes + fsyncs before
KeyDir is updated, so `kill -9` loses at most the in-flight write.

## How compaction works

Snapshot KeyDir, rewrite only live records still pointing into each
old segment into a temp segment, fsync, rename over the old file,
delete the old file if it ends up empty. The active segment is never
compacted.

## Benchmarks

Median of 3 runs (`for i in 1 2 3; do python benchmark.py; echo ---; done`),
seed 42, 50,000 puts (fsync per write) + 50,000 gets (50% hits /
50% misses):

| workload | ops/sec | p50 | p95 | machine |
|---|---|---|---|---|
| put (50k, fsync each) | 326 | — | — | AMD Ryzen 5 5600H, 14 GiB RAM, Linux x86_64 (Fedora), Python 3.14.8 |
| get (50k, 50% hit) | 93,703 | 0.012 ms | 0.023 ms | same |

Resulting store: `keys=50000 segments=2 disk_bytes=2,177,780`.

Python prototype with per-write fsync; not production-tuned.
Reproduce with: `python benchmark.py`

## Demo (3 commands)

```bash
python -m bitcask.cli --dir ./data put user:1 '{"name":"Chinmay"}'
python -m bitcask.cli --dir ./data get user:1
python -m bitcask.cli --dir ./data stats
```

Other commands: `delete`, `compact` (same `--dir` global flag).

## Tradeoffs & limitations

- Single-process; `RLock` serializes everything (compaction blocks
  reads/writes). No multi-process access.
- Python GIL + open-per-read; reads open the segment file each time.
- No hint files, no TTL/expiry, no transactions, no replication.
- Per-write fsync trades throughput for durability — puts are only
  ~300 ops/sec, gets ~93k ops/sec (see Benchmarks).

## File structure

```
bitcask/       db.py  log.py  cli.py  __init__.py  test_db.py
benchmark.py   README.md
```

## Running tests

```bash
python -m pytest -q       # 19 tests, all passing
ruff check bitcask/
```

## Future Work

- Concurrency concerns
- Efficiency Improvement
