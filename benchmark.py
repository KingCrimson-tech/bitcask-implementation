"""Reproducible Bitcask benchmark: 50k puts + 50k mixed gets, seed 42."""
import random
import shutil
import statistics
import time
from pathlib import Path

from bitcask.db import Bitcask

N = 50_000
DATA_DIR = Path("benchmark-data")


def percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(int(len(sorted_vals) * pct), len(sorted_vals) - 1)
    return sorted_vals[idx]


def main() -> None:
    random.seed(42)
    if DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)

    keys = [f"key_{i}" for i in range(N)]

    with Bitcask(DATA_DIR) as db:
        # --- puts --------------------------------------------------------
        t0 = time.perf_counter()
        for i, k in enumerate(keys):
            db.put(k, f"value-{i}")  # every put flushes + fsyncs; this is slow, by design
        t_put = time.perf_counter() - t0

        # --- gets: 50% hits, 50% misses -----------------------------------
        latencies: list[float] = []
        t0 = time.perf_counter()
        for i in range(N):
            if random.random() < 0.5:
                db.get(random.choice(keys))         # hit
            else:
                try:
                    db.get(f"missing_{i}")          # miss -> KeyError
                except KeyError:
                    pass
            latencies.append(time.perf_counter() - t0)
            t0 = time.perf_counter()
        t_get = time.perf_counter() - t0 + sum(latencies)

        stats = db.stats()

    latencies.sort()
    put_ops = N / t_put if t_put else 0
    get_ops = N / t_get if t_get else 0
    p50 = percentile(latencies, 0.50) * 1000  # ms per op
    p95 = percentile(latencies, 0.95) * 1000

    print(f"puts:      {N} in {t_put:.2f}s -> {put_ops:,.0f} ops/sec")
    print(f"gets:      {N} in {t_get:.2f}s -> {get_ops:,.0f} ops/sec (50% hits / 50% misses)")
    print(f"get latency: p50={p50:.3f} ms  p95={p95:.3f} ms")
    print(f"keys={stats['keys']} segments={stats['segments']} "
          f"disk_bytes={stats['disk_bytes']:,} keydir_entries={stats['keydir_entries']}")
    print("note: Python prototype with per-write fsync; not production-tuned")


if __name__ == "__main__":
    main()
