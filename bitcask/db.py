import os
import sys
import threading
import time
import zlib
from pathlib import Path
from typing import Literal, Self

from bitcask.log import (
    HEADER,
    MAGIC,
    TOMBSTONE_FLAG,
    SegmentWriter,
    encode_record,
    iter_records,
)


def _require_key(key: str) -> None:
    if not isinstance(key, str):
        raise TypeError("key must be str")
    if key == "":
        raise ValueError("key must not be empty")


class Bitcask:
    def __init__(self, data_dir: str | Path, segment_size: int = 2 * 1024 * 1024):
            self.data_dir = Path(data_dir)
            self.segments_dir = self.data_dir / "segments"
            self.segments_dir.mkdir(parents=True, exist_ok=True)
            self.segment_size = segment_size
            self._lock = threading.RLock()
            self._keydir: dict[str, tuple[int, int, int, int]] = {}
            self._segments: dict[int, Path] = {}

            # Discover existing segments, sorted NUMERICALLY (not lexically)
            for path in sorted(self.segments_dir.glob("seg_*.log"), key=_seg_id_of):
                seg_id = _seg_id_of(path)
                self._segments[seg_id] = path

            self._replay()

            # Always open a NEW active segment after replay.
            # Do NOT append to a replayed tail — it may have been truncated.
            self._active_id = (max(self._segments) + 1) if self._segments else 1
            active_path = self._segment_path(self._active_id)
            self._segments[self._active_id] = active_path
            self._active: SegmentWriter | None = SegmentWriter(
                active_path, self._active_id
            )

    def put(self, key: str, value: str) -> None:
        _require_key(key)
        with self._lock:
            key_bytes = key.encode("utf-8")
            value_bytes = value.encode("utf-8")
            ts = int(time.time())

            record = encode_record(ts, key_bytes, value_bytes)

            self._maybe_rotate(len(record))

            active = self._active
            if active is None:
                raise ValueError("Bitcask is closed")
            offset = active.append(record)
            self._keydir[key] = (
                self._active_id,
                offset,
                len(value_bytes),
                ts
            )

    def get(self, key: str) -> str:
        _require_key(key)
        with self._lock:
            if key not in self._keydir:
                raise KeyError(key)

            seg_id, offset, _value_size, _ts = self._keydir[key]

            with open(self._segments[seg_id], "rb") as f:
                f.seek(offset)

                header = f.read(HEADER.size)
                if len(header) < HEADER.size:
                    raise ValueError(
                        f"corrupt record at segment {seg_id} offset {offset}: "
                        "short header"
                    )

                magic, _ts, key_len, val_len, crc = HEADER.unpack(header)

                if magic != MAGIC:
                    raise ValueError(
                        f"corrupt record at segment {seg_id} offset {offset}: "
                        "bad magic"
                    )

                is_tombstone = bool(val_len & TOMBSTONE_FLAG)
                real_val_len = val_len & ~TOMBSTONE_FLAG
                payload = f.read(key_len + real_val_len)

                if len(payload) < key_len + real_val_len:
                    raise ValueError(
                        f"corrupt record at segment {seg_id} offset {offset}: "
                        "short payload"
                    )

                key_bytes = payload[:key_len]
                value_bytes = payload[key_len:]

                expect = zlib.crc32(
                    header[:20] + key_bytes + value_bytes
                ) & 0xFFFFFFFF

                if expect != crc:
                    raise ValueError(
                        f"corrupt record at segment {seg_id} offset {offset}: "
                        "CRC mismatch"
                    )

                if is_tombstone:  # deleted keys read as missing
                    raise KeyError(key)

                return value_bytes.decode("utf-8")

    def _rotate(self) -> None:
        active = self._active
        if active is None:
            raise ValueError("Bitcask is closed")
        active.close()
        self._active_id += 1
        path = self.segments_dir / f"seg_{self._active_id:06d}.log"
        self._segments[self._active_id] = path
        self._active = SegmentWriter(path, self._active_id)

    def _segment_path(self, seg_id: int) -> Path:
            return self.segments_dir / f"seg_{seg_id:06d}.log"

    def _replay(self) -> None:
            replayed_keys: set[str] = set()
            for seg_id in sorted(self._segments):  # oldest → newest
                path = self._segments[seg_id]
                last_good = 0
                for offset, ts, key_bytes, value in iter_records(path):
                    key = key_bytes.decode("utf-8")
                    if value is None:
                        self._keydir.pop(key, None)  # tombstone (Ch.7; harmless now)
                    else:
                        self._keydir[key] = (seg_id, offset, len(value), ts)
                    replayed_keys.add(key)
                    last_good = (
                        offset
                        + HEADER.size
                        + len(key_bytes)
                        + (0 if value is None else len(value))
                    )
                # Truncate torn tail if present, never crash
                try:
                    size = path.stat().st_size
                    if last_good < size:
                        os.truncate(path, last_good)
                        print(
                            f"replayed {len(self._segments)} segments, "
                            f"{len(self._keydir)} keys, truncated "
                            f"{size - last_good} bytes from {path.name}",
                            file=sys.stderr,
                        )
                except FileNotFoundError:
                    pass

    def _maybe_rotate(self, record_size: int) -> None:
        active = self._active
        if active is None:
            raise ValueError("Bitcask is closed")
        if active.tell() > 0 and active.tell() + record_size > self.segment_size:
            self._rotate()

    def delete(self, key: str) -> None:
        _require_key(key)
        with self._lock:
            # Always append tombstone so replay stays deleted (see §7.1)
            ts = int(time.time())
            record = encode_record(ts, key.encode("utf-8"), None)
            self._maybe_rotate(len(record))
            active = self._active
            if active is None:
                raise ValueError("Bitcask is closed")
            active.append(record)  # flush + fsync inside SegmentWriter
            self._keydir.pop(key, None)

    def stats(self) -> dict:
        with self._lock:
            keys = len(self._keydir)
            segments = len(self._segments)
            disk_bytes = sum(
                p.stat().st_size
                for p in self._segments.values()
                if p.exists()
            )
            return {
                "keys": keys,
                "keydir_entries": keys,  # kept for plan compatibility
                "segments": segments,
                "disk_bytes": disk_bytes,
                "active_segment_bytes": (
                    self._active.tell() if self._active else 0
                ),
                "active_segment_id": self._active_id,
                "data_dir": str(self.data_dir),
            }

    def compact(self) -> dict:
        with self._lock:  # hold for whole merge (v1: simple + safe)
            if not self._segments:
                return {"segments_merged": 0, "bytes_reclaimed": 0}

            segments_merged = 0
            before = sum(
                p.stat().st_size
                for p in self._segments.values()
                if p.exists()
            )

            # Snapshot: decisions are made against this copy,
            # mutations go to the live dict
            live_snapshot = dict(self._keydir)

            for seg_id in sorted(self._segments):
                if seg_id == self._active_id:
                    continue  # never compact the active segment
                old_path = self._segments[seg_id]
                if not old_path.exists():
                    continue

                tmp_path = old_path.with_suffix(".tmp")  # seg_000001.tmp
                tmp_writer = SegmentWriter(tmp_path, seg_id)  # reuse append/fsync logic

                for offset, ts, key_bytes, value in iter_records(old_path):
                    key = key_bytes.decode("utf-8")
                    loc = live_snapshot.get(key)
                    # Only rewrite if this EXACT record is what's live now.
                    # Tombstones (value is None) are never live — skip.
                    if value is None or loc is None:
                        continue
                    live_seg, live_off, _, _ = loc
                    if live_seg == seg_id and live_off == offset:
                        # Re-encode with the original ts, keeping it.
                        new_record = encode_record(ts, key_bytes, value)
                        new_offset = tmp_writer.append(new_record)
                        self._keydir[key] = (seg_id, new_offset, len(value), ts)

                tmp_writer.close()

                # fsync temp before rename (SegmentWriter already fsyncs per append,
                # but do a final explicit fsync of the file + directory)
                try:
                    fd = os.open(tmp_path, os.O_RDONLY)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                except OSError:
                    pass

                # Atomic replacement: tmp → old_path (same segment id)
                os.replace(tmp_path, old_path)
                # fsync directory so rename is durable (best effort)
                self._fsync_dir(self.segments_dir)

                segments_merged += 1
                if old_path.exists() and old_path.stat().st_size == 0:
                    # No live records — drop the segment entirely
                    old_path.unlink()
                    del self._segments[seg_id]

            after = sum(
                p.stat().st_size
                for p in self._segments.values()
                if p.exists()
            )
            reclaimed = max(0, before - after)
            return {"segments_merged": segments_merged, "bytes_reclaimed": reclaimed}

        # --- helpers (add alongside compact) ---
    def close(self) -> None:
        """Flush and close the active segment. Safe to call twice."""
        with self._lock:
            if self._active is not None:
                self._active.close()  # flush + close file object
                self._active = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc, tb) -> Literal[False]:
        self.close()
        return False

    def _fsync_dir(self, path: Path) -> None:
            try:
                fd = os.open(path, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            except OSError:
                pass  # best-effort on platforms that don't support it

    def _segment_has_live(self, seg_id: int, snapshot: dict) -> bool:
            return any(loc[0] == seg_id for loc in snapshot.values())

def _seg_id_of(path: Path) -> int:
    # seg_000001.log -> 1
    return int(path.stem.split("_", 1)[1])
