import threading
import time
from pathlib import Path
import os
import sys

from bitcask.log import SegmentWriter, encode_record, decode_record, HEADER, MAGIC, iter_records


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
            self._active = SegmentWriter(active_path, self._active_id)

    def put(self, key: str, value: str) -> None:
        with self._lock:
            key_bytes = key.encode("utf-8")
            value_bytes = value.encode("utf-8")
            ts = int(time.time())

            record = encode_record(ts, key_bytes, value_bytes)

            self._maybe_rotate(len(record))

            offset = self._active.append(record)
            self._keydir[key] = (
                self._active_id,
                offset,
                len(value_bytes),
                ts
            )

    def get(self, key: str) -> str:
        with self._lock:
            if key not in self._keydir:
                raise KeyError(key)

            seg_id, offset, value_size, ts = self._keydir[key]

            with open(self._segments[seg_id], "rb") as f:
                f.seek(offset)
                header = f.read(HEADER.size)
                if len(header) < HEADER.size:
                    raise ValueError(f"corrupt record at seg {seg_id} offset {offset}")

                magic, rts, key_len, val_len, crc = HEADER.unpack(header)
                if magic != MAGIC:
                    raise ValueError(f"bad magic at seg {seg_id} offset {offset}")

                payload = f.read(key_len + val_len)

            # Reassemble the full record and let decode_record verify the CRC.
            _, found_key, value, _ = decode_record(header + payload)

            if value is None:  # tombstone — deleted keys read as missing
                raise KeyError(key)

            return value.decode("utf-8")

    def _rotate(self) -> None:
        self._active.close()
        self._active_id += 1
        path = self.segments_dir / f"seg_{self._active_id:06d}.log"
        self._segments[self._active_id] = path
        self._active = SegmentWriter(path, self._active_id)

    def close(self) -> None:
        with self._lock:
            self._active.close()

    def __enter__(self) -> "Bitcask":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

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
                    last_good = offset + HEADER.size + len(key_bytes) + (len(value) if value else 0)
                # Truncate torn tail if present, never crash
                try:
                    size = path.stat().st_size
                    if last_good < size:
                        os.truncate(path, last_good)
                        print(
                            f"replayed {len(self._segments)} segments, {len(self._keydir)} keys, truncated {size - last_good} bytes from {path.name}",
                            file=sys.stderr,
                        )
                except FileNotFoundError:
                    pass

    def _maybe_rotate(self, record_size: int) -> None:
        if (
            self._active.tell() > 0
            and self._active.tell() + record_size > self.segment_size
        ):
            self._rotate()
    
    def delete(self, key: str) -> None:
        if not key:
            raise ValueError("empty key")
        with self._lock:
            # Always append tombstone so replay stays deleted (see §7.1)
            ts = int(time.time())
            record = encode_record(ts, key.encode("utf-8"), None)
            self._maybe_rotate(len(record))
            self._active.append(record)   # flush + fsync inside SegmentWriter
            self._keydir.pop(key, None)
    
    def stats(self) -> dict:
        with self._lock:
            keys = len(self._keydir)
            segments = len(self._segments)
            disk_bytes = sum(p.stat().st_size for p in self._segments.values() if p.exists())
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

def _seg_id_of(path: Path) -> int:
    # seg_000001.log -> 1
    return int(path.stem.split("_", 1)[1])