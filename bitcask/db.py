import threading
import time
from pathlib import Path

from bitcask.log import SegmentWriter, encode_record, decode_record, HEADER, MAGIC


class Bitcask:
    def __init__(self, data_dir: str | Path, segment_size: int = 2 * 1024 * 1024):
        self._lock = threading.RLock()
        self._data_dir = Path(data_dir)
        self._segments_dir = self._data_dir / "segments"
        self._segments_dir.mkdir(parents=True, exist_ok=True)

        self._segment_size = segment_size
        self._keydir: dict[str, tuple[int, int, int, int]] = {}
        self._segments: dict[int, Path] = {}

        self._active_id = 1
        self._segments[1] = self._segments_dir / "seg_000001.log"
        self._active = SegmentWriter(self._segments[1], self._active_id)

    def put(self, key: str, value: str) -> None:
        with self._lock:
            key_bytes = key.encode("utf-8")
            value_bytes = value.encode("utf-8")
            ts = int(time.time())

            record = encode_record(ts, key_bytes, value_bytes)

            # Durable append FIRST, index update SECOND (see §4.2).
            offset = self._active.append(record)
            self._keydir[key] = (self._active_id, offset, len(value_bytes), ts)

            # Rotate AFTER the write that crossed the limit.
            if self._active.tell() > self._segment_size:
                self._rotate()

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
        path = self._segments_dir / f"seg_{self._active_id:06d}.log"
        self._segments[self._active_id] = path
        self._active = SegmentWriter(path, self._active_id)

    def close(self) -> None:
        with self._lock:
            self._active.close()

    def __enter__(self) -> "Bitcask":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
