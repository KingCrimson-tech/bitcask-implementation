# bitcask/log.py
import struct
import zlib
import os
from pathlib import Path

MAGIC = b"BITC"
HEADER = struct.Struct(">4sQIII")             # 24 bytes: magic, ts, key_len, val_len, crc
HEADER_WITHOUT_CRC = struct.Struct(">4sQII")  # 20 bytes: magic, ts, key_len, val_len


def encode_record(ts: int, key: bytes, value: bytes | None) -> bytes:
    """Encode one record. value=None encodes a tombstone (val_len=0)."""
    value_bytes = b"" if value is None else value
    val_len = 0 if value is None else len(value_bytes)

    header_without_crc = HEADER_WITHOUT_CRC.pack(MAGIC, ts, len(key), val_len)
    crc = zlib.crc32(header_without_crc + key + value_bytes) & 0xFFFFFFFF
    header = HEADER.pack(MAGIC, ts, len(key), val_len, crc)

    return header + key + value_bytes


def decode_record(buf: bytes) -> tuple[int, bytes, bytes | None, int]:
    """Decode one record from the start of buf.

    Returns (ts, key, value, record_len) where value is None for a tombstone
    and record_len is the total bytes consumed (header + key + value), so
    callers iterating a file know where the next record starts.

    Raises ValueError on truncated data, bad magic, or CRC mismatch —
    callers (replay, get) treat that as corruption, never as data.
    """
    if len(buf) < HEADER.size:
        raise ValueError("truncated header")

    magic, ts, key_len, val_len, stored_crc = HEADER.unpack(buf[:HEADER.size])
    if magic != MAGIC:
        raise ValueError(f"bad magic: {magic!r}")

    record_len = HEADER.size + key_len + val_len
    if len(buf) < record_len:
        raise ValueError("truncated record")

    key = buf[HEADER.size: HEADER.size + key_len]
    value_bytes = buf[HEADER.size + key_len: record_len]

    crc = zlib.crc32(buf[:HEADER_WITHOUT_CRC.size] + key + value_bytes) & 0xFFFFFFFF
    if crc != stored_crc:
        raise ValueError("CRC mismatch")

    value = None if val_len == 0 else value_bytes
    return ts, key, value, record_len

class SegmentWriter:
    """Append-only writer for one segment file.

    Every record is written, flushed, and fsynced before append() returns,
    so a returned write survives process kill and power loss.
    """

    def __init__(self, path: Path, segment_id: int):
        self.path = Path(path)
        self.segment_id = segment_id
        self._file = open(self.path, "ab")
        # In append mode the OS writes at EOF regardless of the cursor, but
        # tell() must reflect EOF too — seek there explicitly so it does.
        self._file.seek(0, os.SEEK_END)

    def tell(self) -> int:
        """Current end of file = offset where the next record will start."""
        return self._file.tell()

    def append(self, record: bytes) -> int:
        """Append one encoded record durably. Returns the offset it starts at."""
        offset = self.tell()          # capture BEFORE writing — see §4.3
        self._file.write(record)
        self._file.flush()
        os.fsync(self._file.fileno())
        return offset

    def close(self) -> None:
        if not self._file.closed:
            self._file.close()
