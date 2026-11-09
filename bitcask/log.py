# bitcask/log.py
import struct
import zlib
import os
from pathlib import Path

MAGIC = b"BITC"
HEADER = struct.Struct(">4sQIII")             # 24 bytes: magic, ts, key_len, val_len, crc
HEADER_WITHOUT_CRC = struct.Struct(">4sQII")  # 20 bytes: magic, ts, key_len, val_len

# Tombstones and empty values both have zero payload bytes, so they need a
# way to be told apart on disk (§9.4). The high bit of val_len is a tombstone
# flag: a tombstone stores val_len == TOMBSTONE_FLAG (real length 0), while an
# empty value stores val_len == 0. Real lengths stay below 2**31, so the flag
# never collides with a genuine length. Record size (header+key+value) is
# unchanged in both cases.
TOMBSTONE_FLAG = 0x80000000


def encode_record(ts: int, key: bytes, value: bytes | None) -> bytes:
    """Encode one record. value=None encodes a tombstone (flagged val_len)."""
    if value is None:
        value_bytes = b""
        val_len = TOMBSTONE_FLAG
    else:
        value_bytes = value
        val_len = len(value_bytes)

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

    is_tombstone = bool(val_len & TOMBSTONE_FLAG)
    real_val_len = val_len & ~TOMBSTONE_FLAG
    record_len = HEADER.size + key_len + real_val_len
    if len(buf) < record_len:
        raise ValueError("truncated record")

    key = buf[HEADER.size: HEADER.size + key_len]
    value_bytes = buf[HEADER.size + key_len: record_len]

    crc = zlib.crc32(buf[:HEADER_WITHOUT_CRC.size] + key + value_bytes) & 0xFFFFFFFF
    if crc != stored_crc:
        raise ValueError("CRC mismatch")

    value = None if is_tombstone else value_bytes
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
        """Flush and close. Idempotent — safe to call twice (§9.3)."""
        if self._file is not None and not self._file.closed:
            self._file.flush()
            self._file.close()

def iter_records(path: Path):
    """
    Yield (offset, ts, key: bytes, value: bytes | None) per record.
    Stop at first truncated / CRC-mismatched record. Never raise
    for corruption — caller handles truncation.
    """
    offset = 0
    with open(path, "rb") as f:
        while True:
            start = f.tell()
            header = f.read(HEADER.size)
            if len(header) < HEADER.size:
                return  # EOF or torn header — clean stop
            try:
                magic, ts, key_len, val_len, crc = HEADER.unpack(header)
                if magic != MAGIC:
                    return
                is_tombstone = bool(val_len & TOMBSTONE_FLAG)
                real_val_len = val_len & ~TOMBSTONE_FLAG
                payload = f.read(key_len + real_val_len)
                if len(payload) < key_len + real_val_len:
                    return  # torn payload
                key = payload[:key_len]
                value = (
                    None if is_tombstone else payload[key_len:key_len + real_val_len]
                )
                header_without_crc = HEADER_WITHOUT_CRC.pack(
                    magic, ts, key_len, val_len
                )
                expected = zlib.crc32(header_without_crc + key + (value or b"")) & 0xFFFFFFFF
                if expected != crc:
                    return  # stop: do not yield this record
            except struct.error:
                return
            offset = f.tell()
            yield (start, ts, key, value)
            # loop continues; last_good_offset is start + HEADER.size + key_len + val_len