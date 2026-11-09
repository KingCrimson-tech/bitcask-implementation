from bitcask.log import HEADER, decode_record, encode_record
import os
from pathlib import Path

from bitcask.db import Bitcask

def test_encode_decode_roundtrip():
    ts, key, value = 1700000000, b"user:1", b'{"name":"Chinmay"}'

    buf = encode_record(ts, key, value)

    assert buf[:4] == b"BITC"
    assert len(buf) == HEADER.size + len(key) + len(value)  # 24 + 6 + 18 = 48
    assert decode_record(buf) == (ts, key, value, len(buf))

    # Tombstone: value=None -> val_len=0, decodes back to None
    tombstone = encode_record(ts, key, None)
    assert decode_record(tombstone) == (ts, key, None, HEADER.size + len(key))

def test_persistence_restart(tmp_path):
    db = Bitcask(tmp_path)
    db.put("user:1", '{"name":"Chinmay"}')
    db.put("user:2", "hello")
    db.close()

    db2 = Bitcask(tmp_path)  # reopen
    assert db2.get("user:1") == '{"name":"Chinmay"}'
    assert db2.get("user:2") == "hello"
    db2.close()


def test_torn_write_truncates(tmp_path):
    db = Bitcask(tmp_path)
    db.put("a", "1")
    db.put("b", "2")
    db.close()

    # Simulate torn tail: partial header, no payload
    seg = next((tmp_path / "segments").glob("seg_*.log"))
    good_size = seg.stat().st_size
    with(open(seg, "ab")) as f:
        f.write(b"BITC\x00\x00")  # header partial, <24 bytes
        f.flush()
        os.fsync(f.fileno())

    db2 = Bitcask(tmp_path)  # should NOT raise
    assert db2.get("a") == "1"
    assert db2.get("b") == "2"
    db2.close()
    # File was truncated back to last good record (allow +0 for new active segment check)
    # The torn segment itself must be <= good_size + small new-segment overhead
    # — assert it is exactly good_size
    assert seg.stat().st_size == good_size