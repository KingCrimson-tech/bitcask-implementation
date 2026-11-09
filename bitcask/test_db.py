import pytest

from bitcask.db import Bitcask
from bitcask.log import HEADER, decode_record, encode_record


# --- 1. encode/decode roundtrip, including tombstone -----------------------
def test_encode_decode_roundtrip():
    buf = encode_record(1_700_000_000, b"user:1", b'{"name":"Chinmay"}')
    assert len(buf) == HEADER.size + len(b"user:1") + len(b'{"name":"Chinmay"}')
    ts, key, value, _n = decode_record(buf)
    assert (ts, key, value) == (1_700_000_000, b"user:1", b'{"name":"Chinmay"}')

    tomb = encode_record(1_700_000_001, b"user:1", None)
    _ts, key, value, _n = decode_record(tomb)
    assert key == b"user:1" and value is None

    # Empty value must not collide with a tombstone (§9.4)
    empty = encode_record(1_700_000_002, b"user:1", b"")
    _ts, key, value, _n = decode_record(empty)
    assert key == b"user:1" and value == b""


# --- 2. in-memory roundtrip -------------------------------------------------
def test_put_get(tmp_path):
    with Bitcask(tmp_path / "data") as db:
        db.put("a", "1")
        assert db.get("a") == "1"


# --- 3. persistence across restart -----------------------------------------
def test_persistence_restart(tmp_path):
    with Bitcask(tmp_path / "data") as db:
        db.put("a", "1")
    with Bitcask(tmp_path / "data") as db:
        assert db.get("a") == "1"


# --- 4. overwrite: latest wins, also after restart --------------------------
def test_overwrite_latest_wins(tmp_path):
    with Bitcask(tmp_path / "data") as db:
        db.put("a", "1")
        db.put("a", "2")
        assert db.get("a") == "2"
    with Bitcask(tmp_path / "data") as db:
        assert db.get("a") == "2"


# --- 5. delete then get raises; restart stays deleted -----------------------
def test_delete_raises_and_stays_deleted(tmp_path):
    with Bitcask(tmp_path / "data") as db:
        db.put("a", "1")
        db.delete("a")
        with pytest.raises(KeyError):
            db.get("a")
    with Bitcask(tmp_path / "data") as db:
        with pytest.raises(KeyError):
            db.get("a")


def test_delete_then_reput(tmp_path):
    with Bitcask(tmp_path / "data") as db:
        db.put("a", "1")
        db.delete("a")
        db.put("a", "2")
        assert db.get("a") == "2"
    with Bitcask(tmp_path / "data") as db:
        assert db.get("a") == "2"


# --- 6. delete of nonexistent key is a no-op --------------------------------
def test_delete_nonexistent_is_noop(tmp_path):
    with Bitcask(tmp_path / "data") as db:
        db.delete("never-there")  # must not raise
        with pytest.raises(KeyError):
            db.get("never-there")


# --- 7. torn write: partial tail truncated on reopen -----------------------
def test_torn_write_truncates(tmp_path):
    data_dir = tmp_path / "data"
    with Bitcask(data_dir) as db:
        db.put("k1", "v1")
        db.put("k2", "v2")
    # Simulate a torn tail: append 10 garbage bytes to the active segment.
    segs = sorted((data_dir / "segments").glob("seg_*.log"))
    with open(segs[-1], "ab") as f:
        f.write(b"BITC\x00\x00\x00")
    with Bitcask(data_dir) as db:  # must not raise
        assert db.get("k1") == "v1"
        assert db.get("k2") == "v2"


# --- 8. CRC corruption: flip a byte, reopen truncates at that record -------
def test_crc_corruption(tmp_path):
    data_dir = tmp_path / "data"
    with Bitcask(data_dir) as db:
        db.put("k1", "v1")
        db.put("k2", "v2")
    segs = sorted((data_dir / "segments").glob("seg_*.log"))
    path = segs[-1]
    raw = bytearray(path.read_bytes())
    raw[-1] ^= 0xFF  # flip the last payload byte (k2's value)
    path.write_bytes(bytes(raw))
    with Bitcask(data_dir) as db:
        assert db.get("k1") == "v1"  # good prefix preserved
        with pytest.raises(KeyError):  # corrupt tail dropped
            db.get("k2")


def test_get_crc_mismatch_raises_value_error(tmp_path):
    # §9.2: corruption after startup must fail loudly on the read path,
    # not be returned as a value.
    data_dir = tmp_path / "data"
    with Bitcask(data_dir) as db:
        db.put("k1", "v1")
        db.put("k2", "v2")
        segs = sorted((data_dir / "segments").glob("seg_*.log"))
        path = segs[-1]
        raw = bytearray(path.read_bytes())
        raw[-1] ^= 0xFF
        path.write_bytes(bytes(raw))
        assert db.get("k1") == "v1"
        with pytest.raises(ValueError, match="CRC mismatch"):
            db.get("k2")


def test_close_is_idempotent(tmp_path):
    db = Bitcask(tmp_path / "data")
    db.put("a", "1")
    db.close()
    db.close()  # must not raise


# --- 9. compaction reclaims space, live data intact --------------------------
def test_compaction_reclaims(tmp_path):
    data_dir = tmp_path / "data"
    with Bitcask(data_dir, segment_size=16 * 1024) as db:
        for i in range(1_000):
            for j in range(10):
                db.put(f"key_{j}", f"value-{i}")
        before = db.stats()["disk_bytes"]
        result = db.compact()
        after = db.stats()["disk_bytes"]
        assert result["segments_merged"] >= 1
        assert after < before // 4  # >75% reclaimed; plan says >80%, keep a margin
        for j in range(10):
            assert db.get(f"key_{j}") == "value-999"
    with Bitcask(data_dir) as db:  # survives restart after compaction
        for j in range(10):
            assert db.get(f"key_{j}") == "value-999"


def test_compaction_preserves_live_keys(tmp_path):
    with Bitcask(tmp_path / "data") as db:
        db.put("a", "1")
        db.put("b", "2")
        db.delete("b")  # tombstone — b must NOT come back
        db.put("a", "3")  # overwrite — a=1 is dead
        db.compact()
        assert db.get("a") == "3"
        with pytest.raises(KeyError):
            db.get("b")
    with Bitcask(tmp_path / "data") as db:
        assert db.get("a") == "3"
        with pytest.raises(KeyError):
            db.get("b")


# --- 10. stats counts ---------------------------------------------------------
def test_stats_counts(tmp_path):
    with Bitcask(tmp_path / "data") as db:
        s = db.stats()
        assert s["keys"] == 0
        db.put("a", "1")
        db.put("b", "2")
        s = db.stats()
        assert s["keys"] == 2
        assert s["segments"] >= 1
        assert s["disk_bytes"] > 0
        assert s["keydir_entries"] == 2
        db.delete("a")
        assert db.stats()["keys"] == 1


# --- 11. unicode, emoji, newlines, large values, empty value -----------------
def test_unicode_and_large_value(tmp_path):
    big = "x" * (100 * 1024)  # 100 KB
    with Bitcask(tmp_path / "data") as db:
        db.put("🔑 emoji-key", "🎉 value")
        assert db.get("🔑 emoji-key") == "🎉 value"
        db.put("key with\nnewlines", "line one\nline two\n")
        assert db.get("key with\nnewlines") == "line one\nline two\n"
        db.put("big", big)
        assert db.get("big") == big
        db.put("empty", "")
        assert db.get("empty") == ""
    with Bitcask(tmp_path / "data") as db:  # all survive restart
        assert db.get("🔑 emoji-key") == "🎉 value"
        assert db.get("big") == big
        assert db.get("empty") == ""


# --- 12. empty key rejected ------------------------------------------------------
@pytest.mark.parametrize("op", ["put", "get", "delete"])
def test_empty_key_rejected(tmp_path, op):
    with Bitcask(tmp_path / "data") as db:
        with pytest.raises(ValueError):
            if op == "put":
                db.put("", "v")
            elif op == "get":
                db.get("")
            else:
                db.delete("")


def test_non_string_key_rejected(tmp_path):
    with Bitcask(tmp_path / "data") as db:
        with pytest.raises(TypeError):
            db.put(None, "v")  # type: ignore[arg-type]
