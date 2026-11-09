# bitcask/test_db.py
from bitcask.log import HEADER, decode_record, encode_record


def test_encode_decode_roundtrip():
    ts, key, value = 1700000000, b"user:1", b'{"name":"Chinmay"}'

    buf = encode_record(ts, key, value)

    assert buf[:4] == b"BITC"
    assert len(buf) == HEADER.size + len(key) + len(value)  # 24 + 6 + 18 = 48
    assert decode_record(buf) == (ts, key, value, len(buf))

    # Tombstone: value=None -> val_len=0, decodes back to None
    tombstone = encode_record(ts, key, None)
    assert decode_record(tombstone) == (ts, key, None, HEADER.size + len(key))
