import struct
from datetime import datetime, timezone

from rowback import pgoutput as p

PG_EPOCH_US = 946684800 * 1_000_000


def s(text: str) -> bytes:
    return text.encode() + b"\0"


def tuple_data(*cols: object) -> bytes:
    out = struct.pack("!h", len(cols))
    for c in cols:
        if c is None:
            out += b"n"
        elif c is p.UNCHANGED:
            out += b"u"
        else:
            raw = str(c).encode()
            out += b"t" + struct.pack("!i", len(raw)) + raw
    return out


def ts_bytes(dt: datetime) -> bytes:
    return struct.pack("!q", int(dt.timestamp() * 1_000_000) - PG_EPOCH_US)


WHEN = datetime(2026, 10, 9, 14, 14, 0, tzinfo=timezone.utc)


def test_begin():
    msg = p.decode(b"B" + struct.pack("!q", 0x16B3748) + ts_bytes(WHEN) + struct.pack("!i", 755))
    assert msg == p.Begin(final_lsn=0x16B3748, commit_ts=WHEN, xid=755)


def test_commit():
    msg = p.decode(b"C" + b"\0" + struct.pack("!qq", 10, 20) + ts_bytes(WHEN))
    assert msg == p.Commit(commit_lsn=10, end_lsn=20, commit_ts=WHEN)


def test_relation():
    body = struct.pack("!i", 16384) + s("public") + s("orders") + b"f" + struct.pack("!h", 2)
    body += b"\x01" + s("id") + struct.pack("!ii", 20, -1)
    body += b"\x00" + s("note") + struct.pack("!ii", 25, -1)
    msg = p.decode(b"R" + body)
    assert msg == p.Relation(
        oid=16384, schema="public", name="orders", replica_identity="f",
        columns=[p.Column("id", 20, True), p.Column("note", 25, False)],
    )


def test_insert_with_null_and_unicode():
    msg = p.decode(b"I" + struct.pack("!i", 16384) + b"N" + tuple_data(1, None, "café 'x'"))
    assert msg == p.Insert(oid=16384, new=["1", None, "café 'x'"])


def test_update_with_old_full_tuple_and_unchanged_toast():
    data = b"U" + struct.pack("!i", 7) + b"O" + tuple_data(1, "a") + b"N" + tuple_data(1, p.UNCHANGED)
    msg = p.decode(data)
    assert msg == p.Update(oid=7, old_kind="O", old=["1", "a"], new=["1", p.UNCHANGED])


def test_update_without_old_tuple():
    msg = p.decode(b"U" + struct.pack("!i", 7) + b"N" + tuple_data(1, "b"))
    assert msg == p.Update(oid=7, old_kind=None, old=None, new=["1", "b"])


def test_delete_key_only():
    msg = p.decode(b"D" + struct.pack("!i", 7) + b"K" + tuple_data(1, None))
    assert msg == p.Delete(oid=7, old_kind="K", old=["1", None])


def test_truncate():
    msg = p.decode(b"T" + struct.pack("!ib", 2, 1) + struct.pack("!ii", 5, 6))
    assert msg == p.Truncate(oids=[5, 6], options=1)


def test_logical_message():
    content = b'{"role":"app"}'
    data = b"M" + b"\x01" + struct.pack("!q", 99) + s("rowback") + struct.pack("!i", len(content)) + content
    assert p.decode(data) == p.LogicalMessage(transactional=True, lsn=99, prefix="rowback", content=content)


def test_origin_and_type_are_decoded():
    assert p.decode(b"O" + struct.pack("!q", 5) + s("node1")) == p.Origin(lsn=5, name="node1")
    assert p.decode(b"Y" + struct.pack("!i", 3) + s("public") + s("mood")) == p.TypeMsg(3, "public", "mood")


def test_unknown_message_raises():
    import pytest

    with pytest.raises(p.DecodeError):
        p.decode(b"Z")
