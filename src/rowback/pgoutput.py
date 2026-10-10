"""Decoder for the pgoutput logical replication protocol (version 1).

Reference: PostgreSQL docs, "Logical Replication Message Formats". Only text
format column values are produced by pgoutput unless binary mode is
requested, which rowback never does.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


class _Unchanged:
    """Marker for an unchanged TOASTed value that pgoutput did not send."""

    _instance: "_Unchanged | None" = None

    def __new__(cls) -> "_Unchanged":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "UNCHANGED"

    def __reduce__(self) -> str:
        return "UNCHANGED"


UNCHANGED = _Unchanged()
Value = str | None | _Unchanged


class DecodeError(ValueError):
    pass


@dataclass(frozen=True)
class Column:
    name: str
    type_oid: int
    is_key: bool


@dataclass(frozen=True)
class Begin:
    final_lsn: int
    commit_ts: datetime
    xid: int


@dataclass(frozen=True)
class Commit:
    commit_lsn: int
    end_lsn: int
    commit_ts: datetime


@dataclass(frozen=True)
class Origin:
    lsn: int
    name: str


@dataclass(frozen=True)
class Relation:
    oid: int
    schema: str
    name: str
    replica_identity: str
    columns: list[Column] = field(default_factory=list)


@dataclass(frozen=True)
class TypeMsg:
    oid: int
    schema: str
    name: str


@dataclass(frozen=True)
class Insert:
    oid: int
    new: list[Value]


@dataclass(frozen=True)
class Update:
    oid: int
    old_kind: str | None
    old: list[Value] | None
    new: list[Value]


@dataclass(frozen=True)
class Delete:
    oid: int
    old_kind: str
    old: list[Value]


@dataclass(frozen=True)
class Truncate:
    oids: list[int]
    options: int


@dataclass(frozen=True)
class LogicalMessage:
    transactional: bool
    lsn: int
    prefix: str
    content: bytes


Message = (Begin | Commit | Origin | Relation | TypeMsg | Insert | Update | Delete | Truncate
           | LogicalMessage)


_I8, _I16, _I32 = struct.Struct("!b"), struct.Struct("!h"), struct.Struct("!i")
_U32, _I64, _U64 = struct.Struct("!I"), struct.Struct("!q"), struct.Struct("!Q")


class _Reader:
    __slots__ = ("buf", "pos")

    def __init__(self, buf: bytes) -> None:
        self.buf = buf
        self.pos = 0

    def _unpack(self, st: struct.Struct) -> int:
        if self.pos + st.size > len(self.buf):
            raise DecodeError("truncated message")
        (v,) = st.unpack_from(self.buf, self.pos)
        self.pos += st.size
        return v  # type: ignore[no-any-return]

    def int8(self) -> int:
        return self._unpack(_I8)

    def byte(self) -> str:
        if self.pos >= len(self.buf):
            raise DecodeError("truncated message")
        c = chr(self.buf[self.pos])
        self.pos += 1
        return c

    def int16(self) -> int:
        return self._unpack(_I16)

    def int32(self) -> int:
        return self._unpack(_I32)

    def uint32(self) -> int:
        return self._unpack(_U32)

    def int64(self) -> int:
        return self._unpack(_I64)

    def uint64(self) -> int:
        return self._unpack(_U64)

    def string(self) -> str:
        end = self.buf.index(b"\0", self.pos)
        out = self.buf[self.pos:end].decode()
        self.pos = end + 1
        return out

    def raw(self, n: int) -> bytes:
        out = self.buf[self.pos:self.pos + n]
        if len(out) != n:
            raise DecodeError("truncated value")
        self.pos += n
        return out

    def timestamp(self) -> datetime:
        return PG_EPOCH + timedelta(microseconds=self.int64())

    def tuple_data(self) -> list[Value]:
        n = self.int16()
        values: list[Value] = []
        for _ in range(n):
            kind = self.byte()
            if kind == "n":
                values.append(None)
            elif kind == "u":
                values.append(UNCHANGED)
            elif kind == "t":
                values.append(self.raw(self.int32()).decode())
            elif kind == "b":
                raise DecodeError("binary tuple values are not supported")
            else:
                raise DecodeError(f"unknown tuple value kind {kind!r}")
        return values


def decode(data: bytes) -> Message:
    """Decode one pgoutput message."""
    if not data:
        raise DecodeError("empty message")
    r = _Reader(data)
    tag = r.byte()
    if tag == "B":
        return Begin(final_lsn=r.uint64(), commit_ts=r.timestamp(), xid=r.uint32())
    if tag == "C":
        r.int8()
        return Commit(commit_lsn=r.uint64(), end_lsn=r.uint64(), commit_ts=r.timestamp())
    if tag == "O":
        return Origin(lsn=r.uint64(), name=r.string())
    if tag == "R":
        oid = r.uint32()
        schema, name = r.string(), r.string()
        ident = r.byte()
        cols = []
        for _ in range(r.int16()):
            flags = r.int8()
            cname = r.string()
            type_oid = r.uint32()
            r.int32()  # atttypmod
            cols.append(Column(cname, type_oid, bool(flags & 1)))
        return Relation(oid=oid, schema=schema, name=name, replica_identity=ident, columns=cols)
    if tag == "Y":
        return TypeMsg(oid=r.uint32(), schema=r.string(), name=r.string())
    if tag == "I":
        oid = r.uint32()
        if r.byte() != "N":
            raise DecodeError("insert without new tuple")
        return Insert(oid=oid, new=r.tuple_data())
    if tag == "U":
        oid = r.uint32()
        kind = r.byte()
        old_kind: str | None = None
        old: list[Value] | None = None
        if kind in ("K", "O"):
            old_kind, old = kind, r.tuple_data()
            kind = r.byte()
        if kind != "N":
            raise DecodeError("update without new tuple")
        return Update(oid=oid, old_kind=old_kind, old=old, new=r.tuple_data())
    if tag == "D":
        oid = r.uint32()
        kind = r.byte()
        if kind not in ("K", "O"):
            raise DecodeError("delete without old tuple")
        return Delete(oid=oid, old_kind=kind, old=r.tuple_data())
    if tag == "T":
        n = r.int32()
        options = r.int8()
        return Truncate(oids=[r.uint32() for _ in range(n)], options=options)
    if tag == "M":
        transactional = bool(r.int8() & 1)
        lsn = r.uint64()
        prefix = r.string()
        return LogicalMessage(transactional, lsn, prefix, r.raw(r.int32()))
    raise DecodeError(f"unknown pgoutput message type {tag!r}")
