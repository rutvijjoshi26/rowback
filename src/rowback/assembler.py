"""Group decoded pgoutput messages into captured transactions."""
from __future__ import annotations

import json
from collections.abc import Callable

from rowback import pgoutput as p
from rowback.model import CapturedTxn, Change, Row

CONTEXT_PREFIX = "rowback"
MAX_QUERIES = 5


def _fill_unchanged(new: list[p.Value], old: list[p.Value] | None) -> Row:
    out: Row = []
    for i, v in enumerate(new):
        if v is p.UNCHANGED:
            src = old[i] if old is not None else p.UNCHANGED
            # Without a full old image (REPLICA IDENTITY DEFAULT) the value is unknown.
            # Record NULL; such an UPDATE is refused at undo time because its old row is
            # incomplete, so the placeholder can never be written back.
            out.append(None if src is p.UNCHANGED else src)  # type: ignore[arg-type]
        else:
            out.append(v)  # type: ignore[arg-type]
    return out


def _plain(values: list[p.Value] | None) -> Row | None:
    if values is None:
        return None
    return [None if v is p.UNCHANGED else v for v in values]  # type: ignore[misc]


class TxnAssembler:
    """Feed messages in stream order; get a CapturedTxn back on each Commit."""

    def __init__(self, pk_resolver: Callable[[str, str], list[str]] | None = None) -> None:
        self.pk_resolver = pk_resolver
        self._pk_cache: dict[int, list[str]] = {}
        self.relations: dict[int, p.Relation] = {}
        self._txn: CapturedTxn | None = None
        self._seq = 0
        self._queries: list[str] = []

    def feed(self, msg: p.Message) -> CapturedTxn | None:
        if isinstance(msg, p.Relation):
            self.relations[msg.oid] = msg
            self._pk_cache.pop(msg.oid, None)
            return None
        if isinstance(msg, p.Begin):
            self._txn = CapturedTxn(xid=msg.xid, commit_lsn=0, end_lsn=msg.final_lsn,
                                    commit_ts=msg.commit_ts)
            self._seq = 0
            self._queries = []
            return None
        if isinstance(msg, p.Commit):
            txn = self._txn
            self._txn = None
            if txn is None:
                return None
            txn.commit_lsn, txn.end_lsn, txn.commit_ts = msg.commit_lsn, msg.end_lsn, msg.commit_ts
            txn.query = "\n".join(self._queries) if self._queries else None
            return txn
        txn = self._txn
        if txn is None:
            return None
        if isinstance(msg, p.LogicalMessage):
            if msg.prefix == CONTEXT_PREFIX:
                self._apply_context(txn, msg.content)
            return None
        if isinstance(msg, p.Truncate):
            for oid in msg.oids:
                rel = self.relations[oid]
                txn.truncated_tables.append(f"{rel.schema}.{rel.name}")
            return None
        if isinstance(msg, (p.Insert, p.Update, p.Delete)):
            rel = self.relations[msg.oid]
            base = dict(
                seq=self._seq, schema=rel.schema, table=rel.name,
                columns=[c.name for c in rel.columns], types=[c.type_oid for c in rel.columns],
                key_columns=self._key_columns(rel),
            )
            if isinstance(msg, p.Insert):
                change = Change(op="I", old=None, new=_fill_unchanged(msg.new, None), **base)
            elif isinstance(msg, p.Update):
                change = Change(op="U", old=_plain(msg.old), old_kind=msg.old_kind,
                                new=_fill_unchanged(msg.new, msg.old), **base)
            else:
                change = Change(op="D", old=_plain(msg.old), old_kind=msg.old_kind, new=None,
                                **base)
            txn.changes.append(change)
            self._seq += 1
        return None

    def _apply_context(self, txn: CapturedTxn, content: bytes) -> None:
        try:
            ctx = json.loads(content)
        except ValueError:
            return
        if not isinstance(ctx, dict):
            return
        if txn.role is None and ctx.get("role") is not None:
            txn.role = ctx.get("role")
        if txn.application_name is None and ctx.get("app") is not None:
            txn.application_name = ctx.get("app")
        if txn.undo_of is None and isinstance(ctx.get("undo_of"), int):
            txn.undo_of = ctx["undo_of"]
        query = ctx.get("query")
        if query and query not in self._queries and len(self._queries) < MAX_QUERIES:
            self._queries.append(query)

    def _key_columns(self, rel: p.Relation) -> list[str]:
        cached = self._pk_cache.get(rel.oid)
        if cached is None:
            if self.pk_resolver is not None:
                cached = self.pk_resolver(rel.schema, rel.name)
            else:
                cached = [c.name for c in rel.columns if c.is_key]
            self._pk_cache[rel.oid] = cached
        return cached
