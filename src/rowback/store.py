"""Durable, compressed change history kept in a PostgreSQL database."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import msgpack
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import zstandard

from rowback.model import CapturedTxn, Change

CHUNK_SIZE = 5000
ZSTD_LEVEL = 3

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS rowback;
CREATE TABLE IF NOT EXISTS rowback.transactions (
    id               bigserial PRIMARY KEY,
    xid              bigint NOT NULL,
    commit_lsn       pg_lsn NOT NULL UNIQUE,
    end_lsn          pg_lsn NOT NULL,
    commit_ts        timestamptz NOT NULL,
    role             text,
    application_name text,
    query            text,
    n_changes        integer NOT NULL,
    n_insert         integer NOT NULL,
    n_update         integer NOT NULL,
    n_delete         integer NOT NULL,
    tables           text[] NOT NULL,
    truncated        text[] NOT NULL DEFAULT '{}',
    raw_bytes        bigint NOT NULL DEFAULT 0,
    undo_of          bigint,
    undone_by        bigint,
    undone_xid       bigint,
    undone_at        timestamptz,
    captured_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS transactions_commit_ts_idx ON rowback.transactions (commit_ts);
CREATE INDEX IF NOT EXISTS transactions_tables_idx ON rowback.transactions USING gin (tables);
CREATE TABLE IF NOT EXISTS rowback.change_chunks (
    txn_id   bigint NOT NULL REFERENCES rowback.transactions (id) ON DELETE CASCADE,
    chunk_no integer NOT NULL,
    n        integer NOT NULL,
    payload  bytea NOT NULL,
    PRIMARY KEY (txn_id, chunk_no)
);
-- payload is already zstd-compressed; skip pglz
ALTER TABLE rowback.change_chunks ALTER COLUMN payload SET STORAGE EXTERNAL;
CREATE TABLE IF NOT EXISTS rowback.capture_state (
    slot          text PRIMARY KEY,
    confirmed_lsn pg_lsn NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now()
);
"""


def lsn_to_str(lsn: int) -> str:
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


def str_to_lsn(text: str) -> int:
    hi, lo = text.split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


@dataclass
class TxnSummary:
    id: int
    xid: int
    commit_lsn: str
    commit_ts: datetime
    role: str | None
    application_name: str | None
    query: str | None
    n_changes: int
    n_insert: int
    n_update: int
    n_delete: int
    tables: list[str]
    truncated: list[str]
    undo_of: int | None
    undone_by: int | None
    undone_xid: int | None


_SUMMARY_COLS = ("id, xid, commit_lsn::text, commit_ts, role, application_name, query, n_changes,"
                 " n_insert, n_update, n_delete, tables, truncated, undo_of, undone_by, undone_xid")


def encode_chunk(changes: list[Change], cctx: zstandard.ZstdCompressor) -> bytes:
    """Serialize changes with a per-chunk relation dictionary, then compress."""
    rels: dict[tuple[str, str, tuple[str, ...]], int] = {}
    rel_list: list[list[Any]] = []
    rows: list[list[Any]] = []
    for c in changes:
        key = (c.schema, c.table, tuple(c.columns))
        idx = rels.get(key)
        if idx is None:
            idx = rels[key] = len(rel_list)
            rel_list.append([c.schema, c.table, c.columns, c.types, c.key_columns])
        rows.append([c.seq, idx, c.op, c.old_kind, c.old, c.new])
    return cctx.compress(msgpack.packb([rel_list, rows], use_bin_type=True))


def decode_chunk(payload: bytes, dctx: zstandard.ZstdDecompressor) -> list[Change]:
    rel_list, rows = msgpack.unpackb(dctx.decompress(payload), raw=False)
    out = []
    for seq, idx, op, old_kind, old, new in rows:
        schema, table, columns, types, key_columns = rel_list[idx]
        out.append(Change(seq=seq, schema=schema, table=table, op=op, columns=columns,
                          types=types, key_columns=key_columns, old=old, new=new,
                          old_kind=old_kind))
    return out


class HistoryStore:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.conn: psycopg2.extensions.connection = psycopg2.connect(dsn)
        self._cctx = zstandard.ZstdCompressor(level=ZSTD_LEVEL)
        self._dctx = zstandard.ZstdDecompressor()

    def close(self) -> None:
        self.conn.close()

    def ensure_schema(self) -> None:
        with self.conn, self.conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext('rowback.schema'))")
            cur.execute(SCHEMA_SQL)

    # capture progress -------------------------------------------------

    def get_confirmed_lsn(self, slot: str) -> int:
        with self.conn, self.conn.cursor() as cur:
            cur.execute("SELECT confirmed_lsn::text FROM rowback.capture_state WHERE slot = %s",
                        (slot,))
            row = cur.fetchone()
        return str_to_lsn(row[0]) if row else 0

    @staticmethod
    def _upsert_lsn(cur: Any, slot: str, lsn: int) -> None:
        cur.execute(
            "INSERT INTO rowback.capture_state (slot, confirmed_lsn) VALUES (%s, %s::pg_lsn) "
            "ON CONFLICT (slot) DO UPDATE SET confirmed_lsn = GREATEST("
            "rowback.capture_state.confirmed_lsn, EXCLUDED.confirmed_lsn), updated_at = now()",
            (slot, lsn_to_str(lsn)))

    def advance_lsn(self, slot: str, lsn: int) -> None:
        with self.conn, self.conn.cursor() as cur:
            self._upsert_lsn(cur, slot, lsn)

    def save_txn(self, slot: str, txn: CapturedTxn) -> int | None:
        """Persist one transaction and advance the confirmed LSN atomically.

        Returns the new history id, or None if the transaction was already stored.
        """
        return self.save_batch(slot, [txn])[0]

    def save_batch(self, slot: str, txns: list[CapturedTxn]) -> list[int | None]:
        """Persist transactions (in commit order) plus the confirmed LSN in one commit.

        Uses a constant number of statements per batch: with a remote history
        database, round trips and not CPU limit capture throughput.
        """
        if not txns:
            return []
        with self.conn, self.conn.cursor() as cur:
            cur.execute("SELECT confirmed_lsn::text FROM rowback.capture_state "
                        "WHERE slot = %s FOR UPDATE", (slot,))
            row = cur.fetchone()
            confirmed = str_to_lsn(row[0]) if row else 0
            fresh = [t for t in txns if t.end_lsn > confirmed]
            ids_by_lsn: dict[str, int] = {}
            if fresh:
                txn_rows, payloads = [], []
                for txn in fresh:
                    counts = {"I": 0, "U": 0, "D": 0}
                    tables: list[str] = []
                    for c in txn.changes:
                        counts[c.op] += 1
                        if c.qualified not in tables:
                            tables.append(c.qualified)
                    chunks = [encode_chunk(txn.changes[i:i + CHUNK_SIZE], self._cctx)
                              for i in range(0, len(txn.changes), CHUNK_SIZE)]
                    payloads.append(chunks)
                    txn_rows.append((txn.xid, lsn_to_str(txn.commit_lsn), lsn_to_str(txn.end_lsn),
                                     txn.commit_ts, txn.role, txn.application_name, txn.query,
                                     len(txn.changes), counts["I"], counts["U"], counts["D"],
                                     tables, txn.truncated_tables, sum(len(p) for p in chunks),
                                     txn.undo_of))
                inserted = psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO rowback.transactions (xid, commit_lsn, end_lsn, commit_ts, role,"
                    " application_name, query, n_changes, n_insert, n_update, n_delete, tables,"
                    " truncated, raw_bytes, undo_of) VALUES %s"
                    " ON CONFLICT (commit_lsn) DO NOTHING RETURNING id, commit_lsn::text",
                    txn_rows,
                    template="(%s, %s::pg_lsn, %s::pg_lsn, %s, %s, %s, %s, %s, %s, %s, %s,"
                             " %s::text[], %s::text[], %s, %s)",
                    page_size=len(txn_rows), fetch=True)
                ids_by_lsn = {lsn: i for i, lsn in inserted}
                chunk_rows = []
                for txn, chunks in zip(fresh, payloads):
                    txn_id = ids_by_lsn.get(lsn_to_str(txn.commit_lsn))
                    if txn_id is None:
                        continue
                    for n, payload in enumerate(chunks):
                        chunk_rows.append((txn_id, n, min(CHUNK_SIZE,
                                                         len(txn.changes) - n * CHUNK_SIZE),
                                           psycopg2.Binary(payload)))
                if chunk_rows:
                    psycopg2.extras.execute_values(
                        cur, "INSERT INTO rowback.change_chunks (txn_id, chunk_no, n, payload)"
                        " VALUES %s", chunk_rows, page_size=len(chunk_rows))
                links = [(ids_by_lsn[lsn_to_str(t.commit_lsn)], t.undo_of) for t in fresh
                         if t.undo_of is not None and lsn_to_str(t.commit_lsn) in ids_by_lsn]
                for undo_id, orig in links:
                    cur.execute("UPDATE rowback.transactions SET undone_by = %s WHERE id = %s",
                                (undo_id, orig))
            self._upsert_lsn(cur, slot, max(confirmed, txns[-1].end_lsn))
        return [ids_by_lsn.get(lsn_to_str(t.commit_lsn)) if t.end_lsn > confirmed else None
                for t in txns]

    # queries -------------------------------------------------------------

    def search(self, since: datetime | None = None, until: datetime | None = None,
               table: str | None = None, op: str | None = None, min_rows: int | None = None,
               limit: int = 50) -> list[TxnSummary]:
        where: list[str] = ["(n_changes > 0 OR cardinality(truncated) > 0)"]
        params: list[Any] = []
        if since is not None:
            where.append("commit_ts >= %s")
            params.append(since)
        if until is not None:
            where.append("commit_ts <= %s")
            params.append(until)
        if table is not None:
            qualified = table if "." in table else f"public.{table}"
            where.append("(tables @> ARRAY[%s]::text[] OR truncated @> ARRAY[%s]::text[])")
            params += [qualified, qualified]
        if op is not None:
            column = {"I": "n_insert", "U": "n_update", "D": "n_delete"}[op.upper()[0]]
            where.append(f"{column} > 0")
        if min_rows is not None:
            where.append("n_changes >= %s")
            params.append(min_rows)
        params.append(limit)
        with self.conn, self.conn.cursor() as cur:
            cur.execute(f"SELECT {_SUMMARY_COLS} FROM rowback.transactions WHERE "
                        + " AND ".join(where) + " ORDER BY commit_ts DESC, id DESC LIMIT %s",
                        params)
            return [TxnSummary(*r) for r in cur.fetchall()]

    def get_txn(self, txn_id: int) -> TxnSummary:
        with self.conn, self.conn.cursor() as cur:
            cur.execute(f"SELECT {_SUMMARY_COLS} FROM rowback.transactions WHERE id = %s",
                        (txn_id,))
            row = cur.fetchone()
        if row is None:
            from rowback.errors import RowbackError

            raise RowbackError(f"no captured transaction with id {txn_id}")
        return TxnSummary(*row)

    def load_changes(self, txn_id: int) -> list[Change]:
        with self.conn, self.conn.cursor() as cur:
            cur.execute("SELECT payload FROM rowback.change_chunks WHERE txn_id = %s "
                        "ORDER BY chunk_no", (txn_id,))
            out: list[Change] = []
            for (payload,) in cur.fetchall():
                out.extend(decode_chunk(bytes(payload), self._dctx))
        return out

    def mark_undone(self, txn_id: int, undo_id: int) -> None:
        with self.conn, self.conn.cursor() as cur:
            cur.execute("UPDATE rowback.transactions SET undone_by = %s WHERE id = %s",
                        (undo_id, txn_id))
            cur.execute("UPDATE rowback.transactions SET undo_of = %s WHERE id = %s",
                        (txn_id, undo_id))

    def mark_undo_applied(self, txn_id: int, undo_xid: int) -> None:
        """Record immediately (before capture catches up) that a transaction was undone."""
        with self.conn, self.conn.cursor() as cur:
            cur.execute("UPDATE rowback.transactions SET undone_xid = %s, undone_at = now() "
                        "WHERE id = %s", (undo_xid, txn_id))

    def prune(self, older_than: timedelta) -> int:
        with self.conn, self.conn.cursor() as cur:
            cur.execute("DELETE FROM rowback.transactions WHERE commit_ts < now() - %s",
                        (older_than,))
            return cur.rowcount

    def stats(self) -> dict[str, int]:
        with self.conn, self.conn.cursor() as cur:
            cur.execute(
                "SELECT count(*), coalesce(sum(n_changes), 0), coalesce(sum(raw_bytes), 0),"
                " pg_total_relation_size('rowback.transactions'),"
                " pg_total_relation_size('rowback.change_chunks')"
                " FROM rowback.transactions")
            n, changes, payload, t_bytes, c_bytes = cur.fetchone()  # type: ignore[misc]
        return {"transactions": n, "changes": int(changes), "payload_bytes": int(payload),
                "transactions_table_bytes": t_bytes, "chunks_table_bytes": c_bytes,
                "total_bytes": t_bytes + c_bytes}
