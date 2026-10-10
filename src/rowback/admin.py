"""Source-database setup: publication, replication slot, and table opt-in."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg2
import psycopg2.extensions
from psycopg2 import sql as q

from rowback.errors import RowbackError

CONTEXT_TRIGGER = "rowback_context"


def _context_function_sql(query_chars: int) -> str:
    query_expr = f"left(current_query(), {int(query_chars)})" if query_chars > 0 else "NULL"
    # SECURITY DEFINER so unprivileged application roles can emit the message;
    # session_user is therefore used to report the authenticated role.
    return f"""
CREATE SCHEMA IF NOT EXISTS rowback;
CREATE OR REPLACE FUNCTION rowback.emit_context() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $fn$
BEGIN
    PERFORM pg_logical_emit_message(true, 'rowback', json_build_object(
        'role', session_user::text,
        'app', current_setting('application_name', true),
        'query', {query_expr})::text);
    RETURN NULL;
END
$fn$;
"""


@dataclass
class TrackedTable:
    table: str
    primary_key: list[str]
    replica_identity: str  # 'full', 'default', 'index', 'nothing'
    context: bool


_IDENT = {"f": "full", "d": "default", "i": "index", "n": "nothing"}


def _connect(dsn: str) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(dsn)
    return conn


def init_source(source_dsn: str, slot: str, publication: str, query_chars: int = 500,
                create_slot: bool = True) -> None:
    """Create the publication, the logical slot and the context function (idempotent)."""
    conn = _connect(source_dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW wal_level")
            if cur.fetchone()[0] != "logical":  # type: ignore[index]
                raise RowbackError("wal_level must be 'logical' on the source server "
                                   "(ALTER SYSTEM SET wal_level = logical; then restart)")
            cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (publication,))
            if cur.fetchone() is None:
                cur.execute(q.SQL("CREATE PUBLICATION {}").format(q.Identifier(publication)))
            cur.execute(_context_function_sql(query_chars))
            if create_slot:
                cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (slot,))
                if cur.fetchone() is None:
                    cur.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
                                (slot,))
    finally:
        conn.close()


def drop_source(source_dsn: str, slot: str, publication: str) -> None:
    conn = _connect(source_dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                        "WHERE slot_name = %s", (slot,))
            cur.execute(q.SQL("DROP PUBLICATION IF EXISTS {}").format(q.Identifier(publication)))
    finally:
        conn.close()


def resolve_table(cur: Any, table: str) -> tuple[int, str, str]:
    """Return (oid, schema, name) for a table reference such as 'orders' or 's."T"'."""
    cur.execute("SELECT to_regclass(%s)::oid", (table,))
    row = cur.fetchone()
    if row is None or row[0] is None:
        raise RowbackError(f"table {table} does not exist")
    cur.execute("SELECT n.nspname, c.relname, c.relkind FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = %s", (row[0],))
    schema, name, kind = cur.fetchone()
    if kind not in ("r", "p"):
        raise RowbackError(f"{schema}.{name} is not a regular table")
    return row[0], schema, name


def primary_key_columns(conn: psycopg2.extensions.connection, schema: str, table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT a.attname FROM pg_index i "
            "JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true "
            "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum "
            "WHERE i.indrelid = (quote_ident(%s) || '.' || quote_ident(%s))::regclass "
            "AND i.indisprimary ORDER BY k.ord", (schema, table))
        return [r[0] for r in cur.fetchall()]


def track_table(source_dsn: str, table: str, publication: str, identity: str = "full",
                context: bool = True) -> TrackedTable:
    """Opt a table in to change capture."""
    if identity not in ("full", "default"):
        raise RowbackError("identity must be 'full' or 'default'")
    conn = _connect(source_dsn)
    try:
        with conn, conn.cursor() as cur:
            _, schema, name = resolve_table(cur, table)
            pk = primary_key_columns(conn, schema, name)
            if not pk:
                raise RowbackError(
                    f"{schema}.{name} has no primary key. rowback needs a primary key to find "
                    f"rows again when undoing; add one with ALTER TABLE {schema}.{name} "
                    "ADD PRIMARY KEY (...) and retry.")
            ident = q.Identifier(schema, name)
            cur.execute(q.SQL("ALTER TABLE {} REPLICA IDENTITY {}").format(
                ident, q.SQL(identity.upper())))
            cur.execute("SELECT 1 FROM pg_publication_tables WHERE pubname = %s "
                        "AND schemaname = %s AND tablename = %s", (publication, schema, name))
            if cur.fetchone() is None:
                cur.execute(q.SQL("ALTER PUBLICATION {} ADD TABLE {}").format(
                    q.Identifier(publication), ident))
            cur.execute(q.SQL("DROP TRIGGER IF EXISTS {} ON {}").format(
                q.Identifier(CONTEXT_TRIGGER), ident))
            if context:
                cur.execute(q.SQL(
                    "CREATE TRIGGER {} AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {} "
                    "FOR EACH STATEMENT EXECUTE FUNCTION rowback.emit_context()").format(
                    q.Identifier(CONTEXT_TRIGGER), ident))
    finally:
        conn.close()
    return TrackedTable(f"{schema}.{name}", pk, identity, context)


def untrack_table(source_dsn: str, table: str, publication: str) -> None:
    conn = _connect(source_dsn)
    try:
        with conn, conn.cursor() as cur:
            _, schema, name = resolve_table(cur, table)
            ident = q.Identifier(schema, name)
            cur.execute("SELECT 1 FROM pg_publication_tables WHERE pubname = %s "
                        "AND schemaname = %s AND tablename = %s", (publication, schema, name))
            if cur.fetchone() is not None:
                cur.execute(q.SQL("ALTER PUBLICATION {} DROP TABLE {}").format(
                    q.Identifier(publication), ident))
            cur.execute(q.SQL("DROP TRIGGER IF EXISTS {} ON {}").format(
                q.Identifier(CONTEXT_TRIGGER), ident))
            cur.execute(q.SQL("ALTER TABLE {} REPLICA IDENTITY DEFAULT").format(ident))
    finally:
        conn.close()


def list_tracked(source_dsn: str, publication: str) -> list[TrackedTable]:
    conn = _connect(source_dsn)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT pt.schemaname, pt.tablename, c.relreplident, EXISTS ("
                "  SELECT 1 FROM pg_trigger t WHERE t.tgrelid = c.oid AND t.tgname = %s)"
                " FROM pg_publication_tables pt"
                " JOIN pg_namespace n ON n.nspname = pt.schemaname"
                " JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename"
                " WHERE pt.pubname = %s ORDER BY 1, 2", (CONTEXT_TRIGGER, publication))
            rows = cur.fetchall()
            return [TrackedTable(f"{s}.{t}", primary_key_columns(conn, s, t),
                                 _IDENT.get(ident, ident), ctx) for s, t, ident, ctx in rows]
    finally:
        conn.close()
