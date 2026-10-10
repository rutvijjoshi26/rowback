"""Build, preview and apply the inverse of one captured transaction.

The live table is the source of truth for conflict detection: a row conflicts
when its current state is not the state the bad transaction left behind.
Rows are bulk-loaded into temporary tables with COPY and every step is a
single set-based statement, all inside one source transaction.
"""
from __future__ import annotations

import io
import json
from dataclasses import dataclass, field

import psycopg2
import psycopg2.extensions
from psycopg2 import sql as q

from rowback.errors import ConflictError, NotUndoableError, RowbackError
from rowback.model import Change, Row
from rowback.store import HistoryStore

ON_CONFLICT_MODES = ("abort", "skip", "overwrite")
MAX_REPORTED_CONFLICTS = 1000
KIND_ORDER = {"delete": 0, "update": 1, "insert": 2}


@dataclass
class RowImage:
    before: Row | None  # row as it was before the transaction (None: did not exist)
    after: Row | None   # row as the transaction left it (None: deleted)


@dataclass
class UndoStep:
    schema: str
    table: str
    kind: str  # 'delete' (undo an insert), 'update', 'insert' (undo a delete)
    columns: list[str]
    key_columns: list[str]
    rows: list[RowImage] = field(default_factory=list)

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.table}"


@dataclass
class UndoPlan:
    txn_id: int
    steps: list[UndoStep]

    @property
    def n_rows(self) -> int:
        return sum(len(s.rows) for s in self.steps)


@dataclass
class Conflict:
    table: str
    key: dict[str, str | None]
    reason: str  # 'changed', 'missing' or 'exists'


@dataclass
class UndoResult:
    applied: int
    conflicts: list[Conflict]
    skipped: int
    undo_xid: int | None
    dry_run: bool


# plan ------------------------------------------------------------------------


def _key(change_cols: list[str], key_cols: list[str], row: Row) -> tuple[str | None, ...]:
    return tuple(row[change_cols.index(k)] for k in key_cols)


def _require_full(change: Change) -> Row:
    if change.old is None or change.old_kind != "O":
        raise NotUndoableError(
            f"{change.qualified} was captured without full old rows, so the rows this "
            "transaction removed or overwrote are unknown. Track the table with "
            "REPLICA IDENTITY FULL (rowback track --identity full) to make it undoable.")
    return change.old


def build_plan(store: HistoryStore, txn_id: int) -> UndoPlan:
    summary = store.get_txn(txn_id)
    if summary.undone_by is not None or summary.undone_xid is not None:
        ref = summary.undone_by if summary.undone_by is not None else f"xid {summary.undone_xid}"
        raise NotUndoableError(f"transaction {txn_id} was already undone (by {ref})")
    if summary.truncated:
        raise NotUndoableError(
            f"transaction {txn_id} contains TRUNCATE of {', '.join(summary.truncated)}; "
            "TRUNCATE does not log row images, so it cannot be undone row by row")
    changes = store.load_changes(txn_id)
    if not changes:
        raise NotUndoableError(f"transaction {txn_id} has no captured row changes")

    # collapse all changes per row identity: keep first before-image and last after-image
    entries: dict[tuple[str, tuple[str | None, ...]], tuple[Change, RowImage, int]] = {}
    shapes: dict[str, tuple[list[str], list[str]]] = {}
    order: list[tuple[str, tuple[str | None, ...]]] = []
    for c in changes:
        shape = shapes.setdefault(c.qualified, (c.columns, c.key_columns))
        if shape != (c.columns, c.key_columns):
            raise NotUndoableError(f"the columns of {c.qualified} changed inside transaction "
                                   f"{txn_id}; undo across DDL is not supported")
        if not c.key_columns:
            raise NotUndoableError(f"{c.qualified} has no primary key")
        if c.op == "I":
            assert c.new is not None
            k = (c.qualified, _key(c.columns, c.key_columns, c.new))
            if k in entries:
                ch, img, _ = entries[k]
                img.after = c.new
                entries[k] = (ch, img, c.seq)
            else:
                entries[k] = (c, RowImage(None, c.new), c.seq)
                order.append(k)
        elif c.op == "U":
            old = _require_full(c)
            assert c.new is not None
            old_k = (c.qualified, _key(c.columns, c.key_columns, old))
            new_k = (c.qualified, _key(c.columns, c.key_columns, c.new))
            if old_k in entries:
                ch, img, _ = entries.pop(old_k)
            else:
                ch, img = c, RowImage(old, None)
            img.after = c.new
            if new_k in entries:  # key moved onto a slot whose earlier row was deleted
                _, other, _ = entries[new_k]
                if other.after is not None:
                    raise NotUndoableError("conflicting key history inside the transaction")
                if other.before is not None:
                    order.append((c.qualified, ("__moved__", *new_k[1], str(c.seq))))
                    entries[order[-1]] = (ch, RowImage(other.before, None), c.seq)
            entries[new_k] = (ch, img, c.seq)
            order.append(new_k)
        else:
            old = _require_full(c)
            k = (c.qualified, _key(c.columns, c.key_columns, old))
            if k in entries:
                ch, img, _ = entries[k]
                img.after = None
                entries[k] = (ch, img, c.seq)
            else:
                entries[k] = (c, RowImage(old, None), c.seq)
                order.append(k)

    steps: dict[tuple[str, str], UndoStep] = {}
    seen: set[tuple[str, tuple[str | None, ...]]] = set()
    for k in order:
        if k in seen or k not in entries:
            continue
        seen.add(k)
        ch, img, _ = entries[k]
        if img.before is None and img.after is None:
            continue
        if img.before is None:
            kind = "delete"
        elif img.after is None:
            kind = "insert"
        elif img.before == img.after:
            continue
        else:
            kind = "update"
        step = steps.get((ch.qualified, kind))
        if step is None:
            step = steps[(ch.qualified, kind)] = UndoStep(ch.schema, ch.table, kind, ch.columns,
                                                          ch.key_columns)
        step.rows.append(img)
    return UndoPlan(txn_id, list(steps.values()))


def _fk_order(conn: psycopg2.extensions.connection, tables: list[str]) -> list[str]:
    """Return tables with referenced (parent) tables before referencing (child) tables."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT cn.nspname || '.' || cr.relname, pn.nspname || '.' || pr.relname "
            "FROM pg_constraint k "
            "JOIN pg_class cr ON cr.oid = k.conrelid JOIN pg_namespace cn ON cn.oid = cr.relnamespace "
            "JOIN pg_class pr ON pr.oid = k.confrelid JOIN pg_namespace pn ON pn.oid = pr.relnamespace "
            "WHERE k.contype = 'f' AND k.conrelid <> k.confrelid")
        edges = [(child, parent) for child, parent in cur.fetchall()
                 if child in tables and parent in tables]
    ordered: list[str] = []
    remaining = list(tables)
    while remaining:
        ready = [t for t in remaining
                 if not any(c == t and p in remaining for c, p in edges)]
        if not ready:  # cycle: keep the original order for what is left
            ordered.extend(remaining)
            break
        for t in ready:
            ordered.append(t)
            remaining.remove(t)
    return ordered


def _ordered_steps(conn: psycopg2.extensions.connection, plan: UndoPlan) -> list[UndoStep]:
    tables = list(dict.fromkeys(s.qualified for s in plan.steps))
    topo = _fk_order(conn, tables)
    rank = {t: i for i, t in enumerate(topo)}

    def sort_key(s: UndoStep) -> tuple[int, int]:
        r = rank[s.qualified]
        # deletes run children-first, inserts parents-first
        return (KIND_ORDER[s.kind], -r if s.kind == "delete" else r)

    return sorted(plan.steps, key=sort_key)


# preview -----------------------------------------------------------------------


def _lit(v: str | None) -> str:
    return "NULL" if v is None else "'" + v.replace("'", "''") + "'"


def _where(step: UndoStep, row: Row) -> str:
    return " AND ".join(f"{q_ident(k)} = {_lit(row[step.columns.index(k)])}"
                        for k in step.key_columns)


def q_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"' if not name.isidentifier() or name.lower() != name \
        else name


def preview_sql(plan: UndoPlan, limit: int | None = 20) -> str:
    lines = [f"-- rowback undo of transaction {plan.txn_id}: {plan.n_rows} row(s)", "BEGIN;"]
    for step in plan.steps:
        target = f"{q_ident(step.schema)}.{q_ident(step.table)}"
        cols = ", ".join(q_ident(c) for c in step.columns)
        shown = step.rows if limit is None else step.rows[:limit]
        for img in shown:
            if step.kind == "insert":
                assert img.before is not None
                lines.append(f"INSERT INTO {target} ({cols}) VALUES "
                             f"({', '.join(_lit(v) for v in img.before)});")
            elif step.kind == "delete":
                assert img.after is not None
                lines.append(f"DELETE FROM {target} WHERE {_where(step, img.after)};")
            else:
                assert img.before is not None and img.after is not None
                sets = ", ".join(f"{q_ident(c)} = {_lit(v)}" for c, v in zip(step.columns, img.before)
                                 if c in step.key_columns or v != img.after[step.columns.index(c)])
                lines.append(f"UPDATE {target} SET {sets} WHERE {_where(step, img.after)};")
        hidden = len(step.rows) - len(shown)
        if hidden:
            lines.append(f"-- ... {hidden} more row{'s' if hidden != 1 else ''} for {step.kind} "
                         f"on {step.qualified}")
    lines.append("COMMIT;")
    return "\n".join(lines)


# apply -------------------------------------------------------------------------


def _copy_text(v: str | None) -> str:
    if v is None:
        return "\\N"
    return (v.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")
            .replace("\r", "\\r"))


def _load(cur: psycopg2.extensions.cursor, temp: str, columns: list[str],
          rows: list[tuple[int, Row]]) -> None:
    buf = io.StringIO()
    for n, row in rows:
        buf.write(str(n))
        for v in row:
            buf.write("\t")
            buf.write(_copy_text(v))
        buf.write("\n")
    buf.seek(0)
    cols = q.SQL(", ").join([q.Identifier("_rb_n"), *[q.Identifier(c) for c in columns]])
    cur.copy_expert(q.SQL("COPY {} ({}) FROM STDIN").format(q.Identifier(temp), cols)
                    .as_string(cur), buf)


class _StepRunner:
    def __init__(self, cur: psycopg2.extensions.cursor, step: UndoStep, idx: int) -> None:
        self.cur, self.step = cur, step
        self.target = q.Identifier(step.schema, step.table)
        self.cols = [q.Identifier(c) for c in step.columns]
        self.col_list = q.SQL(", ").join(self.cols)
        self.before_t, self.after_t = f"_rb_before_{idx}", f"_rb_after_{idx}"

    def key_join(self, live: str, img: str) -> q.Composable:
        return q.SQL(" AND ").join(
            q.SQL("{}.{} = {}.{}").format(q.Identifier(live), q.Identifier(k), q.Identifier(img),
                                          q.Identifier(k)) for k in self.step.key_columns)

    def same_row(self, live: str, img: str) -> q.Composable:
        return q.SQL(" AND ").join(
            q.SQL("{}.{}::text IS NOT DISTINCT FROM {}.{}::text").format(
                q.Identifier(live), c, q.Identifier(img), c) for c in self.cols)

    def make_temp(self, name: str) -> None:
        self.cur.execute(q.SQL(
            "CREATE TEMP TABLE {} ON COMMIT DROP AS SELECT 0::int AS _rb_n, {} FROM {} WITH NO DATA"
        ).format(q.Identifier(name), self.col_list, self.target))

    def load(self) -> None:
        rows = self.step.rows
        if self.step.kind in ("insert", "update"):
            self.make_temp(self.before_t)
            _load(self.cur, self.before_t, self.step.columns,
                  [(n, r.before) for n, r in enumerate(rows)])  # type: ignore[misc]
        if self.step.kind in ("delete", "update"):
            self.make_temp(self.after_t)
            _load(self.cur, self.after_t, self.step.columns,
                  [(n, r.after) for n, r in enumerate(rows)])  # type: ignore[misc]
        for t in ((self.before_t,) if self.step.kind == "insert" else (self.after_t,)):
            self.cur.execute(q.SQL("CREATE INDEX ON {} ({})").format(
                q.Identifier(t), q.SQL(", ").join(q.Identifier(k) for k in self.step.key_columns)))
            self.cur.execute(q.SQL("ANALYZE {}").format(q.Identifier(t)))

    def find_conflicts(self) -> list[tuple[int, str]]:
        """Lock affected live rows and return (row number, reason) for conflicting rows."""
        img = self.before_t if self.step.kind == "insert" else self.after_t
        self.cur.execute(q.SQL("SELECT 1 FROM {} t JOIN {} i ON {} FOR UPDATE OF t").format(
            self.target, q.Identifier(img), self.key_join("t", "i")))
        if self.step.kind == "insert":
            self.cur.execute(q.SQL("SELECT i._rb_n, 'exists' FROM {} i JOIN {} t ON {}").format(
                q.Identifier(img), self.target, self.key_join("t", "i")))
        else:
            first_key = q.Identifier(self.step.key_columns[0])
            self.cur.execute(q.SQL(
                "SELECT i._rb_n, CASE WHEN t.{} IS NULL THEN 'missing' ELSE 'changed' END "
                "FROM {} i LEFT JOIN {} t ON {} WHERE t.{} IS NULL OR NOT ({})").format(
                first_key, q.Identifier(img), self.target, self.key_join("t", "i"), first_key,
                self.same_row("t", "i")))
        return [(n, reason) for n, reason in self.cur.fetchall()]

    def drop_rows(self, numbers: list[int]) -> None:
        for t in (self.before_t, self.after_t):
            if (t == self.before_t and self.step.kind == "delete") or (
                    t == self.after_t and self.step.kind == "insert"):
                continue
            self.cur.execute(q.SQL("DELETE FROM {} WHERE _rb_n = ANY(%s)").format(q.Identifier(t)),
                             (numbers,))

    def apply(self, overwrite: bool) -> int:
        k = self.step.kind
        if k == "delete":
            cond = q.SQL("") if overwrite else q.SQL(" AND {}").format(self.same_row("t", "i"))
            self.cur.execute(q.SQL("DELETE FROM {} t USING {} i WHERE {}{}").format(
                self.target, q.Identifier(self.after_t), self.key_join("t", "i"), cond))
            return self.cur.rowcount
        if k == "insert":
            if overwrite:
                self.cur.execute(q.SQL("DELETE FROM {} t USING {} i WHERE {}").format(
                    self.target, q.Identifier(self.before_t), self.key_join("t", "i")))
            self.cur.execute(q.SQL(
                "INSERT INTO {} ({}) OVERRIDING SYSTEM VALUE SELECT {} FROM {}").format(
                self.target, self.col_list, self.col_list, q.Identifier(self.before_t)))
            return self.cur.rowcount
        # update: set every column back to the before-image, matched on the after-image key
        sets = q.SQL(", ").join(q.SQL("{} = b.{}").format(c, c) for c in self.cols)
        cond = q.SQL("") if overwrite else q.SQL(" AND {}").format(self.same_row("t", "a"))
        self.cur.execute(q.SQL(
            "UPDATE {} t SET {} FROM {} a JOIN {} b USING (_rb_n) WHERE {}{}").format(
            self.target, sets, q.Identifier(self.after_t), q.Identifier(self.before_t),
            self.key_join("t", "a"), cond))
        n = self.cur.rowcount
        if overwrite:  # rows deleted after the bad update: put the before-image back
            self.cur.execute(q.SQL(
                "INSERT INTO {} ({}) OVERRIDING SYSTEM VALUE SELECT {} FROM {} b JOIN {} a "
                "USING (_rb_n) WHERE NOT EXISTS (SELECT 1 FROM {} t WHERE {})").format(
                self.target, self.col_list,
                q.SQL(", ").join(q.SQL("b.{}").format(c) for c in self.cols),
                q.Identifier(self.before_t), q.Identifier(self.after_t), self.target,
                self.key_join("t", "b")))
            n += self.cur.rowcount
        return n


def _check_columns(cur: psycopg2.extensions.cursor, step: UndoStep) -> None:
    cur.execute("SELECT attname FROM pg_attribute WHERE attrelid = %s::regclass AND attnum > 0 "
                "AND NOT attisdropped",
                (f"{q_ident(step.schema)}.{q_ident(step.table)}",))
    live = {r[0] for r in cur.fetchall()}
    missing = [c for c in step.columns if c not in live]
    if missing:
        raise NotUndoableError(f"{step.qualified} no longer has column(s) {', '.join(missing)}; "
                               "the table changed shape after the transaction was captured")


def apply_undo(source_dsn: str, store: HistoryStore, plan: UndoPlan, on_conflict: str = "abort",
               dry_run: bool = False, replica_role: bool = False,
               lock_timeout: str = "10s") -> UndoResult:
    """Apply the inverse of a captured transaction in a single source transaction."""
    if on_conflict not in ON_CONFLICT_MODES:
        raise RowbackError(f"on_conflict must be one of {', '.join(ON_CONFLICT_MODES)}")
    conn = psycopg2.connect(source_dsn, application_name="rowback-undo")
    conflicts: list[Conflict] = []
    applied = skipped = 0
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = %s", (lock_timeout,))
            cur.execute("SELECT pg_logical_emit_message(true, 'rowback', %s)", (json.dumps({
                "app": "rowback-undo", "query": f"rowback undo {plan.txn_id}",
                "undo_of": plan.txn_id}),))
            if replica_role:
                cur.execute("SET LOCAL session_replication_role = replica")
            cur.execute("SET CONSTRAINTS ALL DEFERRED")
            for idx, step in enumerate(_ordered_steps(conn, plan)):
                _check_columns(cur, step)
                runner = _StepRunner(cur, step, idx)
                runner.load()
                found = runner.find_conflicts()
                for n, reason in found[:MAX_REPORTED_CONFLICTS]:
                    img = step.rows[n].before if step.kind == "insert" else step.rows[n].after
                    assert img is not None
                    conflicts.append(Conflict(step.qualified, {
                        k: img[step.columns.index(k)] for k in step.key_columns}, reason))
                if found and on_conflict == "abort":
                    continue
                if found and on_conflict == "skip":
                    runner.drop_rows([n for n, _ in found])
                    skipped += len(found)
                applied += runner.apply(overwrite=on_conflict == "overwrite")
            if conflicts and on_conflict == "abort":
                conn.rollback()
                raise ConflictError(
                    f"{len(conflicts)} row(s) changed after transaction {plan.txn_id}; nothing "
                    "was undone. Re-run with --on-conflict skip or overwrite.", conflicts)
            cur.execute("SELECT pg_current_xact_id()::text::bigint")
            xid = cur.fetchone()[0]  # type: ignore[index]
            if dry_run:
                conn.rollback()
                return UndoResult(applied, conflicts, skipped, None, True)
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    store.mark_undo_applied(plan.txn_id, xid)
    return UndoResult(applied, conflicts, skipped, xid, False)
