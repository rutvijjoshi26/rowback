"""Policy-enforcing SQL gateway for AI agents and other unreviewed sessions.

Every statement is classified with the PostgreSQL parser. Blocked statements
never run. Statements that need review are queued for a human. Everything else
runs in a transaction; before committing, the gateway reads
pg_stat_xact_user_tables, which counts every row the transaction inserted,
updated or deleted (including rows changed by CTEs, functions, triggers and
cascades). Over the limit, the transaction is rolled back and queued instead.
Every decision is written to rowback.guard_log.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

import psycopg2
import psycopg2.extensions
from pglast import parse_sql

from rowback.errors import RowbackError
from rowback.guard.policy import classify

GUARD_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS rowback;
CREATE TABLE IF NOT EXISTS rowback.guard_requests (
    id          bigserial PRIMARY KEY,
    created_at  timestamptz NOT NULL DEFAULT now(),
    agent       text NOT NULL,
    sql         text NOT NULL,
    reasons     text[] NOT NULL,
    status      text NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'rejected', 'executed', 'failed')),
    decided_by  text,
    decided_at  timestamptz,
    result      jsonb
);
CREATE TABLE IF NOT EXISTS rowback.guard_log (
    id         bigserial PRIMARY KEY,
    ts         timestamptz NOT NULL DEFAULT now(),
    agent      text NOT NULL,
    sql        text NOT NULL,
    action     text NOT NULL,
    reasons    text[] NOT NULL DEFAULT '{}',
    rowcount   bigint,
    request_id bigint,
    error      text
);
"""

_ROWS_CHANGED = ("SELECT coalesce(sum(n_tup_ins + n_tup_upd + n_tup_del), 0)::bigint "
                 "FROM pg_stat_xact_user_tables")


@dataclass
class GuardResult:
    status: str  # executed, blocked, pending, rejected or error
    reasons: list[str] = field(default_factory=list)
    rowcount: int | None = None
    columns: list[str] | None = None
    rows: list[list[Any]] | None = None
    request_id: int | None = None
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _split(sql: str) -> list[str]:
    encoded = sql.encode()
    out = []
    for raw in parse_sql(sql):
        start = raw.stmt_location or 0
        end = start + raw.stmt_len if raw.stmt_len else len(encoded)
        out.append(encoded[start:end].decode().strip())
    return [s for s in out if s]


def _jsonable(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    return str(v)


class Gateway:
    def __init__(self, source_dsn: str, history_dsn: str, max_rows: int = 1000,
                 statement_timeout: str = "30s", max_result_rows: int = 500) -> None:
        self.source_dsn = source_dsn
        self.history_dsn = history_dsn
        self.max_rows = max_rows
        self.statement_timeout = statement_timeout
        self.max_result_rows = max_result_rows

    def _history(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(self.history_dsn)

    def ensure_schema(self) -> None:
        conn = self._history()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(hashtext('rowback.guard'))")
                cur.execute(GUARD_SCHEMA)
        finally:
            conn.close()

    def _log(self, agent: str, sql: str, action: str, reasons: list[str],
             rowcount: int | None = None, request_id: int | None = None,
             error: str | None = None) -> None:
        conn = self._history()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("INSERT INTO rowback.guard_log (agent, sql, action, reasons, rowcount,"
                            " request_id, error) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (agent, sql, action, reasons, rowcount, request_id, error))
        finally:
            conn.close()

    def _queue(self, agent: str, sql: str, reasons: list[str]) -> int:
        conn = self._history()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("INSERT INTO rowback.guard_requests (agent, sql, reasons) "
                            "VALUES (%s, %s, %s) RETURNING id", (agent, sql, reasons))
                return cur.fetchone()[0]  # type: ignore[no-any-return,index]
        finally:
            conn.close()

    def _run(self, sql: str, agent: str, row_limit: int | None) -> GuardResult:
        conn = psycopg2.connect(self.source_dsn, application_name=f"rowback-guard:{agent}"[:63])
        try:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = %s", (self.statement_timeout,))
                columns: list[str] | None = None
                rows: list[list[Any]] | None = None
                for statement in _split(sql):
                    cur.execute(statement)
                    if cur.description:
                        columns = [d[0] for d in cur.description]
                        rows = [[_jsonable(v) for v in r]
                                for r in cur.fetchmany(self.max_result_rows)]
                cur.execute(_ROWS_CHANGED)
                changed = int(cur.fetchone()[0])  # type: ignore[index]
                if row_limit is not None and changed > row_limit:
                    conn.rollback()
                    return GuardResult("over_limit", [
                        f"would change {changed} rows (limit {row_limit}); held for approval"],
                        rowcount=changed)
            conn.commit()
            return GuardResult("executed", rowcount=changed, columns=columns, rows=rows)
        except psycopg2.Error as exc:
            conn.rollback()
            return GuardResult("error", error=str(exc).strip())
        finally:
            conn.close()

    def execute(self, sql: str, agent: str = "agent") -> GuardResult:
        verdict = classify(sql)
        if verdict.action == "block":
            self._log(agent, sql, "block", verdict.reasons)
            return GuardResult("blocked", verdict.reasons)
        if verdict.action == "review":
            rid = self._queue(agent, sql, verdict.reasons)
            self._log(agent, sql, "review", verdict.reasons, request_id=rid)
            return GuardResult("pending", verdict.reasons, request_id=rid)
        result = self._run(sql, agent, self.max_rows)
        if result.status == "over_limit":
            rid = self._queue(agent, sql, result.reasons)
            self._log(agent, sql, "review", result.reasons, rowcount=result.rowcount,
                      request_id=rid)
            return GuardResult("pending", result.reasons, rowcount=result.rowcount, request_id=rid)
        if result.status == "error":
            self._log(agent, sql, "error", [], error=result.error)
            return result
        self._log(agent, sql, "allow", [], rowcount=result.rowcount)
        return result

    def request(self, request_id: int) -> dict[str, Any]:
        conn = self._history()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("SELECT id, created_at, agent, sql, reasons, status, decided_by,"
                            " decided_at, result FROM rowback.guard_requests WHERE id = %s",
                            (request_id,))
                row = cur.fetchone()
        finally:
            conn.close()
        if row is None:
            raise RowbackError(f"no guard request {request_id}")
        keys = ["id", "created_at", "agent", "sql", "reasons", "status", "decided_by",
                "decided_at", "result"]
        return dict(zip(keys, row))

    def pending(self) -> list[dict[str, Any]]:
        conn = self._history()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("SELECT id FROM rowback.guard_requests WHERE status = 'pending' "
                            "ORDER BY id")
                ids = [r[0] for r in cur.fetchall()]
        finally:
            conn.close()
        return [self.request(i) for i in ids]

    def _decide(self, request_id: int, status: str, approver: str,
                result: GuardResult | None) -> None:
        conn = self._history()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("UPDATE rowback.guard_requests SET status = %s, decided_by = %s,"
                            " decided_at = now(), result = %s WHERE id = %s AND status = 'pending'",
                            (status, approver, json.dumps(result.as_dict()) if result else None,
                             request_id))
                if cur.rowcount != 1:
                    raise RowbackError(f"guard request {request_id} is not pending")
        finally:
            conn.close()

    def approve(self, request_id: int, approver: str) -> GuardResult:
        """Run a held statement (no row limit) on behalf of a human approver."""
        req = self.request(request_id)
        if req["status"] != "pending":
            raise RowbackError(f"guard request {request_id} is not pending ({req['status']})")
        if classify(req["sql"]).action == "block":
            raise RowbackError("blocked statements cannot be approved")
        result = self._run(req["sql"], req["agent"], row_limit=None)
        self._decide(request_id, "executed" if result.status == "executed" else "failed",
                     approver, result)
        self._log(req["agent"], req["sql"], "approved" if result.status == "executed" else "error",
                  [f"approved by {approver}"], rowcount=result.rowcount, request_id=request_id,
                  error=result.error)
        return result

    def reject(self, request_id: int, approver: str) -> None:
        req = self.request(request_id)
        self._decide(request_id, "rejected", approver, None)
        self._log(req["agent"], req["sql"], "rejected", [f"rejected by {approver}"],
                  request_id=request_id)
