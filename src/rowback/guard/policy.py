"""SQL policy for agent sessions, using the real PostgreSQL grammar (libpg_query via pglast)."""
from __future__ import annotations

from dataclasses import dataclass, field

from pglast import ast, parse_sql
from pglast.parser import ParseError
from pglast.visitors import Visitor

SEVERITY = {"allow": 0, "review": 1, "block": 2}

# statements an agent session may never run through the gateway
_BLOCKED: dict[type, str] = {
    ast.DropStmt: "DROP", ast.TruncateStmt: "TRUNCATE", ast.AlterTableStmt: "ALTER TABLE",
    ast.GrantStmt: "GRANT/REVOKE", ast.GrantRoleStmt: "GRANT/REVOKE role",
    ast.DropdbStmt: "DROP DATABASE", ast.AlterSystemStmt: "ALTER SYSTEM",
    ast.CreateRoleStmt: "CREATE ROLE", ast.AlterRoleStmt: "ALTER ROLE",
    ast.DropRoleStmt: "DROP ROLE", ast.RenameStmt: "RENAME", ast.DropOwnedStmt: "DROP OWNED",
    ast.ReassignOwnedStmt: "REASSIGN OWNED", ast.TransactionStmt: "transaction control",
    ast.AlterDatabaseStmt: "ALTER DATABASE", ast.VariableSetStmt: "SET",
}


@dataclass
class Verdict:
    action: str  # allow, review or block
    reasons: list[str] = field(default_factory=list)
    writes: bool = False  # contains DML whose row count must be checked

    def escalate(self, action: str, reason: str) -> None:
        if SEVERITY[action] > SEVERITY[self.action]:
            self.action = action
        self.reasons.append(reason)


def _relname(node: ast.RangeVar | None) -> str:
    if node is None:
        return "?"
    return f"{node.schemaname}.{node.relname}" if node.schemaname else node.relname


class _DmlFinder(Visitor):  # type: ignore[misc]
    def __init__(self, verdict: Verdict) -> None:
        super().__init__()
        self.verdict = verdict

    def visit(self, ancestors: object, node: object) -> None:
        if isinstance(node, (ast.DeleteStmt, ast.UpdateStmt)):
            self.verdict.writes = True
            if node.whereClause is None:
                verb = "DELETE" if isinstance(node, ast.DeleteStmt) else "UPDATE"
                self.verdict.escalate(
                    "review", f"{verb} on {_relname(node.relation)} has no WHERE clause")
        elif isinstance(node, (ast.InsertStmt, ast.MergeStmt)):
            self.verdict.writes = True


def classify(sql: str) -> Verdict:
    verdict = Verdict("allow")
    if not sql.strip():
        verdict.escalate("block", "empty statement")
        return verdict
    try:
        stmts = parse_sql(sql)
    except ParseError as exc:
        verdict.escalate("block", f"could not parse SQL: {exc}")
        return verdict
    for raw in stmts:
        stmt = raw.stmt
        blocked = next((label for t, label in _BLOCKED.items() if isinstance(stmt, t)), None)
        if blocked is not None:
            verdict.escalate("block", f"{blocked} is not allowed through the guard")
            continue
        if isinstance(stmt, ast.CopyStmt):
            if stmt.is_program or stmt.filename is not None:
                verdict.escalate("block", "COPY to or from a server file or program")
            continue
        if isinstance(stmt, ast.DoStmt):
            verdict.escalate("review", "DO blocks run opaque procedural code")
            continue
        if isinstance(stmt, ast.CallStmt):
            verdict.escalate("review", "CALL runs a stored procedure")
            continue
        _DmlFinder(verdict)(stmt)
    return verdict
