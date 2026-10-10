"""Command-line interface."""
from __future__ import annotations

import json
import logging
import re
import signal
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import click

from rowback import admin
from rowback.capture import CaptureDaemon
from rowback.config import Settings
from rowback.errors import ConflictError, RowbackError
from rowback.monitor import slot_status
from rowback.store import HistoryStore, TxnSummary

EXIT_ERROR, EXIT_CONFLICT, EXIT_CRITICAL = 1, 3, 4
_DURATION = re.compile(r"^(\d+(?:\.\d+)?)\s*([smhd])$")
_UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}
_OPS = {"insert": "I", "update": "U", "delete": "D", "i": "I", "u": "U", "d": "D"}


def parse_duration(text: str) -> timedelta:
    m = _DURATION.match(text.strip().lower())
    if not m:
        raise click.BadParameter(f"expected a duration such as 90s, 15m, 2h or 7d, got {text!r}")
    return timedelta(**{_UNITS[m.group(2)]: float(m.group(1))})


def parse_time(text: str | None) -> datetime | None:
    if text is None:
        return None
    if _DURATION.match(text.strip().lower()):
        return datetime.now(timezone.utc) - parse_duration(text)
    dt = datetime.fromisoformat(text)
    return dt if dt.tzinfo else dt.astimezone()


def _settings(ctx: click.Context) -> Settings:
    return ctx.obj  # type: ignore[no-any-return]


def _summary_dict(t: TxnSummary) -> dict[str, Any]:
    d = dict(t.__dict__)
    d["commit_ts"] = t.commit_ts.isoformat()
    return d


def _fmt_ops(t: TxnSummary) -> str:
    parts = [f"{n} {name}" for n, name in ((t.n_delete, "delete"), (t.n_update, "update"),
                                          (t.n_insert, "insert")) if n]
    if t.truncated:
        parts.append("TRUNCATE")
    return ", ".join(parts)


class RowbackGroup(click.Group):
    """Turn rowback errors into a one-line message and exit status 1."""

    def invoke(self, ctx: click.Context) -> Any:
        try:
            return super().invoke(ctx)
        except RowbackError as exc:
            click.echo(f"error: {exc}", err=True)
            ctx.exit(EXIT_ERROR)


@click.group(cls=RowbackGroup)
@click.option("--source-dsn", envvar="ROWBACK_SOURCE_DSN", help="DSN of the database to protect.")
@click.option("--history-dsn", envvar="ROWBACK_HISTORY_DSN",
              help="DSN of the history database (defaults to the source).")
@click.option("--slot", envvar="ROWBACK_SLOT", default="rowback", show_default=True)
@click.option("--publication", envvar="ROWBACK_PUBLICATION", default="rowback", show_default=True)
@click.option("-v", "--verbose", is_flag=True)
@click.pass_context
def cli(ctx: click.Context, source_dsn: str | None, history_dsn: str | None, slot: str,
        publication: str, verbose: bool) -> None:
    """rowback: find a bad transaction and undo it, without a full restore."""
    logging.basicConfig(level=logging.INFO if verbose else logging.WARNING,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if not source_dsn:
        raise click.UsageError("set --source-dsn or ROWBACK_SOURCE_DSN")
    ctx.obj = Settings(source_dsn=source_dsn, history_dsn=history_dsn or source_dsn, slot=slot,
                       publication=publication)


@cli.command()
@click.option("--query-chars", default=500, show_default=True,
              help="Characters of statement text to record (0 disables).")
@click.pass_context
def init(ctx: click.Context, query_chars: int) -> None:
    """Create the publication, replication slot and history schema."""
    s = _settings(ctx)
    admin.init_source(s.source_dsn, s.slot, s.publication, query_chars=query_chars)
    store = HistoryStore(s.history_dsn)
    store.ensure_schema()
    store.close()
    click.echo(f"initialized slot {s.slot} and publication {s.publication}")


@cli.command()
@click.argument("tables", nargs=-1, required=True)
@click.option("--identity", type=click.Choice(["full", "default"]), default="full",
              show_default=True, help="REPLICA IDENTITY; full is required to undo DELETE/UPDATE.")
@click.option("--no-context", is_flag=True, help="Do not record role/application/query.")
@click.pass_context
def track(ctx: click.Context, tables: tuple[str, ...], identity: str, no_context: bool) -> None:
    """Opt tables in to change capture."""
    s = _settings(ctx)
    for t in tables:
        info = admin.track_table(s.source_dsn, t, s.publication, identity=identity,
                                 context=not no_context)
        click.echo(f"tracking {info.table} (primary key: {', '.join(info.primary_key)}, "
                   f"replica identity {info.replica_identity})")


@cli.command()
@click.argument("tables", nargs=-1, required=True)
@click.pass_context
def untrack(ctx: click.Context, tables: tuple[str, ...]) -> None:
    """Stop capturing tables."""
    s = _settings(ctx)
    for t in tables:
        admin.untrack_table(s.source_dsn, t, s.publication)
        click.echo(f"stopped tracking {t}")


@cli.command()
@click.pass_context
def tables(ctx: click.Context) -> None:
    """List tracked tables."""
    s = _settings(ctx)
    for t in admin.list_tracked(s.source_dsn, s.publication):
        click.echo(f"{t.table:40} pk=({', '.join(t.primary_key)}) identity={t.replica_identity}"
                   f" context={'on' if t.context else 'off'}")


@cli.command()
@click.option("--once", is_flag=True, help="Capture everything committed so far, then exit.")
@click.option("--retention", default=None, help="Prune history older than this (e.g. 7d).")
@click.pass_context
def capture(ctx: click.Context, once: bool, retention: str | None) -> None:
    """Run the capture daemon."""
    s = _settings(ctx)
    daemon = CaptureDaemon(s)
    if once:
        daemon.run_until_caught_up(timeout=600)
        click.echo(f"captured {daemon.saved_txns} transaction(s), {daemon.saved_changes} change(s)")
        return
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    if retention:
        keep = parse_duration(retention)

        def pruner() -> None:
            store = HistoryStore(s.history_dsn)
            while not stop.wait(3600 if keep > timedelta(hours=1) else 60):
                logging.getLogger("rowback.prune").info("pruned %d", store.prune(keep))

        threading.Thread(target=pruner, daemon=True).start()
    logging.getLogger().setLevel(logging.INFO)
    daemon.run_forever(stop)


@cli.command(name="log")
@click.option("--since", help="Start time (ISO 8601) or age such as 2h.")
@click.option("--until", help="End time (ISO 8601) or age such as 10m.")
@click.option("--table", help="Only transactions touching this table.")
@click.option("--op", type=click.Choice(sorted(_OPS)), help="Only transactions with this operation.")
@click.option("--min-rows", type=int, help="Only transactions changing at least this many rows.")
@click.option("--limit", default=50, show_default=True)
@click.option("--json", "as_json", is_flag=True)
@click.pass_context
def log_cmd(ctx: click.Context, since: str | None, until: str | None, table: str | None,
            op: str | None, min_rows: int | None, limit: int, as_json: bool) -> None:
    """Search captured transactions, newest first."""
    store = HistoryStore(_settings(ctx).history_dsn)
    found = store.search(since=parse_time(since), until=parse_time(until), table=table,
                         op=_OPS[op] if op else None, min_rows=min_rows, limit=limit)
    if as_json:
        click.echo(json.dumps([_summary_dict(t) for t in found], indent=2))
        return
    for t in found:
        flag = " [undone]" if t.undone_by or t.undone_xid else ""
        flag += f" [undo of {t.undo_of}]" if t.undo_of else ""
        who = f"{t.role or '?'}@{t.application_name or '?'}"
        click.echo(f"#{t.id:<6} {t.commit_ts.astimezone():%Y-%m-%d %H:%M:%S} xid={t.xid:<8} "
                   f"{_fmt_ops(t):30} {','.join(t.tables or t.truncated):24} {who}{flag}")
        if t.query:
            click.echo(f"         {t.query.splitlines()[0][:110]}")


@cli.command()
@click.argument("txn_id", type=int)
@click.option("--rows", default=5, show_default=True, help="Sample rows to print.")
@click.pass_context
def show(ctx: click.Context, txn_id: int, rows: int) -> None:
    """Show one captured transaction."""
    store = HistoryStore(_settings(ctx).history_dsn)
    t = store.get_txn(txn_id)
    click.echo(f"transaction #{t.id}  xid {t.xid}  commit {t.commit_ts.astimezone()}  "
               f"lsn {t.commit_lsn}")
    click.echo(f"role {t.role}  application {t.application_name}")
    click.echo(f"changes: {_fmt_ops(t)} on {', '.join(t.tables)}")
    if t.query:
        click.echo("statements:\n  " + "\n  ".join(t.query.splitlines()))
    for c in store.load_changes(txn_id)[:rows]:
        op = {"I": "insert", "U": "update", "D": "delete"}[c.op]
        img = c.new if c.op == "I" else c.old
        click.echo(f"  {op} {c.qualified} {dict(zip(c.columns, img or []))}")


@cli.command()
@click.argument("txn_id", type=int)
@click.option("--preview", is_flag=True, help="Print the inverse SQL and exit.")
@click.option("--preview-rows", default=20, show_default=True)
@click.option("--dry-run", is_flag=True, help="Run the undo and roll it back; report conflicts.")
@click.option("--on-conflict", type=click.Choice(["abort", "skip", "overwrite"]), default="abort",
              show_default=True)
@click.option("--replica-role", is_flag=True,
              help="Disable triggers and FK checks during the undo (needs superuser).")
@click.option("--yes", is_flag=True, help="Do not ask for confirmation.")
@click.pass_context
def undo(ctx: click.Context, txn_id: int, preview: bool, preview_rows: int, dry_run: bool,
         on_conflict: str, replica_role: bool, yes: bool) -> None:
    """Undo one captured transaction atomically, leaving later writes alone."""
    from rowback.undo import apply_undo, build_plan, preview_sql

    s = _settings(ctx)
    store = HistoryStore(s.history_dsn)
    t = store.get_txn(txn_id)
    plan = build_plan(store, txn_id)
    if preview:
        click.echo(preview_sql(plan, limit=preview_rows))
        return
    click.echo(f"undo #{t.id} ({_fmt_ops(t)} on {', '.join(t.tables)}, committed "
               f"{t.commit_ts.astimezone():%Y-%m-%d %H:%M:%S}): {plan.n_rows} row(s) to restore")
    if not dry_run and not yes:
        click.confirm("apply this undo now?", abort=True)
    started = time.perf_counter()
    try:
        result = apply_undo(s.source_dsn, store, plan, on_conflict=on_conflict, dry_run=dry_run,
                            replica_role=replica_role)
    except ConflictError as exc:
        click.echo(str(exc), err=True)
        for c in exc.conflicts[:20]:
            click.echo(f"  {c.reason:8} {c.table} {c.key}", err=True)
        sys.exit(EXIT_CONFLICT)
    elapsed = time.perf_counter() - started
    verb = "would restore" if result.dry_run else "restored"
    click.echo(f"{verb} {result.applied} row(s) in {elapsed:.2f}s"
               + (f", skipped {result.skipped} conflicting row(s)" if result.skipped else "")
               + (f", overwrote {len(result.conflicts)} conflicting row(s)"
                  if on_conflict == "overwrite" and result.conflicts else ""))
    if result.dry_run and result.conflicts:
        for c in result.conflicts[:20]:
            click.echo(f"  conflict {c.reason:8} {c.table} {c.key}")


@cli.command()
@click.option("--json", "as_json", is_flag=True)
@click.option("--warn-lag", default="1GB", show_default=True, help="Lag that triggers a warning.")
@click.pass_context
def status(ctx: click.Context, as_json: bool, warn_lag: str) -> None:
    """Report slot lag and WAL retention on the primary."""
    s = _settings(ctx)
    units = {"kb": 1 << 10, "mb": 1 << 20, "gb": 1 << 30}
    m = re.match(r"^(\d+)\s*(kb|mb|gb)?$", warn_lag.lower())
    if not m:
        raise click.BadParameter("expected a size such as 512MB or 1GB")
    st = slot_status(s.source_dsn, s.slot, int(m.group(1)) * units.get(m.group(2) or "", 1))
    if as_json:
        click.echo(json.dumps(st.as_dict(), indent=2))
    else:
        click.echo(f"slot {st.slot}: {st.health.upper()} - {st.message}")
        click.echo(f"  active={st.active} lag={st.lag_bytes} B retained={st.retained_bytes} B "
                   f"wal_status={st.wal_status} safe_wal_size={st.safe_wal_size} "
                   f"max_slot_wal_keep_size={st.max_slot_wal_keep_size}")
    if st.health == "critical":
        sys.exit(EXIT_CRITICAL)


@cli.command()
@click.option("--older-than", required=True, help="Age such as 7d.")
@click.pass_context
def prune(ctx: click.Context, older_than: str) -> None:
    """Delete history older than the retention window."""
    n = HistoryStore(_settings(ctx).history_dsn).prune(parse_duration(older_than))
    click.echo(f"pruned {n} transaction(s)")


@cli.group()
@click.option("--max-rows", default=1000, show_default=True,
              help="Statements changing more rows than this are held for approval.")
@click.pass_context
def guard(ctx: click.Context, max_rows: int) -> None:
    """Policy gateway for AI agents and unreviewed sessions."""
    from rowback.guard.gateway import Gateway

    s = _settings(ctx)
    gw = Gateway(s.source_dsn, s.history_dsn, max_rows=max_rows)
    gw.ensure_schema()
    ctx.obj = gw


@guard.command(name="run")
@click.argument("sql")
@click.option("--agent", default="cli-agent", show_default=True)
@click.pass_obj
def guard_run(gw: Any, sql: str, agent: str) -> None:
    """Run SQL through the guard policy."""
    click.echo(json.dumps(gw.execute(sql, agent=agent).as_dict(), indent=2, default=str))


@guard.command(name="pending")
@click.pass_obj
def guard_pending(gw: Any) -> None:
    """List statements waiting for approval."""
    for r in gw.pending():
        click.echo(f"#{r['id']} {r['created_at']:%Y-%m-%d %H:%M:%S} agent={r['agent']} "
                   f"reasons={'; '.join(r['reasons'])}\n    {r['sql']}")


@guard.command(name="approve")
@click.argument("request_id", type=int)
@click.option("--approver", envvar="USER", default="human", show_default=True)
@click.pass_obj
def guard_approve(gw: Any, request_id: int, approver: str) -> None:
    """Approve and execute a held statement."""
    click.echo(json.dumps(gw.approve(request_id, approver).as_dict(), indent=2, default=str))


@guard.command(name="reject")
@click.argument("request_id", type=int)
@click.option("--approver", envvar="USER", default="human", show_default=True)
@click.pass_obj
def guard_reject(gw: Any, request_id: int, approver: str) -> None:
    """Reject a held statement."""
    gw.reject(request_id, approver)
    click.echo(f"rejected request {request_id}")


@guard.command(name="mcp")
@click.option("--agent", default="mcp-agent", show_default=True,
              help="Name recorded for this agent in logs and application_name.")
@click.pass_obj
def guard_mcp(gw: Any, agent: str) -> None:
    """Serve the guard as an MCP server over stdio."""
    from rowback.guard.mcp_server import build_server

    build_server(gw, agent=agent).run("stdio")


main = cli
