import json

import pytest
from click.testing import CliRunner

from rowback.cli import main, parse_duration
from rowback.monitor import slot_status
from tests.conftest import sql

pytestmark = pytest.mark.integration


def run(dbs, *args, ok=True):
    result = CliRunner().invoke(main, ["--source-dsn", dbs.source_dsn, "--history-dsn",
                                       dbs.history_dsn, "--slot", dbs.slot, *args],
                                catch_exceptions=False)
    if ok:
        assert result.exit_code == 0, result.output
    return result


def test_parse_duration():
    assert parse_duration("90s").total_seconds() == 90
    assert parse_duration("15m").total_seconds() == 900
    assert parse_duration("2h").total_seconds() == 7200
    assert parse_duration("7d").days == 7


def test_end_to_end_find_and_undo_a_bad_delete(dbs):
    sql(dbs.source_dsn, "CREATE TABLE orders (id int PRIMARY KEY, status text)")
    sql(dbs.source_dsn, "CREATE TABLE nopk (a int)")
    run(dbs, "init")
    run(dbs, "track", "orders")
    bad = run(dbs, "track", "nopk", ok=False)
    assert bad.exit_code == 1 and "no primary key" in bad.output
    sql(dbs.source_dsn, "INSERT INTO orders SELECT g, 'new' FROM generate_series(1, 1000) g")
    sql(dbs.source_dsn, "DELETE FROM orders WHERE id > 100")
    sql(dbs.source_dsn, "INSERT INTO orders VALUES (5000, 'good write')")
    run(dbs, "capture", "--once")

    out = run(dbs, "log", "--table", "orders", "--op", "delete", "--json").output
    rows = json.loads(out)
    assert len(rows) == 1 and rows[0]["n_delete"] == 900
    txn_id = rows[0]["id"]

    human = run(dbs, "log", "--min-rows", "500").output
    assert "900" in human and "DELETE FROM orders WHERE id > 100" in human

    shown = run(dbs, "show", str(txn_id)).output
    assert "public.orders" in shown and "delete" in shown.lower()

    preview = run(dbs, "undo", str(txn_id), "--preview").output
    assert "INSERT INTO public.orders" in preview
    assert sql(dbs.source_dsn, "SELECT count(*) FROM orders") == [(101,)]

    refused = run(dbs, "undo", str(txn_id), ok=False)  # no --yes and no confirmation input
    assert refused.exit_code == 1
    assert sql(dbs.source_dsn, "SELECT count(*) FROM orders") == [(101,)]

    done = run(dbs, "undo", str(txn_id), "--yes").output
    assert "900" in done
    assert sql(dbs.source_dsn, "SELECT count(*) FROM orders") == [(1001,)]

    status = json.loads(run(dbs, "status", "--json").output)
    assert status["slot"] == dbs.slot and status["health"] in ("ok", "warn")

    assert "public.orders" in run(dbs, "tables").output


def test_undo_conflict_exit_code(dbs):
    sql(dbs.source_dsn, "CREATE TABLE orders (id int PRIMARY KEY, status text)")
    run(dbs, "init")
    run(dbs, "track", "orders")
    sql(dbs.source_dsn, "INSERT INTO orders SELECT g, 'new' FROM generate_series(1, 10) g")
    sql(dbs.source_dsn, "UPDATE orders SET status = 'void'")
    sql(dbs.source_dsn, "UPDATE orders SET status = 'kept' WHERE id = 3")
    run(dbs, "capture", "--once")
    txn_id = json.loads(run(dbs, "log", "--op", "update", "--min-rows", "10", "--json").output)[0]["id"]
    result = run(dbs, "undo", str(txn_id), "--yes", ok=False)
    assert result.exit_code == 3 and "changed" in result.output
    ok = run(dbs, "undo", str(txn_id), "--yes", "--on-conflict", "skip").output
    assert "9" in ok and "skipped 1" in ok


def test_slot_status_reports_lag(dbs):
    sql(dbs.source_dsn, "CREATE TABLE t (id int PRIMARY KEY, pad text)")
    run(dbs, "init")
    run(dbs, "track", "t")
    run(dbs, "capture", "--once")
    sql(dbs.source_dsn, "INSERT INTO t SELECT g, repeat('x', 200) FROM generate_series(1, 5000) g")
    lagging = slot_status(dbs.source_dsn, dbs.slot)
    assert lagging.lag_bytes > 1_000_000
    run(dbs, "capture", "--once")
    caught_up = slot_status(dbs.source_dsn, dbs.slot)
    assert caught_up.lag_bytes < 64 * 1024  # the slot really advanced
    assert caught_up.wal_status in ("reserved", "extended")
    assert caught_up.safe_wal_size is not None


def test_prune(dbs):
    sql(dbs.source_dsn, "CREATE TABLE t (id int PRIMARY KEY)")
    run(dbs, "init")
    run(dbs, "track", "t")
    sql(dbs.source_dsn, "INSERT INTO t VALUES (1)")
    run(dbs, "capture", "--once")
    sql(dbs.history_dsn, "UPDATE rowback.transactions SET commit_ts = now() - interval '30 days'")
    assert "pruned 1" in run(dbs, "prune", "--older-than", "7d").output
