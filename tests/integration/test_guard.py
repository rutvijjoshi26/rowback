import asyncio

import pytest

from rowback.guard.gateway import Gateway
from rowback.guard.mcp_server import build_server
from rowback.store import HistoryStore
from tests.conftest import sql
from tests.integration.helpers import capture, setup_tracked

pytestmark = pytest.mark.integration


@pytest.fixture
def gw(dbs):
    sql(dbs.source_dsn, "CREATE TABLE orders (id int PRIMARY KEY, status text)")
    sql(dbs.source_dsn, "INSERT INTO orders SELECT g, 'new' FROM generate_series(1, 100) g")
    g = Gateway(dbs.source_dsn, dbs.history_dsn, max_rows=10)
    g.ensure_schema()
    return g


def count(dbs):
    return sql(dbs.source_dsn, "SELECT count(*) FROM orders")[0][0]


def log_actions(dbs):
    return [r[0] for r in sql(dbs.history_dsn, "SELECT action FROM rowback.guard_log ORDER BY id")]


def test_select_returns_rows(gw):
    r = gw.execute("SELECT id, status FROM orders WHERE id <= 2 ORDER BY id", agent="a")
    assert r.status == "executed" and r.columns == ["id", "status"]
    assert r.rows == [[1, "new"], [2, "new"]]


def test_small_delete_executes(gw, dbs):
    r = gw.execute("DELETE FROM orders WHERE id <= 3", agent="a")
    assert (r.status, r.rowcount) == ("executed", 3)
    assert count(dbs) == 97
    assert log_actions(dbs) == ["allow"]


def test_delete_over_row_limit_is_rolled_back_and_held(gw, dbs):
    r = gw.execute("DELETE FROM orders WHERE id <= 50", agent="a")
    assert r.status == "pending" and r.request_id is not None
    assert "50 rows" in r.reasons[0]
    assert count(dbs) == 100
    approved = gw.approve(r.request_id, approver="dba")
    assert (approved.status, approved.rowcount) == ("executed", 50)
    assert count(dbs) == 50
    assert gw.request(r.request_id)["status"] == "executed"
    assert log_actions(dbs) == ["review", "approved"]


def test_delete_without_where_is_held_without_running(gw, dbs):
    r = gw.execute("DELETE FROM orders", agent="a")
    assert r.status == "pending" and "no WHERE" in r.reasons[0]
    assert count(dbs) == 100
    gw.reject(r.request_id, approver="dba")
    assert gw.request(r.request_id)["status"] == "rejected"
    with pytest.raises(Exception, match="not pending"):
        gw.approve(r.request_id, approver="dba")
    assert count(dbs) == 100


def test_drop_is_blocked(gw, dbs):
    r = gw.execute("DROP TABLE orders", agent="a")
    assert r.status == "blocked" and r.request_id is None
    assert count(dbs) == 100
    assert log_actions(dbs) == ["block"]


def test_rows_changed_inside_cte_and_functions_are_counted(gw, dbs):
    sql(dbs.source_dsn, "CREATE FUNCTION purge() RETURNS int LANGUAGE sql AS "
                        "$$ WITH d AS (DELETE FROM orders WHERE id > 60 RETURNING 1) "
                        "SELECT count(*)::int FROM d $$")
    r1 = gw.execute("WITH d AS (DELETE FROM orders WHERE id <= 20 RETURNING id) "
                    "SELECT count(*) FROM d", agent="a")
    r2 = gw.execute("SELECT purge()", agent="a")
    assert r1.status == "pending" and r2.status == "pending"
    assert count(dbs) == 100


def test_sql_error_is_reported_and_logged(gw, dbs):
    r = gw.execute("SELECT * FROM missing_table", agent="a")
    assert r.status == "error" and "missing_table" in r.error
    assert log_actions(dbs) == ["error"]


def test_guard_sessions_are_tagged_in_the_timeline(dbs):
    setup_tracked(dbs, "CREATE TABLE orders (id int PRIMARY KEY, status text)", ["orders"])
    sql(dbs.source_dsn, "INSERT INTO orders VALUES (1, 'new')")
    g = Gateway(dbs.source_dsn, dbs.history_dsn, max_rows=10)
    g.ensure_schema()
    g.execute("UPDATE orders SET status = 'x' WHERE id = 1", agent="coder-bot")
    capture(dbs)
    top = HistoryStore(dbs.history_dsn).search(op="U")[0]
    assert top.application_name == "rowback-guard:coder-bot"


def test_mcp_server_exposes_tools(gw, dbs):
    server = build_server(gw, agent="mcp-test")
    names = {t.name for t in asyncio.run(server.list_tools())}
    assert names == {"run_sql", "request_status"}
    result = asyncio.run(server.call_tool("run_sql", {"sql": "DELETE FROM orders"}))
    text = str(result)
    assert "pending" in text
    assert count(dbs) == 100


def test_guard_cli_run_pending_approve(gw, dbs):
    import json

    from click.testing import CliRunner

    from rowback.cli import main

    base = ["--source-dsn", dbs.source_dsn, "--history-dsn", dbs.history_dsn, "guard",
            "--max-rows", "5"]
    out = CliRunner().invoke(main, [*base, "run", "DELETE FROM orders WHERE id <= 20"])
    assert out.exit_code == 0, out.output
    rid = json.loads(out.output)["request_id"]
    listed = CliRunner().invoke(main, [*base, "pending"]).output
    assert f"#{rid}" in listed and "limit 5" in listed
    done = CliRunner().invoke(main, [*base, "approve", str(rid), "--approver", "dba"])
    assert json.loads(done.output)["rowcount"] == 20
    assert count(dbs) == 80
