import pytest

from rowback import admin
from rowback.errors import RowbackError
from tests.conftest import sql

pytestmark = pytest.mark.integration


def setup(dbs):
    admin.init_source(dbs.source_dsn, dbs.slot, dbs.publication)


def identity(dsn, table):
    return sql(dsn, "SELECT relreplident FROM pg_class WHERE oid = %s::regclass", (table,))[0][0]


def test_init_is_idempotent_and_creates_slot_and_publication(dbs):
    setup(dbs)
    setup(dbs)
    assert sql(dbs.source_dsn, "SELECT plugin FROM pg_replication_slots WHERE slot_name = %s",
               (dbs.slot,)) == [("pgoutput",)]
    assert sql(dbs.source_dsn, "SELECT count(*) FROM pg_publication WHERE pubname = %s",
               (dbs.publication,)) == [(1,)]


def test_track_rejects_table_without_primary_key(dbs):
    setup(dbs)
    sql(dbs.source_dsn, "CREATE TABLE nopk (a int, b text)")
    with pytest.raises(RowbackError, match="primary key"):
        admin.track_table(dbs.source_dsn, "nopk", dbs.publication)


def test_track_rejects_missing_table(dbs):
    setup(dbs)
    with pytest.raises(RowbackError, match="does not exist"):
        admin.track_table(dbs.source_dsn, "nope", dbs.publication)


def test_track_sets_full_identity_publication_and_trigger(dbs):
    setup(dbs)
    sql(dbs.source_dsn, "CREATE TABLE lines (order_id int, line_no int, sku text, "
                        "PRIMARY KEY (order_id, line_no))")
    info = admin.track_table(dbs.source_dsn, "lines", dbs.publication)
    assert info.primary_key == ["order_id", "line_no"]
    assert identity(dbs.source_dsn, "lines") == "f"
    assert sql(dbs.source_dsn, "SELECT tablename FROM pg_publication_tables WHERE pubname = %s",
               (dbs.publication,)) == [("lines",)]
    assert sql(dbs.source_dsn, "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'lines'::regclass "
                               "AND tgname = 'rowback_context'") == [(1,)]
    # tracking twice is harmless
    admin.track_table(dbs.source_dsn, "lines", dbs.publication)
    assert [t.table for t in admin.list_tracked(dbs.source_dsn, dbs.publication)] == ["public.lines"]


def test_track_with_default_identity_and_no_context(dbs):
    setup(dbs)
    sql(dbs.source_dsn, "CREATE TABLE t (id int PRIMARY KEY)")
    admin.track_table(dbs.source_dsn, "t", dbs.publication, identity="default", context=False)
    assert identity(dbs.source_dsn, "t") == "d"
    assert sql(dbs.source_dsn, "SELECT count(*) FROM pg_trigger WHERE tgname = 'rowback_context'") \
        == [(0,)]


def test_untrack_removes_from_publication_and_trigger(dbs):
    setup(dbs)
    sql(dbs.source_dsn, "CREATE TABLE t (id int PRIMARY KEY)")
    admin.track_table(dbs.source_dsn, "t", dbs.publication)
    admin.untrack_table(dbs.source_dsn, "t", dbs.publication)
    assert admin.list_tracked(dbs.source_dsn, dbs.publication) == []
    assert identity(dbs.source_dsn, "t") == "d"


def test_context_trigger_works_for_unprivileged_role(dbs):
    setup(dbs)
    sql(dbs.source_dsn, "CREATE TABLE t (id int PRIMARY KEY)")
    admin.track_table(dbs.source_dsn, "t", dbs.publication)
    sql(dbs.source_dsn, "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='app_rw')"
                        " THEN CREATE ROLE app_rw LOGIN PASSWORD 'pw'; END IF; END $$")
    sql(dbs.source_dsn, "GRANT ALL ON t TO app_rw")
    sql(dbs.source_dsn, "GRANT USAGE ON SCHEMA public TO app_rw")
    dsn = dbs.source_dsn.replace("user=postgres password=postgres", "user=app_rw password=pw")
    sql(dsn, "INSERT INTO t VALUES (1)")
    assert sql(dbs.source_dsn, "SELECT count(*) FROM t") == [(1,)]


def test_primary_key_columns_helper(dbs):
    sql(dbs.source_dsn, 'CREATE SCHEMA s; CREATE TABLE s."Mixed" (b int, a int, PRIMARY KEY (b, a))')
    import psycopg2

    conn = psycopg2.connect(dbs.source_dsn)
    assert admin.primary_key_columns(conn, "s", "Mixed") == ["b", "a"]
    conn.close()
