import pytest

from rowback import pgoutput as p
from tests.conftest import sql

pytestmark = pytest.mark.integration


def test_decodes_real_server_output(dbs):
    sql(dbs.source_dsn, "CREATE TABLE t (id int PRIMARY KEY, note text, big text)")
    sql(dbs.source_dsn, "ALTER TABLE t REPLICA IDENTITY FULL")
    sql(dbs.source_dsn, "CREATE PUBLICATION pub FOR TABLE t")
    sql(dbs.source_dsn, "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (dbs.slot,))
    sql(dbs.source_dsn, "INSERT INTO t SELECT 1, NULL, string_agg(md5(g::text), '') FROM generate_series(1, 4000) g")
    sql(dbs.source_dsn, "UPDATE t SET note = 'it''s' WHERE id = 1")
    sql(dbs.source_dsn, "SELECT pg_logical_emit_message(true, 'rowback', 'ctx')")
    sql(dbs.source_dsn, "DELETE FROM t WHERE id = 1")
    rows = sql(
        dbs.source_dsn,
        "SELECT data FROM pg_logical_slot_peek_binary_changes(%s, NULL, NULL, "
        "'proto_version', '1', 'publication_names', 'pub', 'messages', 'true')",
        (dbs.slot,),
    )
    msgs = [p.decode(bytes(r[0])) for r in rows]
    kinds = [type(m).__name__ for m in msgs]
    assert kinds == ["Begin", "Relation", "Insert", "Commit",
                     "Begin", "Update", "Commit",
                     "Begin", "LogicalMessage", "Commit",
                     "Begin", "Delete", "Commit"]
    upd = next(m for m in msgs if isinstance(m, p.Update))
    assert upd.old_kind == "O"
    assert upd.old[:2] == ["1", None]
    assert upd.new[1] == "it's"
    # the large TOASTed column did not change, so pgoutput omits it from the new tuple
    assert upd.new[2] is p.UNCHANGED
    assert len(upd.old[2]) == 128000
    rel = next(m for m in msgs if isinstance(m, p.Relation))
    assert rel.replica_identity == "f"
    assert [c.name for c in rel.columns] == ["id", "note", "big"]
