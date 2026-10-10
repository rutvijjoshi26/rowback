import pytest

from rowback.errors import ConflictError, NotUndoableError
from rowback.store import HistoryStore
from rowback.undo import apply_undo, build_plan, preview_sql
from tests.conftest import sql
from tests.integration.helpers import capture, run_in_txn, setup_tracked
from tests.integration.test_capture import ROW_SQL, TYPES_DDL

pytestmark = pytest.mark.integration

ORDERS = ("CREATE TABLE orders (id bigint PRIMARY KEY, customer text NOT NULL, amount numeric(10,2),"
          " status text, note text, created timestamptz DEFAULT now())")


def snapshot(dsn, table="orders", order="id"):
    return sql(dsn, f"SELECT t::text FROM {table} t ORDER BY {order}")


def seed_orders(dbs, n=200):
    setup_tracked(dbs, ORDERS, ["orders"])
    sql(dbs.source_dsn, "INSERT INTO orders (id, customer, amount, status, note) "
                        f"SELECT g, 'c' || (g % 17), g * 1.5, 'new', CASE WHEN g % 3 = 0 THEN NULL "
                        f"ELSE 'n''' || g END FROM generate_series(1, {n}) g")


def last_txn_id(dbs, **filters):
    capture(dbs)
    return HistoryStore(dbs.history_dsn).search(**filters)[0].id


def undo(dbs, txn_id, **kwargs):
    store = HistoryStore(dbs.history_dsn)
    plan = build_plan(store, txn_id)
    return apply_undo(dbs.source_dsn, store, plan, **kwargs)


def test_undo_delete_restores_exact_rows(dbs):
    seed_orders(dbs)
    before = snapshot(dbs.source_dsn)
    sql(dbs.source_dsn, "DELETE FROM orders WHERE id % 2 = 0")
    result = undo(dbs, last_txn_id(dbs, op="D"))
    assert result.applied == 100 and result.conflicts == []
    assert snapshot(dbs.source_dsn) == before


def test_undo_preserves_later_good_writes(dbs):
    seed_orders(dbs)
    sql(dbs.source_dsn, "DELETE FROM orders WHERE id <= 50")
    bad = last_txn_id(dbs, op="D")
    # good writes made after the bad delete, on rows the delete did not touch
    sql(dbs.source_dsn, "INSERT INTO orders (id, customer) VALUES (1000, 'late')")
    sql(dbs.source_dsn, "UPDATE orders SET status = 'shipped' WHERE id = 150")
    expected = sql(dbs.source_dsn, "SELECT count(*) FROM orders")[0][0] + 50
    undo(dbs, bad)
    assert sql(dbs.source_dsn, "SELECT count(*) FROM orders")[0][0] == expected
    assert sql(dbs.source_dsn, "SELECT status FROM orders WHERE id = 150") == [("shipped",)]
    assert sql(dbs.source_dsn, "SELECT customer FROM orders WHERE id = 1000") == [("late",)]
    assert sql(dbs.source_dsn, "SELECT count(*) FROM orders WHERE id <= 50") == [(50,)]


def test_undo_update_restores_old_values(dbs):
    seed_orders(dbs)
    before = snapshot(dbs.source_dsn)
    sql(dbs.source_dsn, "UPDATE orders SET amount = 0, status = 'void', note = NULL")
    undo(dbs, last_txn_id(dbs, op="U"))
    assert snapshot(dbs.source_dsn) == before


def test_undo_insert_deletes_inserted_rows(dbs):
    seed_orders(dbs, n=10)
    before = snapshot(dbs.source_dsn)
    sql(dbs.source_dsn, "INSERT INTO orders (id, customer) SELECT g, 'x' FROM generate_series(11, 30) g")
    undo(dbs, last_txn_id(dbs, op="I", min_rows=20))
    assert snapshot(dbs.source_dsn) == before


def test_conflict_on_row_modified_after_bad_update_abort_changes_nothing(dbs):
    seed_orders(dbs, n=20)
    sql(dbs.source_dsn, "UPDATE orders SET status = 'void'")
    bad = last_txn_id(dbs, op="U")
    sql(dbs.source_dsn, "UPDATE orders SET status = 'refunded' WHERE id = 7")
    after_good_write = snapshot(dbs.source_dsn)
    with pytest.raises(ConflictError) as err:
        undo(dbs, bad)
    assert [(c.table, c.key, c.reason) for c in err.value.conflicts] == [
        ("public.orders", {"id": "7"}, "changed")]
    assert snapshot(dbs.source_dsn) == after_good_write


def test_conflict_skip_undoes_only_unchanged_rows(dbs):
    seed_orders(dbs, n=20)
    sql(dbs.source_dsn, "UPDATE orders SET status = 'void'")
    bad = last_txn_id(dbs, op="U")
    sql(dbs.source_dsn, "UPDATE orders SET status = 'refunded' WHERE id = 7")
    sql(dbs.source_dsn, "DELETE FROM orders WHERE id = 8")
    result = undo(dbs, bad, on_conflict="skip")
    assert result.applied == 18 and result.skipped == 2
    assert sorted((c.key["id"], c.reason) for c in result.conflicts) == [("7", "changed"),
                                                                         ("8", "missing")]
    assert sql(dbs.source_dsn, "SELECT id, status FROM orders WHERE status <> 'new' ORDER BY id") \
        == [(7, "refunded")]


def test_conflict_overwrite_forces_before_image(dbs):
    seed_orders(dbs, n=20)
    before = snapshot(dbs.source_dsn)
    sql(dbs.source_dsn, "UPDATE orders SET status = 'void'")
    bad = last_txn_id(dbs, op="U")
    sql(dbs.source_dsn, "UPDATE orders SET status = 'refunded' WHERE id = 7")
    sql(dbs.source_dsn, "DELETE FROM orders WHERE id = 8")
    result = undo(dbs, bad, on_conflict="overwrite")
    assert len(result.conflicts) == 2
    assert snapshot(dbs.source_dsn) == before


def test_conflict_when_deleted_key_was_reinserted(dbs):
    seed_orders(dbs, n=20)
    sql(dbs.source_dsn, "DELETE FROM orders WHERE id <= 5")
    bad = last_txn_id(dbs, op="D")
    sql(dbs.source_dsn, "INSERT INTO orders (id, customer) VALUES (3, 'new owner')")
    with pytest.raises(ConflictError) as err:
        undo(dbs, bad)
    assert [(c.key, c.reason) for c in err.value.conflicts] == [({"id": "3"}, "exists")]
    assert sql(dbs.source_dsn, "SELECT count(*) FROM orders")[0][0] == 16


def test_composite_key_and_all_types_round_trip(dbs):
    setup_tracked(dbs, TYPES_DDL + "; CREATE TABLE lines (order_id int, line_no int, sku text,"
                  " qty int, PRIMARY KEY (order_id, line_no))", ["typed", "lines"])
    sql(dbs.source_dsn, ROW_SQL)
    sql(dbs.source_dsn, "INSERT INTO typed (id) VALUES (2)")
    sql(dbs.source_dsn, "INSERT INTO lines SELECT o, l, 'sku-' || l, l FROM generate_series(1, 5) o,"
                        " generate_series(1, 3) l")
    typed_before = snapshot(dbs.source_dsn, "typed")
    lines_before = snapshot(dbs.source_dsn, "lines", "order_id, line_no")
    run_in_txn(dbs.source_dsn, "DELETE FROM typed", "DELETE FROM lines WHERE line_no <> 2")
    undo(dbs, last_txn_id(dbs, op="D"))
    assert snapshot(dbs.source_dsn, "typed") == typed_before
    assert snapshot(dbs.source_dsn, "lines", "order_id, line_no") == lines_before


def test_multiple_ops_on_same_row_in_one_txn_collapse(dbs):
    seed_orders(dbs, n=5)
    before = snapshot(dbs.source_dsn)
    run_in_txn(dbs.source_dsn,
               "UPDATE orders SET status = 'a' WHERE id = 1",
               "UPDATE orders SET status = 'b' WHERE id = 1",
               "DELETE FROM orders WHERE id = 2",
               "INSERT INTO orders (id, customer) VALUES (2, 'replaced')",
               "INSERT INTO orders (id, customer) VALUES (99, 'tmp')",
               "UPDATE orders SET id = 98 WHERE id = 99",
               "UPDATE orders SET id = 50 WHERE id = 3")
    undo(dbs, last_txn_id(dbs))
    assert snapshot(dbs.source_dsn) == before


def test_undo_twice_is_refused(dbs):
    seed_orders(dbs, n=5)
    sql(dbs.source_dsn, "DELETE FROM orders")
    bad = last_txn_id(dbs, op="D")
    undo(dbs, bad)
    with pytest.raises(NotUndoableError, match="already undone"):
        undo(dbs, bad)
    # after capture catches up the undo transaction is linked to the original
    capture(dbs)
    store = HistoryStore(dbs.history_dsn)
    undo_txn = store.get_txn(store.get_txn(bad).undone_by)
    assert undo_txn.undo_of == bad and undo_txn.application_name == "rowback-undo"


def test_default_identity_delete_is_refused(dbs):
    setup_tracked(dbs, ORDERS, ["orders"], identity="default")
    sql(dbs.source_dsn, "INSERT INTO orders (id, customer) VALUES (1, 'a')")
    sql(dbs.source_dsn, "DELETE FROM orders")
    with pytest.raises(NotUndoableError, match="REPLICA IDENTITY FULL"):
        undo(dbs, last_txn_id(dbs, op="D"))


def test_truncate_is_refused(dbs):
    seed_orders(dbs, n=5)
    sql(dbs.source_dsn, "TRUNCATE orders")
    capture(dbs)
    txn = HistoryStore(dbs.history_dsn).search()[0]
    with pytest.raises(NotUndoableError, match="TRUNCATE"):
        undo(dbs, txn.id)


def test_dry_run_reports_without_changing(dbs):
    seed_orders(dbs, n=10)
    sql(dbs.source_dsn, "DELETE FROM orders WHERE id <= 4")
    bad = last_txn_id(dbs, op="D")
    after = snapshot(dbs.source_dsn)
    result = undo(dbs, bad, dry_run=True)
    assert result.applied == 4 and result.dry_run
    assert snapshot(dbs.source_dsn) == after
    undo(dbs, bad)  # a dry run does not mark the transaction as undone


def test_preview_sql_lists_inverse_statements(dbs):
    seed_orders(dbs, n=10)
    sql(dbs.source_dsn, "DELETE FROM orders WHERE id IN (3, 4, 5)")
    store = HistoryStore(dbs.history_dsn)
    plan = build_plan(store, last_txn_id(dbs, op="D"))
    text = preview_sql(plan, limit=2)
    assert text.count("INSERT INTO public.orders") == 2
    assert "'n''4'" in text and "1 more row" in text


def test_identity_column_and_generated_column(dbs):
    setup_tracked(dbs, "CREATE TABLE g (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, a int,"
                       " doubled int GENERATED ALWAYS AS (a * 2) STORED)", ["g"])
    sql(dbs.source_dsn, "INSERT INTO g (a) SELECT generate_series(1, 5)")
    before = snapshot(dbs.source_dsn, "g")
    sql(dbs.source_dsn, "DELETE FROM g WHERE a > 2")
    undo(dbs, last_txn_id(dbs, op="D"))
    assert snapshot(dbs.source_dsn, "g") == before


def test_cascading_fk_delete_undo_with_deferred_order(dbs):
    setup_tracked(dbs, "CREATE TABLE p (id int PRIMARY KEY); CREATE TABLE c (id int PRIMARY KEY,"
                       " p_id int REFERENCES p ON DELETE CASCADE)", ["p", "c"])
    sql(dbs.source_dsn, "INSERT INTO p SELECT generate_series(1, 5)")
    sql(dbs.source_dsn, "INSERT INTO c SELECT g, g % 5 + 1 FROM generate_series(1, 20) g")
    before_p, before_c = snapshot(dbs.source_dsn, "p"), snapshot(dbs.source_dsn, "c")
    sql(dbs.source_dsn, "DELETE FROM p WHERE id <= 2")
    undo(dbs, last_txn_id(dbs, op="D"))
    assert snapshot(dbs.source_dsn, "p") == before_p
    assert snapshot(dbs.source_dsn, "c") == before_c


def test_key_moved_into_slot_freed_by_delete(dbs):
    seed_orders(dbs, n=5)
    before = snapshot(dbs.source_dsn)
    run_in_txn(dbs.source_dsn, "DELETE FROM orders WHERE id = 2",
               "UPDATE orders SET id = 2, status = 'moved' WHERE id = 4")
    undo(dbs, last_txn_id(dbs))
    assert snapshot(dbs.source_dsn) == before
