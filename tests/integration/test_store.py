from datetime import datetime, timedelta, timezone

import pytest

from rowback.model import CapturedTxn, Change
from rowback.store import HistoryStore, lsn_to_str, str_to_lsn
from tests.conftest import sql

pytestmark = pytest.mark.integration
NOW = datetime.now(timezone.utc)


def mk_txn(lsn: int, changes: list[Change], ts: datetime = NOW, xid: int = 1) -> CapturedTxn:
    return CapturedTxn(xid=xid, commit_lsn=lsn, end_lsn=lsn + 8, commit_ts=ts, changes=changes,
                       role="app", application_name="psql", query="DELETE FROM orders")


def ch(seq: int, op: str, old=None, new=None, table="orders") -> Change:
    return Change(seq=seq, schema="public", table=table, op=op, columns=["id", "note"],
                  types=[20, 25], key_columns=["id"], old=old, new=new,
                  old_kind="O" if old is not None else None)


@pytest.fixture
def store(dbs):
    s = HistoryStore(dbs.history_dsn)
    s.ensure_schema()
    yield s
    s.close()


def test_lsn_helpers_round_trip():
    assert lsn_to_str(0x16B3748) == "0/16B3748"
    assert str_to_lsn("1/A") == (1 << 32) + 10


def test_save_and_load_round_trip(store):
    changes = [ch(0, "I", new=["1", None]), ch(1, "U", old=["1", None], new=["1", "café"]),
               ch(2, "D", old=["1", "café"], table="items")]
    txn_id = store.save_txn("s", mk_txn(100, changes))
    assert txn_id is not None
    loaded = store.load_changes(txn_id)
    assert [(c.seq, c.qualified, c.op, c.old, c.new, c.key_columns, c.types, c.old_kind)
            for c in loaded] == [
        (c.seq, c.qualified, c.op, c.old, c.new, c.key_columns, c.types, c.old_kind)
        for c in changes]
    summary = store.get_txn(txn_id)
    assert (summary.n_changes, summary.n_insert, summary.n_update, summary.n_delete) == (3, 1, 1, 1)
    assert sorted(summary.tables) == ["public.items", "public.orders"]
    assert summary.role == "app" and summary.query == "DELETE FROM orders"


def test_save_advances_confirmed_lsn_and_dedupes(store):
    assert store.get_confirmed_lsn("s") == 0
    assert store.save_txn("s", mk_txn(100, [ch(0, "I", new=["1", "a"])])) is not None
    assert store.get_confirmed_lsn("s") == 108
    # replaying the same transaction (end_lsn <= confirmed) is ignored
    assert store.save_txn("s", mk_txn(100, [ch(0, "I", new=["1", "a"])])) is None
    assert store.stats()["transactions"] == 1


def test_advance_lsn_never_moves_backwards(store):
    store.advance_lsn("s", 500)
    store.advance_lsn("s", 200)
    assert store.get_confirmed_lsn("s") == 500


def test_search_filters(store):
    old = NOW - timedelta(hours=2)
    a = store.save_txn("s", mk_txn(100, [ch(i, "D", old=[str(i), "x"]) for i in range(10)], ts=old))
    b = store.save_txn("s", mk_txn(200, [ch(0, "I", new=["1", "a"], table="items")]))
    assert [t.id for t in store.search(table="orders")] == [a]
    assert [t.id for t in store.search(table="public.items")] == [b]
    assert [t.id for t in store.search(op="D")] == [a]
    assert [t.id for t in store.search(min_rows=5)] == [a]
    assert [t.id for t in store.search(since=NOW - timedelta(minutes=5))] == [b]
    assert [t.id for t in store.search(until=NOW - timedelta(hours=1))] == [a]
    assert [t.id for t in store.search()] == [b, a]  # newest first


def test_large_txn_is_split_into_chunks(store):
    changes = [ch(i, "I", new=[str(i), "n" * 20]) for i in range(12_000)]
    txn_id = store.save_txn("s", mk_txn(100, changes))
    n_chunks = sql(store.dsn, "SELECT count(*) FROM rowback.change_chunks WHERE txn_id = %s",
                   (txn_id,))[0][0]
    assert n_chunks == 3
    assert [c.new[0] for c in store.load_changes(txn_id)] == [str(i) for i in range(12_000)]


def test_prune_removes_old_transactions_and_chunks(store):
    store.save_txn("s", mk_txn(100, [ch(0, "I", new=["1", "a"])], ts=NOW - timedelta(days=10)))
    keep = store.save_txn("s", mk_txn(200, [ch(0, "I", new=["2", "b"])]))
    assert store.prune(timedelta(days=7)) == 1
    assert [t.id for t in store.search()] == [keep]
    assert sql(store.dsn, "SELECT count(*) FROM rowback.change_chunks")[0][0] == 1


def test_mark_undone_links_both_transactions(store):
    a = store.save_txn("s", mk_txn(100, [ch(0, "I", new=["1", "a"])]))
    b = store.save_txn("s", mk_txn(200, [ch(0, "D", old=["1", "a"])]))
    store.mark_undone(a, b)
    assert store.get_txn(a).undone_by == b
    assert store.get_txn(b).undo_of == a


def test_save_batch_is_atomic_and_dedupes_per_txn(store):
    store.save_txn("s", mk_txn(100, [ch(0, "I", new=["1", "a"])]))
    ids = store.save_batch("s", [mk_txn(100, [ch(0, "I", new=["1", "a"])]),
                                 mk_txn(200, [ch(0, "I", new=["2", "b"])]),
                                 mk_txn(300, [ch(0, "I", new=["3", "c"])])])
    assert ids[0] is None and ids[1] is not None and ids[2] is not None
    assert store.get_confirmed_lsn("s") == 308
    assert store.stats()["transactions"] == 3


def test_undo_of_link_set_on_save(store):
    a = store.save_txn("s", mk_txn(100, [ch(0, "I", new=["1", "a"])]))
    undo = mk_txn(200, [ch(0, "D", old=["1", "a"])])
    undo.undo_of = a
    b = store.save_txn("s", undo)
    assert store.get_txn(a).undone_by == b and store.get_txn(b).undo_of == a


def test_save_batch_uses_a_constant_number_of_statements(store):
    """Round trips dominate when the history DB is remote; a batch must not cost O(txns)."""
    import psycopg2.extensions

    calls = {"n": 0}

    class CountingCursor(psycopg2.extensions.cursor):
        def execute(self, query, vars=None):  # noqa: A002
            calls["n"] += 1
            return super().execute(query, vars)

    store.conn.cursor_factory = CountingCursor
    batch = [mk_txn(1000 + i * 100, [ch(0, "I", new=[str(i), "x"])]) for i in range(300)]
    ids = store.save_batch("s", batch)
    assert all(i is not None for i in ids) and len(set(ids)) == 300
    assert calls["n"] <= 6
    assert [c.new[0] for c in store.load_changes(ids[123])] == ["123"]
