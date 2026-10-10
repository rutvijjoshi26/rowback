import threading

import pytest

from rowback.store import HistoryStore, str_to_lsn
from tests.conftest import sql
from tests.integration.helpers import (capture, history_count, run_in_txn, settings,
                                       setup_tracked)
from rowback.capture import CaptureDaemon

pytestmark = pytest.mark.integration

TYPES_DDL = """
CREATE TABLE typed (
    id bigint PRIMARY KEY, n numeric(12, 4), t text, ts timestamptz, d date, j jsonb,
    arr int[], b bytea, u uuid, flag boolean, f double precision, iv interval, maybe text
)"""
ROW_SQL = ("INSERT INTO typed VALUES (1, 12345.6789, E'it''s a\\nnew \\u00e9 line', "
           "'2026-10-09 14:14:00.123456+00', '2026-10-09', '{\"a\": [1, null, \"x\"]}', "
           "'{1,2,NULL}', '\\xdeadbeef', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', true, 0.1, "
           "'1 day 02:03:04', NULL)")


class Crash(Exception):
    pass


def test_all_types_captured_as_exact_text(dbs):
    setup_tracked(dbs, TYPES_DDL, ["typed"])
    sql(dbs.source_dsn, ROW_SQL)
    expected = list(sql(dbs.source_dsn, "SELECT id::text, n::text, t, ts::text, d::text, j::text, "
                        "arr::text, b::text, u::text, concat(flag), f::text, iv::text, maybe "
                        "FROM typed")[0])
    sql(dbs.source_dsn, "UPDATE typed SET maybe = 'set' WHERE id = 1")
    sql(dbs.source_dsn, "DELETE FROM typed WHERE id = 1")
    capture(dbs)
    store = HistoryStore(dbs.history_dsn)
    txns = list(reversed(store.search()))
    assert [t.n_insert + t.n_update + t.n_delete for t in txns] == [1, 1, 1]
    ins, upd, dele = (store.load_changes(t.id)[0] for t in txns)
    assert ins.op == "I" and ins.new == expected
    assert upd.old == expected and upd.new == expected[:-1] + ["set"]
    assert dele.op == "D" and dele.old == expected[:-1] + ["set"]
    assert ins.key_columns == ["id"]


def test_context_records_role_application_and_query(dbs):
    setup_tracked(dbs, "CREATE TABLE orders (id int PRIMARY KEY, status text)", ["orders"])
    sql(dbs.source_dsn, "INSERT INTO orders SELECT g, 'new' FROM generate_series(1, 10) g")
    run_in_txn(dbs.source_dsn, "DELETE FROM orders WHERE id > 5", application_name="agent-x")
    capture(dbs)
    store = HistoryStore(dbs.history_dsn)
    top = store.search(op="D")[0]
    assert (top.role, top.application_name, top.n_delete) == ("postgres", "agent-x", 5)
    assert "DELETE FROM orders WHERE id > 5" in top.query


def test_untracked_tables_are_not_captured(dbs):
    setup_tracked(dbs, "CREATE TABLE a (id int PRIMARY KEY); CREATE TABLE b (id int PRIMARY KEY)",
                  ["a"])
    sql(dbs.source_dsn, "INSERT INTO b VALUES (1)")
    sql(dbs.source_dsn, "INSERT INTO a VALUES (1)")
    capture(dbs)
    assert [t.tables for t in HistoryStore(dbs.history_dsn).search()] == [["public.a"]]


def test_restart_loses_nothing_and_duplicates_nothing(dbs):
    setup_tracked(dbs, "CREATE TABLE t (id int PRIMARY KEY)", ["t"])
    for i in range(50):
        sql(dbs.source_dsn, f"INSERT INTO t VALUES ({i})")
    capture(dbs)
    for i in range(50, 100):
        sql(dbs.source_dsn, f"INSERT INTO t VALUES ({i})")
    capture(dbs)
    capture(dbs)  # a third run with nothing new is a no-op
    assert history_count(dbs) == (100, 100)


def test_crash_after_history_commit_before_feedback_has_no_duplicates(dbs):
    setup_tracked(dbs, "CREATE TABLE t (id int PRIMARY KEY)", ["t"])
    for i in range(30):
        sql(dbs.source_dsn, f"INSERT INTO t VALUES ({i})")
    calls = {"n": 0}

    def crash_after_second_batch() -> None:
        calls["n"] += 1
        if calls["n"] == 2:
            raise Crash()

    d = CaptureDaemon(settings(dbs), max_batch_txns=5, feedback_interval_s=3600,
                      after_save=crash_after_second_batch)
    with pytest.raises(Crash):
        d.run_until_caught_up(timeout=60)
    stored = HistoryStore(dbs.history_dsn).get_confirmed_lsn(dbs.slot)
    slot_lsn = sql(dbs.source_dsn, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots "
                                   "WHERE slot_name = %s", (dbs.slot,))[0][0]
    # the history store is ahead of the slot: the server will resend these transactions
    assert str_to_lsn(slot_lsn) < stored
    assert history_count(dbs) == (10, 10)
    capture(dbs)
    assert history_count(dbs) == (30, 30)


def test_crash_before_history_commit_loses_nothing(dbs):
    setup_tracked(dbs, "CREATE TABLE t (id int PRIMARY KEY)", ["t"])
    for i in range(30):
        sql(dbs.source_dsn, f"INSERT INTO t VALUES ({i})")

    def crash() -> None:
        raise Crash()

    d = CaptureDaemon(settings(dbs), max_batch_txns=5, before_save=crash)
    with pytest.raises(Crash):
        d.run_until_caught_up(timeout=60)
    assert history_count(dbs) == (0, 0)
    capture(dbs)
    assert history_count(dbs) == (30, 30)
    ids = sorted(int(c.new[0]) for t in HistoryStore(dbs.history_dsn).search(limit=100)
                 for c in HistoryStore(dbs.history_dsn).load_changes(t.id))
    assert ids == list(range(30))


def test_concurrent_writers_while_daemon_runs(dbs):
    setup_tracked(dbs, "CREATE TABLE t (id int PRIMARY KEY, w int)", ["t"])
    stop = threading.Event()
    daemon = CaptureDaemon(settings(dbs))
    runner = threading.Thread(target=daemon.run, kwargs={"stop_event": stop})
    runner.start()

    def writer(w: int) -> None:
        for i in range(100):
            sql(dbs.source_dsn, f"INSERT INTO t VALUES ({w * 1000 + i}, {w})")

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stop.set()
    runner.join(timeout=30)
    capture(dbs)
    assert history_count(dbs) == (400, 400)


def test_truncate_is_recorded(dbs):
    setup_tracked(dbs, "CREATE TABLE t (id int PRIMARY KEY)", ["t"])
    sql(dbs.source_dsn, "INSERT INTO t VALUES (1)")
    sql(dbs.source_dsn, "TRUNCATE t")
    capture(dbs)
    top = HistoryStore(dbs.history_dsn).search()[0]
    assert top.truncated == ["public.t"] and top.n_changes == 0


def _insert_forever(dsn: str, seconds: float) -> None:
    import time

    import psycopg2

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()
    end = time.time() + seconds
    while time.time() < end:
        cur.execute("INSERT INTO t DEFAULT VALUES")
    conn.close()


def test_slot_is_acknowledged_while_writes_keep_streaming(dbs):
    """Feedback must not wait for an idle stream, or WAL piles up under steady load."""
    import multiprocessing
    import time

    setup_tracked(dbs, "CREATE TABLE t (id serial PRIMARY KEY)", ["t"])
    stop_daemon = threading.Event()

    def slot_lsn() -> int:
        return str_to_lsn(sql(dbs.source_dsn, "SELECT confirmed_flush_lsn::text FROM "
                                              "pg_replication_slots WHERE slot_name = %s",
                              (dbs.slot,))[0][0])

    ctx = multiprocessing.get_context("spawn")
    writers = [ctx.Process(target=_insert_forever, args=(dbs.source_dsn, 6.0)) for _ in range(4)]
    for w in writers:
        w.start()
    time.sleep(1.0)  # build a backlog so the stream never goes idle
    daemon = CaptureDaemon(settings(dbs), feedback_interval_s=0.3)
    d = threading.Thread(target=daemon.run, kwargs={"stop_event": stop_daemon})
    d.start()
    try:
        start = slot_lsn()
        time.sleep(3.0)
        assert slot_lsn() > start
    finally:
        for w in writers:
            w.join()
        stop_daemon.set()
        d.join(timeout=60)


def test_idle_slot_advances_past_wal_without_any_transaction(dbs):
    """WAL that decodes to nothing (VACUUM, checkpoints) must still be acknowledged,
    otherwise an idle primary retains WAL and lag never reaches zero."""
    import time

    setup_tracked(dbs, "CREATE TABLE t (id int PRIMARY KEY); CREATE TABLE u (id int)", ["t"])
    sql(dbs.source_dsn, "INSERT INTO t VALUES (1)")
    sql(dbs.source_dsn, "INSERT INTO u SELECT generate_series(1, 200000)")
    stop = threading.Event()
    daemon = CaptureDaemon(settings(dbs), feedback_interval_s=0.2)
    d = threading.Thread(target=daemon.run, kwargs={"stop_event": stop})
    d.start()
    try:
        time.sleep(1.5)
        sql(dbs.source_dsn, "VACUUM (FREEZE) u")  # writes WAL, has no commit record
        target = str_to_lsn(sql(dbs.source_dsn, "SELECT pg_current_wal_lsn()::text")[0][0])
        deadline = time.time() + 20
        confirmed = 0
        while time.time() < deadline:
            confirmed = str_to_lsn(sql(dbs.source_dsn, "SELECT confirmed_flush_lsn::text FROM "
                                                       "pg_replication_slots WHERE slot_name = %s",
                                       (dbs.slot,))[0][0])
            if confirmed >= target:
                break
            time.sleep(0.2)
        assert confirmed >= target
    finally:
        stop.set()
        d.join(timeout=30)
    assert history_count(dbs) == (1, 1)


def _walsender_pid(dbs):
    rows = sql(dbs.source_dsn, f"SELECT active_pid FROM pg_replication_slots WHERE slot_name = '{dbs.slot}'")
    return rows[0][0] if rows else None


def _wait_for(predicate, timeout: float = 30.0) -> None:
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise AssertionError("condition not met before timeout")


def test_daemon_reconnects_after_walsender_is_killed_exactly_once(dbs):
    setup_tracked(dbs, "CREATE TABLE t (id int PRIMARY KEY)", ["t"])
    stop = threading.Event()
    daemon = CaptureDaemon(settings(dbs))
    runner = threading.Thread(target=daemon.run_forever, kwargs={"stop_event": stop, "max_backoff_s": 1.0})
    runner.start()
    try:
        _wait_for(lambda: _walsender_pid(dbs) is not None)
        for i in range(50):
            sql(dbs.source_dsn, f"INSERT INTO t VALUES ({i})")
        first_pid = _walsender_pid(dbs)
        sql(dbs.source_dsn, f"SELECT pg_terminate_backend({first_pid})")
        for i in range(50, 100):
            sql(dbs.source_dsn, f"INSERT INTO t VALUES ({i})")
        _wait_for(lambda: _walsender_pid(dbs) not in (None, first_pid))
        assert runner.is_alive(), "daemon exited instead of reconnecting"
    finally:
        stop.set()
        runner.join(timeout=30)
    capture(dbs)
    assert history_count(dbs) == (100, 100)
