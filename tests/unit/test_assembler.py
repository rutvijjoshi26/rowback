import json
from datetime import datetime, timezone

from rowback import pgoutput as p
from rowback.assembler import TxnAssembler

WHEN = datetime(2026, 10, 9, 14, 14, tzinfo=timezone.utc)
REL = p.Relation(16384, "public", "orders", "f",
                 [p.Column("id", 20, True), p.Column("note", 25, False)])


def feed_all(asm, msgs):
    out = []
    for m in msgs:
        txn = asm.feed(m)
        if txn is not None:
            out.append(txn)
    return out


def test_insert_txn_is_emitted_on_commit():
    asm = TxnAssembler()
    txns = feed_all(asm, [p.Begin(100, WHEN, 7), REL, p.Insert(16384, ["1", None]),
                          p.Commit(90, 100, WHEN)])
    assert len(txns) == 1
    t = txns[0]
    assert (t.xid, t.commit_lsn, t.end_lsn, t.commit_ts) == (7, 90, 100, WHEN)
    c = t.changes[0]
    assert (c.schema, c.table, c.op, c.columns, c.key_columns) == (
        "public", "orders", "I", ["id", "note"], ["id"])
    assert c.new == ["1", None] and c.old is None and c.types == [20, 25]


def test_relation_cache_survives_across_transactions():
    asm = TxnAssembler()
    feed_all(asm, [p.Begin(1, WHEN, 1), REL, p.Commit(1, 1, WHEN)])
    txns = feed_all(asm, [p.Begin(2, WHEN, 2), p.Delete(16384, "O", ["1", "x"]),
                          p.Commit(2, 2, WHEN)])
    c = txns[0].changes[0]
    assert c.op == "D" and c.old == ["1", "x"] and c.old_kind == "O"


def test_unchanged_toast_filled_from_old_image():
    asm = TxnAssembler()
    txns = feed_all(asm, [p.Begin(1, WHEN, 1), REL,
                          p.Update(16384, "O", ["1", "big"], ["1", p.UNCHANGED]),
                          p.Commit(1, 1, WHEN)])
    assert txns[0].changes[0].new == ["1", "big"]


def test_context_role_app_first_wins_and_queries_accumulate():
    asm = TxnAssembler()
    ctx = json.dumps({"role": "agent", "app": "claw", "query": "DELETE FROM orders"}).encode()
    other = json.dumps({"role": "x", "app": "y", "query": "z"}).encode()
    txns = feed_all(asm, [p.Begin(1, WHEN, 1), REL,
                          p.LogicalMessage(True, 1, "rowback", ctx),
                          p.LogicalMessage(True, 1, "rowback", other),
                          p.LogicalMessage(True, 1, "someone-else", b"{}"),
                          p.Insert(16384, ["1", None]), p.Commit(1, 1, WHEN)])
    t = txns[0]
    assert (t.role, t.application_name) == ("agent", "claw")
    assert t.query == "DELETE FROM orders\nz"


def test_context_duplicate_queries_kept_once_and_capped_at_five():
    asm = TxnAssembler()
    msgs = [p.Begin(1, WHEN, 1), REL]
    for q in ["a", "a", "b", "c", "d", "e", "f"]:
        msgs.append(p.LogicalMessage(True, 1, "rowback", json.dumps({"query": q}).encode()))
    msgs.append(p.Commit(1, 1, WHEN))
    assert feed_all(asm, msgs)[0].query == "a\nb\nc\nd\ne"


def test_context_undo_of_is_recorded():
    asm = TxnAssembler()
    ctx = json.dumps({"role": "r", "app": "rowback-undo", "undo_of": 42}).encode()
    txns = feed_all(asm, [p.Begin(1, WHEN, 1), p.LogicalMessage(True, 1, "rowback", ctx),
                          p.Commit(1, 1, WHEN)])
    assert txns[0].undo_of == 42


def test_seq_numbers_increase_and_truncate_is_recorded():
    asm = TxnAssembler()
    txns = feed_all(asm, [p.Begin(1, WHEN, 1), REL, p.Insert(16384, ["1", None]),
                          p.Insert(16384, ["2", None]), p.Truncate([16384], 0),
                          p.Commit(1, 1, WHEN)])
    t = txns[0]
    assert [c.seq for c in t.changes] == [0, 1]
    assert t.truncated_tables == ["public.orders"]


def test_non_transactional_message_outside_txn_is_ignored():
    asm = TxnAssembler()
    assert asm.feed(p.LogicalMessage(False, 1, "rowback", b"{}")) is None


def test_primary_key_comes_from_resolver_not_relation_flags():
    # Under REPLICA IDENTITY FULL pgoutput flags every column as a key column.
    full_rel = p.Relation(1, "public", "orders", "f",
                          [p.Column("id", 20, True), p.Column("note", 25, True)])
    asm = TxnAssembler(pk_resolver=lambda schema, table: ["id"])
    txns = feed_all(asm, [p.Begin(1, WHEN, 1), full_rel, p.Insert(1, ["1", "a"]),
                          p.Commit(1, 1, WHEN)])
    assert txns[0].changes[0].key_columns == ["id"]


def test_unchanged_toast_without_old_image_does_not_crash_capture():
    # REPLICA IDENTITY DEFAULT: no old tuple, unchanged TOAST value omitted by pgoutput
    asm = TxnAssembler()
    txns = feed_all(asm, [p.Begin(1, WHEN, 1), REL,
                          p.Update(16384, None, None, ["1", p.UNCHANGED]),
                          p.Commit(1, 1, WHEN)])
    c = txns[0].changes[0]
    assert c.op == "U" and c.old is None and c.new == ["1", None]
