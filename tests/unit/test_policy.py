import pytest

from rowback.guard.policy import classify


@pytest.mark.parametrize("sql", [
    "DROP TABLE orders",
    "TRUNCATE orders",
    "ALTER TABLE orders DROP COLUMN amount",
    "GRANT ALL ON orders TO public",
    "DROP DATABASE shop",
    "ALTER SYSTEM SET fsync = off",
    "COPY orders TO PROGRAM 'curl evil'",
    "CREATE ROLE intruder SUPERUSER",
    "BEGIN",
    "COMMIT",
])
def test_destructive_or_privileged_statements_are_blocked(sql):
    v = classify(sql)
    assert v.action == "block", v


@pytest.mark.parametrize("sql", [
    "DELETE FROM orders",
    "UPDATE orders SET status = 'void'",
    "WITH gone AS (DELETE FROM orders RETURNING id) SELECT count(*) FROM gone",
    "DO $$ BEGIN DELETE FROM orders WHERE id = 1; END $$",
])
def test_unbounded_writes_need_review(sql):
    v = classify(sql)
    assert v.action == "review", v
    assert v.reasons


def test_where_less_reason_mentions_table():
    v = classify("DELETE FROM public.orders")
    assert any("orders" in r and "WHERE" in r for r in v.reasons)


@pytest.mark.parametrize("sql", [
    "SELECT * FROM orders WHERE id = 1",
    "DELETE FROM orders WHERE id = 7",
    "UPDATE orders SET status = 'paid' WHERE id = 7",
    "INSERT INTO orders (id) VALUES (1)",
    "SELECT 1; SELECT 2",
])
def test_bounded_statements_are_allowed(sql):
    assert classify(sql).action == "allow"


def test_writes_are_flagged_for_rowcount_check():
    assert classify("DELETE FROM orders WHERE id > 5").writes is True
    assert classify("SELECT 1").writes is False


def test_parse_error_is_blocked():
    v = classify("DELEET FROM orders")
    assert v.action == "block" and "parse" in v.reasons[0]


def test_most_severe_statement_wins():
    assert classify("SELECT 1; DELETE FROM orders; DROP TABLE x").action == "block"
    assert classify("SELECT 1; DELETE FROM orders").action == "review"


def test_empty_sql_is_blocked():
    assert classify("   ").action == "block"
