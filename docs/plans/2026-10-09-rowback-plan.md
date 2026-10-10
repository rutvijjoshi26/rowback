# rowback Implementation Plan


**Goal:** A row-level undo button for PostgreSQL: capture row changes with logical decoding, store compressed history, search the timeline, and undo one transaction atomically with conflict detection; plus a guard gateway for AI agents and reproducible benchmarks.

**Architecture:** A Python capture daemon reads a pgoutput logical replication slot, groups changes by transaction, and writes zstd-compressed chunks to a history database in the same transaction as the confirmed LSN. The CLI reads history, builds an inverse plan, checks conflicts against live rows, and applies the undo with set-based SQL in one transaction.

**Tech Stack:** Python 3.12, psycopg2-binary, click, msgpack, zstandard, pglast, mcp, pytest, testcontainers, PostgreSQL 17 in Docker (colima).

**Spec:** `docs/specs/2026-10-09-rowback-design.md`

## Global Constraints

- Python >= 3.11, fully type-annotated, `src/` layout, package name `rowback`.
- PostgreSQL 17 image `postgres:17`, `wal_level=logical`, `max_slot_wal_keep_size` configured.
- No README, no `git commit`, no `git push` (owner rule). No mention of AI tooling in code or docs.
- No em-dashes in docs.
- Every reported metric comes from a script run with output saved under `benchmarks/results/`.

## File Structure

```
pyproject.toml
docker-compose.yml
docker/postgres.conf
src/rowback/__init__.py
src/rowback/errors.py        RowbackError, ConflictError, NotUndoableError
src/rowback/config.py        Settings dataclass (source_dsn, history_dsn, slot, publication)
src/rowback/pgoutput.py      decode(bytes) -> message dataclass
src/rowback/model.py         RelationInfo, Change, CapturedTxn
src/rowback/assembler.py     TxnAssembler.feed(msg, lsn) -> CapturedTxn | None
src/rowback/store.py         HistoryStore
src/rowback/admin.py         init_source, track_table, untrack_table, list_tracked
src/rowback/capture.py       CaptureDaemon
src/rowback/undo.py          build_plan, preview_sql, find_conflicts, apply_undo
src/rowback/monitor.py       slot_status
src/rowback/guard/policy.py  classify(sql) -> Verdict
src/rowback/guard/gateway.py Gateway.execute(sql, agent) -> GuardResult
src/rowback/guard/mcp_server.py
src/rowback/cli.py
tests/conftest.py            postgres container fixture, per-test database
tests/unit/...
tests/integration/...
demo/demo.py
benchmarks/...
```

---

### Task 1: Scaffold, Postgres fixture, pgoutput decoder

**Files:** `pyproject.toml`, `src/rowback/{__init__,errors,config,pgoutput}.py`, `tests/conftest.py`, `tests/unit/test_pgoutput.py`, `tests/integration/test_pgoutput_live.py`

**Produces:**
- `pgoutput.decode(data: bytes) -> Begin | Commit | Relation | Insert | Update | Delete | Truncate | LogicalMessage | Origin | TypeMsg`
- `Begin(final_lsn:int, commit_ts:datetime, xid:int)`, `Commit(commit_lsn:int, end_lsn:int, commit_ts:datetime)`, `Relation(oid, schema, name, replica_identity:str, columns: list[Column(name, type_oid, is_key:bool)])`, `Insert(oid, new: list[str|None|UNCHANGED])`, `Update(oid, old: list|None, old_kind: 'K'|'O'|None, new: list)`, `Delete(oid, old: list, old_kind)`, `Truncate(oids: list[int], options:int)`, `LogicalMessage(transactional: bool, lsn:int, prefix:str, content: bytes)`
- fixture `pg` (session; container with wal_level=logical) and `db` (function; fresh database DSN)

Tests:
- [ ] unit: decode hand-built Begin, Commit, Relation, Insert with NULL and text, Update with old 'O' tuple, Delete with key 'K' tuple, unchanged-toast 'u', Truncate, Message.
- [ ] integration: create slot with pgoutput on a real server, perform insert/update/delete, `pg_logical_slot_peek_binary_changes` and decode every message without error, assert op sequence.

### Task 2: Assembler

**Files:** `src/rowback/model.py`, `src/rowback/assembler.py`, `tests/unit/test_assembler.py`

**Produces:** `Change(seq:int, schema:str, table:str, op:'I'|'U'|'D', columns:list[str], types:list[int], key_columns:list[str], old:list|None, new:list|None)`, `CapturedTxn(xid, commit_lsn:int, end_lsn:int, commit_ts, changes:list[Change], role, application_name, query, truncated_tables:list[str])`. `TxnAssembler().feed(msg) -> CapturedTxn | None` returns a txn on Commit. Unchanged-toast values in UPDATE new tuples are filled from the old tuple when present. Context comes from `LogicalMessage` with prefix `rowback` (JSON: role, app, query; first one wins).

Tests:
- [ ] begin/relation/insert/commit yields a txn with one change and correct column names
- [ ] unchanged toast is filled from old image
- [ ] context message populates role/app/query
- [ ] messages from another prefix are ignored

### Task 3: History store

**Files:** `src/rowback/store.py`, `tests/integration/test_store.py`

**Produces:** `HistoryStore(dsn)` with `ensure_schema()`, `get_confirmed_lsn(slot) -> int`, `save_txn(slot, txn) -> int | None` (atomic with confirmed LSN; returns None if deduped), `advance_lsn(slot, lsn)` (for empty or skipped txns), `search(since=None, until=None, table=None, op=None, min_rows=None, limit=50) -> list[TxnSummary]`, `get_txn(id) -> TxnSummary`, `load_changes(id) -> list[Change]`, `mark_undone(orig_id, undo_id)`, `prune(older_than: timedelta) -> int`, `stats() -> dict` (bytes of tables, change count). LSN helpers `lsn_to_str`, `str_to_lsn`.

Tests:
- [ ] save and load round trip equals original changes (values incl. None and unicode)
- [ ] saving same commit_lsn twice is deduped and returns None
- [ ] confirmed LSN advances atomically with save
- [ ] search filters by table, op, min_rows, time window
- [ ] prune deletes old txns and their chunks
- [ ] chunks split at 5000 changes

### Task 4: Admin (init and opt-in)

**Files:** `src/rowback/admin.py`, `tests/integration/test_admin.py`

**Produces:** `init_source(source_dsn, slot, publication)` creates publication (empty) and slot if absent, and the `rowback_emit_context()` trigger function; `track_table(source_dsn, table, publication, identity='full', context=True)`; `untrack_table(...)`; `list_tracked(...)`; `drop_source(...)`.

Tests:
- [ ] tracking a table without PK raises RowbackError mentioning "primary key"
- [ ] tracking sets REPLICA IDENTITY FULL and adds to publication; composite PK accepted
- [ ] context trigger installed when context=True
- [ ] init is idempotent

### Task 5: Capture daemon

**Files:** `src/rowback/capture.py`, `tests/integration/test_capture.py`

**Produces:** `CaptureDaemon(settings, store)` with `run(stop_event: threading.Event | None = None, max_idle_s: float | None = None)` and `run_until_caught_up(timeout=30)` (helper for tests and benchmarks: consumes until the slot confirmed LSN reaches current WAL LSN at start). Hook `_after_save` used by tests to simulate a crash between history commit and feedback.

Tests:
- [ ] inserts/updates/deletes across many types (int, bigint, numeric, text with quotes/newlines/unicode, NULL, timestamptz, date, jsonb, int[], bytea, uuid, bool) are captured with exact text values
- [ ] context: role and application_name recorded
- [ ] restart: run, write more, run again: total captured == committed, no duplicates
- [ ] crash after save before feedback: restart produces no duplicate
- [ ] crash before save: transaction is not lost
- [ ] untracked tables are not captured

### Task 6: Undo engine

**Files:** `src/rowback/undo.py`, `tests/integration/test_undo.py`

**Produces:** `UndoPlan(txn_id, steps: list[UndoStep(schema, table, kind: 'delete'|'update'|'insert', columns, key_columns, rows: list[RowImage(before, after)])])`; `build_plan(store, txn_id) -> UndoPlan` (raises NotUndoableError for missing old images, TRUNCATE, already undone); `preview_sql(plan, limit=None) -> str`; `apply_undo(source_dsn, store, plan, on_conflict='abort'|'skip'|'overwrite', dry_run=False, replica_role=False) -> UndoResult(applied:int, conflicts:list[Conflict], skipped:int, undo_xid)`.

Tests:
- [ ] undo DELETE restores exact rows (compare full table snapshot with pre-delete snapshot)
- [ ] undo UPDATE restores old values; undo INSERT deletes inserted rows
- [ ] later good writes to other rows are preserved
- [ ] conflict: row updated after bad UPDATE is detected; abort leaves table unchanged
- [ ] conflict modes skip and overwrite behave as specified
- [ ] conflict: deleted key re-inserted later is detected
- [ ] composite PK and all types round-trip through undo
- [ ] multi-op transaction (insert+update+delete same row) collapses correctly
- [ ] undoing twice is refused; DEFAULT identity delete is refused with a clear message
- [ ] preview SQL contains INSERT statements with the deleted values

### Task 7: Monitor, retention and CLI

**Files:** `src/rowback/monitor.py`, `src/rowback/cli.py`, `tests/integration/test_cli.py`

**Produces:** `slot_status(source_dsn, slot) -> SlotStatus(active, lag_bytes, retained_bytes, wal_status, safe_wal_size)`; CLI commands `init`, `track`, `untrack`, `capture`, `log` (search), `show`, `undo` (`--preview`, `--yes`, `--on-conflict`), `status`, `prune`. Settings from env `ROWBACK_SOURCE_DSN`, `ROWBACK_HISTORY_DSN`.

Tests:
- [ ] status reports lag > 0 when writes are pending and ~0 after capture
- [ ] CLI end to end with click's CliRunner: init, track, write, capture, log shows DELETE row count, undo --yes restores rows

### Task 8: docker-compose and demo

**Files:** `docker-compose.yml`, `Dockerfile`, `demo/demo.py`, `demo/schema.sql`

- [ ] compose brings up postgres (logical, max_slot_wal_keep_size) and the capture daemon
- [ ] demo: seed orders, start capture, "agent" deletes 40,000 orders, good writes continue, find the delete, preview, undo, verify counts. Run it and save output to `demo/demo_output.txt`.

### Task 9: Benchmarks

**Files:** `benchmarks/workload.sql`, `benchmarks/overhead.py`, `benchmarks/undo_vs_restore.py`, `benchmarks/summarize.py`, `benchmarks/results/`

- [ ] overhead: 4 configs x 3 runs, pgbench in the container, record TPS, latency avg/p99, WAL bytes, history bytes, changes captured
- [ ] undo vs restore: dataset sizes and bad delete sizes; time undo, time pg_restore baseline, count lost good writes
- [ ] summary.md generated from JSON

### Task 10: Guard mode

**Files:** `src/rowback/guard/{__init__,policy,gateway,mcp_server}.py`, `tests/unit/test_policy.py`, `tests/integration/test_guard.py`

**Produces:** `classify(sql) -> Verdict(action: 'allow'|'block'|'review', reasons)`; `Gateway(source_dsn, history_dsn, max_rows=1000).execute(sql, agent) -> GuardResult(status: 'executed'|'blocked'|'pending', rows, rowcount, request_id, reason)`; `approve(request_id)`, `reject(request_id)`; MCP server with tools `run_sql`, `request_status`.

Tests:
- [ ] unit: DROP/TRUNCATE blocked; DELETE/UPDATE without WHERE held; multi-statement handled; SELECT allowed; parse errors blocked
- [ ] integration: DELETE affecting > max_rows rolled back and pending; approve executes it; small DELETE executes; every decision logged; application_name tagged
- [ ] MCP tool function callable in-process

### Task 11: Verification

- [ ] full test suite run, results recorded
- [ ] final report
