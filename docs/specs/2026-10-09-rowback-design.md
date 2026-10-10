# rowback: design spec

Date: 2026-10-09
Status: approved (self-reviewed; the owner asked for autonomous execution)

## 1. Problem

A destructive `DELETE` or `UPDATE` against production (from a human or an AI coding agent) is usually repaired with a full point-in-time restore. That takes a long time, scales with the size of the whole database, and discards every good write made after the mistake. AWS Aurora Backtrack rewinds an entire cluster and exists only for Aurora MySQL. rowback is a row-level "undo button" for PostgreSQL: it records row changes for opted-in tables and can invert one chosen transaction while leaving later good writes alone.

## 2. Decisions made during brainstorming

The questions below were answered by the implementer, with reasoning, because the owner asked for autonomous execution.

| Question | Decision | Reasoning |
|---|---|---|
| Decoder plugin | `pgoutput` over a psycopg2 replication connection | Built into PostgreSQL 10+, available on RDS, Aurora PostgreSQL, Cloud SQL and Azure without installing an extension. Table opt-in maps naturally onto publication membership. Cost: we must parse the binary protocol ourselves (about 200 lines, fully unit tested against real server output). wal2json would need a custom image and is not installed everywhere. |
| PostgreSQL version | 17 (works on 14+) | `messages` option of pgoutput (needed for transaction context) requires 14+. |
| Driver | psycopg2 | psycopg 3 does not implement the streaming replication protocol. |
| Where history lives | A separate PostgreSQL database (`rowback` by default, may be on another server) | Durable, transactional, queryable with SQL, and lets us persist history and the confirmed LSN in one atomic commit. |
| History encoding | Per transaction: one metadata row, plus change "chunks" of up to 5,000 changes serialized with msgpack and compressed with zstd | Row-per-change storage with jsonb would cost several hundred bytes per change; chunking amortizes per-row overhead and compresses well. Cost: cannot index individual historic rows (not needed, see conflict detection). |
| Old row images | Opt-in sets `REPLICA IDENTITY FULL` by default; `--identity default` is allowed but then DELETE/UPDATE undo is refused | DEFAULT identity only logs the primary key for DELETE, so the deleted row cannot be reconstructed. FULL costs extra WAL, which we measure. |
| Who did it (role, application_name, query) | Optional statement-level trigger on each opted-in table that calls `pg_logical_emit_message(true, 'rowback', ...)` with `current_user`, `application_name` and the first 500 chars of `current_query()` | WAL does not record the session role. A transactional logical message is decoded inside the same transaction, so context and changes arrive together. Statement-level, so one call per statement, not per row. Can be disabled (`--no-context`); overhead is benchmarked. Query text may contain sensitive literals, so it is truncated and can be turned off. |
| Exactly-once capture | History rows and `capture_state.confirmed_lsn` are written in the same history-DB transaction; feedback to the slot is sent only after that commit; on restart, transactions whose commit LSN is at or below the stored LSN are skipped; `UNIQUE(commit_lsn)` is a backstop | A crash before the history commit replays the transaction (no loss); a crash after the history commit but before feedback is deduplicated (no duplicates). |
| Conflict detection | Compare the live row to the bad transaction's after-image, inside the undo transaction, with `SELECT ... FOR UPDATE` | The live table is the source of truth. This also catches writes to rows that were not captured (for example writes made while capture was paused). Comparison uses `col::text IS NOT DISTINCT FROM`, which works for types without an equality operator (json, point). |
| Conflict handling | `--on-conflict abort` (default), `skip` (undo only non-conflicting rows), `overwrite` (force the before-image) | Abort is the safe default; skip covers the common case where a few rows were legitimately touched again. |
| Undo execution | One transaction; rows are bulk loaded with `COPY` into temp tables (`LIKE target`), conflicts computed with set-based SQL, then one `INSERT ... SELECT` / `UPDATE ... FROM` / `DELETE ... USING` per table and operation | Row-by-row statements would be slow for 100K rows. Set-based SQL keeps a 100K-row undo in seconds. |
| Ordering across tables | Inverse operations are applied in reverse commit order, grouped into runs of the same table and operation; `SET CONSTRAINTS ALL DEFERRED` is issued; `--replica-role` sets `session_replication_role = replica` for non-deferrable FK chains (requires superuser or table owner privileges) | Covers the common single-table case cleanly and gives an explicit escape hatch for cascades. |
| Tables without a primary key | Rejected at opt-in with a clear message | Undo needs a stable row identity. |
| Primary disk protection | `max_slot_wal_keep_size` set in compose and documented; `rowback status` reports lag bytes, `wal_status` and `safe_wal_size`; daemon logs lag periodically | A stalled consumer must not fill the primary's disk. If the slot is invalidated, history has a gap, and status reports that. |
| Retention | `rowback prune --older-than 7d` (also run periodically by the daemon if `--retention` is set) | Simple and bounded. |
| Guard mode | `rowback.guard`: parse SQL with `pglast` (the real PostgreSQL parser); policy rules: block `DROP`/`TRUNCATE`/`ALTER`/`GRANT`, hold `DELETE`/`UPDATE` without `WHERE`, run writes in a transaction and roll back plus hold if rowcount exceeds N; held statements go to an approval queue; every decision is logged; exposed as an MCP server tool via the `mcp` SDK | Parsing with the real grammar avoids regex bypasses. The rowcount check is exact, unlike EXPLAIN estimates. Guard sessions set `application_name = rowback-guard:<agent>`, so the timeline shows which agent did what. |
| UI | CLI first (click). Web UI is out of scope unless time remains | Core correctness first. |

## 3. Architecture

```
  app / agent ---> PostgreSQL primary (wal_level=logical)
                    |  publication "rowback" (opted-in tables)
                    |  slot "rowback" (pgoutput)
                    v
           rowback capture daemon  -- psycopg2 replication connection
                    | decode pgoutput, group by transaction
                    v
           history DB (schema rowback): transactions, change_chunks, capture_state
                    ^
                    |  search / preview / undo / status / prune
                 rowback CLI         rowback guard (gateway + MCP server)
```

### Units

- `rowback.pgoutput`: pure decoder of pgoutput protocol v1 messages into dataclasses (Begin, Commit, Relation, Insert, Update, Delete, Truncate, Message). No I/O.
- `rowback.model`: `Change`, `CapturedTxn`, `RelationInfo` dataclasses.
- `rowback.assembler`: turns a stream of decoded messages into `CapturedTxn` objects (relation cache, unchanged-TOAST filling, context messages).
- `rowback.store`: history schema DDL, `save_txn` (atomic with confirmed LSN), search, load changes, prune, stats.
- `rowback.capture`: the daemon loop (start from stored LSN, dedupe, feedback, keepalives, graceful stop, lag logging).
- `rowback.admin`: `init` (publication, slot, history schema), `track`/`untrack` table opt-in with validation (primary key required, sets replica identity, installs context trigger).
- `rowback.undo`: build an `UndoPlan` from a captured transaction, render preview SQL, detect conflicts, apply atomically.
- `rowback.monitor`: slot lag and WAL retention status.
- `rowback.guard`: policy, gateway, approval queue, MCP server.
- `rowback.cli`: click commands.

### History schema (in the history DB)

- `rowback.transactions(id bigserial, xid bigint, commit_lsn pg_lsn unique, commit_ts timestamptz, role text, application_name text, query text, n_changes int, n_insert int, n_update int, n_delete int, tables text[], undone_by bigint null, undo_of bigint null)`
- `rowback.change_chunks(txn_id bigint references transactions on delete cascade, chunk_no int, payload bytea)`; payload is zstd(msgpack(list of changes)), each change `[seq, schema, table, op, columns, types, old_values, new_values, key_columns]` with values in PostgreSQL text format.
- `rowback.capture_state(slot text primary key, confirmed_lsn pg_lsn, updated_at timestamptz)`
- `rowback.guard_log`, `rowback.guard_requests` for guard mode.

## 4. Undo semantics

For a captured transaction T and each changed row:

| Original op | Inverse | Conflict if |
|---|---|---|
| INSERT row r | DELETE r by key | live row with r's key is missing or differs from r |
| UPDATE old o -> new n | UPDATE back to o (key of n) | live row with n's key is missing or differs from n |
| DELETE row o | INSERT o | a live row with o's key already exists |

If a row was changed more than once inside T, only the first before-image and the last after-image matter; the plan collapses them. Undo records itself: the undo transaction is captured like any other, and `transactions.undo_of/undone_by` link them. Undoing the same transaction twice is refused. `TRUNCATE` is captured as an event but cannot be undone (no row images); this is reported clearly.

## 5. Error handling

- Opt-in validation errors (no primary key, unsupported identity) raise `RowbackError` with an actionable message; CLI exits non-zero.
- Daemon: on connection loss, reconnect with backoff and resume from the stored LSN.
- Undo: any failure rolls back the whole transaction; conflicts in `abort` mode raise `ConflictError` listing up to 20 keys.

## 6. Testing

- Unit: pgoutput decoder using byte strings captured from a real server, assembler, guard policy.
- Integration (real PostgreSQL 17 in Docker via testcontainers, one database and slot per test): capture of insert/update/delete across types (numeric, timestamptz, jsonb, arrays, bytea, NULLs, uuid, text with quotes and newlines, composite keys); restart with no loss and no duplicates (including a simulated crash between history commit and feedback); undo restores exact rows; later good writes preserved; conflict detection and each conflict mode; opt-in rejection without PK; guard decisions.

## 7. Benchmarks

- Overhead: pgbench custom write-heavy script (insert, update, delete mix) on 8 clients for a fixed duration. Configurations: baseline (no slot, DEFAULT identity), capture with DEFAULT identity, capture with FULL identity, FULL plus context trigger. Metrics: TPS, average and p99 latency, WAL bytes per transaction, history bytes per million changes, end-of-run slot lag. Each configuration runs 3 times; median reported.
- Undo vs restore: dataset of N orders (1M and larger), bad DELETE of 10K, 50K, 100K rows, followed by good writes. Measure: rowback undo time (end to end CLI path) and verify good writes survive. Baseline: restore of a pre-incident `pg_dump -Fc` into a fresh database with `pg_restore` (time measured), and report the good writes that the restore loses. PITR via base backup is a stretch.
- All raw outputs go to `benchmarks/results/` as JSON, with a generated `summary.md`.

## 8. Out of scope

Web UI (stretch), S3 export (stretch), DDL capture (schema changes between capture and undo are detected only by column mismatch and refused), undo of TRUNCATE, multi-database capture from one daemon.
