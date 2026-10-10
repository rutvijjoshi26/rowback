# rowback operations notes

## Source server settings

| setting | value | why |
|---|---|---|
| `wal_level` | `logical` | required for logical decoding (restart needed) |
| `max_replication_slots`, `max_wal_senders` | at least 1 more than you use today | rowback holds one slot and one walsender |
| `max_slot_wal_keep_size` | a bounded size, e.g. `4GB` (set in `docker-compose.yml`) | protects the primary's disk |

A logical slot makes the primary keep every WAL segment the consumer has not confirmed. If the capture daemon stops and `max_slot_wal_keep_size` is `-1` (the default), WAL grows until the disk fills and the primary stops accepting writes. With a bound, PostgreSQL invalidates the slot instead (`wal_status = lost`). That trades a gap in undo history for the primary's availability, which is the right trade for an undo tool. Size it as "longest daemon outage you want to survive" times "peak WAL rate".

## Monitoring

`rowback status` (or `--json`) reports:

- `lag_bytes`: WAL written but not yet confirmed by the daemon
- `retained_bytes`: WAL the primary keeps for the slot
- `wal_status`: `reserved` (fine), `extended` (beyond `max_wal_size`, still safe), `unreserved` (about to be removed), `lost` (slot invalidated: history has a gap)
- `safe_wal_size`: bytes that can still be written before invalidation

Health is `critical` (exit code 4) for `unreserved`/`lost`, and `warn` when `max_slot_wal_keep_size` is unlimited, no daemon is connected, or lag exceeds `--warn-lag`. The daemon also logs lag every 10 seconds at INFO level.

If a slot is lost: run `rowback init` again (it recreates the slot), and treat everything between the last captured transaction and the new slot as uncaptured.

## Restarts and delivery guarantee

History rows and `rowback.capture_state.confirmed_lsn` are committed in one history-DB transaction. The daemon tells the slot to advance only to LSNs whose batch has committed. After any crash or restart the server resends from the slot position, and the store skips transactions at or below its own confirmed LSN (with `UNIQUE(commit_lsn)` as a backstop). The integration tests cover a crash before the history commit and a crash between the history commit and the slot acknowledgement.

On connection loss the daemon reconnects with backoff and resumes from the slot. A connection error is any `OperationalError` or `InterfaceError`, plus any `DatabaseError` that carries no SQLSTATE. The last case matters under heavy load: libpq can drop the replication stream with `error with status PGRES_COPY_BOTH and no message from the libpq`, which psycopg2 raises as a bare `DatabaseError`. Other database errors that carry a SQLSTATE (for example a permission error) are not retried and stop the daemon.

## Opting tables in

`rowback track <table>` requires a primary key and sets `REPLICA IDENTITY FULL`, so DELETE and UPDATE log the complete old row. `--identity default` lowers WAL volume but makes DELETE/UPDATE undo impossible (rowback refuses with a clear message). `--no-context` skips the statement-level trigger that records role, application_name and the first 500 characters of the statement. Statement text can contain sensitive literals; use `rowback init --query-chars 0` to record none.

## Undo

- `rowback log --since 2h --op delete --min-rows 1000` finds candidates.
- `rowback undo <id> --preview` prints the inverse SQL; `--dry-run` runs the undo and rolls it back, reporting conflicts.
- Conflicts mean a row's live state is not what the bad transaction left (it was changed, deleted, or a deleted key was reused). `--on-conflict abort` (default) changes nothing, `skip` restores only untouched rows, `overwrite` forces the before-image.
- Undo runs in one transaction with `lock_timeout` 10s, deletes run before updates and inserts, and inserts follow foreign-key order (parents first). `--replica-role` disables triggers and FK checks for cycles that ordering cannot solve (needs superuser).

## Known limits

- One single-threaded Python process decodes the stream; see `benchmarks/results/summary.md` for measured throughput. Above that rate the daemon lags and catches up later.
- Whole transactions are buffered in daemon memory until commit (pgoutput protocol v1); a multi-million-row transaction needs proportionate RAM.
- DDL is not captured. If a table's columns change after a transaction was captured, undo refuses when captured columns no longer exist.
- `TRUNCATE` is recorded but cannot be undone row by row.
- The history database should live on a different server from the source in production; the compose file keeps both on one server for convenience.
