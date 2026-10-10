# rowback

An undo button for PostgreSQL. rowback records every row change on the tables you choose, lets you find the transaction that went wrong, and reverses just that transaction in seconds, without touching any of the good writes that happened afterward.

## The problem

Someone runs a bad `DELETE` or `UPDATE` against production. It might be an engineer in the wrong terminal, a migration with a missing `WHERE`, or an AI coding agent asked to "clean up stale test data." The usual fix is a full restore from backup or point-in-time recovery (PITR). That takes minutes to hours, and it throws away every legitimate write made since the restore point.

rowback takes a narrower approach: undo the one bad transaction and leave everything else alone.

```
$ rowback log --since 10m --min-rows 1000
#233  2026-10-09 15:51:58  xid=1201  40000 delete  public.orders  postgres@ai-agent
      DELETE FROM orders WHERE status = 'pending' RETURNING 1

$ rowback undo 233 --yes
undo #233 (40000 delete on public.orders, committed 2026-10-09 15:51:58): 40000 row(s) to restore
restored 40000 row(s) in 0.28s
```

(From `demo/demo_output.txt`.)

## How it works

```
 source PostgreSQL ──(logical replication, pgoutput)──▶ capture daemon ──▶ history PostgreSQL
        ▲                                                                       │
        └──────────── rowback undo (one transaction, conflict checked) ◀────────┘
```

1. **Capture.** `rowback track <table>` adds the table to a publication and sets `REPLICA IDENTITY FULL`, so PostgreSQL logs the complete old row for every `UPDATE` and `DELETE`. The capture daemon reads the change stream from a logical replication slot using the built-in `pgoutput` plugin, with no server extension needed, so it works on managed PostgreSQL. It decodes the binary protocol with its own parser.
2. **Context.** An optional statement-level trigger records the role, `application_name`, and query text for each transaction, so the timeline shows who did what.
3. **History store.** Each transaction is saved to a separate PostgreSQL database as a metadata row plus compressed chunks of row changes (msgpack plus zstd, up to 5,000 changes per chunk). Retention is configurable (`--retention 7d`).
4. **Undo.** `rowback undo <id>` builds the inverse of a transaction: deleted rows are reinserted, updated rows get their old values back, and inserted rows are deleted. It applies the result in a single transaction using set-based SQL. Before applying, it compares each live row with what the bad transaction left behind. If a row was changed again afterward, that is a conflict, handled by `--on-conflict abort|skip|overwrite`. The default is `abort`. `--preview` prints the SQL without running it.
5. **Guard mode.** `rowback guard` is a policy gateway for AI agents and unreviewed sessions. `DROP`, `TRUNCATE`, `ALTER`, and privilege changes are blocked. `DELETE` or `UPDATE` without a `WHERE`, and statements that would change more than `--max-rows` rows, are held for human approval. The check parses the SQL with `pglast` and counts exact rows inside a rolled-back transaction. It is also exposed as an MCP server (`rowback guard mcp`), so agents can be pointed at it directly. Approval is deliberately not exposed to agents.

### Delivery guarantee

History rows and the confirmed log position are committed together in one transaction. The daemon only acknowledges a position to PostgreSQL after that commit, and the store skips anything at or below its confirmed position. After a crash or restart, nothing is lost and nothing is recorded twice. If the connection drops, the daemon reconnects with backoff and resumes from the slot. The integration tests cover crashes before and after the history commit, and a killed replication connection.

## Quick start

Requirements: Docker and Python 3.11+.

```bash
docker compose up -d --build          # PostgreSQL (wal_level=logical) plus the capture daemon
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

export ROWBACK_SOURCE_DSN="host=localhost port=55432 user=postgres password=postgres dbname=shop"
export ROWBACK_HISTORY_DSN="host=localhost port=55432 user=postgres password=postgres dbname=rowback"

python demo/demo.py                   # an agent deletes 40,000 orders, rowback undoes it
```

### Commands

| command | what it does |
|---|---|
| `rowback init` | create the replication slot, publication, and history schema |
| `rowback track <table>` / `untrack` / `tables` | choose which tables are recorded |
| `rowback capture [--retention 7d]` | run the capture daemon |
| `rowback log [--since 10m] [--table t] [--min-rows N]` | search the transaction timeline |
| `rowback show <id>` | inspect one transaction and its statements |
| `rowback undo <id> [--preview] [--dry-run] [--on-conflict ...]` | reverse one transaction |
| `rowback status [--json]` | replication lag and WAL retention health |
| `rowback prune --older-than 7d` | delete old history |
| `rowback guard run/pending/approve/reject/mcp` | policy gateway for agents |

Operational guidance, including how `max_slot_wal_keep_size` protects the primary's disk if the daemon stops, is in [`docs/operations.md`](docs/operations.md). The design is in [`docs/specs`](docs/specs).

## Benchmarks

All numbers come from scripts in `benchmarks/`, and the raw results are in `benchmarks/results/`. Everything ran on one 8 GB Apple M2 laptop: both PostgreSQL servers and pgbench in one 4 vCPU Docker VM, and the daemon on the host. They compete for the same CPU, so treat the overhead figures as an upper bound. Full details and caveats are in [`benchmarks/results/summary.md`](benchmarks/results/summary.md).

### Recovering from a bad DELETE

| table size | rows deleted | rowback undo | pg_restore | PITR | speedup vs PITR |
|---|---|---|---|---|---|
| 1M rows (127 MiB) | 10,000 | 0.57 s | 6.9 s | 14.8 s | 25.8x |
| 1M rows | 100,000 | 3.32 s | 6.9 s | 14.8 s | 4.5x |
| 5M rows (620 MiB) | 10,000 | 2.13 s | 24.7 s | 39.5 s | 18.5x |
| 5M rows | 100,000 | 6.80 s | 24.7 s | 39.5 s | 5.8x |

- **Correctness:** every undo restored the deleted rows exactly, checked with an md5 over the rows before and after.
- **Lost writes:** rowback undo kept every write made after the incident. pg_restore lost the 2,500 inserts made since the dump, and PITR lost the 500 made after the incident.
- **Baselines are lower bounds:** neither baseline includes the time to notice the problem, provision a server, or repoint applications.
- **Single runs:** each timing is one run, not a median.

### Cost of capture

pgbench on a 1M-row table, 8 clients, median of 3 runs of 60 s each:

| configuration | max throughput | p99 latency at a steady 5,000 TPS | history per 1M changes |
|---|---|---|---|
| no capture | 21,271 TPS | 4.0 ms | none |
| capture, REPLICA IDENTITY FULL | 15,122 TPS (-29%) | 69 ms | 521 MiB |
| capture, FULL plus context trigger | 11,911 TPS (-44%) | 173 ms | 640 MiB |

- **Steady load:** at a steady 5,000 TPS, every configuration kept up with the load.
- **No lost changes:** in every run, captured row changes equaled committed row changes.
- **Latency tail:** the p99 increase comes mostly from CPU contention on the shared machine.

The capture daemon, a single Python process, drains about 9,600 single-row transactions per second, and about 84,000 row changes per second for bulk deletes.

## Limitations

- **Schema changes (DDL)** are not recorded, and `TRUNCATE` is recorded but cannot be undone.
- **Primary key required:** tracked tables must have one. Tables without one are rejected with a clear message.
- **Memory:** a transaction is held in the daemon's memory until it commits, so very large transactions need memory proportional to their size.
- **Storage:** full row images are large, about 0.5 GB per million changes, so keep retention short and track only critical tables.

## Development

```bash
pip install -e ".[dev]"
pytest -q              # 114 tests: unit tests plus integration tests against real PostgreSQL in Docker
```

## License

MIT
