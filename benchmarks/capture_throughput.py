"""Capture daemon throughput, measured by draining a pre-built backlog.

With the daemon stopped, a backlog is written to the slot; then a fresh daemon
consumes it (run_until_caught_up) and we time it. Two shapes:
  oltp: many single-row transactions (pgbench: 60% update, 30% insert, 10% delete)
  bulk: one transaction deleting many rows
Usage: python benchmarks/capture_throughput.py [--txns 300000] [--bulk-rows 500000]
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common import (HIST, HIST_PORT, ROOT, SRC, SRC_PORT, dsn, environment, execute,  # noqa: E402
                    recreate_db, save, sh, start_pg, stop_pg)

from rowback import admin  # noqa: E402
from rowback.capture import CaptureDaemon  # noqa: E402
from rowback.config import Settings  # noqa: E402
from rowback.store import HistoryStore  # noqa: E402

DB, HIST_DB = "bench", "history"


def setup(rows: int) -> Settings:
    recreate_db(SRC_PORT, DB)
    recreate_db(HIST_PORT, HIST_DB)
    d = dsn(SRC_PORT, DB)
    execute(d, "CREATE TABLE orders (id bigserial PRIMARY KEY, customer_id int NOT NULL,"
               " amount numeric(10,2) NOT NULL, status text NOT NULL, note text,"
               " created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz)")
    execute(d, f"INSERT INTO orders (customer_id, amount, status, note) SELECT g % 50000 + 1,"
               f" round((g % 1000)::numeric / 3, 2), 'paid', 'checkout from web, order ' || g"
               f" FROM generate_series(1, {rows}) g")
    admin.init_source(d, "rowback", "rowback")
    admin.track_table(d, "orders", "rowback")
    HistoryStore(dsn(HIST_PORT, HIST_DB)).ensure_schema()
    return Settings(source_dsn=d, history_dsn=dsn(HIST_PORT, HIST_DB))


def drain(settings: Settings) -> dict[str, float]:
    lag = int(execute(settings.source_dsn, "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), "
                      "confirmed_flush_lsn) FROM pg_replication_slots")[0][0])
    daemon = CaptureDaemon(settings)
    t0 = time.perf_counter()
    daemon.run_until_caught_up(timeout=3600)
    elapsed = time.perf_counter() - t0
    daemon.store.close()
    return {"backlog_wal_bytes": lag, "drain_s": round(elapsed, 2),
            "txns": daemon.saved_txns, "changes": daemon.saved_changes,
            "txns_per_s": round(daemon.saved_txns / elapsed),
            "changes_per_s": round(daemon.saved_changes / elapsed),
            "wal_mib_per_s": round(lag / elapsed / 2**20, 2)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--txns", type=int, default=300_000)
    ap.add_argument("--bulk-rows", type=int, default=500_000)
    ap.add_argument("--out", default="capture_throughput.json")
    args = ap.parse_args()
    start_pg(SRC, SRC_PORT)
    start_pg(HIST, HIST_PORT)
    try:
        results = {}
        s = setup(1_000_000)
        sh("docker", "exec", SRC, "mkdir", "-p", "/tmp/w")
        for f in ("update.sql", "insert.sql", "delete.sql"):
            sh("docker", "cp", str(ROOT / "workload" / f), f"{SRC}:/tmp/w/{f}")
        per_client = args.txns // 8
        sh("docker", "exec", SRC, "pgbench", "-n", "-h", "localhost", "-U", "postgres", "-c", "8",
           "-j", "4", "-t", str(per_client), "-D", "rows=1000000", "-f", "/tmp/w/update.sql@6",
           "-f", "/tmp/w/insert.sql@3", "-f", "/tmp/w/delete.sql@1", DB)
        results["oltp"] = drain(s)
        print("oltp", results["oltp"], flush=True)
        s = setup(args.bulk_rows)
        execute(s.source_dsn, "DELETE FROM orders")
        results["bulk_delete"] = drain(s)
        print("bulk", results["bulk_delete"], flush=True)
        save(args.out, {"environment": environment(), "args": vars(args), "results": results})
    finally:
        stop_pg(SRC)
        stop_pg(HIST)


if __name__ == "__main__":
    main()
