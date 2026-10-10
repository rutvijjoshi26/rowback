"""Write overhead of change capture under a write-heavy pgbench workload.

Configurations (each repeated, in rotating order):
  baseline              no slot, no publication, REPLICA IDENTITY DEFAULT
  capture_default       rowback capture, REPLICA IDENTITY DEFAULT, no context trigger
  capture_full          rowback capture, REPLICA IDENTITY FULL, no context trigger
  capture_full_context  rowback capture, REPLICA IDENTITY FULL, context trigger (default setup)

Workload: single-statement transactions on a pre-seeded orders table,
60% UPDATE by random id, 30% INSERT, 10% DELETE by random id.
Usage: python benchmarks/overhead.py [--duration 60] [--rows 1000000] [--reps 3] [--rate TPS]
With --rate the arrival rate is fixed (pgbench -R) and latency includes schedule lag.
"""
from __future__ import annotations

import argparse
import os
import re
import signal
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common import (HIST, HIST_PORT, ROOT, SRC, SRC_PORT, dsn, environment, execute,  # noqa: E402
                    recreate_db, save, sh, start_pg, stop_pg)

from rowback import admin  # noqa: E402
from rowback.store import HistoryStore  # noqa: E402

CONFIGS = ["baseline", "capture_default", "capture_full", "capture_full_context"]
SRC_DB, HIST_DB = "bench", "history"
WORK = ROOT / "work"


def seed(rows: int) -> None:
    recreate_db(SRC_PORT, SRC_DB)
    d = dsn(SRC_PORT, SRC_DB)
    execute(d, "CREATE TABLE orders (id bigserial PRIMARY KEY, customer_id int NOT NULL,"
               " amount numeric(10,2) NOT NULL, status text NOT NULL, note text,"
               " created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz)")
    execute(d, f"INSERT INTO orders (customer_id, amount, status, note) SELECT g % 50000 + 1,"
               f" round((g % 1000)::numeric / 3, 2), 'paid', 'checkout from web, order ' || g"
               f" FROM generate_series(1, {rows}) g")
    execute(d, "VACUUM ANALYZE orders")
    execute(d, "CHECKPOINT")


def configure(config: str) -> None:
    d = dsn(SRC_PORT, SRC_DB)
    if config == "baseline":
        return
    admin.init_source(d, "rowback", "rowback")
    admin.track_table(d, "orders", "rowback",
                      identity="default" if config == "capture_default" else "full",
                      context=config == "capture_full_context")
    recreate_db(HIST_PORT, HIST_DB)
    store = HistoryStore(dsn(HIST_PORT, HIST_DB))
    store.ensure_schema()
    store.close()


def start_daemon(log_name: str) -> subprocess.Popen:  # type: ignore[type-arg]
    env = {**os.environ, "ROWBACK_SOURCE_DSN": dsn(SRC_PORT, SRC_DB),
           "ROWBACK_HISTORY_DSN": dsn(HIST_PORT, HIST_DB)}
    WORK.mkdir(exist_ok=True)
    log = open(WORK / log_name, "w")
    proc = subprocess.Popen([sys.executable, "-m", "rowback", "-v", "capture"], env=env,
                            stdout=log, stderr=subprocess.STDOUT)
    deadline = time.time() + 30
    while time.time() < deadline:
        if execute(dsn(SRC_PORT, SRC_DB), "SELECT active FROM pg_replication_slots "
                                          "WHERE slot_name = 'rowback'") == [(True,)]:
            return proc
        time.sleep(0.2)
    proc.kill()
    raise RuntimeError("capture daemon did not attach to the slot")


class LagSampler(threading.Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.stop = threading.Event()
        self.samples: list[int] = []

    def run(self) -> None:
        while not self.stop.wait(1.0):
            rows = execute(dsn(SRC_PORT, SRC_DB), "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),"
                           " confirmed_flush_lsn) FROM pg_replication_slots WHERE slot_name = 'rowback'")
            if rows:
                self.samples.append(int(rows[0][0]))


def table_stats() -> tuple[int, int, int]:
    return execute(dsn(SRC_PORT, SRC_DB), "SELECT n_tup_ins, n_tup_upd, n_tup_del FROM "
                   "pg_stat_user_tables WHERE relname = 'orders'")[0]  # type: ignore[return-value]


def run_pgbench(duration: int, rows: int, clients: int,
                rate: int | None) -> tuple[dict[str, float], list[int]]:
    sh("docker", "exec", SRC, "sh", "-c", "rm -rf /tmp/pgb* && mkdir -p /tmp/w")
    for f in ("update.sql", "insert.sql", "delete.sql"):
        sh("docker", "cp", str(ROOT / "workload" / f), f"{SRC}:/tmp/w/{f}")
    out = sh("docker", "exec", SRC, "pgbench", "-n", "-h", "localhost", "-U", "postgres",
             "-c", str(clients), "-j", str(min(clients, 4)), "-T", str(duration), "-D", f"rows={rows}",
             "-f", "/tmp/w/update.sql@6", "-f", "/tmp/w/insert.sql@3", "-f", "/tmp/w/delete.sql@1",
             "-l", "--log-prefix=/tmp/pgb", *(["-R", str(rate)] if rate else []), SRC_DB)
    tps = float(re.search(r"tps = ([\d.]+) \(without initial connection time\)", out).group(1))
    processed = int(re.search(r"number of transactions actually processed: (\d+)", out).group(1))
    failed = int(m.group(1)) if (m := re.search(r"number of failed transactions: (\d+)", out)) else 0
    lat_avg = float(re.search(r"latency average = ([\d.]+) ms", out).group(1))
    WORK.mkdir(exist_ok=True)
    log_text = sh("docker", "exec", SRC, "sh", "-c", "cat /tmp/pgb.*")
    latencies = sorted(int(line.split()[2]) for line in log_text.splitlines() if line.strip())
    return {"tps": tps, "transactions": processed, "failed": failed, "latency_avg_ms": lat_avg,
            "pgbench_output": out}, latencies


def pct(sorted_values: list[int], p: float) -> float:
    if not sorted_values:
        return float("nan")
    k = min(len(sorted_values) - 1, int(round(p / 100 * (len(sorted_values) - 1))))
    return sorted_values[k] / 1000.0


def one_run(config: str, duration: int, rows: int, clients: int,
            rate: int | None = None) -> dict[str, object]:
    seed(rows)
    configure(config)
    log_name = f"daemon_{config}_{int(time.time())}.log"
    daemon = start_daemon(log_name) if config != "baseline" else None
    sampler = LagSampler() if daemon else None
    try:
        if sampler:
            sampler.start()
        execute(dsn(SRC_PORT, SRC_DB), "CHECKPOINT")
        stats0 = table_stats()
        lsn0 = execute(dsn(SRC_PORT, SRC_DB), "SELECT pg_current_wal_lsn()::text")[0][0]
        pg, latencies = run_pgbench(duration, rows, clients, rate)
        lsn1 = execute(dsn(SRC_PORT, SRC_DB), "SELECT pg_current_wal_lsn()::text")[0][0]
        wal_bytes = int(execute(dsn(SRC_PORT, SRC_DB), "SELECT pg_wal_lsn_diff(%s, %s)",
                                (lsn1, lsn0))[0][0])
        result: dict[str, object] = {
            "config": config, "duration_s": duration, "rows": rows, "clients": clients,
            "target_rate_tps": rate,
            "tps": pg["tps"], "transactions": pg["transactions"], "failed": pg["failed"],
            "latency_avg_ms": pg["latency_avg_ms"], "latency_p50_ms": pct(latencies, 50),
            "latency_p95_ms": pct(latencies, 95), "latency_p99_ms": pct(latencies, 99),
            "wal_bytes": wal_bytes, "wal_bytes_per_txn": wal_bytes / max(1, pg["transactions"]),
        }
        if daemon:
            t0 = time.time()
            while True:
                caught = execute(dsn(SRC_PORT, SRC_DB), "SELECT confirmed_flush_lsn >= %s::pg_lsn "
                                 "FROM pg_replication_slots WHERE slot_name = 'rowback'", (lsn1,))
                if caught == [(True,)]:
                    break
                if daemon.poll() is not None:
                    raise RuntimeError(f"capture daemon exited with {daemon.returncode}; see "
                                       f"benchmarks/work/{log_name}")
                if time.time() - t0 > 600:
                    raise RuntimeError("capture did not catch up within 10 minutes; see "
                                       f"benchmarks/work/{log_name}")
                time.sleep(0.2)
            result["catch_up_after_load_s"] = round(time.time() - t0, 2)
            assert sampler is not None
            sampler.stop.set()
            result["max_slot_lag_bytes"] = max(sampler.samples) if sampler.samples else 0
            result["median_slot_lag_bytes"] = (statistics.median(sampler.samples)
                                               if sampler.samples else 0)
            time.sleep(1.5)  # let backends flush table statistics
            stats1 = table_stats()
            committed_row_changes = sum(b - a for a, b in zip(stats0, stats1))
            store = HistoryStore(dsn(HIST_PORT, HIST_DB))
            hs = store.stats()
            store.close()
            result.update({
                "committed_row_changes": committed_row_changes,
                "captured_changes": hs["changes"], "captured_txns": hs["transactions"],
                "history_total_bytes": hs["total_bytes"],
                "history_payload_bytes": hs["payload_bytes"],
                "history_bytes_per_million_changes": hs["total_bytes"] / max(1, hs["changes"]) * 1e6,
                "payload_bytes_per_change": hs["payload_bytes"] / max(1, hs["changes"]),
            })
        return result
    finally:
        if sampler:
            sampler.stop.set()
        if daemon:
            daemon.send_signal(signal.SIGTERM)
            try:
                daemon.wait(timeout=30)
            except subprocess.TimeoutExpired:
                daemon.kill()
        if config != "baseline":
            admin.drop_source(dsn(SRC_PORT, SRC_DB), "rowback", "rowback")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--clients", type=int, default=8)
    ap.add_argument("--rate", type=int, default=None,
                    help="Fixed arrival rate in TPS (pgbench -R); default is unthrottled.")
    ap.add_argument("--out", default="overhead.json")
    ap.add_argument("--keep-containers", action="store_true")
    ap.add_argument("--configs", nargs="+", default=CONFIGS, choices=CONFIGS)
    args = ap.parse_args()
    start_pg(SRC, SRC_PORT)
    start_pg(HIST, HIST_PORT)
    runs = []
    try:
        for rep in range(args.reps):
            configs = args.configs
            order = configs[rep % len(configs):] + configs[:rep % len(configs)]
            for config in order:
                print(f"rep {rep + 1}/{args.reps} {config} ...", flush=True)
                r = one_run(config, args.duration, args.rows, args.clients, args.rate)
                r["rep"] = rep + 1
                runs.append(r)
                print(f"  tps={r['tps']:.0f} p99={r['latency_p99_ms']:.2f}ms "
                      f"wal/txn={r['wal_bytes_per_txn']:.0f}B "
                      f"captured={r.get('captured_changes', '-')}/"
                      f"{r.get('committed_row_changes', '-')}", flush=True)
                save(args.out, {"environment": environment(), "args": vars(args), "runs": runs})
    finally:
        if not args.keep_containers:
            stop_pg(SRC)
            stop_pg(HIST)
    print(f"saved {args.out}")


if __name__ == "__main__":
    main()
