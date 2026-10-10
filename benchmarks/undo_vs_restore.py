"""Time to recover from a bad DELETE: rowback undo vs. restoring from backup.

For each dataset size N (rows in orders):
  1. seed N rows, track the table (REPLICA IDENTITY FULL + context), start capture
  2. take the "last good backups": pg_dump -Fc and pg_basebackup (WAL archiving on)
  3. ordinary writes happen after the backups (these are what a dump restore loses)
  4. for each K: an agent deletes K random rows; good writes continue; rowback finds and
     undoes the delete; we verify an md5 fingerprint of the victims and that every
     later good write survived
  5. baselines for the same dataset:
       a. pg_restore -j 4 of the dump into a fresh database
       b. point-in-time recovery: copy the base backup, replay archived WAL to just
          before the last bad DELETE, promote
     and count the good writes each baseline loses.
Usage: python benchmarks/undo_vs_restore.py [--sizes 1000000 5000000] [--deletes 10000 50000 100000]
"""
from __future__ import annotations

import argparse
import os
import random
import signal
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common import (HIST, HIST_PORT, IMAGE, SRC, SRC_PORT, BASE_ARGS, docker_run, dsn, environment,  # noqa: E402
                    execute, recreate_db, save, sh, start_pg, stop_pg, wait_ready)

import psycopg2  # noqa: E402

from rowback import admin  # noqa: E402
from rowback.store import HistoryStore  # noqa: E402
from rowback.undo import apply_undo, build_plan  # noqa: E402

DB, HIST_DB = "shop", "history"
ARCHIVE_VOL, BASE_VOL, PITR_VOL = "rowback-bench-archive", "rowback-bench-base", "rowback-bench-pitr"
PITR, PITR_PORT = "rowback-bench-pitr", 56434
ARCHIVE_ARGS = ["-c", "archive_mode=on", "-c",
                "archive_command=test ! -f /archive/%f && cp %p /archive/%f"]


def src() -> str:
    return dsn(SRC_PORT, DB)


def reset_volumes() -> None:
    for v in (ARCHIVE_VOL, BASE_VOL, PITR_VOL):
        sh("docker", "volume", "rm", "-f", v, check=False)
        sh("docker", "volume", "create", v)


def fresh_source() -> None:
    stop_pg(SRC)
    stop_pg(PITR)
    reset_volumes()
    start_pg(SRC, SRC_PORT, ARCHIVE_ARGS,
             volumes=[f"{ARCHIVE_VOL}:/archive", f"{BASE_VOL}:/base"])
    sh("docker", "exec", "-u", "root", SRC, "sh", "-c",
       "chown postgres /archive /base && mkdir -p /backup && chown postgres /backup")


def seed(n: int) -> None:
    recreate_db(SRC_PORT, DB)
    execute(src(), "CREATE TABLE orders (id bigserial PRIMARY KEY, customer_id int NOT NULL,"
                   " amount numeric(10,2) NOT NULL, status text NOT NULL, note text,"
                   " created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz)")
    execute(src(), "CREATE INDEX orders_customer_idx ON orders (customer_id)")
    execute(src(), f"INSERT INTO orders (customer_id, amount, status, note) SELECT g % 50000 + 1,"
                   f" round((g % 1000)::numeric / 3, 2), 'paid', 'checkout from web, order ' || g"
                   f" FROM generate_series(1, {n}) g")
    execute(src(), "VACUUM ANALYZE orders")


def start_daemon() -> subprocess.Popen:  # type: ignore[type-arg]
    env = {**os.environ, "ROWBACK_SOURCE_DSN": src(), "ROWBACK_HISTORY_DSN": dsn(HIST_PORT, HIST_DB)}
    proc = subprocess.Popen([sys.executable, "-m", "rowback", "capture"], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(150):
        if execute(src(), "SELECT active FROM pg_replication_slots WHERE slot_name = 'rowback'") \
                == [(True,)]:
            return proc
        time.sleep(0.2)
    raise RuntimeError("capture daemon did not attach")


def good_writes(n: int, exclude_table: str | None = None) -> tuple[list[int], list[int]]:
    """n inserts and n single-row updates (on rows not in exclude_table), each its own txn.

    The same row can be picked twice, so callers count distinct updated ids.
    """
    conn = psycopg2.connect(src(), application_name="checkout-service")
    conn.autocommit = True
    cur = conn.cursor()
    inserted, updated = [], []
    cur.execute("SELECT max(id) FROM orders")
    top = cur.fetchone()[0]
    for _ in range(n):
        cur.execute("INSERT INTO orders (customer_id, amount, status) VALUES (%s, 42.00, 'paid')"
                    " RETURNING id", (random.randint(1, 50000),))
        inserted.append(cur.fetchone()[0])
        target = random.randint(1, top)
        guard = f" AND id NOT IN (SELECT id FROM {exclude_table})" if exclude_table else ""
        cur.execute(f"UPDATE orders SET status = 'shipped', updated_at = now() WHERE id = %s{guard}"
                    " RETURNING id", (target,))
        row = cur.fetchone()
        if row:
            updated.append(row[0])
    conn.close()
    return inserted, updated


def fingerprint(conn_dsn: str, victims_sql: str) -> tuple[int, str]:
    return execute(conn_dsn, "SELECT count(*), md5(string_agg(o::text, ',' ORDER BY o.id)) FROM "
                   f"orders o WHERE o.id IN ({victims_sql})")[0]  # type: ignore[return-value]


def timed(fn, *args, **kwargs):  # type: ignore[no-untyped-def]
    t0 = time.perf_counter()
    out = fn(*args, **kwargs)
    return out, time.perf_counter() - t0


def incident(k: int, store: HistoryStore) -> dict[str, object]:
    execute(src(), "DROP TABLE IF EXISTS victims")
    execute(src(), f"CREATE TABLE victims AS SELECT id FROM orders ORDER BY random() LIMIT {k}")
    execute(src(), "CREATE UNIQUE INDEX ON victims (id)")
    victims_sql = "SELECT id FROM victims"
    before = fingerprint(src(), victims_sql)
    before_ts = execute(src(), "SELECT clock_timestamp()")[0][0]
    conn = psycopg2.connect(src(), application_name="ai-agent")
    conn.autocommit = True
    with conn.cursor() as cur:
        t_delete0 = time.perf_counter()
        cur.execute("DELETE FROM orders WHERE id IN (SELECT id FROM victims)")
        deleted = cur.rowcount
        t_committed = time.perf_counter()
    conn.close()
    # capture latency: commit -> visible in the history timeline
    while not store.search(op="D", min_rows=k, since=before_ts):
        time.sleep(0.05)
    capture_latency = time.perf_counter() - t_committed
    after_inserts, after_updates = good_writes(500, exclude_table="victims")

    found, t_find = timed(store.search, op="D", min_rows=k, since=before_ts)
    plan, t_plan = timed(build_plan, store, found[0].id)
    result, t_apply = timed(apply_undo, src(), store, plan)
    after = fingerprint(src(), victims_sql)
    survived_inserts = execute(src(), "SELECT count(*) FROM orders WHERE id = ANY(%s)",
                               (after_inserts,))[0][0]
    survived_updates = execute(src(), "SELECT count(*) FROM orders WHERE id = ANY(%s) AND "
                               "status = 'shipped'", (after_updates,))[0][0]
    return {
        "deleted_rows": deleted, "delete_statement_s": round(t_committed - t_delete0, 3),
        "capture_latency_s": round(capture_latency, 3),
        "undo_find_s": round(t_find, 4), "undo_plan_s": round(t_plan, 3),
        "undo_apply_s": round(t_apply, 3), "undo_total_s": round(t_find + t_plan + t_apply, 3),
        "restored_rows": result.applied, "conflicts": len(result.conflicts),
        "fingerprint_match": before == after,
        "good_writes_after_incident": len(after_inserts) + len(set(after_updates)),
        "good_writes_preserved": survived_inserts + survived_updates,
        "pitr_target": before_ts, "after_inserts": after_inserts, "after_updates": after_updates,
        "victims_fingerprint": before,
    }


def baseline_pg_restore(post_backup_ids: list[int], last: dict[str, object]) -> dict[str, object]:
    recreate_db(SRC_PORT, "shop_restore")
    t0 = time.perf_counter()
    sh("docker", "exec", SRC, "pg_restore", "-U", "postgres", "-j", "4", "-d", "shop_restore",
       "/backup/shop.dump")
    restore_s = time.perf_counter() - t0
    restored = dsn(SRC_PORT, "shop_restore")
    lost = len(post_backup_ids) - execute(restored, "SELECT count(*) FROM orders WHERE id = ANY(%s)",
                                         (post_backup_ids,))[0][0]
    after_ids = last["after_inserts"]  # type: ignore[assignment]
    lost_after = len(after_ids) - execute(restored, "SELECT count(*) FROM orders WHERE id = ANY(%s)",
                                         (after_ids,))[0][0]
    execute(dsn(SRC_PORT), "DROP DATABASE shop_restore WITH (FORCE)")
    return {"pg_restore_s": round(restore_s, 2), "inserts_since_backup_lost": lost,
            "inserts_after_incident_lost": lost_after}


def baseline_pitr(last: dict[str, object]) -> dict[str, object]:
    # make sure the WAL covering the incident is archived
    execute(src(), "SELECT pg_switch_wal()")
    wal = execute(src(), "SELECT pg_walfile_name(pg_current_wal_lsn())")[0][0]
    for _ in range(600):
        done = execute(src(), "SELECT last_archived_wal FROM pg_stat_archiver")[0][0]
        if done is not None and done >= wal[:24] or done is not None and done > wal:
            break
        time.sleep(0.2)
    execute(src(), "SELECT pg_switch_wal()")
    time.sleep(2)
    target = last["pitr_target"]
    stop_pg(PITR)
    sh("docker", "volume", "rm", "-f", PITR_VOL, check=False)
    sh("docker", "volume", "create", PITR_VOL)
    t0 = time.perf_counter()
    recovery_conf = (f"restore_command = 'cp /archive/%f %p'\n"
                     f"recovery_target_time = '{target}'\n"
                     "recovery_target_action = 'promote'\n")
    sh("docker", "run", "--rm", "-e", f"RCONF={recovery_conf}", "-v", f"{BASE_VOL}:/base:ro",
       "-v", f"{PITR_VOL}:/pgdata", IMAGE, "sh", "-c",
       "cp -a /base/data/. /pgdata/ && touch /pgdata/recovery.signal && "
       "printf '%s' \"$RCONF\" >> /pgdata/postgresql.auto.conf && chown -R postgres /pgdata")
    t_copy = time.perf_counter() - t0
    docker_run("--name", PITR, "-e", "POSTGRES_PASSWORD=postgres",
       "-e", "PGDATA=/pgdata", "-v", f"{PITR_VOL}:/pgdata", "-v", f"{ARCHIVE_VOL}:/archive:ro",
       "--shm-size=1g", "-p", f"127.0.0.1:{PITR_PORT}:5432", IMAGE, *BASE_ARGS)
    wait_ready(PITR_PORT, timeout=1800)
    while execute(dsn(PITR_PORT), "SELECT pg_is_in_recovery()")[0][0]:
        time.sleep(0.1)
    total = time.perf_counter() - t0
    pitr_dsn = dsn(PITR_PORT, DB)
    fp = execute(pitr_dsn, "SELECT count(*), md5(string_agg(o::text, ',' ORDER BY o.id)) FROM "
                 "orders o WHERE o.id IN (SELECT id FROM victims)")[0]
    after_ids = last["after_inserts"]  # type: ignore[assignment]
    lost_after = len(after_ids) - execute(pitr_dsn, "SELECT count(*) FROM orders WHERE id = ANY(%s)",
                                         (after_ids,))[0][0]
    stop_pg(PITR)
    return {"pitr_copy_base_s": round(t_copy, 2), "pitr_total_s": round(total, 2),
            "pitr_victims_restored_correctly": list(fp) == list(last["victims_fingerprint"]),
            "inserts_after_incident_lost": lost_after}


def run_dataset(n: int, deletes: list[int]) -> dict[str, object]:
    print(f"dataset {n} rows: seeding", flush=True)
    fresh_source()
    seed(n)
    recreate_db(HIST_PORT, HIST_DB)
    admin.init_source(src(), "rowback", "rowback")
    admin.track_table(src(), "orders", "rowback")
    store = HistoryStore(dsn(HIST_PORT, HIST_DB))
    store.ensure_schema()
    daemon = start_daemon()
    try:
        size = execute(src(), "SELECT pg_database_size('shop'), pg_total_relation_size('orders')")[0]
        print("  backups", flush=True)
        _, dump_s = timed(sh, "docker", "exec", SRC, "pg_dump", "-U", "postgres", "-Fc", "-f",
                          "/backup/shop.dump", DB)
        dump_bytes = int(sh("docker", "exec", SRC, "stat", "-c", "%s", "/backup/shop.dump"))
        _, base_s = timed(sh, "docker", "exec", SRC, "pg_basebackup", "-U", "postgres", "-D",
                          "/base/data", "-X", "stream", "-c", "fast")
        print("  writes after the backup", flush=True)
        post_backup_inserts, _ = good_writes(1000)
        incidents = []
        for k in deletes:
            print(f"  incident: delete {k} rows", flush=True)
            r = incident(k, store)
            print(f"    undo {r['undo_total_s']}s (find {r['undo_find_s']} plan {r['undo_plan_s']} "
                  f"apply {r['undo_apply_s']}) match={r['fingerprint_match']} "
                  f"preserved={r['good_writes_preserved']}/{r['good_writes_after_incident']}",
                  flush=True)
            post_backup_inserts += r["after_inserts"]  # type: ignore[operator]
            incidents.append(r)
        print("  baseline: pg_restore", flush=True)
        restore = baseline_pg_restore(post_backup_inserts, incidents[-1])
        print(f"    {restore}", flush=True)
        print("  baseline: PITR", flush=True)
        pitr = baseline_pitr(incidents[-1])
        print(f"    {pitr}", flush=True)
        for r in incidents:
            for key in ("after_inserts", "after_updates"):
                r.pop(key)
        return {"rows": n, "database_bytes": size[0], "orders_total_bytes": size[1],
                "pg_dump_s": round(dump_s, 2), "dump_bytes": dump_bytes,
                "pg_basebackup_s": round(base_s, 2), "incidents": incidents,
                "pg_restore": restore, "pitr": pitr}
    finally:
        daemon.send_signal(signal.SIGTERM)
        daemon.wait(timeout=30)
        store.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", type=int, nargs="+", default=[1_000_000, 5_000_000])
    ap.add_argument("--deletes", type=int, nargs="+", default=[10_000, 50_000, 100_000])
    ap.add_argument("--out", default="undo_vs_restore.json")
    args = ap.parse_args()
    random.seed(7)
    start_pg(HIST, HIST_PORT)
    datasets = []
    try:
        fresh_source()
        env = environment()
        for n in args.sizes:
            datasets.append(run_dataset(n, args.deletes))
            save(args.out, {"environment": env, "args": vars(args), "datasets": datasets})
    finally:
        stop_pg(SRC)
        stop_pg(HIST)
        stop_pg(PITR)
        for v in (ARCHIVE_VOL, BASE_VOL, PITR_VOL):
            sh("docker", "volume", "rm", "-f", v, check=False)
    print(f"saved {args.out}")


if __name__ == "__main__":
    main()
