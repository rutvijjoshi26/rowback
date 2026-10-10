"""End-to-end demo: an AI agent deletes 40,000 orders; rowback undoes it in seconds.

Prerequisite: `docker-compose up -d --build` (PostgreSQL plus the capture daemon).
Run: python demo/demo.py
"""
from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import threading
import time

import psycopg2

HOST = os.environ.get("DEMO_HOST", "localhost")
PORT = os.environ.get("DEMO_PORT", "55432")
SHOP = f"host={HOST} port={PORT} user=postgres password=postgres dbname=shop"
HISTORY = f"host={HOST} port={PORT} user=postgres password=postgres dbname=rowback"
N_ORDERS = 200_000
N_PENDING = 40_000
ENV = {**os.environ, "ROWBACK_SOURCE_DSN": SHOP, "ROWBACK_HISTORY_DSN": HISTORY}


def say(text: str) -> None:
    print(f"\n== {text}", flush=True)


def rowback(*args: str, show: bool = True) -> str:
    cmd = [sys.executable, "-m", "rowback", *args]
    if show:
        print("$ rowback " + " ".join(args), flush=True)
    out = subprocess.run(cmd, env=ENV, check=True, capture_output=True, text=True).stdout
    if show:
        print(out.rstrip(), flush=True)
    return out


def execute(sql: str, params: tuple | None = None, app: str = "demo") -> list[tuple]:
    conn = psycopg2.connect(SHOP, application_name=app)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() if cur.description else []
    conn.close()
    return rows


class GoodTraffic(threading.Thread):
    """Normal application writes that keep happening during and after the incident."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.stop = threading.Event()
        self.inserted: list[int] = []
        self.shipped: list[int] = []

    def run(self) -> None:
        conn = psycopg2.connect(SHOP, application_name="checkout-service")
        conn.autocommit = True
        cur = conn.cursor()
        while not self.stop.is_set():
            cur.execute("INSERT INTO orders (customer_id, amount, status) VALUES (%s, %s, 'paid') "
                        "RETURNING id", (random.randint(1, 5000), round(random.uniform(5, 500), 2)))
            self.inserted.append(cur.fetchone()[0])
            oid = random.randint(N_PENDING + 1, N_ORDERS)  # a paid order, never a pending one
            cur.execute("UPDATE orders SET status = 'shipped' WHERE id = %s AND status = 'paid'",
                        (oid,))
            if cur.rowcount:
                self.shipped.append(oid)
            time.sleep(0.01)
        conn.close()


def main() -> None:
    say("setup: an orders table with 200,000 rows, 40,000 of them pending")
    execute("DROP TABLE IF EXISTS orders")
    execute("CREATE TABLE orders (id bigserial PRIMARY KEY, customer_id int NOT NULL,"
            " amount numeric(10,2) NOT NULL, status text NOT NULL, note text,"
            " created_at timestamptz NOT NULL DEFAULT now())")
    execute(f"INSERT INTO orders (customer_id, amount, status, note) SELECT g % 5000 + 1,"
            f" round((random() * 500)::numeric, 2), CASE WHEN g <= {N_PENDING} THEN 'pending'"
            f" ELSE 'paid' END, CASE WHEN g % 7 = 0 THEN NULL ELSE 'gift wrap: ' || g END"
            f" FROM generate_series(1, {N_ORDERS}) g")
    rowback("init")
    rowback("track", "orders")
    fingerprint_sql = ("SELECT count(*), md5(string_agg(o::text, ',' ORDER BY id)) FROM orders o "
                       f"WHERE id <= {N_PENDING}")
    pending_before = execute(fingerprint_sql)[0]
    print(f"pending orders: {pending_before[0]}, fingerprint {pending_before[1]}")

    say("normal traffic starts (checkout-service inserts and ships orders)")
    traffic = GoodTraffic()
    traffic.start()
    time.sleep(2)

    say("an AI coding agent, asked to 'clean up stale test orders', runs this against prod")
    print("ai-agent> DELETE FROM orders WHERE status = 'pending';")
    deleted = execute("DELETE FROM orders WHERE status = 'pending' RETURNING 1", app="ai-agent")
    incident_at = time.time()
    print(f"DELETE {len(deleted)}")
    inserted_before_incident = len(traffic.inserted)

    say("traffic keeps flowing for 5 more seconds after the incident")
    time.sleep(5)

    say("find the bad transaction in the timeline")
    deadline = time.time() + 60
    found: list[dict] = []
    while time.time() < deadline and not found:
        found = json.loads(rowback("log", "--op", "delete", "--min-rows", "1000", "--json",
                                   show=False))
        if not found:
            time.sleep(0.5)
    if not found:
        raise SystemExit("the capture daemon did not record the delete; is docker-compose up?")
    rowback("log", "--since", "10m", "--min-rows", "1000")
    txn_id = str(found[0]["id"])
    rowback("show", txn_id, "--rows", "2")

    say("preview the inverse SQL")
    rowback("undo", txn_id, "--preview", "--preview-rows", "3")

    say("undo it (one transaction, later writes untouched)")
    traffic_after_incident = len(traffic.inserted) - inserted_before_incident
    started = time.perf_counter()
    rowback("undo", txn_id, "--yes")
    undo_seconds = time.perf_counter() - started
    recovered_at = time.time()
    traffic.stop.set()
    traffic.join()

    say("verify")
    pending_after = execute(fingerprint_sql)[0]
    good_ids = traffic.inserted
    surviving = execute("SELECT count(*) FROM orders WHERE id = ANY(%s)", (good_ids,))[0][0]
    shipped_ok = execute("SELECT count(*) FROM orders WHERE id = ANY(%s) AND status = 'shipped'",
                         (traffic.shipped,))[0][0]
    print(f"pending orders restored: {pending_after[0]} (fingerprint {pending_after[1]})")
    print(f"restored rows identical to the originals: {pending_after == pending_before}")
    print(f"orders written by checkout-service during the demo: {len(good_ids)} "
          f"({traffic_after_incident} after the incident); still present: {surviving}")
    print(f"orders shipped during the demo: {len(traffic.shipped)}; still shipped: {shipped_ok}")
    print(f"undo command wall time: {undo_seconds:.2f}s; incident to recovery: "
          f"{recovered_at - incident_at:.1f}s (includes 5s of deliberate waiting)")
    ok = (pending_after == pending_before and surviving == len(good_ids)
          and shipped_ok == len(traffic.shipped))
    print("RESULT:", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
