"""Shared fixtures: one PostgreSQL 17 container per session, fresh databases per test."""
from __future__ import annotations

import os
import subprocess
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass

import psycopg2
import pytest

IMAGE = os.environ.get("ROWBACK_TEST_IMAGE", "postgres:17")
PG_ARGS = [
    "-c", "wal_level=logical",
    "-c", "max_replication_slots=50",
    "-c", "max_wal_senders=50",
    "-c", "max_slot_wal_keep_size=2GB",
    "-c", "fsync=off",
]


@dataclass(frozen=True)
class PgServer:
    host: str
    port: int

    def dsn(self, dbname: str = "postgres") -> str:
        return f"host={self.host} port={self.port} user=postgres password=postgres dbname={dbname}"


@dataclass(frozen=True)
class TestDbs:
    server: PgServer
    source_dsn: str
    history_dsn: str
    slot: str
    publication: str


def _wait_ready(server: PgServer, timeout: float = 60) -> None:
    deadline = time.time() + timeout
    while True:
        try:
            psycopg2.connect(server.dsn()).close()
            return
        except psycopg2.OperationalError:
            if time.time() > deadline:
                raise
            time.sleep(0.5)


@pytest.fixture(scope="session")
def pg() -> Iterator[PgServer]:
    if os.environ.get("ROWBACK_TEST_HOST"):
        yield PgServer(os.environ["ROWBACK_TEST_HOST"], int(os.environ.get("ROWBACK_TEST_PORT", "5432")))
        return
    name = f"rowback-test-{uuid.uuid4().hex[:8]}"
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--name", name, "-e", "POSTGRES_PASSWORD=postgres",
         "-p", "127.0.0.1::5432", IMAGE, *PG_ARGS],
        check=True, capture_output=True,
    )
    try:
        out = subprocess.run(["docker", "port", name, "5432/tcp"], check=True,
                             capture_output=True, text=True).stdout.strip().splitlines()[0]
        server = PgServer("127.0.0.1", int(out.rsplit(":", 1)[1]))
        # the entrypoint restarts the server once after initdb; wait for the final one
        time.sleep(1.0)
        _wait_ready(server)
        time.sleep(1.0)
        _wait_ready(server)
        yield server
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)


def _admin_exec(server: PgServer, sql: str) -> None:
    conn = psycopg2.connect(server.dsn())
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.close()


@pytest.fixture
def dbs(pg: PgServer) -> Iterator[TestDbs]:
    tag = uuid.uuid4().hex[:10]
    src, hist = f"src_{tag}", f"hist_{tag}"
    _admin_exec(pg, f"CREATE DATABASE {src}")
    _admin_exec(pg, f"CREATE DATABASE {hist}")
    yield TestDbs(pg, pg.dsn(src), pg.dsn(hist), f"slot_{tag}", "rowback")
    conn = psycopg2.connect(pg.dsn())
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
            "WHERE database = %s AND active_pid IS NOT NULL", (src,))
        time.sleep(0.2)
        cur.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                    "WHERE database = %s", (src,))
        cur.execute(f"DROP DATABASE IF EXISTS {src} WITH (FORCE)")
        cur.execute(f"DROP DATABASE IF EXISTS {hist} WITH (FORCE)")
    conn.close()


def sql(dsn: str, statement: str, params: tuple | None = None) -> list[tuple]:
    """Run one statement in autocommit mode and return rows (if any)."""
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(statement, params)
            return cur.fetchall() if cur.description else []
    finally:
        conn.close()
